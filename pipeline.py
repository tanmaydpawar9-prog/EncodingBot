import os
import sys
import time
import re
import subprocess
import logging
import asyncio
import requests
import http.server
import socketserver
import threading
import shutil
import concurrent.futures
import uuid
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardRemove
from pyrogram.errors import FloodWait

# --- Lightning AI GPU Fix ---
# Ensures FFmpeg can find libcuda.so.1 for the RTX 6000 NVENC encoder
cuda_paths = "/usr/local/nvidia/lib64:/usr/local/nvidia/lib:/usr/local/cuda/lib64:/usr/local/cuda/targets/x86_64-linux/lib:/usr/lib/x86_64-linux-gnu:/lib/x86_64-linux-gnu:/usr/lib64:/lib64"
os.environ['LD_LIBRARY_PATH'] = f"{cuda_paths}:{os.environ.get('LD_LIBRARY_PATH', '')}"

# --- Basic Bot Logging ---
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# ==========================================
# 0. HTTP Server for Large File Downloads
# ==========================================
HTTP_PORT = 8000
http_server_thread = None
server_lock = threading.Lock()
file_id_map = {} # Maps a unique ID to a file path

class CustomHTTPRequestHandler(http.server.BaseHTTPRequestHandler):
    """A custom handler that serves files based on a UUID map."""
    def do_GET(self):
        if self.path.startswith('/download/'):
            file_id = self.path.split('/')[-1]
            file_path = file_id_map.get(file_id)
            
            if file_path and os.path.exists(file_path):
                try:
                    with open(file_path, 'rb') as f:
                        self.send_response(200)
                        self.send_header('Content-type', 'application/octet-stream')
                        self.send_header('Content-Disposition', f'attachment; filename="{os.path.basename(file_path)}"')
                        fs = os.fstat(f.fileno())
                        self.send_header("Content-Length", str(fs.st_size))
                        self.end_headers()
                        shutil.copyfileobj(f, self.wfile)
                except Exception as e:
                    logger.error(f"HTTP server error serving file: {e}")
                    self.send_error(500, "Server error while serving file")
            else:
                self.send_error(404, "File not found or link expired")
        else:
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            self.wfile.write(b"Encoding Bot File Server is running.")

def start_http_server():
    """Starts a simple HTTP server in a background thread if not already running."""
    global http_server_thread
    with server_lock:
        if http_server_thread is None or not http_server_thread.is_alive():
            httpd = socketserver.TCPServer(("", HTTP_PORT), CustomHTTPRequestHandler)
            
            def serve():
                logger.info(f"Starting HTTP server on port {HTTP_PORT} to serve files.")
                httpd.serve_forever()
                
            http_server_thread = threading.Thread(target=serve, daemon=True)
            http_server_thread.start()
            logger.info("HTTP server thread started.")

# ==========================================
# 1. Custom Progress Bar Formatting
# ==========================================
class PyrogramProgressViewer:
    def __init__(self, message, action="Downloading", chat_id=None):
        self.message = message
        self.action = action
        self.chat_id = chat_id
        self.start_time = time.time()
        self.last_update = 0
        self.last_text = ""

    async def update(self, current, total):
        # Abort if user triggered the cancel button
        if self.chat_id and active_jobs.get(self.chat_id, {}).get('cancel'):
            raise Exception("User Cancelled")
            
        now = time.time()
        # Throttled to 5s for faster updates (FloodWait handler protects against crashes)
        if now - self.last_update < 5 and current < total:
            return
        self.last_update = now
        
        elapsed = now - self.start_time
        if elapsed < 0.1: elapsed = 0.1
        
        speed = current / elapsed
        
        if total and total > 0:
            progress_pct = (current / total) * 100
            eta_sec = (total - current) / speed if speed > 0 else 0
            tot_mb_str = f"{total / 1048576:.1f}MB"
        else:
            progress_pct = 0
            eta_sec = 0
            tot_mb_str = "Unknown Size"
        
        # [■■■■■■□□□□] exact design match
        filled = int(progress_pct / 10)
        bar = '■' * filled + '□' * (10 - filled)
        
        text = (
            f"Progress: [{bar}] {progress_pct:.1f}%\n"
            f"⚙️ {self.action}: {current / 1048576:.1f}MB of {tot_mb_str}\n"
            f"⚡ Speed: {speed / 1048576:.1f}MB/s\n"
            f"⌛ ETA: {time.strftime('%Hh %Mm %Ss', time.gmtime(eta_sec))}\n"
            f"⏱️ Time elapsed: {time.strftime('%Mm %Ss', time.gmtime(elapsed))}."
        )
        
        if text != self.last_text:
            try:
                keyboard = InlineKeyboardMarkup([
                    [InlineKeyboardButton("❌ Cancel Job", callback_data="cancel_job")]
                ])
                await self.message.edit_text(text, reply_markup=keyboard)
                self.last_text = text
            except FloodWait as e:
                # If Telegram rate-limits us, automatically pause to respect the limit
                await asyncio.sleep(e.value)
            except Exception:
                pass

class EncodingProgressViewer:
    def __init__(self, message, chat_id=None):
        self.message = message
        self.chat_id = chat_id
        self.start_time = time.time()
        self.last_update = 0
        self.last_text = ""

    async def update(self, current_sec, total_sec, fps, speed):
        if self.chat_id and active_jobs.get(self.chat_id, {}).get('cancel'):
            raise Exception("User Cancelled")
            
        now = time.time()
        if now - self.last_update < 5 and current_sec < total_sec:
            return
        self.last_update = now
        
        elapsed = now - self.start_time
        if elapsed < 0.1: elapsed = 0.1
        
        if total_sec > 0:
            progress_pct = min((current_sec / total_sec) * 100, 100.0)
            eta_sec = (total_sec - current_sec) / (current_sec / elapsed) if current_sec > 0 else 0
        else:
            progress_pct = 0
            eta_sec = 0
            
        filled = int(progress_pct / 10)
        bar = '■' * filled + '□' * (10 - filled)
        
        text = (
            f"Progress: [{bar}] {progress_pct:.1f}%\n"
            f"🎬 Encoding: {time.strftime('%H:%M:%S', time.gmtime(current_sec))} / {time.strftime('%H:%M:%S', time.gmtime(total_sec))}\n"
            f"⚡ Speed: {fps} fps ({speed})\n"
            f"⌛ ETA: {time.strftime('%Hh %Mm %Ss', time.gmtime(eta_sec))}\n"
            f"⏱️ Time elapsed: {time.strftime('%Mm %Ss', time.gmtime(elapsed))}."
        )
        
        if text != self.last_text:
            try:
                keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel Job", callback_data="cancel_job")]])
                await self.message.edit_text(text, reply_markup=keyboard)
                self.last_text = text
            except FloodWait as e:
                await asyncio.sleep(e.value)
            except Exception:
                pass

# ==========================================
# 2 & 3. Naming and Size/Bitrate Logic
# ==========================================
def process_metadata(original_name, quality_choice):
    """Replaces [4K], [1080P], etc., with the user's chosen output quality."""
    pattern = r'(?i)\[(4k|2k|1080p|720p|360p)\]'
    new_name = re.sub(pattern, f'[{quality_choice.upper()}]', original_name)
    
    if new_name == original_name:
        base, ext = os.path.splitext(original_name)
        new_name = f"{base} [{quality_choice.upper()}]{ext}"
        
    # Force .mkv extension to support all streams and avoid weird MP4 containers
    base, _ = os.path.splitext(new_name)
    new_name = f"{base}.mkv"
        
    # Ensure the output is in the downloads directory for serving
    return os.path.join("downloads", os.path.basename(new_name))

def get_target_bitrate(input_file, quality_choice):
    """Dynamically calculates bitrate scaling."""
    cmd = [
        'ffprobe', '-v', 'error', '-show_entries', 'format=bit_rate', 
        '-of', 'default=noprint_wrappers=1:nokey=1', input_file
    ]
    original_bitrate = 16_000_000
    try:
        res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
        if res.stdout:
            out = res.stdout.strip()
            if out and out.lower() != 'n/a':
                original_bitrate = int(out)
    except Exception as e:
        logger.warning(f"Could not get original bitrate via ffprobe: {e}. Falling back to default.")

    qual = quality_choice.upper()
    
    if '2K' in qual:
        br = int(original_bitrate * 0.75)
    elif '1080' in qual:
        br = int(original_bitrate * 0.5)
    elif '720' in qual:
        br = int(original_bitrate * 0.25)
    else:
        br = original_bitrate
    
    return max(br, 500_000) # Prevents NVENC from crashing with a 0 or extremely low bitrate

def get_video_duration(input_file):
    """Gets the total duration of the video in seconds."""
    cmd = [
        'ffprobe', '-v', 'error', '-show_entries', 'format=duration', 
        '-of', 'default=noprint_wrappers=1:nokey=1', input_file
    ]
    try:
        res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
        if res.stdout:
            out = res.stdout.strip()
            if out and out.lower() != 'n/a':
                return float(out)
    except Exception as e:
        logger.warning(f"Could not get video duration from format: {e}. Trying stream duration.")
        
    # Fallback to video stream duration (specifically for MKV files)
    cmd_stream = ['ffprobe', '-v', 'error', '-select_streams', 'v:0', '-show_entries', 'stream=duration', '-of', 'default=noprint_wrappers=1:nokey=1', input_file]
    try:
        res = subprocess.run(cmd_stream, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
        if res.stdout:
            out = res.stdout.strip()
            if out and out.lower() != 'n/a':
                return float(out.split('\n')[0])
    except Exception as e:
        logger.warning(f"Could not get video duration from stream: {e}. Fallback to 0.")
    return 0.0

# ==========================================
# Fast Download, Encode, and Upload 
# ==========================================

def _download_range(url, start, end, output_path, progress_list, idx, chat_id):
    retries = 15

    for attempt in range(retries):
        current_start = start + progress_list[idx]
        if current_start > end:
            return
        headers = {
            "Range": f"bytes={current_start}-{end}", 
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        try:
            # Shorter timeout for chunk downloads to fail faster on stalls
            res = requests.get(url, headers=headers, stream=True, allow_redirects=True, timeout=(10, 30))
            res.raise_for_status()
            
            if res.status_code == 200:
                raise Exception("Server ignored Range header, parallel download not supported.")
                
            with open(output_path, "rb+") as f:
                f.seek(current_start)
                buffer = bytearray()
                try:
                    # 128KB chunks for better throughput, buffered to 1MB before disk write
                    for chunk in res.iter_content(chunk_size=128 * 1024):
                        if chat_id and active_jobs.get(chat_id, {}).get('cancel'):
                            return
                        if chunk:
                            buffer.extend(chunk)
                            # Buffer writes to 1MB to prevent disk stuttering
                            if len(buffer) >= 1024 * 1024:
                                f.write(buffer)
                                progress_list[idx] += len(buffer)
                                buffer.clear()
                            if start + progress_list[idx] + len(buffer) > end:
                                break
                finally:
                    if buffer:
                        f.write(buffer)
                        progress_list[idx] += len(buffer)
                        buffer.clear()
                        
            if start + progress_list[idx] > end:
                return # Success
        except Exception as e:
            logger.warning(f"Thread {idx} dropped connection (attempt {attempt+1}/{retries}): {e}")
            time.sleep(2)
            
    raise Exception(f"Download thread {idx} permanently failed after {retries} retries.")
    
def extract_thumbnail(video_path, thumb_path):
    """Extracts a frame at the 1-second mark to use as the Telegram video thumbnail."""
    cmd = [
        'ffmpeg', '-y', '-i', video_path,
        '-ss', '00:00:01',
        '-vframes', '1',
        '-vf', 'scale=320:-1',
        '-q:v', '2',
        thumb_path
    ]
    subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

async def delayed_delete(filepath, delay=7200):
    """Deletes a file after a specific delay (default 2 hours)."""
    await asyncio.sleep(delay)
    if os.path.exists(filepath):
        try: os.remove(filepath)
        except Exception: pass

async def download_video_from_url(url, output_path, progress_viewer):
    loop = asyncio.get_running_loop()
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    
    for attempt in range(3):
        try:
            # Reduced timeout to fail faster on unresponsive servers
            res = await asyncio.to_thread(requests.get, url, headers=headers, stream=True, allow_redirects=True, timeout=(10, 20))
            res.raise_for_status()
            break
        except requests.exceptions.RequestException as e:
            if attempt == 2:
                raise ValueError(f"Download connection failed after 3 attempts: {e}")
            logger.warning(f"Initial connection dropped (attempt {attempt+1}/3): {e}")
            await asyncio.sleep(3)

    content_type = res.headers.get('Content-Type', '')
    if 'text/' in content_type.lower():
        raise ValueError(f"URL returned text/webpage ({content_type}), not a direct video file.")
        
    total_size = int(res.headers.get('content-length', 0))
    accept_ranges = res.headers.get('accept-ranges', '').lower() == 'bytes'
    chat_id = progress_viewer.chat_id
    
    # If the server doesn't support chunked ranges, fallback to standard single-thread
    if total_size == 0 or not accept_ranges:
        def _single_download():
            downloaded = 0
            with open(output_path, 'wb') as f:
                for chunk in res.iter_content(chunk_size=2*1024*1024):
                    if chat_id and active_jobs.get(chat_id, {}).get('cancel'):
                        raise Exception("User Cancelled")
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        asyncio.run_coroutine_threadsafe(progress_viewer.update(downloaded, total_size), loop)
            if os.path.getsize(output_path) < 100 * 1024:
                raise ValueError("Downloaded file is invalid or too small.")
        await asyncio.to_thread(_single_download)
        return

    res.close()
    
    # 8-Part Parallel Download Implementation
    with open(output_path, "wb") as f:
        f.truncate(total_size) # Pre-allocate file on disk
        
    num_threads = 8
    chunk_size = total_size // num_threads
    progress_list = [0] * num_threads
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        for i in range(num_threads):
            start = i * chunk_size
            end = total_size - 1 if i == num_threads - 1 else (i + 1) * chunk_size - 1
            futures.append(loop.run_in_executor(
                executor, _download_range, url, start, end, output_path, progress_list, i, chat_id
            ))
        
        while True:
            downloaded = sum(progress_list)
            if chat_id and active_jobs.get(chat_id, {}).get('cancel'):
                raise Exception("User Cancelled")
                
            await progress_viewer.update(downloaded, total_size)
            
            if downloaded >= total_size or all(f.done() for f in futures):
                break
            await asyncio.sleep(1)
            
        await asyncio.gather(*futures)
        
    if os.path.getsize(output_path) < 100 * 1024:
        raise ValueError("Downloaded file is invalid or too small.")

async def encode_video(input_file, output_file, quality_choice, chat_id=None, status_message=None):
    """Runs FFmpeg in a separate thread to avoid blocking the bot."""
    target_bitrate = get_target_bitrate(input_file, quality_choice)
    total_duration = get_video_duration(input_file)
        
    # Map quality to fixed width, letting height auto-scale (-2)
    qual = quality_choice.upper()
    if '2K' in qual:
        scale = '2560:-2'
    elif '1080' in qual:
        scale = '1920:-2'
    elif '720' in qual:
        scale = '1280:-2'
    else:
        scale = '1920:-2'

    print(f"\n[INFO] 🎬 Encoding to {quality_choice} (Target Bitrate: {target_bitrate//1000} kbps)...")
    
    # RTX 6000 hardware-accelerated NVENC settings for speed and quality
    cmd_nvenc = [
        'ffmpeg', '-y', '-loglevel', 'error', '-stats', '-i', input_file,
        '-map', '0:v:0',           # Maps ONLY the main video stream (avoids broken thumbnails)
        '-map', '0:a?',            # Maps all audio streams
        '-map', '0:s?',            # Maps all subtitle streams
        '-vf', f'scale={scale},format=yuv420p', # Ensure 8-bit YUV420p to fix NVENC invalid param 8
        '-c:v', 'h264_nvenc', '-preset', 'p4', '-tune', 'hq',
        '-b:v', str(target_bitrate),
        '-c:a', 'copy',            # Copies all audio streams without re-encoding
        '-c:s', 'copy',            # Copies all subtitle streams without re-encoding
        output_file
    ]
    
    try:
        env = os.environ.copy()
        # Use asyncio subprocess so we can gracefully terminate FFmpeg if cancelled
        process = await asyncio.create_subprocess_exec(
            *cmd_nvenc,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env
        )
        
        if chat_id and chat_id in active_jobs:
            if 'processes' not in active_jobs[chat_id]:
                active_jobs[chat_id]['processes'] = []
            active_jobs[chat_id]['processes'].append(process)
            
        viewer = EncodingProgressViewer(status_message, chat_id) if status_message else None
        
        time_pattern = re.compile(r"time=(\d+):(\d+):(\d+\.\d+)")
        fps_pattern = re.compile(r"fps=\s*([\d\.]+)")
        speed_pattern = re.compile(r"speed=\s*([\d\.x]+)")
        
        stderr_buffer = []
        while True:
            try:
                # FFmpeg separates progress updates with \r instead of \n
                line = await process.stderr.readuntil(b'\r')
            except asyncio.exceptions.IncompleteReadError as e:
                line = e.partial
                
            if not line:
                break
                
            line_str = line.decode('utf-8', errors='ignore').strip()
            if not line_str:
                continue
                
            stderr_buffer.append(line_str)
            if len(stderr_buffer) > 10:
                stderr_buffer.pop(0)
                
            if viewer:
                time_match = time_pattern.search(line_str)
                if time_match:
                    h, m, s = time_match.groups()
                    current_sec = int(h) * 3600 + int(m) * 60 + float(s)
                    fps_match = fps_pattern.search(line_str)
                    fps = fps_match.group(1) if fps_match else "0"
                    speed_match = speed_pattern.search(line_str)
                    speed = speed_match.group(1) if speed_match else "0x"
                    
                    try:
                        await viewer.update(current_sec, total_duration, fps, speed)
                    except Exception as e:
                        if "User Cancelled" in str(e):
                            process.terminate()
                            raise
        
        await process.wait()
        
        if chat_id and active_jobs.get(chat_id, {}).get('cancel'):
            raise Exception("User Cancelled")
            
        if process.returncode != 0:
            error_msg = "\n".join(stderr_buffer)
            raise Exception(f"FFmpeg failed:\n{error_msg}")
            
        print("⚡ Encoding via RTX 6000 NVENC completed successfully!")
    except Exception as e:
        if "User Cancelled" not in str(e):
            print(f"⚠️ GPU Encoding Failed: {e}")
        raise

# ==========================================
# 6. Auto-Shutdown to Save Credits
# ==========================================

def deactivate_machine():
    """Immediately shuts down the Lightning AI environment to save GPU credits."""
    print("\n🛑 OPERATION COMPLETE. INITIATING IMMEDIATE SHUTDOWN TO SAVE RTX 6000 CREDITS! 🛑")
    time.sleep(2)
    try:
        # This command powers down the Linux VM running the studio
        subprocess.run(["sudo", "shutdown", "-h", "now"], check=True)
    except Exception as e:
        print(f"Could not automatically shut down. Please stop manually. Error: {e}")

# ==========================================
# Bot Conversation Logic
# ==========================================

API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")

if not all([API_ID, API_HASH, BOT_TOKEN]):
    print("❌ ERROR: You must set API_ID, API_HASH, and BOT_TOKEN environment variables!")
    sys.exit(1)

app = Client(
    "encoding_bot", 
    api_id=API_ID, 
    api_hash=API_HASH, 
    bot_token=BOT_TOKEN,
    max_concurrent_transmissions=8  # Safe optimized limit for Telegram
)

# Memory storage for the conversation state
user_sessions = {}
active_jobs = {}
os.makedirs("downloads", exist_ok=True)

# --- Pipeline Concurrency Controls ---
# Semaphores to allow processing different files at different stages concurrently
dl_semaphore = asyncio.Semaphore(4)  # Max concurrent downloads (increased for faster processing)
mux_semaphore = asyncio.Semaphore(3) # Max concurrent GPU encodes (allows 3 parallel)
ul_semaphore = asyncio.Semaphore(6)  # Max concurrent uploads (increased for faster processing)

def is_allowed(user_id):
    if not user_id:
        return False
    allowed_user = os.getenv("ALLOWED_USER_ID")
    if allowed_user:
        # Support multiple users separated by commas (e.g., "12345, 67890")
        allowed_users = [u.strip().strip('"').strip("'") for u in allowed_user.split(',')]
        if str(user_id).strip() not in allowed_users:
            return False
    return True

@app.on_message(filters.command("start") & filters.private)
async def start(client, message):
    if not is_allowed(message.from_user.id):
        await message.reply_text("❌ Access Denied: You are not authorized to use this bot.")
        return
        
    await message.reply_text(
        "Welcome! I am a video encoding bot.\n"
        "Reply to any video message with `/encode`, OR send `/encode <url>` to start a new job.\n"
        "To reuse a cached video, send `/log <video_id>`.",
        reply_markup=ReplyKeyboardRemove()
    )

@app.on_message(filters.command("shutdown") & filters.private)
async def shutdown_cmd(client, message):
    if not is_allowed(message.from_user.id):
        return
    await message.reply_text("🛑 Shutting down Lightning AI server...")
    deactivate_machine()

@app.on_message(filters.command("encode") & filters.private)
async def encode_command(client, message):
    if not is_allowed(message.from_user.id):
        await message.reply_text("❌ Access Denied.")
        return

    # Check if a URL was provided
    if len(message.command) > 1:
        url = message.text.split(maxsplit=1)[1]
        if url.startswith("http"):
            original_name = url.split("/")[-1].split("?")[0]
            if not original_name or "." not in original_name:
                original_name = "video.mkv"
                
            user_sessions[message.chat.id] = {
                'source_type': 'url',
                'url': url,
                'original_name': original_name,
                'message_to_reply': message.id,
                'awaiting_name': True
            }
        else:
            await message.reply_text("⚠️ Please provide a valid HTTP/HTTPS URL.")
            return
    else:
        # Check if it's a reply
        reply = message.reply_to_message
        if not reply or not (reply.video or reply.document):
            await message.reply_text("⚠️ Please reply directly to a video message with /encode, OR send /encode <url>.")
            return
            
        media = reply.video or reply.document
        file_id = media.file_id
        original_name = getattr(media, 'file_name', 'video.mkv') or 'video.mkv'

        user_sessions[message.chat.id] = {
            'source_type': 'telegram',
            'file_id': file_id,
            'original_name': original_name,
            'message_to_reply': reply.id,
            'awaiting_name': True
        }
    
    await message.reply_text(
        f"✨ Found video: `{original_name}`\nPlease reply with the desired output filename (or type `/skip` to use the original name):",
        reply_markup=ReplyKeyboardRemove()
    )
    
@app.on_message(filters.command("log") & filters.private)
async def log_command(client, message):
    if not is_allowed(message.from_user.id):
        return
    if len(message.command) < 2:
        await message.reply_text("⚠️ Usage: `/log <video_id>`")
        return
        
    vid_id = message.command[1]
    local_path = f"downloads/{vid_id}.mkv"
    if not os.path.exists(local_path):
        await message.reply_text("❌ Video not found on server. It may have expired and been deleted.")
        return
        
    user_sessions[message.chat.id] = {
        'source_type': 'local',
        'vid_id': vid_id,
        'original_name': f"video_{vid_id}.mkv",
        'message_to_reply': message.id,
        'awaiting_name': True
    }
    await message.reply_text("📁 Video found in server cache!\nPlease reply with the desired output filename (or type `/skip` to use default):")

@app.on_message((filters.text | filters.photo | filters.document | filters.video) & filters.private & ~filters.command(["start", "encode", "log", "shutdown"]))
async def meta_handler(client, message):
    chat_id = message.chat.id
    if chat_id not in user_sessions:
        return
        
    session = user_sessions[chat_id]
    
    # Prevent spam if the user forwards an album (multiple photos/videos at once)
    if message.media_group_id:
        if session.get('last_media_group_id') == message.media_group_id:
            return
        session['last_media_group_id'] = message.media_group_id

    # Ignore Userbot/PM-guard auto-replies that break the bot's conversation flow
    text_content = message.text or message.caption
    if text_content and any(phrase in text_content.lower() for phrase in ["access denied", "access blocked", "⛔"]):
        return

    now = time.time()

    if session.get('awaiting_name'):
        if not message.text:
            # Debounce: Only send the error message if we haven't sent one in the last 2 seconds
            if now - session.get('last_error_time', 0) > 2:
                await message.reply_text("⚠️ I'm waiting for a filename as text. Please reply with the desired output filename, or type `/cancel` to abort.")
                session['last_error_time'] = now
            return

        if message.text.strip().lower() != '/skip':
            new_name = message.text.strip()
            if not "." in new_name:
                new_name += ".mkv"
            session['original_name'] = new_name

        session['awaiting_name'] = False
        session['awaiting_thumbnail'] = True
        await message.reply_text("🖼️ Please send a custom thumbnail photo (or type `/skip` to auto-extract one):")
        return
        
    elif session.get('awaiting_thumbnail'):
        if message.text and message.text.strip().lower() == '/cancel':
            del user_sessions[chat_id]
            await message.reply_text("❌ Operation cancelled.")
            return

        if message.photo:
            session['custom_thumb'] = message.photo.file_id
        elif message.text and message.text.strip().lower() == '/skip':
            session['custom_thumb'] = None
        else:
            if now - session.get('last_error_time', 0) > 2:
                await message.reply_text("⚠️ Please send a compressed PHOTO (not a file/document), or type `/cancel` to abort.")
                session['last_error_time'] = now
            return

        session['awaiting_thumbnail'] = False
        
        source_map = {
            'url': 'URL Link',
            'local': f"Cached Video (`{session.get('vid_id')}`)",
            'telegram': 'Telegram Message'
        }
        source_text = source_map.get(session['source_type'], 'Unknown')
        
        # Pre-calculate to show the user exactly what the final names will be
        name_2k = process_metadata(session['original_name'], "2K")
        name_1080 = process_metadata(session['original_name'], "1080P")
        name_720 = process_metadata(session['original_name'], "720P")

        summary = (
            f"Okay, here is the plan:\n\n"
            f"🔹 **Source:** `{source_text}`\n"
            f"🔹 **Base Filename:** `{session['original_name']}`\n\n"
            f"🔹 **Output Files:**\n"
            f"  `{name_2k}`\n"
            f"  `{name_1080}`\n"
            f"  `{name_720}`\n"
            f"This will generate three separate video files. Do you want to begin the process?"
        )
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("Yes, Start", callback_data="start_yes"),
                InlineKeyboardButton("Cancel", callback_data="start_cancel"),
            ]
        ])
        await message.reply_text(summary, reply_markup=keyboard)
        return

@app.on_callback_query(filters.regex(r"^start_"))
async def start_callback(client, callback_query):
    chat_id = callback_query.message.chat.id
    if chat_id not in user_sessions:
        await callback_query.answer("Session expired.", show_alert=True)
        return

    action = callback_query.data.split("_")[1]
    if action == "cancel":
        await callback_query.edit_message_text("❌ Job cancelled.")
        del user_sessions[chat_id]
        return

    session = user_sessions.pop(chat_id)
    status_message = await callback_query.edit_message_text("⏳ Queuing job...")
    
    # Register active job
    active_jobs[chat_id] = {'cancel': False, 'processes': []}
    download_complete = False
    
    local_input = None
    thumb_path = None
    encoded_files_to_cleanup = []
    files_to_keep = [] # For files > 2GB that will be served via HTTP
    
    try:
        vid_id = session.get('vid_id', uuid.uuid4().hex[:8])
        local_input = f"downloads/{vid_id}.mkv"
        thumb_path = f"downloads/thumb_{vid_id}.jpg"
        
        # --- 1. Download ---
        if session['source_type'] != 'local':
            await status_message.edit_text("⏳ Waiting for a download slot in the pipeline...")
            async with dl_semaphore:
                if active_jobs.get(chat_id, {}).get('cancel'):
                    raise Exception("User Cancelled")
                
                await status_message.edit_text("🚀 Starting download...")
                dl_viewer = PyrogramProgressViewer(status_message, "Downloading", chat_id)
                if session['source_type'] == 'url':
                    await download_video_from_url(session['url'], local_input, dl_viewer)
                else:
                    async def dl_progress(current, total):
                        await dl_viewer.update(current, total)
                    await client.download_media(session['file_id'], file_name=local_input, progress=dl_progress)
                
                asyncio.create_task(delayed_delete(local_input, 7200)) # 2 Hour expiry timer
                await client.send_message(chat_id, f"✅ Download complete!\n\n💾 **Video ID:** `{vid_id}`\n\nThis video is cached on the server for 2 hours. To encode it again into a different quality without re-downloading, use:\n`/log {vid_id}`")
        else:
            await status_message.edit_text("✅ Local cache ready!")
            
        download_complete = True

        # --- Thumbnail ---
        if session.get('custom_thumb'):
            await status_message.edit_text("✅ Download Complete! Downloading custom thumbnail...")
            await client.download_media(session['custom_thumb'], file_name=thumb_path)
        else:
            await status_message.edit_text("✅ Download Complete! Extracting thumbnail...")
            extract_thumbnail(local_input, thumb_path)

        # Determine qualities to encode
        qualities_to_encode = [
            ("2K", process_metadata(session['original_name'], "2K")),
            ("1080P", process_metadata(session['original_name'], "1080P")),
            ("720P", process_metadata(session['original_name'], "720P")),
        ]

        async def process_quality(quality, final_output_name):
            q_status = await client.send_message(chat_id, f"⏳ Queuing {quality} version...")
            try:
                # --- 2. Encode ---
                await q_status.edit_text(f"⏳ Waiting for GPU to encode {quality} version...")
                async with mux_semaphore:
                    if active_jobs.get(chat_id, {}).get('cancel'):
                        raise Exception("User Cancelled")
                    await q_status.edit_text(f"🎬 Encoding {quality} version...")
                    await encode_video(local_input, final_output_name, quality, chat_id, q_status)
                
                # --- 3. Upload or Link ---
                file_size = os.path.getsize(final_output_name)
                TELEGRAM_LIMIT_BYTES = 2 * 1024 * 1024 * 1024

                if file_size >= TELEGRAM_LIMIT_BYTES:
                    await q_status.edit_text(f"✅ {quality} encoded. File is {file_size / (1024**3):.2f} GB, too large for Telegram. Generating download link...")
                    start_http_server()
                    files_to_keep.append(final_output_name)
                    
                    # Generate a unique ID for this file and map it
                    file_id = uuid.uuid4().hex
                    file_id_map[file_id] = final_output_name
                    download_path = f"/download/{file_id}"
                    
                    public_url = os.getenv("LIGHTNING_APP_STATE_URL") or os.getenv("LIGHTNING_HOST")

                    if public_url:
                        if not public_url.startswith("http"):
                            public_url = "https://" + public_url
                        public_url = public_url.rstrip('/')
                        
                        # Best guess for the URL, replacing the default app port with our HTTP port
                        download_link = f"{public_url.replace('7860', str(HTTP_PORT))}{download_path}"
                        await client.send_message(
                            chat_id,
                            (
                                f"🔗 **{quality} version is ready!**\n\n"
                                f"File is too large for Telegram. Use this direct download link:\n`{download_link}`\n\n"
                                f"**Note:** You must expose port `{HTTP_PORT}` in the Lightning AI UI for this link to work. The port in the URL may need to be adjusted manually if it's incorrect."
                            ),
                            reply_to_message_id=session['message_to_reply']
                        )
                    else:
                        # This is the case you likely hit, where no public URL env var is set.
                        file_name_for_display = os.path.basename(final_output_name)
                        await client.send_message(
                            chat_id,
                            f"✅ **{quality} version is ready!**\n\n"
                            f"File is too large for Telegram. It has been saved on the server as `{file_name_for_display}`.\n\n"
                            f"To download, please use the Lightning AI UI to expose port `{HTTP_PORT}` and then access the following path on your public URL:\n"
                            f"`{download_path}`\n\n"
                            f"Example: `http://<your-public-url-for-port-{HTTP_PORT}>{download_path}`",
                            reply_to_message_id=session['message_to_reply']
                        )
                else:
                    await q_status.edit_text(f"⏳ Waiting to upload {quality} version...")
                    async with ul_semaphore:
                        if active_jobs.get(chat_id, {}).get('cancel'):
                            raise Exception("User Cancelled")
                        await q_status.edit_text(f"🚀 Uploading {quality} version...")
                        ul_viewer = PyrogramProgressViewer(q_status, f"Uploading {quality}", chat_id)
                        
                        async def ul_progress(current, total):
                            await ul_viewer.update(current, total)
                            
                        await client.send_document(
                            chat_id, 
                            document=final_output_name, 
                            thumb=thumb_path if os.path.exists(thumb_path) else None,
                            reply_to_message_id=session['message_to_reply'],
                            progress=ul_progress
                        )

                # Clean up the individual progress message once done
                try:
                    await q_status.delete()
                except Exception:
                    pass
            except Exception as e:
                if "User Cancelled" in str(e):
                    await q_status.edit_text(f"❌ {quality} cancelled.")
                else:
                    await q_status.edit_text(f"❌ {quality} failed: {e}")
                raise e

        await status_message.edit_text("🚀 Starting parallel encoding tasks...")
        tasks = []
        for quality, final_output_name in qualities_to_encode:
            encoded_files_to_cleanup.append(final_output_name)
            tasks.append(process_quality(quality, final_output_name))
            
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            failed_tasks = [res for res in results if isinstance(res, Exception)]
            if failed_tasks:
                # Combine error messages from all failed tasks for a clear report
                error_summary = "\n".join([f"- {type(e).__name__}: {e}" for e in failed_tasks])
                raise Exception(f"One or more encoding tasks failed:\n{error_summary}")
        
        # Clean up the chat by deleting the prompt/progress message!
        try:
            await status_message.delete()
        except Exception:
            pass
        
        # Ask the user if they want to shut down or keep going
        power_keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("🛑 Shut Down Server", callback_data="power_off"),
                InlineKeyboardButton("✅ Keep Alive (Reuse Video)", callback_data="power_on")
            ]
        ])
        await client.send_message(chat_id, "🎉 Job complete! Do you want to shut down the Lightning AI server now to save credits?", reply_markup=power_keyboard)
        
    except Exception as e:
        if "User Cancelled" in str(e):
            await status_message.edit_text("❌ Job was cancelled by the user.")
        else:
            logger.error(f"Pipeline failed: {e}", exc_info=True)
            await status_message.edit_text(f"❌ Pipeline crashed: {e}")
    finally:
        active_jobs.pop(chat_id, None)
        if not download_complete and local_input and os.path.exists(local_input):
            os.remove(local_input) # Only delete if it crashed mid-download
        
        for f in encoded_files_to_cleanup:
            if f not in files_to_keep and os.path.exists(f):
                os.remove(f)

        if thumb_path and os.path.exists(thumb_path):
            os.remove(thumb_path)

@app.on_callback_query(filters.regex(r"^cancel_job$"))
async def cancel_job_callback(client, callback_query):
    chat_id = callback_query.message.chat.id
    if chat_id in active_jobs:
        active_jobs[chat_id]['cancel'] = True
        await callback_query.answer("Cancelling job... Please wait.", show_alert=True)
        
        # Kill FFmpeg if it's currently running
        processes = active_jobs[chat_id].get('processes', [])
        for process in processes:
            try:
                process.terminate()
            except Exception:
                pass
    else:
        await callback_query.answer("No active job to cancel.", show_alert=True)

@app.on_callback_query(filters.regex(r"^power_"))
async def power_callback(client, callback_query):
    if not is_allowed(callback_query.from_user.id):
        await callback_query.answer("❌ Access Denied.", show_alert=True)
        return
    action = callback_query.data.split("_")[1]
    if action == "off":
        await callback_query.edit_message_text("🛑 Shutting down Lightning AI server...")
        deactivate_machine()
    else:
        await callback_query.edit_message_text("✅ Server kept alive. You can use `/shutdown` when you are finished.")

if __name__ == "__main__":
    print("🌩️ Pyrogram Bot Starting. Waiting for Telegram connection...")
    app.run()
