import os
import sys
import time
import re
import subprocess
import logging
import asyncio
import requests
import concurrent.futures
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import FloodWait

# --- Basic Bot Logging ---
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

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

# ==========================================
# 2 & 3. Naming and Size/Bitrate Logic
# ==========================================
def process_metadata(original_name, quality_choice):
    """Replaces [4K], [1080P], etc., with the user's chosen output quality."""
    pattern = r'(?i)\[(4k|1080p|720p|480p|360p)\]'
    new_name = re.sub(pattern, f'[{quality_choice.upper()}]', original_name)
    
    if new_name == original_name:
        base, ext = os.path.splitext(original_name)
        new_name = f"{base} [{quality_choice.upper()}]{ext}"
        
    # Force .mkv extension to support all streams and avoid weird MP4 containers
    base, _ = os.path.splitext(new_name)
    new_name = f"{base}.mkv"
        
    return new_name

def get_target_bitrate(input_file, quality_choice):
    """Dynamically calculates bitrate scaling."""
    cmd = [
        'ffprobe', '-v', 'error', '-show_entries', 'format=bit_rate', 
        '-of', 'default=noprint_wrappers=1:nokey=1', input_file
    ]
    try:
        original_bitrate = int(subprocess.check_output(cmd).decode().strip())
    except Exception:
        original_bitrate = 16_000_000

    qual = quality_choice.upper()
    
    if '1080' in qual:
        return int(original_bitrate * 0.5)
    elif '720' in qual:
        return int(original_bitrate * 0.25)
    elif '480' in qual:
        return int(original_bitrate * 0.125)
    
    return original_bitrate

# ==========================================
# Fast Download, Encode, and Upload 
# ==========================================

def _download_range(url, start, end, output_path, progress_list, idx, chat_id):
    headers = {
        "Range": f"bytes={start}-{end}", 
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    retries = 3
    for attempt in range(retries):
        current_start = start + progress_list[idx]
        if current_start > end:
            return
            
        headers = {
            "Range": f"bytes={current_start}-{end}", 
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        try:
            # Strict timeout prevents the infinite 40s freezing
            res = requests.get(url, headers=headers, stream=True, allow_redirects=True, timeout=(5, 15))
            res.raise_for_status()
            
            if res.status_code == 200:
                raise Exception("Server ignored Range header, parallel download not supported.")
                
            with open(output_path, "rb+") as f:
                # Resume exactly where this specific thread left off
                f.seek(current_start)
                for chunk in res.iter_content(chunk_size=2*1024*1024):
                    if chat_id and active_jobs.get(chat_id, {}).get('cancel'):
                        return
                    if chunk:
                        f.write(chunk)
                        progress_list[idx] += len(chunk)
                        if start + progress_list[idx] > end:
                            break
            return # Success
        except requests.exceptions.RequestException as e:
            logger.warning(f"Thread {idx} dropped connection (attempt {attempt+1}/{retries}): {e}")
            time.sleep(2)
        except Exception as e:
            logger.warning(f"Thread {idx} error: {e}")
            time.sleep(2)
            
    raise Exception(f"Download thread {idx} permanently failed.")

async def download_video_from_url(url, output_path, progress_viewer):
    loop = asyncio.get_running_loop()
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    
    res = await asyncio.to_thread(requests.get, url, headers=headers, stream=True, allow_redirects=True, timeout=(5, 15))
    
    try:
        res.raise_for_status()
    except requests.exceptions.HTTPError:
        raise ValueError(f"Download failed: Server returned HTTP {res.status_code}.")

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

async def encode_video(input_file, output_file, quality_choice, chat_id=None):
    """Runs FFmpeg in a separate thread to avoid blocking the bot."""
    target_bitrate = get_target_bitrate(input_file, quality_choice)
        
    # Map quality to fixed width, letting height auto-scale (-2)
    qual = quality_choice.upper()
    if '1080' in qual:
        scale = '1920:-2'
    elif '720' in qual:
        scale = '1280:-2'
    elif '480' in qual:
        scale = '854:-2'
    else:
        scale = '1920:-2'

    print(f"\n[INFO] 🎬 Encoding to {quality_choice} (Target Bitrate: {target_bitrate//1000} kbps)...")
    
    # RTX 6000 hardware-accelerated NVENC settings for speed and quality
    cmd_nvenc = [
        'ffmpeg', '-y', '-hwaccel', 'cuda', '-i', input_file,
        '-map', '0',               # Maps all streams (video, multiple audio, subtitles)
        '-vf', f'scale={scale}',
        '-c:v', 'h264_nvenc', '-preset', 'p4', '-tune', 'hq',
        '-b:v', str(target_bitrate),
        '-c:a', 'copy',            # Copies all audio streams without re-encoding
        '-c:s', 'copy',            # Copies all subtitle streams without re-encoding
        output_file
    ]
    
    try:
        # Use asyncio subprocess so we can gracefully terminate FFmpeg if cancelled
        process = await asyncio.create_subprocess_exec(
            *cmd_nvenc,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        if chat_id and chat_id in active_jobs:
            active_jobs[chat_id]['process'] = process
            
        stdout, stderr = await process.communicate()
        
        if chat_id and active_jobs.get(chat_id, {}).get('cancel'):
            raise Exception("User Cancelled")
            
        if process.returncode != 0:
            raise Exception(f"FFmpeg failed: {stderr.decode()}")
            
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
        os.system("sudo shutdown -h now")
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

def is_allowed(user_id):
    allowed_user = os.getenv("ALLOWED_USER_ID")
    if allowed_user and str(user_id) != allowed_user:
        return False
    return True

@app.on_message(filters.command("start") & filters.private)
async def start(client, message):
    if not is_allowed(message.from_user.id):
        await message.reply_text("❌ Access Denied: You are not authorized to use this bot.")
        return
        
    await message.reply_text(
        "Welcome! I am a video encoding bot.\n"
        "Reply to any video message with `/encode`, OR send `/encode <url>` to start a new job."
    )

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
                'message_to_reply': message.id
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
            'message_to_reply': reply.id
        }
    
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("1080P", callback_data="qual_1080P"),
            InlineKeyboardButton("720P", callback_data="qual_720P"),
            InlineKeyboardButton("480P", callback_data="qual_480P"),
        ]
    ])
    
    await message.reply_text(
        f"✨ Found video: `{original_name}`\nPlease select the desired output quality.",
        reply_markup=keyboard
    )

@app.on_callback_query(filters.regex(r"^qual_"))
async def quality_callback(client, callback_query):
    chat_id = callback_query.message.chat.id
    if chat_id not in user_sessions:
        await callback_query.answer("Session expired. Please send /encode again.", show_alert=True)
        return
        
    quality = callback_query.data.split("_")[1]
    user_sessions[chat_id]['quality'] = quality
    
    final_output_name = process_metadata(user_sessions[chat_id]['original_name'], quality)
    user_sessions[chat_id]['final_output_name'] = final_output_name

    summary = (
        f"Okay, here is the plan:\n\n"
        f"🔹 **Source:** `{'URL Link' if user_sessions[chat_id]['source_type'] == 'url' else 'Telegram Message'}`\n"
        f"🔹 **Output Quality:** `{quality}`\n"
        f"🔹 **Final Filename:** `{final_output_name}`\n\n"
        f"Do you want to begin the process?"
    )
    
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Yes, Start", callback_data="start_yes"),
            InlineKeyboardButton("Cancel", callback_data="start_cancel"),
        ]
    ])
    await callback_query.edit_message_text(summary, reply_markup=keyboard)

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
    status_message = await callback_query.edit_message_text("🚀 Starting job... Downloading from Telegram (this may take a moment)...")
    
    # Register active job
    active_jobs[chat_id] = {'cancel': False, 'process': None}
    
    try:
        local_input = "input_temp.mkv"
        final_output_name = session['final_output_name']
        
        # --- 1. Download ---
        dl_viewer = PyrogramProgressViewer(status_message, "Downloading", chat_id)
        if session['source_type'] == 'url':
            await download_video_from_url(session['url'], local_input, dl_viewer)
        else:
            async def dl_progress(current, total):
                await dl_viewer.update(current, total)
            await client.download_media(session['file_id'], file_name=local_input, progress=dl_progress)
            
        await status_message.edit_text("✅ Download Complete! Starting encode...")

        # --- 2. Encode ---
        await encode_video(local_input, final_output_name, session['quality'], chat_id)
        await status_message.edit_text("✅ Encode Complete! Starting upload...")

        # --- 3. Upload (up to 2GB) ---
        ul_viewer = PyrogramProgressViewer(status_message, "Uploading", chat_id)
        async def ul_progress(current, total):
            await ul_viewer.update(current, total)
            
        await client.send_document(
            chat_id, 
            document=final_output_name, 
            reply_to_message_id=session['message_to_reply'],
            progress=ul_progress
        )
        
        # Clean up the chat by deleting the prompt/progress message!
        try:
            await status_message.delete()
        except Exception:
            pass

        # --- 4. Shutdown to save credits ---
        deactivate_machine()
        
    except Exception as e:
        if "User Cancelled" in str(e):
            await status_message.edit_text("❌ Job was cancelled by the user.")
        else:
            logger.error(f"Pipeline failed: {e}", exc_info=True)
            await status_message.edit_text(f"❌ Pipeline crashed: {e}")
    finally:
        active_jobs.pop(chat_id, None)
        if os.path.exists("input_temp.mkv"):
            os.remove("input_temp.mkv")
        if 'final_output_name' in locals() and os.path.exists(final_output_name):
            os.remove(final_output_name)

@app.on_callback_query(filters.regex(r"^cancel_job$"))
async def cancel_job_callback(client, callback_query):
    chat_id = callback_query.message.chat.id
    if chat_id in active_jobs:
        active_jobs[chat_id]['cancel'] = True
        await callback_query.answer("Cancelling job... Please wait.", show_alert=True)
        
        # Kill FFmpeg if it's currently running
        process = active_jobs[chat_id].get('process')
        if process:
            try:
                process.terminate()
            except Exception:
                pass
    else:
        await callback_query.answer("No active job to cancel.", show_alert=True)

if __name__ == "__main__":
    print("🌩️ Pyrogram Bot Starting. Waiting for Telegram connection...")
    app.run()
