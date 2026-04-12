import os
import sys
import time
import re
import subprocess
import logging
import asyncio
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton

# --- Basic Bot Logging ---
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# ==========================================
# 1. Custom Progress Bar Formatting
# ==========================================
class PyrogramProgressViewer:
    def __init__(self, message, action="Downloading"):
        self.message = message
        self.action = action
        self.start_time = time.time()
        self.last_update = 0
        self.last_text = ""

    async def update(self, current, total):
        now = time.time()
        # Throttle updates to 2s to avoid hitting Telegram API rate limits
        if now - self.last_update < 2 and current < total:
            return
        self.last_update = now
        
        elapsed = now - self.start_time
        if elapsed < 0.1: elapsed = 0.1
        
        speed = current / elapsed
        
        if total > 0:
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
                await self.message.edit_text(text)
                self.last_text = text
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

async def encode_video(input_file, output_file, quality_choice):
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
        # Run the blocking subprocess in a separate thread
        process = await asyncio.to_thread(
            subprocess.run, cmd_nvenc, check=True, capture_output=True, text=True
        )
        print("⚡ Encoding via RTX 6000 NVENC completed successfully!")
    except subprocess.CalledProcessError as e:
        print(f"⚠️ GPU Encoding Failed: {e}\nFFmpeg stderr: {e.stderr}")
        print("Please ensure FFmpeg is installed and NVIDIA drivers are loaded.")
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

app = Client("encoding_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# Memory storage for the conversation state
user_sessions = {}

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
        "Reply to any video message with the /encode command to start a new job."
    )

@app.on_message(filters.command("encode") & filters.private)
async def encode_command(client, message):
    if not is_allowed(message.from_user.id):
        await message.reply_text("❌ Access Denied.")
        return

    reply = message.reply_to_message
    if not reply or not (reply.video or reply.document):
        await message.reply_text("⚠️ Please reply directly to a video message with /encode.")
        return
        
    media = reply.video or reply.document
    file_id = media.file_id
    original_name = getattr(media, 'file_name', 'video.mp4') or 'video.mp4'

    user_sessions[message.chat.id] = {
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
        f"🔹 **Source:** `Telegram Message Reply`\n"
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
    
    try:
        local_input = "input_temp.mkv"
        final_output_name = session['final_output_name']
        
        # --- 1. Download (up to 2GB) ---
        dl_viewer = PyrogramProgressViewer(status_message, "Downloading")
        async def dl_progress(current, total):
            await dl_viewer.update(current, total)
            
        await client.download_media(session['file_id'], file_name=local_input, progress=dl_progress)
        await status_message.edit_text("✅ Download Complete! Starting encode...")

        # --- 2. Encode ---
        await encode_video(local_input, final_output_name, session['quality'])
        await status_message.edit_text("✅ Encode Complete! Starting upload...")

        # --- 3. Upload (up to 2GB) ---
        ul_viewer = PyrogramProgressViewer(status_message, "Uploading")
        async def ul_progress(current, total):
            await ul_viewer.update(current, total)
            
        await client.send_document(
            chat_id, 
            document=final_output_name, 
            reply_to_message_id=session['message_to_reply'],
            progress=ul_progress
        )
        await status_message.edit_text("🎉 All operations finished securely.")

        # --- 4. Shutdown to save credits ---
        deactivate_machine()
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        await status_message.edit_text(f"❌ Pipeline crashed: {e}")
    finally:
        if os.path.exists("input_temp.mkv"):
            os.remove("input_temp.mkv")
        if 'final_output_name' in locals() and os.path.exists(final_output_name):
            os.remove(final_output_name)

if __name__ == "__main__":
    print("🌩️ Pyrogram Bot Starting. Waiting for Telegram connection...")
    app.run()
