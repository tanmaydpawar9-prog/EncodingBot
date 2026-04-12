import os
import sys
import time
import re
import subprocess
import requests
import logging
import asyncio
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

# --- Basic Bot Logging ---
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# ==========================================
# 1. Custom Progress Bar Formatting
# ==========================================
class TelegramProgressViewer:
    def __init__(self, message, context: ContextTypes.DEFAULT_TYPE, action="Downloading"):
        self.message = message
        self.context = context
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
        progress_pct = (current / total) * 100 if total > 0 else 0
        
        # [■■■■■■□□□□] exact design match
        filled = int(progress_pct / 10)
        bar = '■' * filled + '□' * (10 - filled)
        
        dl_mb = current / (1024 * 1024)
        tot_mb = total / (1024 * 1024)
        speed_mb = speed / (1024 * 1024)
        
        eta_sec = (total - current) / speed if (speed > 0 and total > 0) else 0
        
        eta_str = time.strftime('%Hh %Mm %Ss', time.gmtime(eta_sec))
        elapsed_str = time.strftime('%Mm %Ss', time.gmtime(elapsed))
        
        text = (
            f"Progress: [{bar}] {progress_pct:.1f}%\n"
            f"⚙️ {self.action}: {dl_mb:.1f}MB of {tot_mb:.1f}MB\n"
            f"⚡ Speed: {speed_mb:.1f}MB/s\n"
            f"⌛ ETA: {eta_str}\n"
            f"⏱️ Time elapsed: {elapsed_str}."
        )
        
        # Only edit the message if the text has changed
        if text != self.last_text:
            try:
                await self.context.bot.edit_message_text(text=text, chat_id=self.message.chat_id, message_id=self.message.message_id)
                self.last_text = text
            except Exception as e:
                # Ignore errors if message is not modified
                logger.warning(f"Failed to edit Telegram message: {e}")

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
async def download_video(url, output_path, progress_viewer: TelegramProgressViewer):
    response = requests.get(url, stream=True)
    response.raise_for_status()
    
    total_size = int(response.headers.get('content-length', 0))
    downloaded = 0
    
    with open(output_path, 'wb') as f:
        # 4MB chunks for fast I/O throughput
        for chunk in response.iter_content(chunk_size=4*1024*1024):
            if chunk:
                f.write(chunk)
                downloaded += len(chunk)
                await progress_viewer.update(downloaded, total_size)
                
    return output_path

async def encode_video(input_file, output_file, quality_choice):
    """Runs FFmpeg in a separate thread to avoid blocking the bot."""
    target_bitrate = get_target_bitrate(input_file, quality_choice)
    height = ''.join(filter(str.isdigit, quality_choice))
    if not height: 
        height = "1080"
        
    print(f"\n[INFO] 🎬 Encoding to {quality_choice} (Target Bitrate: {target_bitrate//1000} kbps)...")
    
    # RTX 6000 hardware-accelerated NVENC settings for speed and quality
    cmd_nvenc = [
        'ffmpeg', '-y', '-hwaccel', 'cuda', '-i', input_file,
        '-vf', f'scale=-2:{height}',
        '-c:v', 'h264_nvenc', '-preset', 'p4', '-tune', 'hq',
        '-b:v', str(target_bitrate),
        '-c:a', 'copy',
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

async def upload_video(file_path, chat_id, context: ContextTypes.DEFAULT_TYPE):
    """Uploads the final video file back to the user in Telegram."""
    try:
        await context.bot.send_document(chat_id=chat_id, document=open(file_path, 'rb'))
    except Exception as e:
        await context.bot.send_message(chat_id=chat_id, text=f"❌ Failed to upload file: {e}")
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

# Define conversation states
(GET_URL, GET_ORIGINAL_NAME, GET_QUALITY, CONFIRMATION) = range(4)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "Welcome! I am a video encoding bot.\n"
        "Use the /encode command to start a new job."
    )

async def encode_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Starts the conversation and asks for the video URL."""
    await update.message.reply_text("▶️ Starting new encoding job. Please send me the video source URL.")
    return GET_URL

async def get_url(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Stores the URL and asks for the original filename."""
    context.user_data['source_url'] = update.message.text
    await update.message.reply_text("📁 Great! Now, what is the original filename? (e.g., My.Video.S01E01.[4K].mkv)")
    return GET_ORIGINAL_NAME

async def get_original_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Stores the filename and asks for the output quality."""
    context.user_data['original_name'] = update.message.text
    reply_keyboard = [["1080P", "720P", "480P"]]
    await update.message.reply_text(
        "✨ Got it. Please select the desired output quality.",
        reply_markup=ReplyKeyboardMarkup(reply_keyboard, one_time_keyboard=True),
    )
    return GET_QUALITY

async def get_quality(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Stores the quality and asks for final confirmation."""
    context.user_data['quality_choice'] = update.message.text
    
    # Generate final name for confirmation
    final_output_name = process_metadata(context.user_data['original_name'], context.user_data['quality_choice'])
    context.user_data['final_output_name'] = final_output_name

    summary = (
        f"Okay, here is the plan:\n\n"
        f"🔹 **Source:** `{context.user_data['source_url']}`\n"
        f"🔹 **Output Quality:** `{context.user_data['quality_choice']}`\n"
        f"🔹 **Final Filename:** `{final_output_name}`\n\n"
        f"Type **Yes** to begin the process."
    )
    await update.message.reply_text(summary, reply_markup=ReplyKeyboardRemove(), parse_mode='Markdown')
    return CONFIRMATION

async def process_job(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """The main worker function that executes the pipeline."""
    if update.message.text.lower() != 'yes':
        await update.message.reply_text("❌ Job cancelled.")
        return ConversationHandler.END

    chat_id = update.effective_chat.id
    status_message = await update.message.reply_text("🚀 Starting job... Initializing download.")
    
    try:
        # --- 1. Download ---
        progress_viewer = TelegramProgressViewer(status_message, context, "Downloading")
        local_input = "input_temp.mp4"
        await download_video(context.user_data['source_url'], local_input, progress_viewer)
        await context.bot.edit_message_text("✅ Download Complete! Starting encode...", chat_id=chat_id, message_id=status_message.message_id)

        # --- 2. Encode ---
        final_output_name = context.user_data['final_output_name']
        await encode_video(local_input, final_output_name, context.user_data['quality_choice'])
        await context.bot.edit_message_text("✅ Encode Complete! Starting upload...", chat_id=chat_id, message_id=status_message.message_id)

        # --- 3. Upload ---
        await upload_video(final_output_name, chat_id, context)
        await context.bot.send_message(chat_id=chat_id, text="🎉 All operations finished securely.")

        # --- 4. Shutdown to save credits ---
        deactivate_machine()
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        await context.bot.send_message(chat_id=chat_id, text=f"❌ Pipeline crashed: {e}")
    finally:
        # Cleanup temporary files
        if os.path.exists("input_temp.mp4"):
            os.remove("input_temp.mp4")
        if 'final_output_name' in context.user_data and os.path.exists(context.user_data['final_output_name']):
            os.remove(context.user_data['final_output_name'])
            
    return ConversationHandler.END

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Cancels and ends the conversation."""
    await update.message.reply_text("Job cancelled.", reply_markup=ReplyKeyboardRemove())
    return ConversationHandler.END

def main() -> None:
    """Run the bot."""
    # IMPORTANT: Get your bot token from environment variables, not hardcoded.
    BOT_TOKEN = os.getenv("BOT_TOKEN")
    if not BOT_TOKEN:
        raise ValueError("BOT_TOKEN environment variable not set!")

    application = Application.builder().token(BOT_TOKEN).build()

    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("encode", encode_command)],
        states={
            GET_URL: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_url)],
            GET_ORIGINAL_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_original_name)],
            GET_QUALITY: [MessageHandler(filters.Regex("^(1080P|720P|480P)$"), get_quality)],
            CONFIRMATION: [MessageHandler(filters.Regex("(?i)^yes$"), process_job)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    application.add_handler(CommandHandler("start", start))
    application.add_handler(conv_handler)

    # Run the bot until the user presses Ctrl-C
    application.run_polling()

if __name__ == "__main__":
    main()
