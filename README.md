# Fast Lightning AI Video Pipeline

A high-speed, crash-proof video downloading, encoding, and uploading pipeline configured for Lightning AI Studio (RTX 6000). 

## Setup Instructions
1. **Set Environment Variables:** To support 2GB files, this bot uses Pyrogram. You must get an `API_ID` and `API_HASH` from my.telegram.org. Set your variables in the Lightning AI terminal:
   ```bash
   export BOT_TOKEN="YOUR_TELEGRAM_BOT_TOKEN_HERE"
   export API_ID="YOUR_API_ID_HERE"
   export API_HASH="YOUR_API_HASH_HERE"
   export ALLOWED_USER_ID="YOUR_TELEGRAM_USER_ID_HERE"
   ```

2. **Install System Dependencies (FFmpeg):**
   ```bash
   sudo apt-get update
   sudo apt-get install ffmpeg -y
   ```

3. **Install Python Requirements:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Run the Bot:**
   ```bash
   python pipeline.py
   ```

5. **Interact with the Bot:** Open Telegram and send the `/encode` command to your bot to start a job.

**IMPORTANT:** The script contains an automated `sudo shutdown -h now` command that triggers after a job is successfully completed. This is intentional and ensures you do not drain your Lightning AI credits by leaving an idle RTX 6000 running.