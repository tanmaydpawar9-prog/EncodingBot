# Fast Lightning AI Video Pipeline

A high-speed, crash-proof video downloading, encoding, and uploading pipeline configured for Lightning AI Studio (RTX 6000). 

## Setup Instructions

1. **Install System Dependencies (FFmpeg):**
   ```bash
   sudo apt-get update
   sudo apt-get install ffmpeg -y
   ```

2. **Install Python Requirements:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Run the Pipeline:**
   ```bash
   python pipeline.py
   ```

**Important:** The script contains an automated `sudo shutdown -h now` command that triggers instantly upon completion. This is intentional and ensures you do not drain your Lightning AI credits by leaving an idle RTX 6000 running.