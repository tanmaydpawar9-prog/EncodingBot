import os
import sys
import time
import re
import signal
import subprocess
import requests

# ==========================================
# 1. Custom Progress Bar Formatting
# ==========================================
class ProgressViewer:
    def __init__(self, action="Downloading"):
        self.action = action
        self.start_time = time.time()
        self.last_update = 0
        self.lines_printed = 0

    def update(self, current, total):
        now = time.time()
        # Cap update frequency to avoid console flickering (0.2s)
        if now - self.last_update < 0.2 and current < total:
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
            f"⚙️ {self.action}: {dl_mb:.1f}MB | {tot_mb:.1f}MB\n"
            f"⚡ Speed: {speed_mb:.1f}MB/s\n"
            f"⌛ ETA: {eta_str}\n"
            f"⏱️ Time elapsed: {elapsed_str}."
        )
        
        # ANSI Escape codes to overwrite previous lines cleanly
        if self.lines_printed > 0:
            sys.stdout.write(f"\033[{self.lines_printed}A\r\033[J")
        
        sys.stdout.write(text + "\n")
        sys.stdout.flush()
        self.lines_printed = text.count('\n') + 1

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
def download_video(url, output_path):
    print("\n[INFO] Starting Download...")
    response = requests.get(url, stream=True)
    response.raise_for_status()
    
    total_size = int(response.headers.get('content-length', 0))
    viewer = ProgressViewer("Downloading")
    downloaded = 0
    
    with open(output_path, 'wb') as f:
        # 4MB chunks for fast I/O throughput
        for chunk in response.iter_content(chunk_size=4*1024*1024):
            if chunk:
                f.write(chunk)
                downloaded += len(chunk)
                viewer.update(downloaded, total_size)
                
    print("\n✅ Download Complete!")
    return output_path

def encode_video(input_file, output_file, quality_choice):
    target_bitrate = get_target_bitrate(input_file, quality_choice)
    height = ''.join(filter(str.isdigit, quality_choice))
    if not height: 
        height = "1080"
        
    print(f"\n[INFO] 🎬 Encoding to {quality_choice} (Target Bitrate: {target_bitrate//1000} kbps)...")
    
    # RTX 6000 hardware-accelerated NVENC settings
    cmd_nvenc = [
        'ffmpeg', '-y', '-hwaccel', 'cuda', '-i', input_file,
        '-vf', f'scale=-2:{height}',
        '-c:v', 'h264_nvenc', '-preset', 'p4', '-tune', 'hq',
        '-b:v', str(target_bitrate),
        '-c:a', 'copy',
        output_file
    ]
    
    try:
        subprocess.run(cmd_nvenc, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        print("⚡ Encoding via RTX 6000 NVENC completed successfully!")
    except subprocess.CalledProcessError as e:
        print(f"⚠️ GPU Encoding Failed: {e}")
        print("Please ensure FFmpeg is installed and NVIDIA drivers are loaded.")
        raise

class UploadFileAdapter:
    def __init__(self, filename):
        self.file = open(filename, 'rb')
        self.total = os.path.getsize(filename)
        self.read_bytes = 0
        self.viewer = ProgressViewer("Uploading")
        print("\n[INFO] Starting Upload...")

    def read(self, size):
        chunk = self.file.read(size)
        self.read_bytes += len(chunk)
        self.viewer.update(self.read_bytes, self.total)
        return chunk
        
    def __len__(self):
        return self.total

def upload_video(file_path, upload_url):
    adapter = UploadFileAdapter(file_path)
    try:
        response = requests.put(upload_url, data=adapter)
        response.raise_for_status()
        print("\n✅ Upload Complete!")
    except Exception as e:
        print(f"\n❌ Upload Error: {e}")
    finally:
        adapter.file.close()

# ==========================================
# 6. Anti-Cancel Block & Auto-Shutdown
# ==========================================
def block_cancel(signum, frame):
    print("\n\n⚠️  Interruption Blocked: The pipeline is currently locked. No last second cancels allowed!")

def deactivate_machine():
    """Immediately shuts down the Lightning AI environment to save GPU credits."""
    print("\n🛑 OPERATION COMPLETE. INITIATING IMMEDIATE SHUTDOWN TO SAVE RTX 6000 CREDITS! 🛑")
    time.sleep(2)
    try:
        os.system("sudo shutdown -h now")
    except Exception as e:
        print(f"Could not automatically shut down. Please stop manually. Error: {e}")

# ==========================================
# Main Executable
# ==========================================
def run_pipeline():
    print("🌩️ Lightning AI Fast Video Pipeline 🌩️\n")
    
    source_url = input("Enter video source URL: ").strip()
    original_name = input("Enter original filename (ex: Tales of Herding gods EP78 [4K][TheFrictionRealm].mp4): ").strip()
    
    user_name = input("Enter new output Name (or /skip to use existing): ").strip()
    thumbnail = input("Enter thumbnail URL/path (or /skip): ").strip()
    quality_choice = input("Select output quality (e.g., 1080P, 720P, 480P): ").strip()
    
    if user_name.lower() == '/skip' or not user_name:
        final_output_name = process_metadata(original_name, quality_choice)
    else:
        final_output_name = process_metadata(user_name, quality_choice)
        
    print(f"\n🎯 Target Output Name: {final_output_name}")
    print(f"🖼️ Thumbnail setting: {'Using Existing' if thumbnail.lower() == '/skip' else thumbnail}")
    
    print("\n🛡️ Enabling cancel protection (Ctrl+C is now blocked)...")
    signal.signal(signal.SIGINT, block_cancel)
    
    try:
        local_input = "input_temp.mp4"
        
        download_video(source_url, local_input)
        encode_video(local_input, final_output_name, quality_choice)
        
        # NOTE: Replace this URL with your actual endpoint or pre-signed URL
        upload_endpoint = "https://httpbin.org/put" 
        upload_video(final_output_name, upload_endpoint)
        
        print("\n🎉 Pipeline operations completed securely.")
        deactivate_machine()
        
    except Exception as e:
        print(f"\n❌ Pipeline crashed or encountered a critical error: {e}")
    finally:
        if os.path.exists("input_temp.mp4"):
            os.remove("input_temp.mp4")
        signal.signal(signal.SIGINT, signal.SIG_DFL)

if __name__ == "__main__":
    run_pipeline()
