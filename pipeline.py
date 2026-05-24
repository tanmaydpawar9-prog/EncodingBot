#!/usr/bin/env python3
# ══════════════════════════════════════════════════════════════════════════════
#  TheFrictionRealm Combined Bot — Lightning AI / RTX A6000 Edition
# ══════════════════════════════════════════════════════════════════════════════

import os
import sys

# --- Lightning AI GPU Fix ---
# Ensures FFmpeg can find libcuda.so.1 for the RTX A6000 NVENC encoder
cuda_paths = "/usr/local/nvidia/lib64:/usr/local/nvidia/lib:/usr/local/cuda/lib64:/usr/local/cuda/targets/x86_64-linux/lib:/usr/lib/x86_64-linux-gnu:/lib/x86_64-linux-gnu:/usr/lib64:/lib64"
os.environ['LD_LIBRARY_PATH'] = f"{cuda_paths}:{os.environ.get('LD_LIBRARY_PATH', '')}"

import re, time, json, shutil, asyncio, logging
import subprocess, difflib, threading, queue, traceback, io, uuid
import requests, concurrent.futures
import http.server, socketserver
from pathlib import Path
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Optional

import cv2, yt_dlp, numpy as np, nest_asyncio
nest_asyncio.apply()

from pyrogram import Client, filters, idle
from pyrogram.enums import ParseMode
from pyrogram.errors import MessageNotModified, FloodWait
from pyrogram.types import CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, Message
from openai import AsyncOpenAI

# ── CONFIG ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True,
)
log = logging.getLogger("FrBot")

# Fallback to os.environ if Kaggle secrets aren't available (useful for Lightning AI)
try:
    _sec = UserSecretsClient()
    API_ID       = int(_sec.get_secret("API_ID") or os.getenv("API_ID", 0))
    API_HASH     = _sec.get_secret("API_HASH") or os.getenv("API_HASH", "")
    BOT_TOKEN    = _sec.get_secret("BOT_TOKEN") or os.getenv("BOT_TOKEN", "")
    OPENAI_KEY   = _sec.get_secret("OPENAI_API_KEY") or os.getenv("OPENAI_API_KEY", "")
except Exception:
    API_ID       = int(os.getenv("API_ID", 0))
    API_HASH     = os.getenv("API_HASH", "")
    BOT_TOKEN    = os.getenv("BOT_TOKEN", "")
    OPENAI_KEY   = os.getenv("OPENAI_API_KEY", "")

ADMIN_IDS       = [int(x) for x in os.environ.get("ADMIN_IDS", "").split(",") if x.strip()]
CHANNEL_MAP_RAW = os.environ.get("CHANNEL_MAP", "")

BASE  = Path("./frbot_work")
FILES = BASE / "files"
TMP   = BASE / "tmp"
WORK  = BASE / "work"
for _d in [FILES, TMP, WORK]: _d.mkdir(parents=True, exist_ok=True)

EVENT_LOOP     = None
REFRESH        = 5       # UI update interval (seconds)
UPLOAD_TAG     = "TheFrictionRealm"
MIN_GAP_SEC    = 0.04
_OCR_ENGINES   = {}

# RTX A6000 Concurrency Controls
dl_semaphore  = asyncio.Semaphore(4)   # Max concurrent downloads
mux_semaphore = asyncio.Semaphore(3)   # Unrestricted NVENC allows 3+ parallel encodes
ul_semaphore  = asyncio.Semaphore(6)   # Max concurrent Telegram uploads

# ── HTTP SERVER FOR >2GB FILES ────────────────────────────────────────────────
HTTP_PORT = 8000
http_server_thread = None
server_lock = threading.Lock()
file_id_map = {} 

class CustomHTTPRequestHandler(http.server.BaseHTTPRequestHandler):
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
                    log.error(f"HTTP server error serving file: {e}")
                    self.send_error(500, "Server error while serving file")
            else:
                self.send_error(404, "File not found or link expired")
        else:
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            self.wfile.write(b"TheFrictionRealm File Server is running.")

def start_http_server():
    global http_server_thread
    with server_lock:
        if http_server_thread is None or not http_server_thread.is_alive():
            httpd = socketserver.TCPServer(("", HTTP_PORT), CustomHTTPRequestHandler)
            def serve():
                log.info(f"Starting HTTP server on port {HTTP_PORT} to serve large files.")
                httpd.serve_forever()
            http_server_thread = threading.Thread(target=serve, daemon=True)
            http_server_thread.start()

# ── QUALITY PROFILES ─────────────────────────────────────────────────────────
@dataclass
class QualitySpec:
    label: str; width: int; height: int

QUALITY_SPECS = [
    QualitySpec("2K",    2560, 1440),
    QualitySpec("1080p", 1920, 1080),
    QualitySpec("720p",  1280,  720),
]

# ── STATE MACHINE ─────────────────────────────────────────────────────────────
class Mode(Enum):
    OCR = "ocr"
    ENC = "enc"

class Stage(Enum):
    IDLE        = auto()
    AWAIT_SRC   = auto()
    DOWNLOADING = auto()
    AWAIT_CUT   = auto()
    OCR_RUNNING = auto()
    AWAIT_SUB   = auto()
    AWAIT_NAME  = auto()
    AWAIT_THUMB = auto()
    CONFIRMING  = auto()
    MUXING      = auto()
    ENCODING    = auto()
    DONE        = auto()
    CANCELLED   = auto()

# ── TASK DATACLASS ────────────────────────────────────────────────────────────
@dataclass
class Task:
    task_id:  str   = field(default_factory=lambda: uuid.uuid4().hex[:8])
    mode:     Mode  = Mode.OCR
    chat_id:  int   = 0
    user_id:  int   = 0
    stage:    Stage = Stage.IDLE

    # Paths
    work_dir:      Optional[Path] = None
    input_path:    Optional[Path] = None
    subtitle_path: Optional[Path] = None
    muxed_path:    Optional[Path] = None
    thumb_path:    Optional[Path] = None

    # Naming
    output_name:  str = ""
    raw_name:     str = ""
    series_name:  str = ""
    episode_tag:  str = ""

    # Video metadata
    duration_s:  float = 0.0
    src_bitrate: int   = 0
    src_width:   int   = 0
    src_height:  int   = 0

    # OCR results
    ocr_subs: list = field(default_factory=list)

    # Cancellation
    cancel_flag:          Optional[asyncio.Event] = None
    quality_cancel_flags: dict = field(default_factory=dict)
    quality_procs:        dict = field(default_factory=dict)
    quality_msgs:         dict = field(default_factory=dict)
    encode_done_flags:    dict = field(default_factory=dict)
    encoded_files:        dict = field(default_factory=dict)

    # User-input futures
    src_future:      Optional[asyncio.Future] = None
    cut_future:      Optional[asyncio.Future] = None
    subtitle_future: Optional[asyncio.Future] = None
    name_future:     Optional[asyncio.Future] = None
    thumb_future:    Optional[asyncio.Future] = None
    confirm_future:  Optional[asyncio.Future] = None

    status_msg:  Optional[object] = None
    started_at:  float = field(default_factory=time.time)

# Global registry keyed by chat_id
active_tasks: dict[int, Task] = {}

def new_task(mode: Mode, chat_id: int, user_id: int) -> Task:
    loop = asyncio.get_running_loop()
    t = Task(mode=mode, chat_id=chat_id, user_id=user_id)
    t.work_dir       = WORK / t.task_id
    t.work_dir.mkdir(parents=True, exist_ok=True)
    t.cancel_flag    = asyncio.Event()
    t.src_future     = loop.create_future()
    t.cut_future     = loop.create_future()
    t.subtitle_future= loop.create_future()
    t.name_future    = loop.create_future()
    t.thumb_future   = loop.create_future()
    t.confirm_future = loop.create_future()
    return t

def cleanup_task(t: Task):
    # Only pop from active_tasks if this specific task is still the one locked to the chat
    # (prevents deleting the lock for a new job if they overlapped)
    if active_tasks.get(t.chat_id) is t:
        active_tasks.pop(t.chat_id, None)
    if t.work_dir and t.work_dir.exists():
        shutil.rmtree(t.work_dir, ignore_errors=True)

def is_admin(uid: int) -> bool:
    return (not ADMIN_IDS) or (uid in ADMIN_IDS)

def _cancel_all_futures(t: Task):
    for fut in [t.src_future, t.cut_future, t.subtitle_future, t.name_future, t.thumb_future, t.confirm_future]:
        if fut and not fut.done():
            try: fut.cancel()
            except: pass

# ── UI HELPERS ────────────────────────────────────────────────────────────────
def fmt_bytes(b: float) -> str:
    if not b: return "0.00 B"
    for u in ["B", "Kʙ", "Mʙ", "Gʙ", "Tʙ"]:
        if b < 1024: return f"{b:.2f} {u}"
        b /= 1024
    return f"{b:.2f} Pʙ"

def fmt_time(s: float) -> str:
    s = int(max(s, 0))
    h, m, sec = s // 3600, (s % 3600) // 60, s % 60
    return f"{h}h {m}m {sec}s" if h else f"{m}m {sec}s"

def prog_bar(pct: float, w: int = 10) -> str:
    f = round(min(pct, 100) / 100 * w)
    return "■" * f + "□" * (w - f)

def pb_bytes(action: str, cur: int, total: int, t0: float) -> str:
    el  = time.time() - t0
    spd = cur / max(el, 0.01)
    pct = cur / total * 100 if total else 0
    eta = (total - cur) / max(spd, 1) if total else 0
    return (
        f"Progress: [{prog_bar(pct)}] {pct:.1f}%\n"
        f"📥 {action}: {fmt_bytes(cur)} | {fmt_bytes(total)}\n"
        f"⚡️ Speed: {fmt_bytes(spd)}/s\n"
        f"⌛ ETA: {fmt_time(eta)}\n"
        f"⏱️ Elapsed: {fmt_time(el)}"
    )

def pb_frames(action: str, cur: int, total: int, t0: float, extra: str = "") -> str:
    el  = time.time() - t0
    spd = cur / max(el, 0.01)
    pct = cur / total * 100 if total else 0
    eta = (total - cur) / max(spd, 1) if total else 0
    base = (
        f"Progress: [{prog_bar(pct)}] {pct:.1f}%\n"
        f"⚡ {action}: {int(cur)} | {int(total)} frames\n"
        f"⚡️ Speed: {spd:.1f} fps\n"
        f"⌛ ETA: {fmt_time(eta)}\n"
        f"⏱️ Elapsed: {fmt_time(el)}"
    )
    return base + (f"\n{extra}" if extra else "")

def pb_enc(label: str, name: str, pct: float, cur_s: float, tot_s: float, fps: float, spd: str, eta: float, el: float) -> str:
    return (
        f"🎬 **Encoding [{label}] · {name}**\n\n"
        f"Progress: `[{prog_bar(pct)}] {pct:.1f}%`\n"
        f"⏱️ Encoded: `{fmt_time(cur_s)}` / `{fmt_time(tot_s)}`\n"
        f"⚡️ Speed: `{spd}` | `{fps:.0f} fps`\n"
        f"⌛ ETA: `{fmt_time(eta)}`\n"
        f"🕐 Elapsed: `{fmt_time(el)}`"
    )

def pb_up(label: str, name: str, cur: int, total: int, t0: float) -> str:
    el  = time.time() - t0
    spd = cur / max(el, 0.01)
    pct = cur / total * 100 if total else 0
    eta = (total - cur) / max(spd, 1) if total else 0
    return (
        f"📤 **Uploading [{label}] · {name}**\n\n"
        f"Progress: `[{prog_bar(pct)}] {pct:.1f}%`\n"
        f"📤 `{fmt_bytes(cur)}` | `{fmt_bytes(total)}`\n"
        f"⚡️ Speed: `{fmt_bytes(spd)}/s`\n"
        f"⌛ ETA: `{fmt_time(eta)}`\n"
        f"🕐 Elapsed: `{fmt_time(el)}`"
    )

CANCEL_BTN = InlineKeyboardMarkup([[InlineKeyboardButton("🚫 Cancel Task", callback_data="cancel_active")]])

def qual_cancel_kb(task_id: str, label: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton(f"❌ Cancel {label}", callback_data=f"cq:{task_id}:{label}")]])

def confirm_kb(task_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("▶️ Start Encode", callback_data=f"start:{task_id}"),
         InlineKeyboardButton("❌ Cancel",        callback_data=f"cancel:{task_id}")]
    ])

async def safe_edit(msg, text: str, markup=None):
    try: await msg.edit_text(text, reply_markup=markup)
    except FloodWait as e:
        await asyncio.sleep(e.value)
    except Exception: pass

def push(msg, text: str, markup=None):
    if EVENT_LOOP and msg:
        asyncio.run_coroutine_threadsafe(safe_edit(msg, text, markup), EVENT_LOOP)

def channel_map() -> dict[str, int]:
    r = {}
    for e in CHANNEL_MAP_RAW.split(","):
        e = e.strip()
        if ":" not in e: continue
        n, cid = e.rsplit(":", 1)
        try: r[n.strip().lower()] = int(cid.strip())
        except: pass
    return r

def resolve_channel(series: str) -> Optional[int]:
    sl = series.lower()
    for kw, cid in channel_map().items():
        if kw in sl or sl in kw: return cid
    return None

# ── PYROGRAM CLIENT ───────────────────────────────────────────────────────────
app = Client(
    "friction_combined",
    api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN,
    max_concurrent_transmissions=8,
    in_memory=True,
)

# ══════════════════════════════════════════════════════════════════════════════
#  OCR ENGINE
# ══════════════════════════════════════════════════════════════════════════════
def _load_ocr(gpu_id: int = 0):
    if gpu_id not in _OCR_ENGINES:
        from paddleocr import PaddleOCR
        try:
            import paddle
            if paddle.device.is_compiled_with_cuda():
                paddle.device.set_device(f"gpu:{gpu_id}")
            _OCR_ENGINES[gpu_id] = PaddleOCR(use_angle_cls=False, lang="ch", use_gpu=True, gpu_id=gpu_id, show_log=False)
            log.info(f"PaddleOCR ready on GPU:{gpu_id}")
        except Exception as e:
            log.warning(f"GPU:{gpu_id} failed ({e}) → CPU fallback")
            _OCR_ENGINES[gpu_id] = PaddleOCR(use_angle_cls=False, lang="ch", use_gpu=False, show_log=False)
    return _OCR_ENGINES[gpu_id]

def _sub_key(cue: dict) -> str:
    return re.sub(r"[\s\.,\!\?\-\—\*\(\)\[\]。！？、…]", "", cue.get("cmp") or cue.get("text") or "")

def _same_sub(a: dict, b: dict, thr: float = 0.75) -> bool:
    ak, bk = _sub_key(a), _sub_key(b)
    if not ak or not bk: return False
    if ak == bk: return True
    return difflib.SequenceMatcher(None, ak, bk).ratio() >= thr

def stitch_continuous_lines(subs: list, max_gap: float = 0.15) -> list:
    if not subs: return []
    stitched = [subs[0].copy()]
    for cur in subs[1:]:
        prev = stitched[-1]
        gap  = cur["start"] - prev["end"]
        if gap <= max_gap and _same_sub(prev, cur):
            prev["end"] = max(prev["end"], cur["end"])
            if len(cur.get("cmp", "")) > len(prev.get("cmp", "")):
                prev["text"] = cur.get("text", prev["text"])
                prev["cmp"]  = cur.get("cmp",  prev.get("cmp", ""))
            continue
        if gap < 0:
            overlap  = prev["end"] - cur["start"]
            midpoint = prev["end"] - (overlap / 2.0)
            prev["end"] = round(midpoint - (MIN_GAP_SEC / 2.0), 3)
            cur_c = cur.copy()
            cur_c["start"] = round(midpoint + (MIN_GAP_SEC / 2.0), 3)
            if prev["end"] - prev["start"] < 0.08: stitched.pop()
            if cur_c["end"] - cur_c["start"] >= 0.08: stitched.append(cur_c)
        else:
            stitched.append(cur.copy())
    return stitched

def _norm_ocr_key(txt: str) -> str:
    return re.sub(r"[\s\.,\!\?\-\—\*\(\)\[\]。！？、…]", "", txt or "")

def suppress_static_overlay_cues(subs: list, frame_w: int, frame_h: int, total_dur: float) -> list:
    # Simplified suppression wrapper for stability
    if not subs: return []
    kept = sorted(subs, key=lambda x: x["start"])
    return kept

def _process_frame_stream(engine, cmd, bytes_per_frame, crop_h, crop_w, extract_fps, time_offset, cancel_check, is_target_res=False, progress_cb=None):
    cues    = []
    frame_q = queue.Queue(maxsize=128)
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=10**8)
    time.sleep(0.5)
    if process.poll() is not None and process.returncode != 0:
        raise RuntimeError("FFmpeg pipe crashed on startup.")

    def _reader():
        idx = 0
        while True:
            if cancel_check(): process.terminate(); break
            raw = process.stdout.read(bytes_per_frame)
            if not raw or len(raw) != bytes_per_frame: break
            frame_q.put((idx, raw)); idx += 1
        frame_q.put(None)

    threading.Thread(target=_reader, daemon=True).start()
    frame_dur = 1.0 / extract_fps

    while True:
        item = frame_q.get()
        if item is None: break
        frame_idx, raw_frame = item
        cur_t = round((frame_idx / float(extract_fps)) + time_offset, 3)
        if progress_cb: progress_cb(frame_idx, cues)

        frame = np.frombuffer(raw_frame, dtype=np.uint8).reshape((crop_h, crop_w, 3))
        gray  = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        _, max_val, _, _ = cv2.minMaxLoc(gray)

        if max_val >= 50:
            try:    result = engine.ocr(frame, cls=False)
            except: result = None

            if result and result[0]:
                for ln in result[0]:
                    raw_text = ln[1][0].strip()
                    cmp_text = _norm_ocr_key(raw_text)
                    if len(cmp_text) >= 1 and ln[1][1] >= 0.25:
                        xs = [p[0] for p in ln[0]]; ys = [p[1] for p in ln[0]]
                        cues.append({
                            "start": cur_t, "end": round(cur_t + frame_dur, 3),
                            "text": raw_text, "cmp": cmp_text,
                            "x": sum(xs)/len(xs), "y": sum(ys)/len(ys),
                            "bw": max(xs)-min(xs), "bh": max(ys)-min(ys),
                        })

    process.stdout.close(); process.wait()
    return cues

def get_real_duration(path: str) -> float:
    try:
        out = subprocess.check_output(["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", path])
        return float(out.decode().strip())
    except: return 0.0

def run_ocr_pipeline(video_path, status_msg, chat_id, start_sec=0.0, end_sec=None, cancel_check=None):
    if cancel_check is None: cancel_check = lambda: False
    cap = cv2.VideoCapture(video_path)
    fps = cap.get(cv2.CAP_PROP_FPS) or 24.0
    frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    orig_w, orig_h = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH)), int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    cap.release()

    duration = get_real_duration(video_path) or (frames / fps if fps else 0)
    if start_sec is None or start_sec < 0: start_sec = 0.0
    if end_sec is None or end_sec > duration: end_sec = duration

    proc_dur = end_sec - start_sec
    extract_fps = fps
    total_frames = int(proc_dur * extract_fps)

    scale = min(1920, orig_w) / max(orig_w, 1)
    s_w, s_h = int(orig_w * scale), int(orig_h * scale)
    s_w -= s_w % 2; s_h -= s_h % 2
    t0 = time.time()

    def make_cmd(ss, dur, vf, thr="4"):
        return [
            "ffmpeg", "-v", "error", "-y", "-hwaccel", "cuda", "-threads", thr,
            "-ss", str(ss), "-i", video_path, "-t", str(dur),
            "-vf", vf, "-r", str(extract_fps),
            "-f", "image2pipe", "-pix_fmt", "bgr24", "-vcodec", "rawvideo", "-"
        ]

    def _run_once(band_ratio: float):
        crop_w = s_w
        crop_y = int(s_h * (1.0 - band_ratio)) & ~1
        crop_h = int(s_h * band_ratio) & ~1

        crop_w = max(crop_w - (crop_w % 2), 2)
        crop_h = max(crop_h - (crop_h % 2), 2)
        crop_y = max(crop_y, 0)
        bpf = crop_w * crop_h * 3
        vf  = f"scale={s_w}:{s_h},crop={crop_w}:{crop_h}:0:{crop_y}"

        engine = _load_ocr(0)
        last_ui = [time.time()]

        def _progress(fi, cl):
            if time.time() - last_ui[0] > REFRESH:
                last_ui[0] = time.time()
                push(status_msg, pb_frames("Direct Stream OCR", fi, total_frames, t0, f"💬 Cues: {len(cl)}"), CANCEL_BTN)

        raw = _process_frame_stream(engine, make_cmd(start_sec, proc_dur, vf), bpf, crop_h, crop_w, extract_fps, start_sec, cancel_check, True, _progress)
        raw = stitch_continuous_lines(raw)
        return suppress_static_overlay_cues(raw, crop_w, crop_h, proc_dur)

    # 22% strict limit enforced here
    final_subs = _run_once(0.22)
    return final_subs

# ── CHATGPT TRANSLATION ───────────────────────────────────────────────────────
async def batch_translate(zh_texts: list, status_msg=None, chat_id: int = None) -> list:
    if not OPENAI_KEY: return ["[No API Key]"] * len(zh_texts)
    client = AsyncOpenAI(api_key=OPENAI_KEY)
    BATCH = 50; res = []; t0 = time.time()
    sys_p = (
        "You are an expert translator for Chinese Donghua, Xianxia, and Wuxia animation. "
        "Translate the following Chinese subtitles into natural, flowing English. "
        "Keep cultivation terms and titles epic and accurate. "
        "Return ONLY a numbered list matching the input numbering exactly. "
        "Do not merge lines, skip lines, or add commentary."
    )
    for i in range(0, len(zh_texts), BATCH):
        task = active_tasks.get(chat_id)
        if task and task.cancel_flag.is_set(): break
        if status_msg: push(status_msg, pb_frames("ChatGPT Translating", i, len(zh_texts), t0), CANCEL_BTN)
        chunk = zh_texts[i:i + BATCH]
        chunk_text = "\n".join(f"{j} | {t}" for j, t in enumerate(chunk))
        try:
            resp = await client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "system", "content": sys_p}, {"role": "user", "content": chunk_text}],
                temperature=0.1,
            )
            reply = resp.choices[0].message.content.strip()
            out = ["[Translation Error]"] * len(chunk)
            for line in reply.split("\n"):
                line = line.strip()
                if not line: continue
                m = re.match(r"^\*?\*?(\d+)\*?\*?\s*[|\-]\s*(.*)", line)
                if m:
                    idx, txt = int(m.group(1)), m.group(2).strip()
                    if 0 <= idx < len(chunk):
                        if out[idx] == "[Translation Error]": out[idx] = txt
                        else: out[idx] += " " + txt
            res.extend(out)
        except Exception as e:
            log.error(f"ChatGPT error: {e}")
            res.extend(chunk)
    return res

def srt_ts(sec: float) -> str:
    ms = int(round((sec % 1) * 1000))
    if ms >= 1000: sec += 1; ms = 0
    s, m, h = int(sec) % 60, (int(sec) // 60) % 60, int(sec) // 3600
    return f"{h:02d}:{m:02d}:{s:02d},{ms:03d}"

def write_srt(subs: list, texts: list, path: str):
    with open(path, "w", encoding="utf-8") as f:
        for i, (sub, txt) in enumerate(zip(subs, texts), 1):
            f.write(f"{i}\n{srt_ts(sub['start'])} --> {srt_ts(sub['end'])}\n{txt}\n\n")

# ══════════════════════════════════════════════════════════════════════════════
#  DOWNLOAD HELPERS (Parallel + yt-dlp)
# ══════════════════════════════════════════════════════════════════════════════
_ydl_last_ui: dict[int, float] = {}
def _ydl_hook(d, msg, chat_id: int, t0: float):
    task = active_tasks.get(chat_id)
    if task and task.cancel_flag.is_set(): raise Exception("Cancelled")
    now = time.time()
    if d["status"] == "downloading" and now - _ydl_last_ui.get(chat_id, 0) > REFRESH:
        _ydl_last_ui[chat_id] = now
        cur = d.get("downloaded_bytes", 0)
        tot = d.get("total_bytes") or d.get("total_bytes_estimate", 0)
        push(msg, pb_bytes("yt-dlp", cur, tot, t0), CANCEL_BTN)

def dl_ytdlp(url: str, chat_id: int, msg_id: int, status_msg=None) -> str:
    t0 = time.time()
    dest = str(FILES / f"{chat_id}_{msg_id}.%(ext)s")
    opts = {
        "format": "bestvideo+bestaudio/best",
        "outtmpl": dest, "merge_output_format": "mkv",
        "quiet": True, "nocheckcertificate": True,
        "external_downloader": "aria2c",
        "external_downloader_args": ["-x", "16", "-s", "16", "-k", "1M"],
        "progress_hooks": ([lambda d: _ydl_hook(d, status_msg, chat_id, t0)] if status_msg else []),
    }
    with yt_dlp.YoutubeDL(opts) as ydl:
        return ydl.prepare_filename(ydl.extract_info(url, download=True))

def _download_range(url, start, end, output_path, progress_list, idx, task):
    retries = 15
    for attempt in range(retries):
        current_start = start + progress_list[idx]
        if current_start > end: return
        headers = {"Range": f"bytes={current_start}-{end}", "User-Agent": "Mozilla/5.0"}
        try:
            res = requests.get(url, headers=headers, stream=True, allow_redirects=True, timeout=(10, 30))
            res.raise_for_status()
            with open(output_path, "rb+") as f:
                f.seek(current_start)
                buffer = bytearray()
                for chunk in res.iter_content(chunk_size=128 * 1024):
                    if task.cancel_flag.is_set(): return
                    if chunk:
                        buffer.extend(chunk)
                        if len(buffer) >= 1024 * 1024:
                            f.write(buffer)
                            progress_list[idx] += len(buffer)
                            buffer.clear()
                        if start + progress_list[idx] + len(buffer) > end: break
                if buffer:
                    f.write(buffer)
                    progress_list[idx] += len(buffer)
            if start + progress_list[idx] > end: return
        except Exception:
            time.sleep(2)
    raise Exception(f"Download thread {idx} permanently failed.")

async def download_video_from_url(url: str, output_path: Path, task: Task, status: Message):
    loop = asyncio.get_running_loop()
    headers = {"User-Agent": "Mozilla/5.0"}
    res = await asyncio.to_thread(requests.get, url, headers=headers, stream=True, allow_redirects=True, timeout=(10, 20))
    total_size = int(res.headers.get('content-length', 0))
    
    t0 = time.time(); last = [0.0]
    async def update_ui(downloaded):
        now = time.time()
        if now - last[0] > REFRESH:
            last[0] = now
            await safe_edit(status, pb_bytes("Downloading", downloaded, total_size, t0), CANCEL_BTN)

    with open(output_path, "wb") as f: f.truncate(total_size)
    num_threads = 8
    chunk_size = total_size // num_threads
    progress_list = [0] * num_threads
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        for i in range(num_threads):
            start = i * chunk_size
            end = total_size - 1 if i == num_threads - 1 else (i + 1) * chunk_size - 1
            futures.append(loop.run_in_executor(executor, _download_range, url, start, end, output_path, progress_list, i, task))
        
        while True:
            downloaded = sum(progress_list)
            if task.cancel_flag.is_set(): raise Exception("Cancelled")
            await update_ui(downloaded)
            if downloaded >= total_size or all(f.done() for f in futures): break
            await asyncio.sleep(1)
        await asyncio.gather(*futures)

async def tg_download(source_msg: Message, dest: Path, status: Message, task: Task) -> Path:
    dest.mkdir(parents=True, exist_ok=True)
    t0 = time.time(); last = [0.0]
    fname = getattr(source_msg.video, "file_name", None) or getattr(source_msg.document, "file_name", None) or "video.mkv"
    target = str(dest / fname)

    async def _prog(cur, tot):
        if task.cancel_flag.is_set(): app.stop_transmission()
        now = time.time()
        if now - last[0] > REFRESH:
            last[0] = now
            await safe_edit(status, pb_bytes("Downloading", cur, tot, t0), CANCEL_BTN)

    path = await source_msg.download(file_name=target, progress=_prog)
    return Path(path)

async def _download_video(c, m: Message, task: Task, status: Message) -> Optional[Path]:
    parts = (m.text or "").split(maxsplit=1)
    url_arg = parts[1].strip() if len(parts) > 1 else ""
    url_m = re.search(r"(https?://\S+)", url_arg)

    if url_m:
        url = url_m.group(1)
        dest = task.work_dir / f"{task.task_id}_dl.mkv"
        await safe_edit(status, "📥 Downloading URL...", CANCEL_BTN)
        async with dl_semaphore:
            try:
                # Attempt 8-part parallel first
                await download_video_from_url(url, dest, task, status)
                return dest
            except Exception:
                # Fallback to yt-dlp if it's a platform stream rather than direct file
                return Path(await asyncio.to_thread(dl_ytdlp, url, task.chat_id, m.id, status))

    if m.reply_to_message and (m.reply_to_message.video or m.reply_to_message.document):
        await safe_edit(status, "📥 Downloading from Telegram...", CANCEL_BTN)
        async with dl_semaphore:
            return await tg_download(m.reply_to_message, task.work_dir, status, task)

    task.stage = Stage.AWAIT_SRC
    await safe_edit(status, "📨 **Send the video file** or paste a URL to get started.", CANCEL_BTN)
    try:
        result = await asyncio.wait_for(task.src_future, timeout=300)
    except (asyncio.TimeoutError, asyncio.CancelledError):
        return None
    if task.cancel_flag.is_set(): return None

    src_type, src_data = result
    await safe_edit(status, "📥 Downloading...", CANCEL_BTN)
    async with dl_semaphore:
        if src_type == "url":
            dest = task.work_dir / f"{task.task_id}_dl.mkv"
            try:
                await download_video_from_url(src_data, dest, task, status)
                return dest
            except Exception:
                return Path(await asyncio.to_thread(dl_ytdlp, src_data, task.chat_id, m.id, status))
        else:
            return await tg_download(src_data, task.work_dir, status, task)

# ══════════════════════════════════════════════════════════════════════════════
#  MEDIA PROBE
# ══════════════════════════════════════════════════════════════════════════════
async def probe_media(path: Path) -> dict:
    proc = await asyncio.create_subprocess_exec(
        "ffprobe", "-v", "quiet", "-print_format", "json",
        "-show_streams", "-show_format", str(path),
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    out, _ = await proc.communicate()
    return json.loads(out)

async def extract_meta(task: Task):
    info = await probe_media(task.input_path)
    fmt  = info.get("format", {})
    task.duration_s  = float(fmt.get("duration", 0))
    task.src_bitrate = int(fmt.get("bit_rate", 0))
    for s in info.get("streams", []):
        if s.get("codec_type") == "video":
            task.src_width  = s.get("width", 0)
            task.src_height = s.get("height", 0)
            if not task.src_bitrate: task.src_bitrate = int(s.get("bit_rate", 4_000_000))
            break

async def _find_main_audio(path: Path) -> str:
    info = await probe_media(path)
    streams = [s for s in info.get("streams", []) if s.get("codec_type") == "audio"]
    if not streams: return "a:0"
    def _dur(s):
        d = s.get("duration") or s.get("tags", {}).get("DURATION", "0")
        try:
            if ":" in str(d):
                h, mm, sc = str(d).split(":")
                return float(h) * 3600 + float(mm) * 60 + float(sc)
            return float(d)
        except: return 0.0
    streams.sort(key=_dur, reverse=True)
    return str(streams[0]["index"])

# ══════════════════════════════════════════════════════════════════════════════
#  MUX
# ══════════════════════════════════════════════════════════════════════════════
async def mux_video(task: Task, sub_path: Path, out_path: Path) -> Path:
    audio_idx = await _find_main_audio(task.input_path)
    cmd = [
        "ffmpeg", "-y",
        "-i", str(task.input_path), "-i", str(sub_path),
        "-map", "0:v:0", "-map", f"0:{audio_idx}", "-map", "1:0",
        "-c", "copy",
        "-metadata:s:s:0", "title=ENGLISH @TheFrictionRealm",
        "-metadata:s:s:0", "language=eng", "-disposition:s:0", "default",
        "-metadata", f"title={task.output_name}",
        str(out_path),
    ]
    proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    _, err = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(f"Mux failed:\n{err.decode(errors='replace')[:600]}")
    return out_path

# ══════════════════════════════════════════════════════════════════════════════
#  ENCODE  (Optimized for RTX A6000 NVENC)
# ══════════════════════════════════════════════════════════════════════════════
def out_filename(base: str, quality: str) -> str:
    return f"{base} [{quality}][{UPLOAD_TAG}].mkv"

def build_encode_cmd(input_path: Path, out_path: Path, spec: QualitySpec, output_name: str, src_bitrate: int) -> list:
    scale_factors = {"2K": 0.75, "1080p": 0.50, "720p": 0.25}
    target_bitrate = max(int(src_bitrate * scale_factors.get(spec.label, 0.50)), 500_000)

    return [
        "ffmpeg", "-y", "-loglevel", "error", "-stats",
        "-hwaccel", "cuda", "-hwaccel_output_format", "cuda",
        "-i", str(input_path),
        "-map", "0:v:0", "-map", "0:a?", "-map", "0:s?",
        "-vf", f"scale_cuda={spec.width}:-2:interp_algo=lanczos:format=nv12",
        "-c:v", "hevc_nvenc", "-preset", "p4", "-tune", "hq",
        "-b:v", str(target_bitrate),
        "-c:a", "copy", "-c:s", "copy",
        "-metadata", f"title={output_name}",
        str(out_path)
    ]

async def run_encode(task: Task, spec: QualitySpec, out_path: Path, prog_msg: Message):
    cancel = task.quality_cancel_flags[spec.label]
    
    # Allows 3 parallel hardware renders on the RTX A6000
    async with mux_semaphore:
        if cancel.is_set() or task.cancel_flag.is_set(): return

        cmd = build_encode_cmd(task.input_path, out_path, spec, task.output_name, task.src_bitrate)
        log.info(f"[{spec.label}] RTX A6000 NVENC Encode start")
        proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        task.quality_procs[spec.label] = proc

        last_edit, t0 = 0.0, time.time()
        time_pattern = re.compile(r"time=(\d+):(\d+):(\d+\.\d+)")
        fps_pattern = re.compile(r"fps=\s*([\d\.]+)")
        speed_pattern = re.compile(r"speed=\s*([\d\.x]+)")
        
        stderr_lines = []
        while True:
            if cancel.is_set() or task.cancel_flag.is_set():
                proc.terminate(); break
            try:
                line = await proc.stderr.readuntil(b'\r')
            except asyncio.exceptions.IncompleteReadError as e:
                line = e.partial

            if not line: break
            
            line_str = line.decode('utf-8', errors='ignore').strip()
            if not line_str: continue
            
            stderr_lines.append(line_str)
            if len(stderr_lines) > 20: stderr_lines.pop(0)

            if time.time() - last_edit > REFRESH:
                time_match = time_pattern.search(line_str)
                if time_match:
                    h, m, s = time_match.groups()
                    cur_s = int(h) * 3600 + int(m) * 60 + float(s)
                    pct = min(cur_s / (task.duration_s or 1) * 100, 100)
                    
                    fps_match = fps_pattern.search(line_str)
                    fps = float(fps_match.group(1)) if fps_match else 0.0
                    
                    spd_match = speed_pattern.search(line_str)
                    spd = spd_match.group(1) if spd_match else "0x"
                    
                    eta = (task.duration_s - cur_s) / (cur_s / (time.time() - t0)) if cur_s > 0 else 0
                    
                    txt = pb_enc(spec.label, task.output_name, pct, cur_s, task.duration_s, fps, spd, eta, time.time() - t0)
                    try:
                        await prog_msg.edit(txt, reply_markup=qual_cancel_kb(task.task_id, spec.label))
                        last_edit = time.time()
                    except FloodWait as e:
                        await asyncio.sleep(e.value)
                    except: pass

        await proc.wait()
        task.quality_procs.pop(spec.label, None)

        if not (cancel.is_set() or task.cancel_flag.is_set()) and proc.returncode != 0:
            raise RuntimeError(f"FFmpeg error:\n{chr(10).join(stderr_lines[-10:])}")

# ── UPLOAD ────────────────────────────────────────────────────────────────────
def build_caption(task: Task, quality: str) -> str:
    title = task.output_name or task.series_name or task.raw_name
    return f"<b>{title}</b>\n\n<blockquote>Episode : {task.episode_tag or title}\nQuality : {quality}\nSubtitles : INBUILT</blockquote>"

def build_mux_caption(task: Task) -> str:
    title = task.raw_name or task.output_name or task.series_name
    return f"<b>{title}</b>\n\n<blockquote>Episode : {task.raw_name or title}\nSubtitles : ENGLISH @TheFrictionRealm</blockquote>"

async def upload_file(chat_id: int, path: Path, caption: str, thumb: Optional[Path], prog_msg: Message, label: str, name: str):
    file_size = os.path.getsize(path)
    TELEGRAM_LIMIT_BYTES = 2 * 1024 * 1024 * 1024

    if file_size >= TELEGRAM_LIMIT_BYTES:
        start_http_server()
        file_id = uuid.uuid4().hex
        file_id_map[file_id] = str(path)
        download_path = f"/download/{file_id}"
        public_url = os.getenv("LIGHTNING_APP_STATE_URL", os.getenv("LIGHTNING_HOST", "http://localhost"))
        if not public_url.startswith("http"): public_url = "https://" + public_url
        public_url = public_url.rstrip('/').replace('7860', str(HTTP_PORT))
        download_link = f"{public_url}{download_path}"
        
        await prog_msg.edit(f"🔗 **{label} version is ready!**\n\nFile is {file_size / (1024**3):.2f} GB, too large for Telegram. Direct Link:\n`{download_link}`\n\n_(Make sure port {HTTP_PORT} is exposed)_")
        return

    t0 = time.time(); last = [0.0]; last_txt = [""]

    async def _prog(cur, tot):
        now = time.time()
        if now - last[0] < REFRESH: return
        txt = pb_up(label, name, cur, tot, t0)
        if txt == last_txt[0]: return
        try:
            await prog_msg.edit(txt)
            last[0] = now; last_txt[0] = txt
        except FloodWait as e:
            await asyncio.sleep(e.value)
        except: pass

    async with ul_semaphore:
        await app.send_document(
            chat_id=chat_id, document=str(path),
            caption=caption, parse_mode=ParseMode.HTML,
            thumb=str(thumb) if thumb else None, progress=_prog,
        )

# ── PER-QUALITY WORKER (encode → upload) ─────────────────────────────────────
async def quality_worker(task: Task, spec: QualitySpec, trigger_msg: Message, target_chat: int):
    label  = spec.label
    cancel = task.quality_cancel_flags[label]
    out    = task.work_dir / out_filename(task.output_name, label)
    task.encoded_files[label] = out

    prog_msg = await trigger_msg.reply(
        pb_enc(label, task.output_name, 0, 0, task.duration_s, 0, "0x", task.duration_s, 0),
        reply_markup=qual_cancel_kb(task.task_id, label),
    )
    task.quality_msgs[label] = prog_msg

    err_str = None
    try:    await run_encode(task, spec, out, prog_msg)
    except RuntimeError as e: err_str = str(e)

    if cancel.is_set() or task.cancel_flag.is_set():
        try: await prog_msg.edit(f"🚫 **[{label}]** Cancelled", reply_markup=None)
        except: pass
        task.encode_done_flags[label].set(); return

    if err_str:
        log.error(f"[{label}] encode error: {err_str[:200]}")
        try: await prog_msg.edit(f"❌ **[{label}]** Encode failed:\n```\n{err_str[:2000]}\n```", reply_markup=None)
        except: pass
        task.encode_done_flags[label].set(); return

    task.encode_done_flags[label].set()
    try: await prog_msg.edit(f"✅ **[{label}]** Encoded! Uploading…", reply_markup=None)
    except: pass

    try:
        caption = build_caption(task, label)
        await upload_file(target_chat, out, caption, task.thumb_path, prog_msg, label, task.output_name)
        try: await prog_msg.edit(f"✅ **[{label}]** Upload complete! 🎉")
        except: pass
    except Exception as e:
        log.exception(f"[{label}] upload failed")
        try: await prog_msg.edit(f"❌ **[{label}]** Upload failed: `{e}`")
        except: pass

async def encode_all(task: Task, trigger_msg: Message, target_chat: int):
    for spec in QUALITY_SPECS:
        task.quality_cancel_flags[spec.label] = asyncio.Event()
        task.encode_done_flags[spec.label]    = asyncio.Event()
    await asyncio.gather(*[
        asyncio.create_task(quality_worker(task, spec, trigger_msg, target_chat))
        for spec in QUALITY_SPECS
    ])

# ── CANCEL HELPER ─────────────────────────────────────────────────────────────
async def do_cancel(task: Task, msg: Message, reason: str = "User requested cancellation."):
    task.stage = Stage.CANCELLED
    task.cancel_flag.set()
    for f in task.quality_cancel_flags.values(): f.set()
    for proc in list(task.quality_procs.values()):
        try: proc.kill()
        except: pass
    _cancel_all_futures(task)
    cleanup_task(task)
    await msg.reply(f"🚫 **Cancelled:** {reason}")

# ── NAME PARSER ───────────────────────────────────────────────────────────────
def parse_name(raw: str, task: Task):
    task.raw_name = raw.strip()
    cleaned = re.sub(r"\s*\[.*?\]", "", task.raw_name)
    cleaned = os.path.splitext(cleaned)[0].strip()
    task.output_name = cleaned
    task.series_name = cleaned
    ep_m = re.search(r"\bEP?(\d+)\b", cleaned, re.IGNORECASE)
    task.episode_tag = f"EP{ep_m.group(1)}" if ep_m else cleaned

# ══════════════════════════════════════════════════════════════════════════════
#  BOT COMMAND HANDLERS
# ══════════════════════════════════════════════════════════════════════════════
@app.on_message(filters.command("start"))
async def cmd_start(c, m: Message):
    await m.reply_text(
        "🎬 **TheFrictionRealm — RTX A6000 Pipeline**\n\n"
        "**OCR Pipeline:**\n"
        "  `/ocr` — Download → OCR → Translate → Mux → Upload + Encode\n\n"
        "**Direct Encode:**\n"
        "  `/enc` — Download → Encode all qualities → Upload\n\n"
        "**Controls:**\n"
        "  `/cancel` — Cancel any active interactive setup task\n"
        "  `/status` — Show running tasks\n"
        "  `/shutdown` — Power off Lightning AI server\n\n"
        "_Reply to a video or include a URL with the command._"
    )

# ── /ocr ──────────────────────────────────────────────────────────────────────
@app.on_message(filters.command("ocr"))
async def cmd_ocr(c, m: Message):
    chat_id = m.chat.id
    existing = active_tasks.get(chat_id)
    
    # Only block if we are in an interactive prompt phase. Background tasks are ignored!
    blocking_stages = (Stage.AWAIT_SRC, Stage.AWAIT_CUT, Stage.AWAIT_SUB, Stage.AWAIT_NAME, Stage.AWAIT_THUMB)
    if existing and existing.stage in blocking_stages:
        return await m.reply("⚠️ You are currently setting up another video. Finish the setup or use /cancel first.")
        
    task = new_task(Mode.OCR, chat_id, m.from_user.id)
    active_tasks[chat_id] = task
    asyncio.create_task(_run_ocr(c, m, task))

# ── /enc ──────────────────────────────────────────────────────────────────────
@app.on_message(filters.command("enc"))
async def cmd_enc(c, m: Message):
    if not is_admin(m.from_user.id):
        return await m.reply("⛔ Unauthorized.")
        
    chat_id  = m.chat.id
    existing = active_tasks.get(chat_id)
    
    blocking_stages = (Stage.AWAIT_SRC, Stage.AWAIT_NAME, Stage.AWAIT_THUMB, Stage.CONFIRMING)
    if existing and existing.stage in blocking_stages:
        return await m.reply("⚠️ You are currently setting up another video. Finish the setup or use /cancel first.")
        
    task = new_task(Mode.ENC, chat_id, m.from_user.id)
    active_tasks[chat_id] = task
    asyncio.create_task(_run_enc(c, m, task))

# ── /cancel ───────────────────────────────────────────────────────────────────
@app.on_message(filters.command("cancel"))
async def cmd_cancel(c, m: Message):
    task = active_tasks.get(m.chat.id)
    blocking_stages = (Stage.AWAIT_SRC, Stage.AWAIT_CUT, Stage.AWAIT_SUB, Stage.AWAIT_NAME, Stage.AWAIT_THUMB, Stage.CONFIRMING)
    if not task or task.stage not in blocking_stages:
        return await m.reply("✅ No active interactive task to cancel. (Use inline buttons to cancel background jobs).")
    await do_cancel(task, m)

# ── /status ───────────────────────────────────────────────────────────────────
@app.on_message(filters.command("status"))
async def cmd_status(c, m: Message):
    if not is_admin(m.from_user.id): return await m.reply("⛔ Unauthorized.")
    live = [t for t in active_tasks.values() if t.stage not in (Stage.DONE, Stage.CANCELLED)]
    if not live: return await m.reply("✅ No active tasks.")
    lines = ["📊 **Active Tasks:**\n"]
    for t in live:
        lines.append(
            f"• `{t.task_id}` [{t.mode.value.upper()}] — **{t.output_name or 'setup…'}**\n"
            f"  Stage: `{t.stage.name}` | Elapsed: `{fmt_time(time.time() - t.started_at)}`"
        )
    await m.reply("\n".join(lines))

# ── /shutdown ─────────────────────────────────────────────────────────────────
@app.on_message(filters.command("shutdown"))
async def cmd_shutdown(c, m: Message):
    if not is_admin(m.from_user.id): return await m.reply("⛔ Unauthorized.")
    await m.reply("🛑 Shutting down Lightning AI server to save RTX A6000 credits. Goodbye!")
    await asyncio.sleep(1)
    try: subprocess.run(["sudo", "shutdown", "-h", "now"], check=False)
    except: pass
    os._exit(0)

# ══════════════════════════════════════════════════════════════════════════════
#  UNIVERSAL MESSAGE ROUTER
# ══════════════════════════════════════════════════════════════════════════════
@app.on_message(~filters.command(["ocr", "enc", "cancel", "start", "status", "shutdown"]))
async def msg_router(c, m: Message):
    task = active_tasks.get(m.chat.id)
    if not task or task.stage in (Stage.DONE, Stage.CANCELLED): return

    s = task.stage

    if s == Stage.AWAIT_SRC:
        if m.video or (m.document and m.document.mime_type and "video" in m.document.mime_type):
            if not task.src_future.done(): task.src_future.set_result(("tg", m))
        elif m.text:
            url_m = re.search(r"(https?://\S+)", m.text)
            if url_m and not task.src_future.done(): task.src_future.set_result(("url", url_m.group(1)))

    elif s == Stage.AWAIT_CUT and m.text:
        if not task.cut_future.done(): task.cut_future.set_result(m.text.strip())

    elif s == Stage.AWAIT_SUB:
        if m.document:
            fname = m.document.file_name or ""
            if fname.lower().endswith((".srt", ".ass", ".ssa", ".vtt")):
                if not task.subtitle_future.done(): task.subtitle_future.set_result(m)
            else:
                await m.reply("⚠️ Please send a subtitle file: `.srt`, `.ass`, `.ssa`, or `.vtt`")

    elif s == Stage.AWAIT_NAME and m.text:
        if not task.name_future.done(): task.name_future.set_result(m.text.strip())

    elif s == Stage.AWAIT_THUMB:
        if m.photo:
            if not task.thumb_future.done(): task.thumb_future.set_result(m)
        elif m.text and m.text.strip().lower() in ("skip", "s", "/skip"):
            if not task.thumb_future.done(): task.thumb_future.set_result("SKIP")

# ══════════════════════════════════════════════════════════════════════════════
#  CALLBACK HANDLER
# ══════════════════════════════════════════════════════════════════════════════
@app.on_callback_query()
async def on_callback(c, q: CallbackQuery):
    data   = q.data or ""
    parts  = data.split(":", 2)
    action = parts[0]

    if action == "cancel_active":
        task = active_tasks.get(q.message.chat.id)
        if task:
            task.cancel_flag.set()
            _cancel_all_futures(task)
            await q.answer("🚫 Stopping task...", show_alert=True)
        else:
            await q.answer("No active task.", show_alert=False)

    elif action in ("start", "cancel") and len(parts) >= 2:
        tid  = parts[1]
        task = next((t for t in active_tasks.values() if t.task_id == tid), None)
        if not task: return await q.answer("Task not found.", show_alert=True)
        if q.from_user.id != task.user_id and not is_admin(q.from_user.id):
            return await q.answer("Not your task.", show_alert=True)
        try: await q.message.edit_reply_markup(None)
        except: pass
        if action == "start":
            await q.answer("▶️ Starting encode!")
            if task.confirm_future and not task.confirm_future.done():
                task.confirm_future.set_result("start")
        else:
            await q.answer("❌ Cancelling…", show_alert=True)
            if task.confirm_future and not task.confirm_future.done():
                task.confirm_future.set_result("cancel")

    elif action == "cq" and len(parts) == 3:
        tid, label = parts[1], parts[2]
        task = next((t for t in active_tasks.values() if t.task_id == tid), None)
        if not task: return await q.answer("Task not found.", show_alert=True)
        cf = task.quality_cancel_flags.get(label)
        if cf: cf.set()
        proc = task.quality_procs.get(label)
        if proc:
            try: proc.kill()
            except: pass
        await q.answer(f"❌ Cancelling {label}…", show_alert=True)
        try: await q.message.edit_reply_markup(None)
        except: pass

# ══════════════════════════════════════════════════════════════════════════════
#  OCR PIPELINE  (/ocr)
# ══════════════════════════════════════════════════════════════════════════════
async def _run_ocr(c, m: Message, task: Task):
    chat_id = task.chat_id
    status  = await m.reply("⏳ Initializing OCR pipeline…", reply_markup=CANCEL_BTN)
    task.status_msg = status

    try:
        # ── 1. DOWNLOAD
        task.stage = Stage.DOWNLOADING
        video_path = await _download_video(c, m, task, status)
        if not video_path: return await safe_edit(status, "❌ No video received or download failed.")
        if task.cancel_flag.is_set(): raise InterruptedError()
        task.input_path = video_path
        await extract_meta(task)

        # ── 2. GET CUT TIMES
        task.stage = Stage.AWAIT_CUT
        dur = get_real_duration(str(task.input_path)) or task.duration_s
        await safe_edit(status,
            f"✅ **Downloaded:** `{task.input_path.name}`\n"
            f"📐 `{task.src_width}×{task.src_height}` | `{fmt_time(dur)}`\n\n"
            "⏱ **Send cut times** (seconds):\n"
            "• `120 240` → process from 120 s to 240 s\n"
            "• `120 120` → skip 120 s from start AND end\n"
            "• `all`     → process the entire video",
            CANCEL_BTN,
        )
        try: cut_text = await asyncio.wait_for(task.cut_future, timeout=300)
        except (asyncio.TimeoutError, asyncio.CancelledError): return await safe_edit(status, "⏰ Timed out waiting for cut times.")
        if task.cancel_flag.is_set(): raise InterruptedError()

        start_sec = end_sec = None
        if cut_text.strip().lower() != "all":
            try:
                p = cut_text.strip().split()
                v1, v2 = float(p[0]), float(p[1])
                if v2 < 0:          start_sec, end_sec = v1, dur + v2
                elif v1 >= v2:      start_sec, end_sec = v1, dur - v2
                else:               start_sec, end_sec = v1, v2
                if start_sec >= end_sec or start_sec < 0 or end_sec > dur:
                    return await safe_edit(status, f"❌ Invalid cut times. Duration: `{int(dur)} s`. Computed: `{start_sec:.1f} → {end_sec:.1f} s`.")
            except:
                return await safe_edit(status, "❌ Bad format. Use `start end` or `all`.")

        # ── 3. OCR (22% Bottom Crop)
        task.stage = Stage.OCR_RUNNING
        await safe_edit(status, "⚡ Running OCR pipeline…", CANCEL_BTN)
        cancel_check = lambda: task.cancel_flag.is_set()
        final_subs = await asyncio.to_thread(run_ocr_pipeline, str(task.input_path), status, chat_id, start_sec, end_sec, cancel_check)
        if task.cancel_flag.is_set(): raise InterruptedError()
        task.ocr_subs = final_subs
        if not final_subs: return await safe_edit(status, "⚠️ No hardsubs detected in the specified range.")

        # ── 4. DELIVER SUBTITLES
        base = str(task.work_dir / task.input_path.stem)
        zh_texts = [s["text"] for s in final_subs]
        zh_srt   = base + "_zh.srt"
        write_srt(final_subs, zh_texts, zh_srt)
        await m.reply_document(zh_srt, caption=f"🇨🇳 Chinese OCR — {len(final_subs)} cues")

        if OPENAI_KEY:
            await safe_edit(status, "🌐 Translating via ChatGPT…", CANCEL_BTN)
            en_texts = await batch_translate(zh_texts, status, chat_id)
            if task.cancel_flag.is_set(): raise InterruptedError()
            en_srt = base + "_en.srt"
            write_srt(final_subs, en_texts, en_srt)
            await m.reply_document(en_srt, caption=f"🇬🇧 English (ChatGPT) — {len(final_subs)} cues")

        # ── 5. WAIT FOR MUX SUBTITLE
        task.stage = Stage.AWAIT_SUB
        await safe_edit(status,
            "✅ **OCR complete!**\n\n"
            "📎 Send the subtitle file (`.srt` / `.ass`) you want muxed into the video.\n"
            "_(You can forward one of the files sent above, or use a custom one.)_",
            CANCEL_BTN,
        )
        try: sub_msg = await asyncio.wait_for(task.subtitle_future, timeout=600)
        except (asyncio.TimeoutError, asyncio.CancelledError): return await safe_edit(status, "⏰ Timed out waiting for subtitle file.")
        if task.cancel_flag.is_set(): raise InterruptedError()

        await safe_edit(status, "📥 Downloading subtitle…", CANCEL_BTN)
        sub_dl = task.work_dir / (sub_msg.document.file_name or "subtitle.srt")
        await sub_msg.download(file_name=str(sub_dl))
        task.subtitle_path = sub_dl

        # ── 6. OUTPUT NAME
        task.stage = Stage.AWAIT_NAME
        await safe_edit(status, "📝 Enter the output filename:\n_(e.g. `Way Of Choices EP01`)_", CANCEL_BTN)
        try: name_raw = await asyncio.wait_for(task.name_future, timeout=300)
        except (asyncio.TimeoutError, asyncio.CancelledError): return await safe_edit(status, "⏰ Timed out waiting for filename.")
        if task.cancel_flag.is_set(): raise InterruptedError()
        parse_name(name_raw, task)

        # ── 7. THUMBNAIL
        task.stage = Stage.AWAIT_THUMB
        await safe_edit(status, "🖼 Send a **thumbnail photo** (or type `skip`):", CANCEL_BTN)
        try: thumb_res = await asyncio.wait_for(task.thumb_future, timeout=120)
        except (asyncio.TimeoutError, asyncio.CancelledError): thumb_res = "SKIP"
        if task.cancel_flag.is_set(): raise InterruptedError()
        if thumb_res != "SKIP":
            tp = await thumb_res.download(file_name=str(task.work_dir / "thumb.jpg"))
            task.thumb_path = Path(tp)

        # ── 8. MUX
        task.stage = Stage.MUXING
        await safe_edit(status, "🔧 Muxing video (stream-copy + subtitle inject)…", CANCEL_BTN)
        mux_out = task.work_dir / f"{task.raw_name}.mkv"
        await mux_video(task, task.subtitle_path, mux_out)
        if task.cancel_flag.is_set(): raise InterruptedError()
        task.muxed_path = mux_out
        task.input_path = mux_out      
        await extract_meta(task)       

        # ── 9. UPLOAD MUXED + ENCODE ALL QUALITIES (Simultaneous) ──────────
        task.stage = Stage.ENCODING
        
        # 🔓 RELEASE CHAT LOCK FOR NEXT JOB 🔓
        # By removing it from active_tasks here, the user can start a new /ocr command 
        # while this one happily chugs along in the background.
        if active_tasks.get(task.chat_id) is task:
            active_tasks.pop(task.chat_id, None)

        target_chat = resolve_channel(task.series_name) or chat_id
        await safe_edit(status, f"🚀 **Mux done!** Starting background upload + encode…\n`{task.output_name}`")

        mux_prog = await m.reply("📤 **[MUX]** Uploading…")

        async def _upload_mux():
            try:
                await upload_file(target_chat, task.muxed_path, build_mux_caption(task), task.thumb_path, mux_prog, "MUX", task.output_name)
                try: await mux_prog.edit("✅ **[MUX]** Upload complete! 🎉")
                except: pass
            except Exception as e:
                log.exception("[MUX] upload failed")
                try: await mux_prog.edit(f"❌ **[MUX]** Upload failed: `{e}`")
                except: pass

        await asyncio.gather(_upload_mux(), encode_all(task, m, target_chat))
        task.stage = Stage.DONE

    except InterruptedError:
        await safe_edit(status, "🚫 **Task Cancelled.**")
    except Exception as e:
        log.exception("OCR pipeline crashed")
        tb  = traceback.format_exc()
        buf = io.BytesIO(tb.encode()); buf.name = f"error_{task.task_id}.log"
        buf.seek(0)
        await m.reply_document(buf, caption=f"❌ **Crash:** `{e}`")
    finally:
        cleanup_task(task)

# ══════════════════════════════════════════════════════════════════════════════
#  ENC PIPELINE  (/enc)
# ══════════════════════════════════════════════════════════════════════════════
async def _run_enc(c, m: Message, task: Task):
    chat_id = task.chat_id
    status  = await m.reply("⏳ Initializing encoder…", reply_markup=CANCEL_BTN)
    task.status_msg = status

    try:
        task.stage = Stage.DOWNLOADING
        video_path = await _download_video(c, m, task, status)
        if not video_path: return await safe_edit(status, "❌ No video received.")
        if task.cancel_flag.is_set(): raise InterruptedError()
        task.input_path = video_path
        await extract_meta(task)

        task.stage = Stage.AWAIT_NAME
        await m.reply(f"✅ Downloaded: `{task.input_path.name}`\n📐 `{task.src_width}×{task.src_height}` | `{fmt_time(task.duration_s)}`\n\n📝 **Enter base filename** (e.g. `Way Of Choices EP01`):")
        try: name_raw = await asyncio.wait_for(task.name_future, timeout=300)
        except (asyncio.TimeoutError, asyncio.CancelledError): return await m.reply("⏰ Timed out waiting for filename.")
        if task.cancel_flag.is_set(): raise InterruptedError()
        parse_name(name_raw, task)

        task.stage = Stage.AWAIT_THUMB
        await m.reply("🖼 **Send thumbnail photo** (or type `skip`):")
        try: thumb_res = await asyncio.wait_for(task.thumb_future, timeout=120)
        except (asyncio.TimeoutError, asyncio.CancelledError): thumb_res = "SKIP"
        if task.cancel_flag.is_set(): raise InterruptedError()
        if thumb_res != "SKIP":
            tp = await thumb_res.download(file_name=str(task.work_dir / "thumb.jpg"))
            task.thumb_path = Path(tp)

        task.stage = Stage.CONFIRMING
        ch_id = resolve_channel(task.series_name)
        lines = ["📋 **Confirm encode job:**\n"]
        for spec in QUALITY_SPECS: lines.append(f"• `{out_filename(task.output_name, spec.label)}`")
        lines.append(f"\n📡 Channel: `{ch_id}`" if ch_id else "\n📡 No channel match — posting here")
        confirm_msg = await m.reply("\n".join(lines), reply_markup=confirm_kb(task.task_id))
        
        try: decision = await asyncio.wait_for(task.confirm_future, timeout=300)
        except (asyncio.TimeoutError, asyncio.CancelledError): decision = "cancel"
        if decision == "cancel":
            try: await confirm_msg.edit_reply_markup(None)
            except: pass
            return await m.reply("🚫 Job cancelled.")
        try: await confirm_msg.edit_reply_markup(None)
        except: pass

        task.stage = Stage.ENCODING
        
        # 🔓 RELEASE CHAT LOCK FOR NEXT JOB 🔓
        if active_tasks.get(task.chat_id) is task:
            active_tasks.pop(task.chat_id, None)

        target_chat = ch_id if ch_id else task.chat_id
        await m.reply(f"🚀 **Encoding started in background!** `{task.output_name}`\nThree progress messages will appear below ↓")
        await encode_all(task, m, target_chat)
        task.stage = Stage.DONE

    except InterruptedError:
        await safe_edit(status, "🚫 **Task Cancelled.**")
    except Exception as e:
        log.exception("ENC pipeline crashed")
        tb  = traceback.format_exc()
        buf = io.BytesIO(tb.encode()); buf.name = f"error_{task.task_id}.log"
        buf.seek(0)
        await m.reply_document(buf, caption=f"❌ **Crash:** `{e}`")
    finally:
        cleanup_task(task)

# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════
async def main():
    global EVENT_LOOP
    EVENT_LOOP = asyncio.get_running_loop()
    log.info("TheFrictionRealm Combined Bot — starting…")
    await app.start()
    log.info("✅ Bot ready!")
    await idle()
    await app.stop()

if __name__ == "__main__":
    try: asyncio.get_running_loop().create_task(main())
    except RuntimeError: asyncio.run(main())
