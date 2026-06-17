#!/usr/bin/env python3
"""
TheFrictionRealm — Lightning AI Unified Bot v4.4 (RTX PRO 6000 Blackwell Max Juice)
GPU  : RTX PRO 6000 Blackwell Server Edition (96 GB GDDR7 ECC, 4x NVENC, 4x NVDEC)
       — 4 independent OCR PROCESSES, real parallel GPU usage
OCR  : PaddleOCR Full Frame 100% Scan @ NATIVE FPS (Every single frame)
Subs : PlayRes source/2 | Exact Style match | Anti-overlap positioning
Enc  : hevc_nvenc p7 + multipass fullres + cq19 + maxed AQ
"""

import os, sys

# ── Lightning AI CUDA path fix ──────────────
_CUDA_LIB = (
    "/usr/local/nvidia/lib64:/usr/local/nvidia/lib:"
    "/usr/local/cuda/lib64:/usr/local/cuda/targets/x86_64-linux/lib:"
    "/usr/lib/x86_64-linux-gnu:/lib/x86_64-linux-gnu:/usr/lib64:/lib64"
)
os.environ["LD_LIBRARY_PATH"] = f"{_CUDA_LIB}:{os.environ.get('LD_LIBRARY_PATH', '')}"

try:
    import nest_asyncio; nest_asyncio.apply()
except ImportError:
    pass

import re, time, json, shutil, asyncio, logging, subprocess
import difflib, threading, queue, traceback, io, uuid
import http.server, socketserver, concurrent.futures
import multiprocessing as mp

_MP_CTX = mp.get_context("spawn")
from pathlib import Path
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Optional

import cv2, yt_dlp, numpy as np, requests
from aiohttp import web as _web
from pyrogram import Client, filters, idle
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait, MessageNotModified
from pyrogram.types import (
    CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, Message,
)

try:
    from openai import AsyncOpenAI
    _HAS_OPENAI = True
except ImportError:
    _HAS_OPENAI = False

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)-8s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True,
)
log = logging.getLogger("FrBot")

# ══════════════════════════════════════════════════════════════════════════════
#  Config
# ══════════════════════════════════════════════════════════════════════════════
API_ID     = int(os.getenv("API_ID", "0"))
API_HASH   = os.getenv("API_HASH", "")
BOT_TOKEN  = os.getenv("BOT_TOKEN", "")
OPENAI_KEY = os.getenv("OPENAI_API_KEY", "")

_ALLOWED_RAW = os.getenv("ALLOWED_USER_ID", os.getenv("ADMIN_IDS", ""))
ADMIN_IDS    = [int(x.strip().strip("\"'")) for x in _ALLOWED_RAW.split(",") if x.strip()]
CHANNEL_MAP_RAW = os.getenv("CHANNEL_MAP", "")
UPLOAD_TAG      = "TheFrictionRealm"

if not all([API_ID, API_HASH, BOT_TOKEN]):
    print("❌  Set API_ID, API_HASH, BOT_TOKEN in environment and restart.")
    sys.exit(1)

def _detect_gpus() -> int:
    try:
        out = subprocess.check_output(
            ["nvidia-smi", "--query-gpu=name", "--format=csv,noheader"],
            stderr=subprocess.DEVNULL)
        return len([l for l in out.decode().strip().split("\n") if l.strip()])
    except Exception:
        return 1

NUM_GPUS = _detect_gpus()
log.info(f"Detected {NUM_GPUS} GPU(s)")

BASE  = Path("/tmp/frbot")
FILES = BASE / "files"
TMP   = BASE / "tmp"
WORK  = BASE / "work"
DL    = BASE / "downloads"
for _d in [FILES, TMP, WORK, DL]: _d.mkdir(parents=True, exist_ok=True)

EVENT_LOOP  = None
REFRESH     = 5        
_OCR_ENGINES: dict = {}

# ══════════════════════════════════════════════════════════════════════════════
#  HTTP file server
# ══════════════════════════════════════════════════════════════════════════════
HTTP_PORT      = 8000
_file_id_map: dict[str, str] = {}
_http_thread: Optional[threading.Thread] = None
_http_lock   = threading.Lock()

class _FileHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *_): pass

    def do_GET(self):
        if self.path.startswith("/download/"):
            fid  = self.path.split("/")[-1]
            path = _file_id_map.get(fid)
            if path and os.path.exists(path):
                try:
                    with open(path, "rb") as f:
                        self.send_response(200)
                        self.send_header("Content-Type", "application/octet-stream")
                        self.send_header("Content-Disposition", f'attachment; filename="{os.path.basename(path)}"')
                        self.send_header("Content-Length", str(os.path.getsize(path)))
                        self.end_headers()
                        shutil.copyfileobj(f, self.wfile)
                except Exception as e:
                    log.error(f"HTTP serve error: {e}")
                    self.send_error(500)
            else:
                self.send_error(404, "File not found or link expired")
        else:
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"TheFrictionRealm file server OK")

def _ensure_http_server():
    global _http_thread
    with _http_lock:
        if _http_thread and _http_thread.is_alive(): return
        httpd = socketserver.TCPServer(("", HTTP_PORT), _FileHandler)
        httpd.allow_reuse_address = True
        _http_thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        _http_thread.start()

def _register_file(path: str) -> str:
    fid = uuid.uuid4().hex
    _file_id_map[fid] = path
    return fid

# ══════════════════════════════════════════════════════════════════════════════
#  Quality profiles
# ══════════════════════════════════════════════════════════════════════════════
@dataclass
class QualitySpec:
    label: str
    width: int
    height: int

QUALITY_SPECS = [
    QualitySpec("2K",    2560, 1440),
    QualitySpec("1080p", 1920, 1080),
    QualitySpec("720p",  1280,  720),
]

_encode_sem = asyncio.Semaphore(3)

# ══════════════════════════════════════════════════════════════════════════════
#  State machine
# ══════════════════════════════════════════════════════════════════════════════
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

@dataclass
class Task:
    task_id:  str   = field(default_factory=lambda: uuid.uuid4().hex[:8])
    mode:     Mode  = Mode.OCR
    chat_id:  int   = 0
    user_id:  int   = 0
    stage:    Stage = Stage.IDLE

    work_dir:      Optional[Path] = None
    input_path:    Optional[Path] = None
    subtitle_path: Optional[Path] = None
    muxed_path:    Optional[Path] = None
    thumb_path:    Optional[Path] = None

    output_name:  str = ""
    raw_name:     str = ""
    series_name:  str = ""
    episode_tag:  str = ""

    duration_s:  float = 0.0
    src_bitrate: int   = 0
    src_width:   int   = 0
    src_height:  int   = 0

    ocr_subs:    list = field(default_factory=list)
    ocr_frame_w: int  = 0
    ocr_frame_h: int  = 0

    cancel_flag:          Optional[asyncio.Event] = None
    quality_cancel_flags: dict = field(default_factory=dict)
    quality_procs:        dict = field(default_factory=dict)
    quality_msgs:         dict = field(default_factory=dict)
    encode_done_flags:    dict = field(default_factory=dict)
    encoded_files:        dict = field(default_factory=dict)

    src_future:      Optional[asyncio.Future] = None
    cut_future:      Optional[asyncio.Future] = None
    subtitle_future: Optional[asyncio.Future] = None
    name_future:     Optional[asyncio.Future] = None
    thumb_future:    Optional[asyncio.Future] = None
    confirm_future:  Optional[asyncio.Future] = None

    status_msg:    Optional[object] = None
    started_at:    float = field(default_factory=time.time)
    skip_download: bool  = False

active_tasks: dict[int, Task] = {}

def new_task(mode: Mode, chat_id: int, user_id: int) -> Task:
    loop = asyncio.get_running_loop()
    t = Task(mode=mode, chat_id=chat_id, user_id=user_id)
    t.work_dir        = WORK / t.task_id
    t.work_dir.mkdir(parents=True, exist_ok=True)
    t.cancel_flag     = asyncio.Event()
    t.src_future      = loop.create_future()
    t.cut_future      = loop.create_future()
    t.subtitle_future = loop.create_future()
    t.name_future     = loop.create_future()
    t.thumb_future    = loop.create_future()
    t.confirm_future  = loop.create_future()
    return t

def cleanup_task(t: Task):
    active_tasks.pop(t.chat_id, None)
    if t.work_dir and t.work_dir.exists():
        shutil.rmtree(t.work_dir, ignore_errors=True)

def is_admin(uid: int) -> bool:
    return (not ADMIN_IDS) or (uid in ADMIN_IDS)

def _cancel_all_futures(t: Task):
    for fut in [t.src_future, t.cut_future, t.subtitle_future,
                t.name_future, t.thumb_future, t.confirm_future]:
        if fut and not fut.done():
            try: fut.cancel()
            except: pass

# ══════════════════════════════════════════════════════════════════════════════
#  UI helpers
# ══════════════════════════════════════════════════════════════════════════════
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
    return (f"Progress: [{prog_bar(pct)}] {pct:.1f}%\n"
            f"📥 {action}: {fmt_bytes(cur)} | {fmt_bytes(total)}\n"
            f"⚡️ Speed: {fmt_bytes(spd)}/s\n"
            f"⌛ ETA: {fmt_time(eta)}\n"
            f"⏱️ Elapsed: {fmt_time(el)}")

def pb_frames(action: str, cur: int, total: int, t0: float, extra: str = "") -> str:
    el  = time.time() - t0
    spd = cur / max(el, 0.01)
    pct = cur / total * 100 if total else 0
    eta = (total - cur) / max(spd, 1) if total else 0
    base = (f"Progress: [{prog_bar(pct)}] {pct:.1f}%\n"
            f"⚡ {action}: {int(cur)} | {int(total)} frames\n"
            f"⚡️ Speed: {spd:.1f} fps\n"
            f"⌛ ETA: {fmt_time(eta)}\n"
            f"⏱️ Elapsed: {fmt_time(el)}")
    return base + (f"\n{extra}" if extra else "")

def pb_enc(label: str, name: str, pct: float, cur_s: float,
           tot_s: float, fps: float, spd: str, eta: float, el: float) -> str:
    return (f"🎬 **Encoding [{label}] · {name}**\n\n"
            f"Progress: `[{prog_bar(pct)}] {pct:.1f}%`\n"
            f"⏱️ Encoded: `{fmt_time(cur_s)}` / `{fmt_time(tot_s)}`\n"
            f"⚡️ Speed: `{spd}` | `{fps:.0f} fps`\n"
            f"⌛ ETA: `{fmt_time(eta)}`\n"
            f"🕐 Elapsed: `{fmt_time(el)}`")

def pb_up(label: str, name: str, cur: int, total: int, t0: float) -> str:
    el  = time.time() - t0
    spd = cur / max(el, 0.01)
    pct = cur / total * 100 if total else 0
    eta = (total - cur) / max(spd, 1) if total else 0
    return (f"📤 **Uploading [{label}] · {name}**\n\n"
            f"Progress: `[{prog_bar(pct)}] {pct:.1f}%`\n"
            f"📤 `{fmt_bytes(cur)}` | `{fmt_bytes(total)}`\n"
            f"⚡️ Speed: `{fmt_bytes(spd)}/s`\n"
            f"⌛ ETA: `{fmt_time(eta)}`\n"
            f"🕐 Elapsed: `{fmt_time(el)}`")

CANCEL_BTN = InlineKeyboardMarkup([[InlineKeyboardButton("🚫 Cancel Task", callback_data="cancel_active")]])

def qual_cancel_kb(task_id: str, label: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton(f"❌ Cancel {label}", callback_data=f"cq:{task_id}:{label}")]])

def confirm_kb(task_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("▶️ Start Encode", callback_data=f"start:{task_id}"),
        InlineKeyboardButton("❌ Cancel",        callback_data=f"cancel:{task_id}"),
    ]])

_POWER_KB = InlineKeyboardMarkup([[
    InlineKeyboardButton("🛑 Shut Down Server", callback_data="power_off"),
    InlineKeyboardButton("✅ Keep Alive",        callback_data="power_on"),
]])

async def safe_edit(msg, text: str, markup=None):
    try: await msg.edit_text(text, reply_markup=markup)
    except: pass

def push(msg, text: str, markup=None):
    if EVENT_LOOP and msg:
        asyncio.run_coroutine_threadsafe(safe_edit(msg, text, markup), EVENT_LOOP)

def channel_map() -> dict[str, int]:
    r: dict[str, int] = {}
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

# ── Pyrogram client ───────────────────────────────────────────────────────────
app = Client(
    "fr_lightning",
    api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN,
    max_concurrent_transmissions=8,
    in_memory=True,
)

# ══════════════════════════════════════════════════════════════════════════════
#  OCR engine  — PaddleOCR (each worker PROCESS loads its own instance)
# ══════════════════════════════════════════════════════════════════════════════
def _load_ocr(gpu_id: int = 0):
    if gpu_id not in _OCR_ENGINES:
        from paddleocr import PaddleOCR
        last_err = None
        for attempt in range(1, 4):
            try:
                log.info(f"PaddleOCR init — GPU:{gpu_id} (pid={os.getpid()}, attempt {attempt}/3)")
                _OCR_ENGINES[gpu_id] = PaddleOCR(
                    use_angle_cls=False,   
                    lang="ch",             
                    use_gpu=True,
                    gpu_id=gpu_id,
                    show_log=False,
                    det_db_box_thresh=0.5,
                    rec_batch_num=32,      
                    cpu_threads=4,
                )
                log.info(f"PaddleOCR ready GPU:{gpu_id} (pid={os.getpid()})")
                break
            except Exception as e:
                last_err = e
                log.warning(f"PaddleOCR init attempt {attempt} failed: {e}")
                shutil.rmtree(os.path.expanduser("~/.paddleocr"), ignore_errors=True)
                time.sleep(2)
        else:
            log.warning(f"GPU OCR init failed after 3 attempts: {last_err}")
            log.warning("Falling back to CPU PaddleOCR")
            _OCR_ENGINES[gpu_id] = PaddleOCR(use_angle_cls=False, lang="ch", use_gpu=False, show_log=False)
            
    return _OCR_ENGINES[gpu_id]

# ── Spatial & Fuzzy Merger Logic ──────────────────────────────────────────────
def _norm(txt: str) -> str:
    return re.sub(r"[\s\.,\!\?\-\—\*\(\)\[\]。！？、…]", "", txt or "")

def _same_sub(a: dict, b: dict, thr: float = 0.80) -> bool:
    ak, bk = _norm(a.get("text", "")), _norm(b.get("text", ""))
    if not ak or not bk: return False
    if ak == bk: return True
    return difflib.SequenceMatcher(None, ak, bk).ratio() >= thr

def group_and_merge(cues: list, frame_h: int, max_gap: float = 0.75) -> list:
    if not cues:
        return []

    band_px = max(frame_h * 0.05, 1.0)
    bands = {}

    for cue in cues:
        y = float(cue.get("y", frame_h / 2))
        key = int(y / band_px)
        bands.setdefault(key, []).append(cue)

    result = []

    for key in sorted(bands.keys()):
        band_cues = sorted(bands[key], key=lambda c: (c["start"], c.get("x", 0)))
        if not band_cues:
            continue

        cur = band_cues[0].copy()

        for nxt in band_cues[1:]:
            gap = float(nxt["start"]) - float(cur["end"])

            cur_txt = _norm(cur.get("cmp") or cur.get("text", ""))
            nxt_txt = _norm(nxt.get("cmp") or nxt.get("text", ""))

            same_text = (
                bool(cur_txt)
                and bool(nxt_txt)
                and (
                    cur_txt == nxt_txt
                    or difflib.SequenceMatcher(None, cur_txt, nxt_txt).ratio() >= 0.90
                )
            )

            same_pos = abs(float(nxt.get("y", 0)) - float(cur.get("y", 0))) <= max(80.0, frame_h * 0.04)

            if same_text and same_pos and gap <= max_gap:
                cur["end"] = max(float(cur["end"]), float(nxt["end"]))

                if len(nxt_txt) > len(cur_txt):
                    cur["text"] = nxt.get("text", cur.get("text", ""))
                    cur["cmp"] = nxt.get("cmp", cur.get("cmp", ""))

                cur["x"] = (float(cur.get("x", 0)) + float(nxt.get("x", 0))) / 2.0
                cur["y"] = (float(cur.get("y", 0)) + float(nxt.get("y", 0))) / 2.0
                cur["bh"] = max(float(cur.get("bh", 0)), float(nxt.get("bh", 0)))
            else:
                result.append(cur)
                cur = nxt.copy()

        result.append(cur)

    result.sort(key=lambda c: c["start"])
    return result
       
def _flatten_paddle_result(result) -> list:
    if not result:
        return []
    first = result[0]
    if (isinstance(first, list) and first
            and isinstance(first[0], (list, tuple)) and len(first[0]) == 2
            and isinstance(first[0][0], (list, tuple)) and len(first[0][0]) >= 3):
        return first  
    return result

def _extract_text_paddle(engine, frame: np.ndarray) -> list:
    img_rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
    unique_lines = []

    try:
        result = engine.ocr(img_rgb, cls=False)
    except Exception as e:
        log.debug(f"OCR pass error: {e}")
        return []

    for item in _flatten_paddle_result(result):
        if not item or len(item) != 2:
            continue
        box, txt_conf = item
        if not txt_conf or len(txt_conf) != 2:
            continue
        text, conf = txt_conf
        if conf is None or conf < 0.15:
            continue
        full_frame_pts = [[float(p[0]), float(p[1])] for p in box]
        unique_lines.append((full_frame_pts, (text, conf)))

    return unique_lines

# ══════════════════════════════════════════════════════════════════════════════
#  Frame-stream processor
# ══════════════════════════════════════════════════════════════════════════════
def _process_frame_stream(engine, cmd: list, bpf: int,
                           frame_h: int, frame_w: int,
                           extract_fps: float, time_offset: float,
                           cancel_check, progress_cb=None) -> list:
    cues: list = []
    fq: queue.Queue = queue.Queue(maxsize=512)

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE, bufsize=10**8)
    time.sleep(0.5)
    if proc.poll() is not None and proc.returncode != 0:
        err = proc.stderr.read(2000).decode(errors="replace")
        raise RuntimeError(f"FFmpeg pipe failed (rc={proc.returncode}):\n{err}")

    def _reader():
        idx = 0
        while True:
            if cancel_check(): proc.terminate(); break
            raw = proc.stdout.read(bpf)
            if not raw or len(raw) != bpf: break
            fq.put((idx, raw)); idx += 1
        fq.put(None)

    threading.Thread(target=_reader, daemon=True).start()
    frame_dur = 1.0 / extract_fps
    
    while True:
        item = fq.get()
        if item is None: break
        idx, raw = item
        cur_t = round((idx / extract_fps) + time_offset, 3)
        if progress_cb: progress_cb(idx, cues)

        frame = np.frombuffer(raw, dtype=np.uint8).reshape((frame_h, frame_w, 3))
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        mn, mx = cv2.minMaxLoc(gray)[:2]
        if mx < 50:
            continue

        lines = _extract_text_paddle(engine, frame)

        for pts, (raw_text, conf) in lines:
            if conf < 0.15: continue
            raw_text = raw_text.strip()
            cmp_text = _norm(raw_text)
            if not cmp_text: continue

            xs = [p[0] for p in pts]; ys = [p[1] for p in pts]
            cues.append({
                "start": cur_t,
                "end":   round(cur_t + frame_dur, 3),
                "text":  raw_text,
                "cmp":   cmp_text,
                "conf":  conf,
                "x":     float(sum(xs) / len(xs)),
                "y":     float(sum(ys) / len(ys)),
                "bw":    float(max(xs) - min(xs)),
                "bh":    float(max(ys) - min(ys)),
            })

    proc.stdout.close(); proc.wait()
    return cues

def _make_extract_cmd(video_path: str, ss: float, dur: float,
                       s_w: int, s_h: int, extract_fps: float, thr: str = "2") -> list:
    return [
        "ffmpeg", "-v", "error", "-y",
        "-hwaccel", "cuda",
        "-threads", thr,
        "-ss", str(ss), "-i", video_path, "-t", str(dur),
        "-vf", f"scale={s_w}:{s_h}:flags=lanczos",
        "-r", str(extract_fps),
        "-s", f"{s_w}x{s_h}",
        "-f", "image2pipe",
        "-pix_fmt", "bgr24",
        "-vcodec", "rawvideo",
        "-"
    ]

def _mp_ocr_worker(idx: int, video_path: str, ss: float, dur: float,
                    bpf: int, s_w: int, s_h: int, extract_fps: float,
                    progress_q, result_q, cancel_val):
    try:
        engine = _load_ocr(0)
        cmd = _make_extract_cmd(video_path, ss, dur, s_w, s_h, extract_fps)
        cancel_check = lambda: cancel_val.value != 0
        cues = _process_frame_stream(
            engine, cmd, bpf, s_h, s_w, extract_fps, ss, cancel_check,
            progress_cb=lambda fi, cl: progress_q.put((idx, fi, len(cl))),
        )
        result_q.put((idx, cues))
    except Exception:
        log.exception(f"OCR worker {idx} crashed")
        result_q.put((idx, []))

# ══════════════════════════════════════════════════════════════════════════════
#  Hyper-Parallel Full-frame OCR pipeline
# ══════════════════════════════════════════════════════════════════════════════
def get_real_duration(path: str) -> float:
    try:
        out = subprocess.check_output(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
             "-of", "default=noprint_wrappers=1:nokey=1", path])
        return float(out.decode().strip())
    except: return 0.0

def run_ocr_pipeline(video_path: str, status_msg, chat_id: int,
                     start_sec: float = 0.0, end_sec: float = None,
                     cancel_check=None) -> tuple:
    if cancel_check is None: cancel_check = lambda: False

    cap    = cv2.VideoCapture(video_path)
    fps    = cap.get(cv2.CAP_PROP_FPS) or 24.0
    orig_w = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    orig_h = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    cap.release()

    duration  = get_real_duration(video_path) or 0.0
    start_sec = max(start_sec or 0.0, 0.0)
    end_sec   = min(end_sec if end_sec is not None else duration, duration)
    proc_dur  = end_sec - start_sec

    extract_fps = fps
    total_frames = int(proc_dur * extract_fps)

    OCR_SCAN_WIDTH = 1280
    scale = min(OCR_SCAN_WIDTH, orig_w) / max(orig_w, 1)
    s_w   = (int(orig_w * scale) >> 1) << 1
    s_h   = (int(orig_h * scale) >> 1) << 1

    bpf   = s_w * s_h * 3
    t0 = time.time()

    NUM_WORKERS = 8

    push(status_msg,
         f"⚡ **Multi-Process Full Frame OCR** — {s_w}×{s_h} @ {extract_fps:.2f} fps\n"
         f"🚀 {NUM_WORKERS} independent GPU worker processes\n"
         f"Scanning 100% of {total_frames:,} frames…",
         CANCEL_BTN)

    _test_cmd = _make_extract_cmd(video_path, start_sec, min(2.0, proc_dur), s_w, s_h, extract_fps)
    _tp = subprocess.Popen(_test_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=bpf * 4)
    _raw = _tp.stdout.read(bpf)
    _tp.stdout.close(); _tp.wait()
    if len(_raw) != bpf:
        _err = _tp.stderr.read(2000).decode(errors="replace")
        raise RuntimeError(f"FFmpeg pipe geometry mismatch. stderr:\n{_err}")

    progress_q = _MP_CTX.Queue()
    result_q   = _MP_CTX.Queue()
    cancel_val = _MP_CTX.Value("i", 0)

    seg      = [[] for _ in range(NUM_WORKERS)]
    _prog    = [0] * NUM_WORKERS
    _cues_ct = [0] * NUM_WORKERS

    chunk_dur = proc_dur / NUM_WORKERS
    procs = []
    for i in range(NUM_WORKERS):
        chunk_start = start_sec + (i * chunk_dur)
        chunk_length = chunk_dur + 1.0 if i < NUM_WORKERS - 1 else chunk_dur
        p = _MP_CTX.Process(
            target=_mp_ocr_worker,
            args=(i, video_path, chunk_start, chunk_length, bpf, s_w, s_h, extract_fps,
                  progress_q, result_q, cancel_val),
            daemon=True,
        )
        p.start()
        procs.append(p)

    def _drain_progress():
        while True:
            try:
                gi, fi, cl_len = progress_q.get_nowait()
            except queue.Empty:
                break
            _prog[gi] = fi
            _cues_ct[gi] = cl_len

    last_ui = time.time()
    results_received = 0

    while any(p.is_alive() for p in procs) or results_received < NUM_WORKERS:
        if cancel_check() and cancel_val.value == 0:
            cancel_val.value = 1   
        
        _drain_progress()
        
        while True:
            try:
                gi, cues = result_q.get_nowait()
                seg[gi] = cues
                results_received += 1
            except queue.Empty:
                break

        if time.time() - last_ui > REFRESH:
            last_ui = time.time()
            total_p = sum(_prog)
            total_c = sum(_cues_ct)
            progress_str = " | ".join([f"W{i}:{_prog[i]}" for i in range(NUM_WORKERS)])
            fps_actual = total_p / max(time.time() - t0, 0.01)
            push(status_msg,
                 pb_frames("⚡ Multi-Process Parallel OCR", total_p, total_frames, t0,
                            f"💬 Cues: {total_c} | GPU Throughput: {fps_actual:.0f} fps\n"
                            f"⚙️ {progress_str}"),
                 CANCEL_BTN)
                 
        if not any(p.is_alive() for p in procs) and result_q.empty():
            break
            
        time.sleep(0.5)

    for p in procs: p.join(timeout=10)
    for p in procs:
        if p.is_alive(): p.terminate()

    raw = []
    for i in range(NUM_WORKERS):
        chunk_end = start_sec + ((i + 1) * chunk_dur)
        for s in seg[i]:
            if i < NUM_WORKERS - 1 and s["start"] >= chunk_end:
                continue
            raw.append(s)
            
    raw.sort(key=lambda x: x["start"])

    before_merge = len(raw)
    raw = group_and_merge(raw, s_h, max_gap=0.75)
    after_merge = len(raw)

    log.info(
        f"MERGE RESULT: {before_merge:,} -> {after_merge:,} "
        f"(saved {before_merge - after_merge:,} cues)"
    )

    return raw, s_w, s_h

# ══════════════════════════════════════════════════════════════════════════════
#  ChatGPT translation
# ══════════════════════════════════════════════════════════════════════════════
async def batch_translate(zh_texts: list, status_msg=None, chat_id: int = None) -> list:
    if not (OPENAI_KEY and _HAS_OPENAI):
        return [""] * len(zh_texts)
    client = AsyncOpenAI(api_key=OPENAI_KEY)
    BATCH  = 50; res: list = []; t0 = time.time()
    sys_p  = (
        "You are a senior subtitle translator for Chinese Donghua, Xianxia, and Wuxia animation. "
        "Translate Chinese subtitles to natural English. "
        "For cultivation terms, special technique names, and skill names — keep them epic and accurate. "
        "Return ONLY a numbered list matching input numbering exactly. "
        "Do not merge, skip, or add commentary."
    )
    for i in range(0, len(zh_texts), BATCH):
        task = active_tasks.get(chat_id)
        if task and task.cancel_flag.is_set(): break
        if status_msg:
            push(status_msg, pb_frames("ChatGPT Translating", i, len(zh_texts), t0), CANCEL_BTN)
        chunk      = zh_texts[i:i + BATCH]
        chunk_text = "\n".join(f"{j} | {t}" for j, t in enumerate(chunk))
        try:
            resp  = await client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "system", "content": sys_p},
                          {"role": "user",   "content": chunk_text}],
                temperature=0.1,
            )
            reply = resp.choices[0].message.content.strip()
            out   = [""] * len(chunk)
            for line in reply.split("\n"):
                m = re.match(r"^\*?\*?(\d+)\*?\*?\s*[|\-]\s*(.*)", line.strip())
                if m:
                    idx, txt = int(m.group(1)), m.group(2).strip()
                    if 0 <= idx < len(chunk):
                        if not out[idx]: out[idx] = txt
                        else: out[idx] += " " + txt
            res.extend(out)
        except Exception as e:
            log.error(f"ChatGPT error: {e}")
            res.extend([""] * len(chunk))
    return res

# ══════════════════════════════════════════════════════════════════════════════
#  ASS Subtitle Writer (Strict Rule Styling + Horizonatal positioning)
# ══════════════════════════════════════════════════════════════════════════════
def ass_ts(sec: float) -> str:
    cc = int(round(sec * 100))
    h  = cc // 360000; cc %= 360000
    m  = cc //   6000; cc %=   6000
    s  = cc //    100; cc %=    100
    return f"{h}:{m:02d}:{s:02d}.{cc:02d}"

def _ass_escape(text: str) -> str:
    return text.replace("\\", "\u2060").replace("{", r"\{").replace("}", r"\}")
       
def merge_ass_dialogues(ass_text: str, max_gap: float = 0.90) -> str:
    lines = ass_text.splitlines()
    out = []

    def parse_dialogue(line: str):
        if not line.startswith("Dialogue:"):
            return None
        parts = line.split(",", 9)
        if len(parts) < 10:
            return None
        try:
            start = _ass_time_to_sec(parts[1].strip())
            end = _ass_time_to_sec(parts[2].strip())
        except:
            return None
        text = parts[9]
        clean = _norm(re.sub(r"\{.*?\}", "", text).replace("\\N", ""))
        return {
            "parts": parts,
            "start": start,
            "end": end,
            "text": text,
            "clean": clean,
            "raw_line": line,
        }

    cur = None

    for line in lines:
        item = parse_dialogue(line)
        if item is None:
            if cur is not None:
                out.append(cur["raw_line"])
                cur = None
            out.append(line)
            continue

        if cur is None:
            cur = item
            continue

        same_text = (
            cur["clean"] == item["clean"]
            or (
                cur["clean"] and item["clean"]
                and difflib.SequenceMatcher(None, cur["clean"], item["clean"]).ratio() >= 0.90
            )
        )
        same_style = cur["parts"][3:9] == item["parts"][3:9]
        gap = item["start"] - cur["end"]

        if same_text and same_style and gap <= max_gap:
            cur["end"] = max(cur["end"], item["end"])
            cur["parts"][2] = ass_ts(cur["end"])
            cur["raw_line"] = ",".join(cur["parts"])
        else:
            out.append(cur["raw_line"])
            cur = item

    if cur is not None:
        out.append(cur["raw_line"])

    return "\n".join(out)
       
def write_smart_ass(subs: list, en_texts: list, path: str, frame_w: int, frame_h: int, orig_w: int = 0, orig_h: int = 0) -> None:
    # Exact PlayRes source/2 rule
    play_x = (orig_w // 2) if orig_w else 960
    play_y = (orig_h // 2) if orig_h else 540
    
    scale_x = play_x / max(frame_w, 1)
    scale_y = play_y / max(frame_h, 1)

    # UPDATED: Fontsize scaled to 80, MarginV scaled to 150 for mobile readability
    header = f"""\ufeff[Script Info]
ScriptType: v4.00+
PlayResX: {play_x}
PlayResY: {play_y}

[V4+ Styles]
Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding
Style: Default,Arial,80,&H00FFFFFF,&H000000FF,&H00000000,&H00000000,-1,0,0,0,70,90,1,0,1,2,2,2,400,400,150,1

[Events]
Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text
"""
    events: list[str] = []

    for sub, en in zip(subs, en_texts):
        zh       = _ass_escape(sub["text"])
        en_clean = _ass_escape(en.strip()) if en and en.strip() else ""
        ts_s     = ass_ts(sub["start"])
        ts_e     = ass_ts(sub["end"])

        x  = float(sub.get("x", frame_w / 2)) * scale_x
        y  = float(sub.get("y", frame_h * 0.9)) * scale_y
        bh = float(sub.get("bh", 0)) * scale_y

        # UPDATED: Scaled translation tag up to \fs60 to match the new mobile-friendly proportions
        trans_tag = r"{\fs60\c&HD0D0D0&}"

        # 1. BOTTOM DIALOGUE (Lower 20% of screen)
        if y > (play_y * 0.80):
            if en_clean:
                # English \N Chinese -> English renders ABOVE the Chinese text.
                # MarginV 150 pushes the whole block above the video's hardsub.
                text = f"{en_clean}\\N{zh}"
            else:
                text = zh
            events.append(f"Dialogue: 0,{ts_s},{ts_e},Default,,0,0,0,,{text}")
            
        # 2. UPPER 80% NON-DIALOGUE (Moves, Intros)
        else:
            # Forced horizontal rendering. No vertical \N injections.
            if y < (play_y * 0.50):
                # Top half of screen -> Push subtitle slightly BELOW the Chinese hardsub
                y_pos = y + (bh / 2) + (play_y * 0.02)
                align = r"\an8" 
            else:
                # Middle of screen -> Push subtitle slightly ABOVE the Chinese hardsub
                y_pos = y - (bh / 2) - (play_y * 0.02)
                align = r"\an2" 

            if en_clean:
                if align == r"\an8":
                    text = f"{{{align}\\pos({x:.0f},{y_pos:.0f})}}{zh}\\N{trans_tag}{en_clean}"
                else:
                    text = f"{{{align}\\pos({x:.0f},{y_pos:.0f})}}{trans_tag}{en_clean}\\N{{\\r}}{zh}"
            else:
                text = f"{{{align}\\pos({x:.0f},{y_pos:.0f})}}{zh}"

            events.append(f"Dialogue: 0,{ts_s},{ts_e},Default,,0,0,0,,{text}")

    with open(path, "w", encoding="utf-8-sig") as f:
        f.write(header)
        for ev in events:
            f.write(ev + "\n")

# ══════════════════════════════════════════════════════════════════════════════
#  Download helpers
# ══════════════════════════════════════════════════════════════════════════════
_ydl_last_ui: dict[int, float] = {}

def _ydl_hook(d: dict, msg, chat_id: int, t0: float):
    task = active_tasks.get(chat_id)
    if task and task.cancel_flag.is_set(): raise Exception("Cancelled")
    now = time.time()
    if d["status"] == "downloading" and now - _ydl_last_ui.get(chat_id, 0) > REFRESH:
        _ydl_last_ui[chat_id] = now
        cur = d.get("downloaded_bytes", 0)
        tot = d.get("total_bytes") or d.get("total_bytes_estimate", 0)
        push(msg, pb_bytes("yt-dlp", cur, tot, t0), CANCEL_BTN)

def dl_ytdlp(url: str, chat_id: int, msg_id: int, status_msg=None) -> str:
    t0   = time.time()
    dest = str(FILES / f"{chat_id}_{msg_id}.%(ext)s")
    opts = {
        "format": "bestvideo+bestaudio/best",
        "outtmpl": dest, "merge_output_format": "mkv",
        "quiet": True, "nocheckcertificate": True,
        "retries": 10, "fragment_retries": 10,
        "socket_timeout": 20,
        "external_downloader": "aria2c",
        "external_downloader_args": ["-x", "16", "-s", "16", "-k", "1M"],
        "progress_hooks": ([lambda d: _ydl_hook(d, status_msg, chat_id, t0)]
                           if status_msg else []),
    }
    with yt_dlp.YoutubeDL(opts) as ydl:
        return ydl.prepare_filename(ydl.extract_info(url, download=True))

def _dl_range_worker(url: str, start: int, end: int, path: str,
                     progress: list, idx: int, chat_id: int):
    headers = {"User-Agent": "Mozilla/5.0"}
    for attempt in range(15):
        cur = start + progress[idx]
        if cur > end: return
        try:
            r = requests.get(url, headers={"Range": f"bytes={cur}-{end}",
                                           **headers},
                             stream=True, timeout=(10, 30))
            r.raise_for_status()
            if r.status_code == 200: raise Exception("No range support")
            with open(path, "rb+") as f:
                f.seek(cur)
                buf = bytearray()
                for chunk in r.iter_content(128 * 1024):
                    t = active_tasks.get(chat_id)
                    if t and t.cancel_flag.is_set(): return
                    if chunk:
                        buf.extend(chunk)
                        if len(buf) >= 1024 * 1024:
                            f.write(buf); progress[idx] += len(buf); buf.clear()
                if buf: f.write(buf); progress[idx] += len(buf)
            if start + progress[idx] > end: return
        except Exception as e:
            log.warning(f"DL thread {idx} attempt {attempt+1}: {e}")
            time.sleep(2)
    raise Exception(f"DL thread {idx} failed permanently")

async def dl_parallel_http(url: str, dest: Path, status_msg, chat_id: int, t0: float):
    for attempt in range(3):
        try:
            r = await asyncio.to_thread(requests.get, url, headers={"User-Agent": "Mozilla/5.0"}, stream=True, timeout=(10, 20))
            r.raise_for_status(); break
        except Exception as e:
            if attempt == 2: raise ValueError(f"Connection failed: {e}")
            await asyncio.sleep(3)

    total  = int(r.headers.get("content-length", 0))
    ranges = r.headers.get("accept-ranges", "").lower() == "bytes"
    if not (total and ranges):
        def _single():
            with open(dest, "wb") as f:
                for chunk in r.iter_content(2*1024*1024):
                    if chunk: f.write(chunk)
        await asyncio.to_thread(_single); return

    r.close()
    with open(dest, "wb") as f: f.truncate(total)

    N, loop = 8, asyncio.get_running_loop()
    csz     = total // N
    prog    = [0] * N
    last_u  = [time.time()]

    with concurrent.futures.ThreadPoolExecutor(max_workers=N) as ex:
        futures = [
            loop.run_in_executor(
                ex, _dl_range_worker, url,
                i * csz, (total - 1 if i == N-1 else (i+1)*csz - 1),
                str(dest), prog, i, chat_id)
            for i in range(N)
        ]
        while True:
            done = sum(prog)
            t = active_tasks.get(chat_id)
            if t and t.cancel_flag.is_set(): raise Exception("Cancelled")
            if time.time() - last_u[0] > REFRESH:
                last_u[0] = time.time()
                push(status_msg, pb_bytes("Downloading", done, total, t0), CANCEL_BTN)
            if done >= total or all(f.done() for f in futures): break
            await asyncio.sleep(1)
        await asyncio.gather(*futures)

async def tg_download(source_msg: Message, dest: Path, status: Message, task: Task) -> Path:
    dest.mkdir(parents=True, exist_ok=True)
    t0 = time.time(); last = [0.0]
    fname  = (getattr(source_msg.video, "file_name", None) or
              getattr(source_msg.document, "file_name", None) or "video.mkv")
    target = str(dest / fname)

    async def _prog(cur, tot):
        if task.cancel_flag.is_set(): app.stop_transmission()
        if time.time() - last[0] > REFRESH:
            last[0] = time.time()
            await safe_edit(status, pb_bytes("Downloading", cur, tot, t0), CANCEL_BTN)

    return Path(await source_msg.download(file_name=target, progress=_prog))

async def _download_video(c, m: Message, task: Task, status: Message) -> Optional[Path]:
    parts   = (m.text or "").split(maxsplit=1)
    url_arg = parts[1].strip() if len(parts) > 1 else ""
    url_m   = re.search(r"(https?://\S+)", url_arg)
    if url_m:
        url  = url_m.group(1)
        t0   = time.time()
        dest = FILES / f"{task.chat_id}_{task.task_id}.mkv"
        await safe_edit(status, "📥 Downloading…", CANCEL_BTN)
        try:
            return Path(await asyncio.to_thread(dl_ytdlp, url, task.chat_id, m.id, status))
        except Exception:
            await dl_parallel_http(url, dest, status, task.chat_id, t0)
            return dest

    if m.reply_to_message and (m.reply_to_message.video or m.reply_to_message.document):
        await safe_edit(status, "📥 Downloading from Telegram…", CANCEL_BTN)
        return await tg_download(m.reply_to_message, task.work_dir, status, task)

    task.stage = Stage.AWAIT_SRC
    await safe_edit(status, "📨 **Send a video file** or paste a URL:", CANCEL_BTN)
    try:
        result = await asyncio.wait_for(task.src_future, timeout=300)
    except (asyncio.TimeoutError, asyncio.CancelledError):
        return None
    if task.cancel_flag.is_set(): return None

    src_type, src_data = result
    await safe_edit(status, "📥 Downloading…", CANCEL_BTN)
    t0   = time.time()
    dest = FILES / f"{task.chat_id}_{task.task_id}.mkv"
    if src_type == "url":
        try:
            return Path(await asyncio.to_thread(dl_ytdlp, src_data, task.chat_id, m.id, status))
        except Exception:
            await dl_parallel_http(src_data, dest, status, task.chat_id, t0)
            return dest
    else:
        return await tg_download(src_data, task.work_dir, status, task)

async def delayed_delete(path: str, delay: int = 7200):
    await asyncio.sleep(delay)
    try: os.remove(path)
    except: pass

def extract_thumbnail(video_path: str, thumb_path: str):
    subprocess.run(
        ["ffmpeg", "-y", "-i", video_path, "-ss", "00:00:01",
         "-vframes", "1", "-vf", "scale=320:-1", "-q:v", "2", thumb_path],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

# ══════════════════════════════════════════════════════════════════════════════
#  Media probe
# ══════════════════════════════════════════════════════════════════════════════
async def probe_media(path: Path) -> dict:
    proc = await asyncio.create_subprocess_exec(
        "ffprobe", "-v", "quiet", "-print_format", "json",
        "-show_streams", "-show_format", str(path),
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
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
            if not task.src_bitrate:
                task.src_bitrate = int(s.get("bit_rate", 4_000_000))
            break

async def _find_main_audio(path: Path) -> str:
    info    = await probe_media(path)
    streams = [s for s in info.get("streams", []) if s.get("codec_type") == "audio"]
    if not streams: return "a:0"
    def _dur(s):
        d = s.get("duration") or s.get("tags", {}).get("DURATION", "0")
        try:
            if ":" in str(d):
                h, mm, sc = str(d).split(":")
                return float(h)*3600 + float(mm)*60 + float(sc)
            return float(d)
        except: return 0.0
    streams.sort(key=_dur, reverse=True)
    return str(streams[0]["index"])

# ══════════════════════════════════════════════════════════════════════════════
#  Mux  — stream-copy + subtitle injection
# ══════════════════════════════════════════════════════════════════════════════
async def mux_video(task: Task, sub_path: Path, out_path: Path) -> Path:
    audio_idx = await _find_main_audio(task.input_path)
    cmd = [
        "ffmpeg", "-y",
        "-i", str(task.input_path),
        "-i", str(sub_path),
        "-map", "0:v:0",
        "-map", f"0:{audio_idx}",
        "-map", "1:0",
        "-c", "copy",
        "-metadata:s:s:0", "title=ENGLISH @TheFrictionRealm",
        "-metadata:s:s:0", "language=eng",
        "-disposition:s:0", "default",
        "-metadata", f"title={task.output_name}",
        str(out_path),
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    _, err = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(f"Mux failed:\n{err.decode(errors='replace')[:600]}")
    return out_path

# ══════════════════════════════════════════════════════════════════════════════
#  Encode  — RTX 6000 Ada Max NVENC Optimized
# ══════════════════════════════════════════════════════════════════════════════
def out_filename(base: str, quality: str) -> str:
    return f"{base} [{quality}][{UPLOAD_TAG}].mkv"

def build_encode_cmd(input_path: Path, out_path: Path, spec: QualitySpec,
                     output_name: str, src_bitrate: int,
                     audio_idx: str = "a:0", has_subs: bool = True) -> list:
    scale_factors = {"2K": 0.65, "1080p": 0.45, "720p": 0.30}
    maxrate = max(int(src_bitrate * scale_factors.get(spec.label, 0.45) * 1.4), 500_000)
    bufsize = int(maxrate * 2.5)   

    return [
        "ffmpeg", "-v", "warning", "-y",
        "-hwaccel", "cuda", "-hwaccel_output_format", "cuda",
        "-i", str(input_path),
        "-map", "0:v:0",
        "-map", f"0:{audio_idx}",
        *([ "-map", "0:s"] if has_subs else []),

        "-vf", f"scale_cuda={spec.width}:-2:interp_algo=lanczos:format=nv12",

        "-c:v", "hevc_nvenc",
        "-preset",    "p7",           
        "-tune",      "hq",
        "-profile:v", "main",
        "-level",     "auto",

        "-rc",         "vbr",
        "-multipass",  "fullres",     
        "-cq",         "19",          
        "-b:v",        "0",           
        "-maxrate:v",  str(maxrate),
        "-bufsize:v",  str(bufsize),
        "-gpu",        "0",           

        "-spatial-aq",  "1",
        "-temporal-aq", "1",          
        "-aq-strength", "15",         

        "-rc-lookahead", "50",        
        "-bf",           "4",
        "-b_ref_mode",   "middle",
        "-refs",         "4",
        "-weighted_pred","1",         

        "-c:a", "copy",
        *([ "-c:s", "copy"] if has_subs else []),

        "-metadata", f"title={output_name}",
        "-progress", "pipe:1", "-nostats",
        str(out_path),
    ]

async def run_encode(task: Task, spec: QualitySpec, out_path: Path, prog_msg: Message):
    cancel = task.quality_cancel_flags[spec.label]

    async with _encode_sem:   
        if cancel.is_set() or task.cancel_flag.is_set(): return

        audio_idx = await _find_main_audio(task.input_path)
        info      = await probe_media(task.input_path)
        has_subs  = any(s.get("codec_type") == "subtitle" for s in info.get("streams", []))

        cmd  = build_encode_cmd(task.input_path, out_path, spec, task.output_name, task.src_bitrate, audio_idx, has_subs)
        log.info(f"[{spec.label}] hevc_nvenc p7+multipass start")
        proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        task.quality_procs[spec.label] = proc

        kv: dict = {}; last_edit = 0.0; t0 = time.time()
        last_spd = 1.0; last_txt = ""; stderr_lines: list[str] = []

        async def _drain():
            while True:
                lb = await proc.stderr.readline()
                if not lb: break
                stderr_lines.append(lb.decode(errors="ignore").rstrip())
                if len(stderr_lines) > 50: stderr_lines.pop(0)

        drain_task = asyncio.create_task(_drain())

        async for lb in proc.stdout:
            if cancel.is_set() or task.cancel_flag.is_set():
                proc.kill(); break
            line = lb.decode(errors="ignore").strip()
            if "=" in line:
                k, _, v = line.partition("="); kv[k.strip()] = v.strip()
            if "progress" in kv and time.time() - last_edit > REFRESH:
                cur_s = int(kv.get("out_time_us", 0) or 0) / 1_000_000
                pct   = min(cur_s / (task.duration_s or 1) * 100, 100)
                spd_s = kv.get("speed", "0x")
                mm    = re.search(r"([\d\.]+)", spd_s)
                if mm:
                    v = float(mm.group(1))
                    if 0 < v < 100: last_spd = v
                eta = (task.duration_s - cur_s) / max(last_spd, 0.01)
                txt = pb_enc(spec.label, task.output_name, pct, cur_s,
                             task.duration_s, float(kv.get("fps", 0) or 0), spd_s, eta, time.time() - t0)
                if txt != last_txt:
                    try:
                        await prog_msg.edit(txt, reply_markup=qual_cancel_kb(task.task_id, spec.label))
                        last_edit = time.time(); last_txt = txt
                    except: pass

        await drain_task; await proc.wait()
        task.quality_procs.pop(spec.label, None)
        if not (cancel.is_set() or task.cancel_flag.is_set()) and proc.returncode != 0:
            raise RuntimeError(f"FFmpeg error:\n{''.join(stderr_lines[-20:])}")

# ── Upload ────────────────────────────────────────────────────────────────────
TG_LIMIT = 2 * 1024 * 1024 * 1024   

def build_caption(task: Task, quality: str) -> str:
    title = task.output_name or task.series_name or task.raw_name
    return (f"<b>{title}</b>\n\n"
            f"<blockquote>Episode : {task.episode_tag or title}\n"
            f"Quality : {quality}\n"
            f"Subtitles : INBUILT</blockquote>")

def build_mux_caption(task: Task) -> str:
    title = task.raw_name or task.output_name
    return (f"<b>{title}</b>\n\n"
            f"<blockquote>Episode : {title}\n"
            f"Subtitles : ENGLISH @TheFrictionRealm</blockquote>")

async def upload_file(chat_id: int, path: Path, caption: str,
                      thumb: Optional[Path], prog_msg: Message,
                      label: str, name: str):
    if path.stat().st_size >= TG_LIMIT:
        _ensure_http_server()
        fid = _register_file(str(path))
        dl  = f"/download/{fid}"
        pub = os.getenv("LIGHTNING_APP_STATE_URL") or os.getenv("LIGHTNING_HOST", "")
        if pub:
            if not pub.startswith("http"): pub = "https://" + pub
            link = f"{pub.rstrip('/').replace('7860', str(HTTP_PORT))}{dl}"
            msg  = (f"🔗 **[{label}]** too large for Telegram ({path.stat().st_size/1e9:.2f} GB)\n`{link}`")
        else:
            msg = (f"✅ **[{label}]** encoded. Expose port `{HTTP_PORT}` and access `{dl}`")
        try: await prog_msg.edit(msg)
        except: pass
        return

    t0 = time.time(); last = [0.0]; last_txt = [""]

    async def _prog(cur, tot):
        if time.time() - last[0] < REFRESH: return
        txt = pb_up(label, name, cur, tot, t0)
        if txt == last_txt[0]: return
        try: await prog_msg.edit(txt); last[0] = time.time(); last_txt[0] = txt
        except: pass

    await app.send_document(
        chat_id=chat_id, document=str(path),
        caption=caption, parse_mode=ParseMode.HTML,
        thumb=str(thumb) if thumb else None,
        progress=_prog)

async def quality_worker(task: Task, spec: QualitySpec, trigger_msg: Message, target_chat: int):
    label  = spec.label
    cancel = task.quality_cancel_flags[label]
    out    = task.work_dir / out_filename(task.output_name, label)
    task.encoded_files[label] = out

    prog_msg = await trigger_msg.reply(
        pb_enc(label, task.output_name, 0, 0, task.duration_s, 0, "0x", task.duration_s, 0),
        reply_markup=qual_cancel_kb(task.task_id, label))
    task.quality_msgs[label] = prog_msg

    err_str: Optional[str] = None
    try:    await run_encode(task, spec, out, prog_msg)
    except RuntimeError as e: err_str = str(e)

    if cancel.is_set() or task.cancel_flag.is_set():
        try: await prog_msg.edit(f"🚫 **[{label}]** Cancelled", reply_markup=None)
        except: pass
        task.encode_done_flags[label].set(); return

    if err_str:
        log.error(f"[{label}] encode error: {err_str[:200]}")
        try: await prog_msg.edit(f"❌ **[{label}]** Failed:\n{err_str[:1800]}", reply_markup=None)
        except: pass
        task.encode_done_flags[label].set(); return

    task.encode_done_flags[label].set()
    try: await prog_msg.edit(f"✅ **[{label}]** Encoded! Uploading…", reply_markup=None)
    except: pass
    try:
        await upload_file(target_chat, out, build_caption(task, label), task.thumb_path, prog_msg, label, task.output_name)
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
        for spec in QUALITY_SPECS])

# ── Cancel helper ─────────────────────────────────────────────────────────────
async def do_cancel(task: Task, msg: Message, reason: str = "User requested."):
    task.stage = Stage.CANCELLED
    task.cancel_flag.set()
    for f in task.quality_cancel_flags.values(): f.set()
    for proc in list(task.quality_procs.values()):
        try: proc.kill()
        except: pass
    _cancel_all_futures(task); cleanup_task(task)
    await msg.reply(f"🚫 **Cancelled:** {reason}")

def parse_name(raw: str, task: Task):
    task.raw_name    = raw.strip()
    cleaned          = re.sub(r"\s*\[.*?\]", "", task.raw_name)
    cleaned          = os.path.splitext(cleaned)[0].strip()
    task.output_name = cleaned; task.series_name = cleaned
    ep_m             = re.search(r"\bEP?(\d+)\b", cleaned, re.IGNORECASE)
    task.episode_tag = f"EP{ep_m.group(1)}" if ep_m else cleaned

def deactivate_machine():
    log.info("🛑  Shutting down Lightning AI to save RTX 6000 credits…")
    time.sleep(2)
    try: subprocess.run(["sudo", "shutdown", "-h", "now"], check=True)
    except Exception as e: log.error(f"Auto-shutdown failed: {e}")

# ══════════════════════════════════════════════════════════════════════════════
#  Bot command handlers
# ══════════════════════════════════════════════════════════════════════════════
@app.on_message(filters.command("start"))
async def cmd_start(c, m: Message):
    await m.reply_text(
        "🎬 **TheFrictionRealm — Lightning AI Bot v4.4**\n\n"
        "🔬 **OCR:** Max-Batch GPU Scan 100% Frame @ NATIVE FPS\n"
        "📄 **Subs:** Exact ASS Layout + Spatial Merging\n"
        "🎞 **Encode:** hevc_nvenc p7 + multipass fullres + cq19\n\n"
        "**Commands:**\n"
        "  `/ocr` — OCR → Translate → ASS → Mux → Encode\n"
        "  `/enc` — Direct encode 2K / 1080p / 720p\n"
        "  `/log <id>` — Re-encode cached video\n"
        "  `/clean` — Clean an uploaded .srt or .ass file\n"
        "  `/cancel` · `/status` · `/shutdown`\n\n"
        "_Reply to a video or include a URL._"
    )

@app.on_message(filters.command("ocr"))
async def cmd_ocr(c, m: Message):
    if not is_admin(m.from_user.id): return await m.reply("⛔ Unauthorized.")
    chat_id = m.chat.id
    ex = active_tasks.get(chat_id)
    if ex and ex.stage not in (Stage.DONE, Stage.CANCELLED):
        return await m.reply("⚠️ Active task. Use /cancel first.")
    task = new_task(Mode.OCR, chat_id, m.from_user.id)
    active_tasks[chat_id] = task
    asyncio.create_task(_run_ocr(c, m, task))

@app.on_message(filters.command("enc"))
async def cmd_enc(c, m: Message):
    if not is_admin(m.from_user.id): return await m.reply("⛔ Unauthorized.")
    chat_id = m.chat.id
    ex = active_tasks.get(chat_id)
    if ex and ex.stage not in (Stage.DONE, Stage.CANCELLED):
        return await m.reply("⚠️ Active task. Use /cancel first.")
    task = new_task(Mode.ENC, chat_id, m.from_user.id)
    active_tasks[chat_id] = task
    asyncio.create_task(_run_enc(c, m, task))

@app.on_message(filters.command("log"))
async def cmd_log(c, m: Message):
    if not is_admin(m.from_user.id): return await m.reply("⛔ Unauthorized.")
    if len(m.command) < 2:
        return await m.reply("⚠️ Usage: `/log <vid_id>`")
    vid_id = m.command[1]; local = DL / f"{vid_id}.mkv"
    if not local.exists():
        return await m.reply("❌ Cached video not found (2-hour TTL expired).")
    chat_id = m.chat.id
    ex = active_tasks.get(chat_id)
    if ex and ex.stage not in (Stage.DONE, Stage.CANCELLED):
        return await m.reply("⚠️ Active task. Use /cancel first.")
    task = new_task(Mode.ENC, chat_id, m.from_user.id)
    task.input_path = local; task.skip_download = True
    active_tasks[chat_id] = task
    asyncio.create_task(_run_enc(c, m, task, skip_dl=True))

@app.on_message(filters.command("cancel"))
async def cmd_cancel(c, m: Message):
    task = active_tasks.get(m.chat.id)
    if not task or task.stage in (Stage.DONE, Stage.CANCELLED):
        return await m.reply("✅ No active task.")
    await do_cancel(task, m)

@app.on_message(filters.command("status"))
async def cmd_status(c, m: Message):
    if not is_admin(m.from_user.id): return await m.reply("⛔ Unauthorized.")
    live = [t for t in active_tasks.values() if t.stage not in (Stage.DONE, Stage.CANCELLED)]
    if not live: return await m.reply("✅ No active tasks.")
    lines = ["📊 **Active Tasks:**\n"]
    for t in live:
        lines.append(f"• `{t.task_id}` [{t.mode.value.upper()}] — **{t.output_name or 'setup…'}**\n"
                     f"  Stage: `{t.stage.name}` | Elapsed: `{fmt_time(time.time() - t.started_at)}`")
    await m.reply("\n".join(lines))

@app.on_message(filters.command("shutdown"))
async def cmd_shutdown(c, m: Message):
    if not is_admin(m.from_user.id): return await m.reply("⛔ Unauthorized.")
    await m.reply("🛑 Shutting down Lightning AI server…")
    deactivate_machine()

# ── /clean Logic (SRT + ASS files) ────────────────────────────────────────────
def _ts_to_sec(ts: str) -> float:
    ts = ts.replace(",", ".")
    h, m, s = ts.split(":")
    return int(h) * 3600 + int(m) * 60 + float(s)

def _sec_to_ts(sec: float) -> str:
    h  = int(sec // 3600)
    m  = int((sec % 3600) // 60)
    s  = sec % 60
    ms = round((s % 1) * 1000)
    return f"{h:02d}:{m:02d}:{int(s):02d},{ms:03d}"

def _ass_time_to_sec(t_str: str) -> float:
    try:
        h, m, s = t_str.split(":")
        return int(h) * 3600 + int(m) * 60 + float(s)
    except: return 0.0

def parse_srt(text: str) -> list:
    cues = []
    for block in re.split(r"\n\s*\n", text.strip()):
        ls = block.strip().split("\n")
        if len(ls) < 3: continue
        try: int(ls[0].strip())
        except ValueError: continue
        mt = re.match(r"(\d{2}:\d{2}:\d{2}[,.]\d+)\s*-->\s*(\d{2}:\d{2}:\d{2}[,.]\d+)", ls[1].strip())
        if not mt: continue
        body = "\n".join(ls[2:]).strip()
        if body:
            cues.append({"start": _ts_to_sec(mt.group(1)), "end": _ts_to_sec(mt.group(2)), "text": body})
    return cues

def cues_to_srt(cues: list) -> str:
    out = []
    for i, c in enumerate(cues, 1):
        out.append(f"{i}\n{_sec_to_ts(c['start'])} --> {_sec_to_ts(c['end'])}\n{c['text']}\n")
    return "\n".join(out)

def clean_srt_file(text: str, wm_min_dur: float = 300.0) -> tuple:
    from collections import defaultdict
    cues = parse_srt(text)
    if not cues: return text, 0, 0
    dur_map: dict = defaultdict(float)
    for c in cues: dur_map[_norm(c["text"])] += c["end"] - c["start"]
    blacklist = {k for k, v in dur_map.items() if k and v >= wm_min_dur}
    cleaned = [c for c in cues if _norm(c["text"]) not in blacklist]
    return cues_to_srt(cleaned), len(cues), len(cleaned)

def clean_ass_file(text: str, wm_min_dur: float = 300.0) -> tuple:
    lines = text.splitlines()
    from collections import defaultdict
    dur_map = defaultdict(float)
    
    for line in lines:
        if line.startswith("Dialogue:"):
            parts = line.split(",", 9)
            if len(parts) >= 10:
                start_sec = _ass_time_to_sec(parts[1].strip())
                end_sec   = _ass_time_to_sec(parts[2].strip())
                raw_txt   = parts[9]
                clean_txt = _norm(re.sub(r"\{.*?\}", "", raw_txt).replace("\\N", ""))
                if len(clean_txt) >= 3: 
                    dur_map[clean_txt] += (end_sec - start_sec)
                
    blacklist = {k for k, v in dur_map.items() if k and v >= wm_min_dur}
    
    out_lines = []
    removed = 0
    total = 0
    
    for line in lines:
        if line.startswith("Dialogue:"):
            total += 1
            parts = line.split(",", 9)
            if len(parts) >= 10:
                raw_txt   = parts[9]
                clean_txt = _norm(re.sub(r"\{.*?\}", "", raw_txt).replace("\\N", ""))
                if clean_txt in blacklist:
                    removed += 1
                    continue
        out_lines.append(line)
        
    return "\n".join(out_lines), total, total - removed

@app.on_message(filters.command("clean") & filters.private)
async def cmd_clean(c, m: Message):
    reply = m.reply_to_message
    if not reply or not reply.document:
        return await m.reply("↩️ Reply to a **.srt** or **.ass** file with `/clean`")
    
    fname = (reply.document.file_name or "").lower()
    if not (fname.endswith(".srt") or fname.endswith(".ass")):
        return await m.reply("Only **.srt** and **.ass** files are supported.")
    
    msg  = await m.reply("⚙️ Cleaning file…")
    path = Path(await reply.download())
    try:
        raw = path.read_text(encoding="utf-8", errors="replace")
        
        if fname.endswith(".srt"):
            cleaned, b, a = clean_srt_file(raw)
            encoding_out = "utf-8"
        else:
            cleaned, b, a = clean_ass_file(raw)
            encoding_out = "utf-8-sig" # Keeps BOM for ASS files
            
        saved = b - a
        out   = path.with_stem(path.stem + "_clean")
        out.write_text(cleaned, encoding=encoding_out)
        
        await msg.edit(f"✅ **Done** — {b} → {a} cues (**{saved}** removed)")
        await c.send_document(m.chat.id, str(out), caption=f"🧹 {out.name}")
    except Exception as e:
        await msg.edit(f"❌ {e}")
    finally:
        path.unlink(missing_ok=True)

# ── Universal message router ──────────────────────────────────────────────────
@app.on_message(~filters.command(["ocr","enc","log","cancel","start","status","shutdown", "clean"]))
async def msg_router(c, m: Message):
    if not m.from_user or m.from_user.is_bot: return

    task = active_tasks.get(m.chat.id)
    if not task or task.stage in (Stage.DONE, Stage.CANCELLED): return

    s = task.stage
    if s == Stage.AWAIT_SRC:
        is_video = (m.video or (m.document and m.document.mime_type and "video" in m.document.mime_type))
        if is_video:
            if not task.src_future.done(): task.src_future.set_result(("tg", m))
        elif m.text:
            url_m = re.search(r"(https?://\S+)", m.text)
            if url_m and not task.src_future.done(): task.src_future.set_result(("url", url_m.group(1)))

    elif s == Stage.AWAIT_CUT and m.text:
        if not task.cut_future.done(): task.cut_future.set_result(m.text.strip())

    elif s == Stage.AWAIT_SUB and m.document:
        fname = m.document.file_name or ""
        if fname.lower().endswith((".srt", ".ass", ".ssa", ".vtt")):
            if not task.subtitle_future.done(): task.subtitle_future.set_result(m)
        else:
            await m.reply("⚠️ Send a subtitle file: `.srt` `.ass` `.ssa` `.vtt`")

    elif s == Stage.AWAIT_NAME and m.text:
        if not task.name_future.done(): task.name_future.set_result(m.text.strip())

    elif s == Stage.AWAIT_THUMB:
        if m.photo:
            if not task.thumb_future.done(): task.thumb_future.set_result(m)
        elif m.text and m.text.strip().lower() in ("skip", "s", "/skip"):
            if not task.thumb_future.done(): task.thumb_future.set_result("SKIP")

# ── Callback handler ──────────────────────────────────────────────────────────
@app.on_callback_query()
async def on_callback(c, q: CallbackQuery):
    data   = q.data or ""; parts = data.split(":", 2); action = parts[0]

    if action == "cancel_active":
        task = active_tasks.get(q.message.chat.id)
        if task:
            task.cancel_flag.set(); _cancel_all_futures(task)
            await q.answer("🚫 Stopping…", show_alert=True)
        else: await q.answer("No active task.", show_alert=False)

    elif action in ("start", "cancel") and len(parts) >= 2:
        tid  = parts[1]
        task = next((t for t in active_tasks.values() if t.task_id == tid), None)
        if not task: return await q.answer("Task not found.", show_alert=True)
        if q.from_user.id != task.user_id and not is_admin(q.from_user.id):
            return await q.answer("Not your task.", show_alert=True)
        try: await q.message.edit_reply_markup(None)
        except: pass
        if action == "start":
            await q.answer("▶️ Starting!")
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

    elif action == "power_off":
        if not is_admin(q.from_user.id): return await q.answer("❌ Access Denied.", show_alert=True)
        await q.edit_message_text("🛑 Shutting down Lightning AI server…")
        deactivate_machine()

    elif action == "power_on":
        await q.edit_message_text("✅ Server kept alive. Use `/shutdown` when finished.")

# ══════════════════════════════════════════════════════════════════════════════
#  OCR pipeline  (/ocr)
# ══════════════════════════════════════════════════════════════════════════════
async def _run_ocr(c, m: Message, task: Task):
    chat_id = task.chat_id
    status  = await m.reply("⏳ Initializing full-frame OCR pipeline…", reply_markup=CANCEL_BTN)
    task.status_msg = status
    try:
        # 1. Download
        task.stage = Stage.DOWNLOADING
        video_path = await _download_video(c, m, task, status)
        if not video_path: return await safe_edit(status, "❌ No video received or download failed.")
        if task.cancel_flag.is_set(): raise InterruptedError()
        task.input_path = video_path
        await extract_meta(task)

        # 2. Cut times
        task.stage = Stage.AWAIT_CUT
        dur = get_real_duration(str(task.input_path)) or task.duration_s
        await safe_edit(status,
            f"✅ **Downloaded:** `{task.input_path.name}`\n"
            f"📐 `{task.src_width}×{task.src_height}` | `{fmt_time(dur)}`\n\n"
            "⏱ **Send cut times** (seconds):\n"
            "• `120 240` — process 120 s → 240 s\n"
            "• `120 120` — skip 120 s from each end\n"
            "• `all`     — entire video", CANCEL_BTN)
        try: cut_text = await asyncio.wait_for(task.cut_future, timeout=300)
        except (asyncio.TimeoutError, asyncio.CancelledError): return await safe_edit(status, "⏰ Timed out.")
        if task.cancel_flag.is_set(): raise InterruptedError()

        start_sec = end_sec = None
        if cut_text.strip().lower() != "all":
            try:
                p = cut_text.strip().split()
                v1, v2 = float(p[0]), float(p[1])
                if v2 < 0:     start_sec, end_sec = v1, dur + v2
                elif v1 >= v2: start_sec, end_sec = v1, dur - v2
                else:          start_sec, end_sec = v1, v2
                if start_sec >= end_sec or start_sec < 0 or end_sec > dur:
                    return await safe_edit(status, f"❌ Invalid cut times. Duration: `{int(dur)} s`. Got: `{start_sec:.1f} → {end_sec:.1f} s`.")
            except:
                return await safe_edit(status, "❌ Format: `start end` or `all`.")

        # 3. Full-frame OCR
        task.stage = Stage.OCR_RUNNING
        await safe_edit(status, "⚡ **Multi-Process OCR running…**\nRTX 6000 Ada scanning frames…", CANCEL_BTN)
        ocr_result = await asyncio.to_thread(
            run_ocr_pipeline, str(task.input_path), status, chat_id,
            start_sec, end_sec, lambda: task.cancel_flag.is_set()
        )
        if task.cancel_flag.is_set(): raise InterruptedError()

        final_subs, ocr_w, ocr_h = ocr_result
        task.ocr_subs    = final_subs
        task.ocr_frame_w = ocr_w
        task.ocr_frame_h = ocr_h

        if not final_subs: return await safe_edit(status, "⚠️ No text detected in specified range.")

        # 4. Prepare Text for Translation
        base     = str(task.work_dir / task.input_path.stem)
        zh_texts = [s["text"] for s in final_subs]

        # 5. ChatGPT translation
        en_texts: list[str] = []
        if OPENAI_KEY and _HAS_OPENAI:
            await safe_edit(status, "🌐 Translating via ChatGPT…", CANCEL_BTN)
            en_texts = await batch_translate(zh_texts, status, chat_id)
            if task.cancel_flag.is_set(): raise InterruptedError()
        else:
            en_texts = [""] * len(final_subs)

        # 6. Write Requested ASS format
        final_ass_path = base + "_translated.ass"
        write_smart_ass(final_subs, en_texts, final_ass_path, ocr_w, ocr_h, task.src_width, task.src_height)

        with open(final_ass_path, "r", encoding="utf-8-sig", errors="replace") as f:
            ass_raw = f.read()

        ass_merged = merge_ass_dialogues(ass_raw, max_gap=0.90)

        with open(final_ass_path, "w", encoding="utf-8-sig") as f:
            f.write(ass_merged)

        await m.reply_document(final_ass_path, caption=f"📄 Final Subtitles (.ass) — {len(final_subs)} cues\nCustom Fixed Layout.")
        # 7. Wait for mux subtitle
        task.stage = Stage.AWAIT_SUB
        await safe_edit(status,
            "✅ **OCR + Translation complete!**\n\n"
            "📎 Send the subtitle to mux (Use the `.ass` generated above).\n"
            "_Or send a custom `.ass` / `.srt` file._", CANCEL_BTN)
        try: sub_msg = await asyncio.wait_for(task.subtitle_future, timeout=600)
        except (asyncio.TimeoutError, asyncio.CancelledError): return await safe_edit(status, "⏰ Timed out.")
        if task.cancel_flag.is_set(): raise InterruptedError()

        await safe_edit(status, "📥 Downloading subtitle…", CANCEL_BTN)
        sub_dl = task.work_dir / (sub_msg.document.file_name or "subtitle.ass")
        await sub_msg.download(file_name=str(sub_dl))
        task.subtitle_path = sub_dl

        # 8. Output name
        task.stage = Stage.AWAIT_NAME
        await safe_edit(status, "📝 Enter output filename:\n_(e.g. `Way Of Choices EP01`)_", CANCEL_BTN)
        try: name_raw = await asyncio.wait_for(task.name_future, timeout=300)
        except (asyncio.TimeoutError, asyncio.CancelledError): return await safe_edit(status, "⏰ Timed out.")
        if task.cancel_flag.is_set(): raise InterruptedError()
        parse_name(name_raw, task)

        # 9. Thumbnail
        task.stage = Stage.AWAIT_THUMB
        await safe_edit(status, "🖼 Send **thumbnail** (or type `skip`):", CANCEL_BTN)
        try: thumb_res = await asyncio.wait_for(task.thumb_future, timeout=120)
        except (asyncio.TimeoutError, asyncio.CancelledError): thumb_res = "SKIP"
        if task.cancel_flag.is_set(): raise InterruptedError()
        if thumb_res != "SKIP":
            tp = await thumb_res.download(file_name=str(task.work_dir / "thumb.jpg"))
            task.thumb_path = Path(tp)

        # 10. Mux
        task.stage = Stage.MUXING
        await safe_edit(status, "🔧 Muxing subtitle into video…", CANCEL_BTN)
        mux_out = task.work_dir / f"{task.raw_name}.mkv"
        await mux_video(task, task.subtitle_path, mux_out)
        if task.cancel_flag.is_set(): raise InterruptedError()
        task.muxed_path = mux_out
        task.input_path = mux_out
        await extract_meta(task)

        # 11. Upload mux + encode simultaneously
        task.stage  = Stage.ENCODING
        target_chat = resolve_channel(task.series_name) or chat_id
        await safe_edit(status, f"🚀 **Mux done!** Starting upload + encode…\n`{task.output_name}`")

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
        await safe_edit(status, f"🏁 **All done!** `{task.output_name}`\nTotal: `{fmt_time(time.time() - task.started_at)}`")
        await m.reply("🎉 Job complete! Shut down the server?", reply_markup=_POWER_KB)

    except InterruptedError:
        await safe_edit(status, "🚫 **Task Cancelled.**")
    except Exception as e:
        log.exception("OCR pipeline crashed")
        tb  = traceback.format_exc()
        buf = io.BytesIO(tb.encode()); buf.name = f"error_{task.task_id}.log"; buf.seek(0)
        await m.reply_document(buf, caption=f"❌ **Crash:** `{e}`")
    finally:
        cleanup_task(task)

# ══════════════════════════════════════════════════════════════════════════════
#  ENC pipeline  (/enc and /log)
# ══════════════════════════════════════════════════════════════════════════════
async def _run_enc(c, m: Message, task: Task, skip_dl: bool = False):
    chat_id = task.chat_id
    status  = await m.reply("⏳ Initializing encoder…", reply_markup=CANCEL_BTN)
    task.status_msg = status
    try:
        if not skip_dl:
            task.stage = Stage.DOWNLOADING
            video_path = await _download_video(c, m, task, status)
            if not video_path: return await safe_edit(status, "❌ No video received.")
            if task.cancel_flag.is_set(): raise InterruptedError()
            task.input_path = video_path
            vid_id  = uuid.uuid4().hex[:8]
            cache_p = DL / f"{vid_id}.mkv"
            try:
                shutil.copy2(str(task.input_path), str(cache_p))
                asyncio.create_task(delayed_delete(str(cache_p), 7200))
                await m.reply(f"✅ Download complete!\n\n💾 **Video ID:** `{vid_id}`\nCached 2 h. Re-encode anytime:\n`/log {vid_id}`")
            except Exception: pass

        await extract_meta(task)

        task.stage = Stage.AWAIT_NAME
        await m.reply(f"✅ Ready: `{task.input_path.name}`\n📐 `{task.src_width}×{task.src_height}` | `{fmt_time(task.duration_s)}`\n\n📝 **Enter base filename** (e.g. `Way Of Choices EP01`):")
        try: name_raw = await asyncio.wait_for(task.name_future, timeout=300)
        except (asyncio.TimeoutError, asyncio.CancelledError): return await m.reply("⏰ Timed out.")
        if task.cancel_flag.is_set(): raise InterruptedError()
        parse_name(name_raw, task)

        task.stage = Stage.AWAIT_THUMB
        await m.reply("🖼 **Send thumbnail** (or type `skip`):")
        try: thumb_res = await asyncio.wait_for(task.thumb_future, timeout=120)
        except (asyncio.TimeoutError, asyncio.CancelledError): thumb_res = "SKIP"
        if task.cancel_flag.is_set(): raise InterruptedError()
        if thumb_res == "SKIP":
            tp = str(task.work_dir / "thumb.jpg")
            await asyncio.to_thread(extract_thumbnail, str(task.input_path), tp)
            if os.path.exists(tp): task.thumb_path = Path(tp)
        else:
            tp = await thumb_res.download(file_name=str(task.work_dir / "thumb.jpg"))
            task.thumb_path = Path(tp)

        task.stage = Stage.CONFIRMING
        ch_id  = resolve_channel(task.series_name)
        lines  = ["📋 **Confirm encode job (hevc_nvenc p7 + multipass):**\n"]
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

        task.stage  = Stage.ENCODING
        target_chat = ch_id or task.chat_id
        await m.reply(f"🚀 **Encoding started!** `{task.output_name}`\nThree progress messages will appear below ↓")
        await encode_all(task, m, target_chat)

        task.stage = Stage.DONE
        await m.reply(f"🏁 **All done!** `{task.output_name}`\nTotal: `{fmt_time(time.time() - task.started_at)}`")
        await m.reply("🎉 Job complete! Shut down the server?", reply_markup=_POWER_KB)

    except InterruptedError:
        await safe_edit(status, "🚫 **Task Cancelled.**")
    except Exception as e:
        log.exception("ENC pipeline crashed")
        tb  = traceback.format_exc()
        buf = io.BytesIO(tb.encode()); buf.name = f"error_{task.task_id}.log"; buf.seek(0)
        await m.reply_document(buf, caption=f"❌ **Crash:** `{e}`")
    finally:
        cleanup_task(task)

# ══════════════════════════════════════════════════════════════════════════════
#  aiohttp Telegram stream server
# ══════════════════════════════════════════════════════════════════════════════
_stream_runner = None

async def _stream_handler(request):
    chat_id    = int(request.match_info["chat_id"])
    message_id = int(request.match_info["message_id"])
    try: msg = await app.get_messages(chat_id, message_id)
    except Exception as e: raise _web.HTTPNotFound(reason=str(e))
    media = msg.document or msg.video
    if not media: raise _web.HTTPNotFound(reason="No media")
    fsize = media.file_size
    rng   = request.headers.get("Range", "")
    offset, limit, code = 0, None, 200
    if rng.startswith("bytes="):
        try:
            r      = rng[6:].split("-")
            offset = int(r[0]) if r[0] else 0
            end    = int(r[1]) if len(r) > 1 and r[1] else fsize - 1
            limit  = end - offset + 1; code = 206
        except: pass
    headers = {"Content-Type": "application/octet-stream", "Content-Length": str(limit or fsize), "Accept-Ranges": "bytes"}
    if code == 206: headers["Content-Range"] = (f"bytes {offset}-{offset + (limit or fsize) - 1}/{fsize}")
    resp = _web.StreamResponse(status=code, headers=headers)
    await resp.prepare(request)
    try:
        async for chunk in app.stream_media(msg, offset=offset, limit=limit):
            await resp.write(chunk)
            if resp.task and resp.task.cancelled(): break
    except Exception as e: log.warning(f"Stream error: {e}")
    await resp.write_eof()
    return resp

async def start_stream_server():
    global _stream_runner
    srv = _web.Application()
    srv.router.add_get("/stream/{chat_id}/{message_id}", _stream_handler)
    _stream_runner = _web.AppRunner(srv)
    await _stream_runner.setup()
    await _web.TCPSite(_stream_runner, "127.0.0.1", 8181).start()
    log.info("Telegram stream server on :8181")

# ══════════════════════════════════════════════════════════════════════════════
#  Entry point
# ══════════════════════════════════════════════════════════════════════════════
async def main():
    global EVENT_LOOP
    EVENT_LOOP = asyncio.get_running_loop()
    log.info("TheFrictionRealm Lightning AI Bot v4.4 — starting…")
    log.info(f"GPUs: {NUM_GPUS} | Encode semaphore: 3 concurrent | Full-frame OCR")
    await app.start()
    await start_stream_server()
    log.info("✅ Bot ready!")
    await idle()
    await app.stop()

if __name__ == "__main__":
    asyncio.run(main())
