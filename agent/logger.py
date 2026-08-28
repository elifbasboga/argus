"""
logger.py — Argus dual-channel RFC5424 logging.

İki ayrı log dosyası tutar:
  logs/argus_heartbeat.log  -> sistem/telemetri kanalı (heartbeat send+response)
  logs/argus_commands.log   -> komut çalıştırma kanalı (komut+sonuç)

Her satır tek satırda istek+cevabı birlikte taşır, örnek:
<134>1 2026-08-19T13:47:15.003Z host ArgusAgent 8201 HEARTBEAT
    [req method="POST" path="/heartbeat"] [res status="200" pending="1"]
    POST /heartbeat -> 200 OK
"""

import os
import socket
import threading
from datetime import datetime, timezone
from pathlib import Path

HOSTNAME = socket.gethostname()
APP_NAME = "ArgusAgent"
AGENT_PID = str(os.getpid())

BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)
HEARTBEAT_LOG = LOG_DIR / "argus_heartbeat.log"
COMMANDS_LOG = LOG_DIR / "argus_commands.log"

FACILITY_USER = 1
SEVERITY_INFO = 6
SEVERITY_WARNING = 4
SEVERITY_ERROR = 3

_log_lock = threading.Lock()


def _sd_block(sd_id: str, fields: dict) -> str:
    pairs = " ".join(f'{k}="{v}"' for k, v in fields.items())
    return f"[{sd_id} {pairs}]"


def _write_log(path: Path, line: str) -> None:
    with _log_lock:
        with open(path, "a") as f:
            f.write(line + "\n")


def log_event(path: Path, msgid: str, req_fields: dict, res_fields: dict, summary: str, severity: int = SEVERITY_INFO) -> str:
    """
    Bir olayı tek satırda, istek+cevap birlikte, RFC5424 formatında yazar.
    Not (düzeltme): [req] ve [res] STRUCTURED-DATA elemanları RFC5424'e göre
    BİTİŞİK yazılmalı (spec'te ardışık SD-ELEMENT'ler arasında ayraç yoktur,
    sadece STRUCTURED-DATA ile MSG arasında tek boşluk vardır). Önceki sürüm
    araya boşluk koyuyordu, bu düzeltildi.
    """
    pri = FACILITY_USER * 8 + severity
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    sd = f"{_sd_block('req', req_fields)}{_sd_block('res', res_fields)}"
    safe_summary = summary.replace("\n", " ").strip()
    line = f"<{pri}>1 {timestamp} {HOSTNAME} {APP_NAME} {AGENT_PID} {msgid} {sd} {safe_summary}"
    _write_log(path, line)
    return line


def log_reject(path: Path, msgid: str, req_fields: dict, reason: str, summary: str) -> str:
    """Reddedilen bir isteği (bilinmeyen komut/task) WARNING seviyesinde loglar."""
    pri = FACILITY_USER * 8 + SEVERITY_WARNING
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    sd = f"{_sd_block('req', req_fields)}{_sd_block('res', {'status': 'rejected', 'reason': reason})}"
    safe_summary = summary.replace("\n", " ").strip()
    line = f"<{pri}>1 {timestamp} {HOSTNAME} {APP_NAME} {AGENT_PID} {msgid} {sd} {safe_summary}"
    _write_log(path, line)
    return line
