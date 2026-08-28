"""
logger.py — Argus Server RFC5424 loglama.

Agent tarafındaki logger.py ile aynı format ve davranış: satırlar SADECE
dosyaya yazılıyor, konsola basılmıyor (uvicorn'un kendi kısa "POST
/heartbeat 200 OK" satırları konsolu meşgul etsin diye — detaylı RFC5424
kayıtları için ilgili log dosyasını `tail -f` ile izle).

İki ayrı log dosyası tutulur (agent'taki dual-channel deseniyle aynı fikir):
  logs/argus_server_events.log -> genel sunucu olayları (heartbeat kaydı,
                                    cleanup, komut sonucu, kuyruk, config vs.)
  logs/kill_audit.log          -> SADECE kill onay zincirinin adımları
                                    (istek/onay/red/dispatch/delivered/sonuç).
                                    Dashboard'da "kill_audit.log" olarak
                                    gösterilen şey artık gerçekten var olan,
                                    diskten okunan bir dosya — önceden bu isim
                                    altında sadece bellek-içi bir JSON listesi
                                    gösteriliyordu, gerçek dosya yoktu.

RFC5424 satır gövdesi:
  <PRI>1 TIMESTAMP HOSTNAME APP-NAME PROCID MSGID [req ...][res ...] MSG

Not (düzeltme): iki STRUCTURED-DATA elemanı ([req] ve [res]) RFC5424'e göre
BİTİŞİK yazılmalı — spec'te ardışık SD-ELEMENT'ler arasında ayraç yok, sadece
STRUCTURED-DATA ile MSG arasında tek boşluk var. Önceki sürüm "[req ...] [res
...]" şeklinde araya boşluk koyuyordu, bu düzeltildi.
"""

import os
import socket
import threading
from datetime import datetime, timezone
from pathlib import Path

HOSTNAME = socket.gethostname()
APP_NAME = "ArgusServer"
SERVER_PID = str(os.getpid())

BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)
EVENTS_LOG = LOG_DIR / "argus_server_events.log"
KILL_LOG = LOG_DIR / "kill_audit.log"

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
    """Tek satırda, istek+sonuç birlikte, RFC5424 formatında `path`e yazar (konsola basmaz)."""
    pri = FACILITY_USER * 8 + severity
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    sd = f"{_sd_block('req', req_fields)}{_sd_block('res', res_fields)}"
    safe_summary = summary.replace("\n", " ").strip()
    line = f"<{pri}>1 {timestamp} {HOSTNAME} {APP_NAME} {SERVER_PID} {msgid} {sd} {safe_summary}"
    _write_log(path, line)
    return line


def log_reject(path: Path, msgid: str, req_fields: dict, reason: str, summary: str) -> str:
    """Reddedilen bir isteği (örn. geçersiz kill onayı) WARNING seviyesinde loglar."""
    pri = FACILITY_USER * 8 + SEVERITY_WARNING
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    sd = f"{_sd_block('req', req_fields)}{_sd_block('res', {'status': 'rejected', 'reason': reason})}"
    safe_summary = summary.replace("\n", " ").strip()
    line = f"<{pri}>1 {timestamp} {HOSTNAME} {APP_NAME} {SERVER_PID} {msgid} {sd} {safe_summary}"
    _write_log(path, line)
    return line
