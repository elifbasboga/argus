#!/usr/bin/env python3
"""
agent.py — Argus Agent ana çalışma döngüsü.

Bu dosya artık sadece şunu yapar: konfigürasyon + heartbeat loop.
Loglama -> logger.py, veri toplama -> collectors.py,
komut çalıştırma/allowlist -> dispatcher.py içinde.

Çalıştırma: sudo python3 agent.py
"""

import getpass
import os
import time
from datetime import datetime, timezone

import psutil
import requests

from logger import HEARTBEAT_LOG, COMMANDS_LOG, HOSTNAME, AGENT_PID, log_event, SEVERITY_ERROR
from collectors import collect_system_info, get_computer_name, get_local_ip
from dispatcher import dispatch_command
from agent_config import load_agent_config

_AGENT_CONFIG = load_agent_config()
SERVER_URL = _AGENT_CONFIG["server_url"]

# Sunucunun aynı cihazı ağ değişse bile tanıyabilmesi için sabit kimlik —
# bkz. agent_config.py. Bir kere üretilip agent_config.json'a yazılır,
# bundan sonra her heartbeat'te aynı değer gönderilir.
ENDPOINT_ID = _AGENT_CONFIG["endpoint_id"]
# Config'den okunuyor (varsayılan 3sn) — server_config.json'daki
# cleanup_interval_hours gibi, artık kodu değiştirmeden ayarlanabilir.
HEARTBEAT_INTERVAL = _AGENT_CONFIG.get("heartbeat_interval_seconds", 3)

# Sunucu erişilemez olduğunda kullanılan sabit tekrar deneme aralığı.
# Katlanarak artan (exponential) backoff yerine bilinçli olarak sabit
# tutuldu — basit, öngörülebilir, mentöre anlatması kolay: "her zaman
# aynı sürede tekrar dener."
RETRY_INTERVAL = HEARTBEAT_INTERVAL  # saniye

# Sunucuya gönderilemeyen komut sonuçları burada birikir.
# Bir sonraki başarılı heartbeat'te toplu olarak gönderilmeye çalışılır.
# Böylece geçici bir ağ kesintisinde hiçbir komut sonucu kaybolmaz.
_pending_result_queue: list[dict] = []


def calculate_retry_delay(consecutive_failures: int) -> float:
    """
    Bir sonraki denemeye kadar beklenecek süreyi döner. Sabit aralık —
    ardışık başarısızlık sayısına göre büyümez. Parametre yine de
    alınıyor (loglama ve olası ileri seviye ayarlar için), ama sonucu
    etkilemiyor. Saf fonksiyon — dış bağımlılık yok, testte gerçek
    ağ/sudo olmadan doğrulanabilir.
    """
    return RETRY_INTERVAL


def send_heartbeat() -> dict | None:
    payload = {
        # Kimlik: endpoint_id SABİT (ağ/VM değişse bile aynı kalır, dashboard
        # "aynı cihaz" eşleştirmesini buna göre yapar). computer_name de
        # görece stabil (kullanıcı elle değiştirmediği sürece). hostname ve
        # local_ip ise o anki ağa göre değişebilir, sadece bilgi amaçlı.
        "endpoint_id": ENDPOINT_ID,
        "hostname": HOSTNAME,
        "computer_name": get_computer_name(),
        "local_ip": get_local_ip(),
        "user": getpass.getuser(),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "system_info": collect_system_info(),
        "process_count": len(psutil.pids()),
    }
    req_fields = {"method": "POST", "path": "/heartbeat", "interval": f"{HEARTBEAT_INTERVAL}s"}

    try:
        resp = requests.post(f"{SERVER_URL}/heartbeat", json=payload, timeout=5)
        resp.raise_for_status()
        data = resp.json()
        pending = len(data.get("pending_commands", []))
        log_event(
            HEARTBEAT_LOG, "HEARTBEAT", req_fields,
            {"status": str(resp.status_code), "pending": str(pending)},
            f"POST /heartbeat -> {resp.status_code} OK ({pending} pending)",
        )
        return data
    except requests.RequestException as e:
        log_event(
            HEARTBEAT_LOG, "HEARTBEAT", req_fields,
            {"status": "ERR", "detail": str(e)[:120]},
            "POST /heartbeat -> FAILED",
            severity=SEVERITY_ERROR,
        )
        return None


def send_command_result(result: dict) -> bool:
    """Tek bir komut sonucunu göndermeyi dener. Başarılıysa True döner."""
    try:
        requests.post(f"{SERVER_URL}/command_result", json=result, timeout=5)
        return True
    except requests.RequestException as e:
        log_event(
            COMMANDS_LOG, "CMD-RESULT-SEND", {"command": result.get("command", "?")},
            {"status": "ERR", "detail": str(e)[:120]},
            "POST /command_result -> FAILED, queued for retry",
            severity=SEVERITY_ERROR,
        )
        return False


def flush_pending_results():
    """
    Kuyrukta bekleyen (daha önce gönderilemeyen) komut sonuçlarını
    tek bir istekte toplu olarak göndermeyi dener. Bu, yalnızca
    heartbeat başarılı olduktan sonra (yani sunucuya ulaşılabildiği
    kesinleştikten sonra) çağrılır.
    """
    global _pending_result_queue
    if not _pending_result_queue:
        return

    count = len(_pending_result_queue)
    try:
        requests.post(
            f"{SERVER_URL}/command_results/batch",
            # endpoint_id: her result'ta zaten var (yukarıda eklendi), ama
            # server.py batch düzeyinde de bir fallback okuyor
            # (bkz. server.py command_results_batch / batch_endpoint_id) —
            # aynı bilgiyi burada da vermek zarar vermez, ekstra güvenlik.
            json={"results": _pending_result_queue, "endpoint_id": ENDPOINT_ID},
            timeout=5,
        )
        log_event(
            COMMANDS_LOG, "CMD-RESULT-FLUSH", {"queued": str(count)},
            {"status": "ok"},
            f"flushed {count} queued result(s) to server",
        )
        _pending_result_queue = []
    except requests.RequestException as e:
        log_event(
            COMMANDS_LOG, "CMD-RESULT-FLUSH", {"queued": str(count)},
            {"status": "ERR", "detail": str(e)[:120]},
            f"batch flush failed, {count} result(s) still queued",
            severity=SEVERITY_ERROR,
        )


def run_agent_loop():
    print(f"[Argus Agent] running (pid {AGENT_PID}) — hostname={HOSTNAME}")
    print(f"[Argus Agent] logs: {HEARTBEAT_LOG}  |  {COMMANDS_LOG}")
    print("[Argus Agent] terminal is intentionally quiet — tail -f the log files to watch activity.")

    consecutive_failures = 0

    while True:
        response = send_heartbeat()

        if response is not None:
            if consecutive_failures > 0:
                log_event(
                    HEARTBEAT_LOG, "HEARTBEAT-RECOVERED", {"consecutive_failures": str(consecutive_failures)},
                    {"status": "ok"},
                    f"connection recovered after {consecutive_failures} failed attempt(s)",
                )
            consecutive_failures = 0

            # Önce daha önce gönderilemeyen sonuçları toplu olarak boşaltmayı dene
            flush_pending_results()

            if response.get("pending_commands"):
                for command in response["pending_commands"]:
                    result = dispatch_command(command)
                    # Sunucu (_resolve_result_client_id, server.py) sonucu doğru
                    # cihaza yazabilmek için önce burada bir endpoint_id arıyor;
                    # bulamazsa "en son heartbeat atan client" gibi yanlış bir
                    # varsayıma düşüyor (çoklu cihaz senaryosunda bu, bir
                    # cihazın process/sistem verisinin başka bir cihazın
                    # kartında görünmesine yol açar). dispatcher.py henüz bunu
                    # eklemediği için burada, gönderilmeden/kuyruklanmadan
                    # hemen önce ekliyoruz — setdefault kullanıyoruz ki
                    # dispatcher.py ileride kendi endpoint_id'sini eklerse
                    # onu ezmesin.
                    result.setdefault("endpoint_id", ENDPOINT_ID)
                    if not send_command_result(result):
                        _pending_result_queue.append(result)
            time.sleep(HEARTBEAT_INTERVAL)
        else:
            consecutive_failures += 1
            delay = calculate_retry_delay(consecutive_failures)
            log_event(
                HEARTBEAT_LOG, "HEARTBEAT-RETRY",
                {"consecutive_failures": str(consecutive_failures)},
                {"status": "waiting", "next_retry_in": f"{delay}s"},
                f"server unreachable, retry #{consecutive_failures} in {delay}s",
                severity=SEVERITY_ERROR,
            )
            time.sleep(delay)


if __name__ == "__main__":
    if os.geteuid() != 0:
        print("Warning: not running as root. Some collectors/tasks may fail. "
              "Run with: sudo python3 agent.py")
    run_agent_loop()
