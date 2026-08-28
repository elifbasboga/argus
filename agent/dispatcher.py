"""
dispatcher.py — Argus Command Dispatcher (çekirdek güvenlik sınırı).

Sunucudan gelen komutlar YALNIZCA burada tanımlı COMMAND_REGISTRY ve
TASK_REGISTRY sözlükleri üzerinden isim eşleştirmesiyle çalıştırılır.
Sunucu hiçbir zaman kod veya shell string göndermez — yalnızca bir
komut adı ve doğrulanacak parametreler gönderir. Eşleşme yoksa istek
reddedilir ve hiçbir şey OS katmanına ulaşmaz.

Sonuç formatı: her komut çalıştırıldığında dispatch_command şu şekli
döner: {"command": "...", "status": "ok/error/rejected", "data": {...}}.
"data" alanı, komutun ürettiği her şeyi kapsar. Bunu bilerek normalize
ediyoruz — heartbeat'in kendi telemetri gövdesindeki alanlarla
(örn. "system_info") isim çakışması olmasın, sunucu tarafında
"bu bir komut sonucudur" her zaman netçe ayırt edilsin diye.

Day 7 (çoklu client): dönen sözlüğe artık "endpoint_id" alanı da ekleniyor
(bkz. _ENDPOINT_ID / _resolve_endpoint_id aşağıda). agent.py bu sözlüğü
olduğu gibi /command_result (ya da /command_results/batch içindeki
"results" listesine) POST ediyorsa, sunucu artık HANGİ command_result'ın
HANGİ fiziksel cihazdan geldiğini "en son heartbeat atan client" gibi
belirsiz bir varsayıma değil, doğrudan bu endpoint_id'ye bakarak
çözebiliyor (bkz. server/server.py _resolve_result_client_id). Birden
fazla agent aynı anda aktifken bu, sonuçların yanlış cihaza yazılmasını
önleyen tek güvenilir yol.
"""

import json
import subprocess
from datetime import datetime

import psutil

from logger import (
    COMMANDS_LOG, HEARTBEAT_LOG, log_event, log_reject,
    SEVERITY_INFO, SEVERITY_WARNING, SEVERITY_ERROR,
)
from collectors import (
    collect_process_list, collect_system_info, check_disk_space, get_network_interfaces,
)


def _resolve_endpoint_id():
    """
    agent_config.py'den bu agent'ın kalıcı endpoint_id'sini okur — heartbeat'in
    zaten gönderdiği AYNI kimlik (bkz. server/server.py Heartbeat modeli ve
    ARGUS-xxxxxxxxxxxx formatı, dashboard'daki "ENDPOINT ID" alanı).

    agent_config.py'nin tam arayüzünü (fonksiyon/değişken adı) elimde
    görmediğim için BURASI KASITLI OLARAK SAVUNMACI: sırayla birkaç
    olası isimlendirmeyi dener, hiçbiri tutmazsa sessizce None döner.
    None dönerse dispatch_command sonuçlarında "endpoint_id" alanı boş
    gider ve sunucu tarafı eski tek-client fallback'ine düşer (bkz.
    server.py _resolve_result_client_id) — yani hiçbir şey KIRILMAZ,
    sadece çoklu-client kazanımı devreye girmemiş olur.

    NOT: agent_config.py'nin gerçek arayüzü bunlardan farklıysa (örn.
    endpoint_id başka bir modülde/dosyada tutuluyorsa), bu fonksiyonu
    doğru isimle güncellemek için agent_config.py'yi paylaşman yeterli.
    """
    try:
        import agent_config as _ac
    except ImportError:
        return None

    for fn_name in ("load_agent_config", "get_agent_config", "get_config", "load_config"):
        fn = getattr(_ac, fn_name, None)
        if callable(fn):
            try:
                cfg = fn()
            except Exception:
                continue
            if isinstance(cfg, dict) and cfg.get("endpoint_id"):
                return cfg["endpoint_id"]
            eid = getattr(cfg, "endpoint_id", None)
            if eid:
                return eid

    for attr_name in ("ENDPOINT_ID", "endpoint_id", "AGENT_CONFIG", "CONFIG"):
        val = getattr(_ac, attr_name, None)
        if isinstance(val, str) and val:
            return val
        if isinstance(val, dict) and val.get("endpoint_id"):
            return val["endpoint_id"]

    get_fn = getattr(_ac, "get_endpoint_id", None)
    if callable(get_fn):
        try:
            eid = get_fn()
            if eid:
                return eid
        except Exception:
            pass

    return None


# Süreç ömrü boyunca bir kez çözülüp önbelleğe alınır — endpoint_id kalıcı
# bir kimlik olduğu için (agent yeniden başlamadıkça değişmez), her komut
# sonucunda tekrar tekrar agent_config'e gitmenin bir anlamı yok.
_ENDPOINT_ID = _resolve_endpoint_id()
if _ENDPOINT_ID is None:
    log_event(
        COMMANDS_LOG, "CMD-INIT", {},
        {"status": "warning", "detail": "endpoint_id resolved to None"},
        "dispatcher agent_config'den endpoint_id okuyamadı — command_result'lar "
        "sunucuda eski tek-client fallback'ine düşecek (bkz. dispatcher.py "
        "_resolve_endpoint_id docstring'i)",
        severity=SEVERITY_WARNING,
    )


# ---------------------------------------------------------------------------
# ACTIVE COMMANDS — spesifik, parametreli, doğrulamalı (generic exec DEĞİL)
# ---------------------------------------------------------------------------
def cmd_kill_process(params: dict) -> dict:
    pid = params.get("pid")
    if not isinstance(pid, int):
        return {"status": "error", "detail": "pid must be an integer", "pid": pid}
    if not psutil.pid_exists(pid):
        return {"status": "error", "detail": f"pid {pid} does not exist", "pid": pid}
    try:
        p = psutil.Process(pid)
        name = p.name()
        p.terminate()
        p.wait(timeout=3)
        return {"status": "ok", "detail": f"terminated pid {pid} ({name})", "pid": pid}
    except Exception as e:
        return {"status": "error", "detail": str(e), "pid": pid}


# ---------------------------------------------------------------------------
# PREDEFINED TASKS — sunucu yalnızca İSİM tetikler, kod göndermez
# ---------------------------------------------------------------------------
def task_flush_dns_cache(params: dict) -> dict:
    try:
        subprocess.run(["dscacheutil", "-flushcache"], check=True, timeout=10)
        subprocess.run(["killall", "-HUP", "mDNSResponder"], check=True, timeout=10)
        return {"status": "ok", "detail": "DNS cache flushed"}
    except Exception as e:
        return {"status": "error", "detail": str(e)}


def task_run_disk_cleanup_report(params: dict) -> dict:
    """Sadece rapor üretir, hiçbir şey silmez."""
    return {"status": "ok", "report": check_disk_space()}


def task_rotate_local_logs(params: dict) -> dict:
    import shutil
    rotated = []
    for log_file in (HEARTBEAT_LOG, COMMANDS_LOG):
        if log_file.exists():
            stamp = datetime.now().strftime("%Y%m%d%H%M%S")
            new_name = log_file.with_name(f"{log_file.stem}.{stamp}{log_file.suffix}")
            shutil.move(str(log_file), str(new_name))
            rotated.append(new_name.name)
    return {"status": "ok", "detail": f"rotated: {', '.join(rotated) if rotated else 'nothing to rotate'}"}


def task_run_quick_diagnostics(params: dict) -> dict:
    return {
        "status": "ok",
        "system_info": collect_system_info(),
        "disk_space": check_disk_space(),
        "network_interfaces": get_network_interfaces(),
        "process_count": len(collect_process_list()),
    }


def task_empty_trash(params: dict) -> dict:
    try:
        subprocess.run(
            ["osascript", "-e", 'tell application "Finder" to empty trash'],
            check=True, timeout=15,
        )
        return {"status": "ok", "detail": "trash emptied"}
    except Exception as e:
        return {"status": "error", "detail": str(e)}


def task_lock_screen(params: dict) -> dict:
    try:
        subprocess.run(
            ["osascript", "-e", 'tell application "System Events" to keystroke "q" using {control down, command down}'],
            check=True, timeout=10,
        )
        return {"status": "ok", "detail": "screen lock triggered"}
    except Exception as e:
        return {"status": "error", "detail": str(e)}


def task_restart_agent(params: dict) -> dict:
    # Day 1: sadece onaylıyor. Gerçek launchd restart Day 6'da bağlanacak.
    return {"status": "ok", "detail": "restart acknowledged (launchd handoff arrives on a later day)"}


TASK_REGISTRY = {
    "flush_dns_cache": task_flush_dns_cache,
    "run_disk_cleanup_report": task_run_disk_cleanup_report,
    "rotate_local_logs": task_rotate_local_logs,
    "run_quick_diagnostics": task_run_quick_diagnostics,
    "empty_trash": task_empty_trash,
    "lock_screen": task_lock_screen,
    "restart_agent": task_restart_agent,
}


def cmd_run_predefined_task(params: dict) -> dict:
    task_name = params.get("task_name")
    task_fn = TASK_REGISTRY.get(task_name)
    if task_fn is None:
        return {"status": "rejected", "detail": f"unknown task_name '{task_name}'", "task_name": task_name}
    result = task_fn(params)
    # task_name'i sonuca ekliyoruz — sunucu tarafında command_results
    # tablosuna "run_predefined_task" olarak tek bir isimle düşüyor, hangi
    # ALT görevin (flush_dns_cache, empty_trash gibi AKTİF/sistemi değiştiren
    # bir görev mi, yoksa run_quick_diagnostics gibi PASİF/salt-okunur bir
    # görev mi) çalıştığı bu alan olmadan data_json'dan hiç anlaşılamaz.
    # bkz. server/db.py ACTIVE_TASK_NAMES ve cleanup_old_command_results.
    result["task_name"] = task_name
    return result


# ---------------------------------------------------------------------------
# COMMAND DISPATCHER — kapalı allowlist
# ---------------------------------------------------------------------------
# Not: list_directory, list_installed_applications, get_log_tail kaldırıldı.
# Process kategorileme dashboard'da (Day 5, Process Classifier) yapılacağı
# için ayrı bir dizin/uygulama listeleme komutuna ihtiyaç kalmadı.
COMMAND_REGISTRY = {
    # passive
    "get_process_list": lambda params: {"status": "ok", "processes": collect_process_list()},
    "get_system_info": lambda params: {"status": "ok", "system_info": collect_system_info()},
    "check_disk_space": lambda params: {"status": "ok", "disk": check_disk_space()},
    "get_network_interfaces": lambda params: {"status": "ok", "interfaces": get_network_interfaces()},
    # active - spesifik & doğrulamalı
    "kill_process": cmd_kill_process,
    # active - predefined task registry
    "run_predefined_task": cmd_run_predefined_task,
}


def dispatch_command(command: dict) -> dict:
    name = command.get("command")
    params = command.get("params", {})
    handler = COMMAND_REGISTRY.get(name)

    req_fields = {"name": name or "?", "params": json.dumps(params)}

    if handler is None:
        log_reject(COMMANDS_LOG, "CMD", req_fields, "unknown_command", f"{name} -> rejected")
        return {"command": name, "status": "rejected", "data": {}, "endpoint_id": _ENDPOINT_ID}

    try:
        result = handler(params)
        status = result.get("status", "unknown")
        # "status" dışındaki her şeyi "data" altına topla -> sunucu tarafında
        # komut sonucu ile heartbeat telemetrisi arasında alan adı çakışması olmaz.
        data = {k: v for k, v in result.items() if k != "status"}

        severity = SEVERITY_INFO if status == "ok" else SEVERITY_WARNING
        log_event(
            COMMANDS_LOG, "CMD", req_fields,
            {"status": status, "detail": str(data.get("detail", ""))[:120]},
            f"{name} -> {status}",
            severity=severity,
        )
        # endpoint_id: bu sonucun hangi fiziksel cihazdan geldiğini sunucuya
        # açıkça bildirir (bkz. dosya başındaki Day 7 notu). agent.py bu
        # dict'i /command_result veya /command_results/batch'e olduğu gibi
        # POST ediyorsa ekstra bir değişikliğe gerek kalmadan çalışır.
        return {"command": name, "status": status, "data": data, "endpoint_id": _ENDPOINT_ID}
    except Exception as e:
        log_event(
            COMMANDS_LOG, "CMD", req_fields,
            {"status": "error", "detail": str(e)[:120]},
            f"{name} -> exception",
            severity=SEVERITY_ERROR,
        )
        return {"command": name, "status": "error", "data": {"detail": str(e)}, "endpoint_id": _ENDPOINT_ID}
