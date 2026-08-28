#!/usr/bin/env python3
"""
Argus Server (Day 3) — gerçek, kalıcı depolu sunucu
======================================================
Day 1-2'deki mock_server.py'ın yerini alıyor. Artık:
  - Telemetri ve komut sonuçları SQLite'a kalıcı olarak yazılıyor (db.py)
  - Ayarlar server_config.json'dan okunuyor (server_config.py, yoksa varsayılanlar kullanılır)
  - Eski heartbeat kayıtları arka planda çalışan bir thread ile periyodik temizleniyor

Day 7: çoklu client desteği eklendi. Veritabanı şeması zaten normalizeydi
(clients/heartbeats/command_results); API katmanı artık bir `client_id`
alabiliyor (GET /clients ile keşfedilir) ve /state, /processes/latest,
/system_info/latest, /queue_command, /kill/request bu client_id'ye göre
filtreleniyor. client_id verilmeyen eski çağrılar hâlâ "en son görülen
client" davranışına düşer (geriye dönük uyumluluk) ama birden fazla agent
aynı anda aktifken bu artık YANLIŞ cihazı hedefleyebilir — dashboard'un her
zaman client_id göndermesi gerekiyor.

Bilinen eksik: /command_result ve /command_results/batch, agent'ın hangi
endpoint'e ait olduğunu güvenilir şekilde bilemiyor çünkü bu gövdeler
şu an endpoint_id taşımıyor (dispatcher.py henüz güncellenmedi). Kod bir
"endpoint_id" alanı gelirse onu kullanacak şekilde hazır (bkz.
_resolve_result_client_id) — agent tarafı eklenince otomatik doğru
çalışacak, eklenene kadar tek-client fallback'i kullanılıyor.

Çalıştırma (temiz konsol için --no-access-log önerilir, aksi halde
uvicorn'un kendi "INFO: 127.0.0.1 - POST /heartbeat 200 OK" satırları
bizim RFC5424 satırlarımızla karışır):
    uvicorn server:app --host 0.0.0.0 --port 8000 --no-access-log
(host/port aslında server_config.json'dan okunuyor; uvicorn'a elle
--host/--port vermek istersen server_config.json'daki değerle aynı
tutmayı unutma, yoksa config'teki host/port sadece log'da görünür,
gerçek bağlanma adresi uvicorn komutundaki olur)

Komut kuyruğa ekle:
  curl -X POST http://127.0.0.1:8000/queue_command \
       -H "Content-Type: application/json" \
       -d '{"command": "get_system_info", "params": {}}'

Sadece en son komut sonucunu gör (telemetri karışmadan, artık DB'den okunuyor):
  curl http://127.0.0.1:8000/latest_result

Genel durum (telemetri + son komut + bekleyen komutlar):
  curl http://127.0.0.1:8000/state
"""

import re
import threading
import time
import uuid
from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional

import db
from server_config import load_server_config
from logger import log_event, LOG_DIR, EVENTS_LOG, KILL_LOG, SEVERITY_INFO, SEVERITY_WARNING, SEVERITY_ERROR

config = load_server_config()
# Kill onay parolası: server_config.json içinde "admin_passcode" alanı yoksa
# (server_config.py bu alanı henüz bilmiyor olabilir), güvenli tarafta kalıp
# geçici bir varsayılan atıyoruz — PRODUCTION'A ALINMADAN server_config.json
# içine gerçek bir "admin_passcode" değeri eklenmeli, aksi halde varsayılan
# değer tahmin edilebilir olur. bkz. /kill/{request_id}/approve.
config.setdefault("admin_passcode", "argus-admin-change-me")

app = FastAPI(title="Argus Server")

# Dashboard tarayıcıdan (file:// ya da farklı bir origin'den) fetch ile
# bu sunucuya erişebilsin diye CORS açık. Bu araç tek kullanıcılı, yerel
# bir yönetim aracı olduğu için "*" ile tüm origin'lere izin vermek kabul
# edilebilir bir risk — dışa açık bir üretim servisi olsaydı bu kadar
# gevşek tutulmazdı.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Bekleyen komut kuyruğu BİLEREK bellekte kalıyor, kalıcı değil (Day 3 kararı):
# sunucu yeniden başlarsa kuyruktaki (henüz agent'a iletilmemiş) komutların
# kaybolması kabul edilebilir bir risk. Dashboard, sabit ve önceden bilinen
# get_process_list/get_system_info komutlarını periyodik tetikliyor —
# serbest metinli/keyfi komut girişi yok, allowlist ilkesi değişmiyor.
# (Not: connectivity/check_connectivity Day 4'te tamamen kaldırıldı.)
# Telemetri ve komut SONUÇLARI (zaten gerçekleşmiş olaylar) ise SQLite'ta kalıcı.
#
# Day 7 (çoklu client): tek bir liste yerine client_id -> [komut, ...]
# sözlüğü. Her agent kendi heartbeat'inde SADECE kendi client_id'sine
# kuyruklanmış komutları alır — aksi halde A cihazı için istenen bir
# kill_process, B cihazının bir sonraki heartbeat'inde ona gönderilebilirdi.
_pending_commands: dict[int, list[dict]] = {}


def _queue_for_client(client_id: int, cmd: dict) -> int:
    """cmd'yi client_id'nin kuyruğuna ekler, o kuyruğun yeni derinliğini döner."""
    _pending_commands.setdefault(client_id, []).append(cmd)
    return len(_pending_commands[client_id])


def _pop_pending_for_client(client_id: int) -> list[dict]:
    """client_id'nin kuyruğundaki tüm komutları çıkarır (kuyruğu boşaltır) ve döner."""
    return _pending_commands.pop(client_id, [])

# ---------------------------------------------------------------------------
# Kill onay iş akışı (Day 4) — BİLEREK bellekte, kalıcı değil: bunlar bir
# oturumluk yetkilendirme kayıtları, heartbeat kuyruğu gibi. Her adım
# (istek/onay/red/teslim/sonuç) logger.py üzerinden RFC5424 tek satır
# olarak diske de yazılıyor — bu sözlük sadece dashboard'un audit
# görünümü için hızlı bir bellek-içi geçmiş.
# Şema: {request_id: {pid, process_name, reason, requested_by, status,
#                      requested_at, approved_by, approved_at}}
# status sırası: pending_approval -> approved|denied -> dispatched -> done|error
_kill_requests: dict[str, dict] = {}


class Heartbeat(BaseModel):
    # endpoint_id: agent_config.json'da kalıcı, ağ değişse bile sabit kimlik
    # (bkz. agent/agent_config.py). Opsiyonel bırakıldı ki eski bir agent
    # (henüz güncellenmemiş) heartbeat atmaya devam ederse 422 ile reddedilip
    # tamamen kesilmesin — endpoint_id boşsa upsert_client hostname'e düşer.
    endpoint_id: str | None = None
    computer_name: str | None = None
    local_ip: str | None = None
    hostname: str
    user: str
    timestamp: str
    system_info: dict
    process_count: int


class CommandQueueRequest(BaseModel):
    command: str
    params: dict = {}
    # Day 7: hangi endpoint'e gideceği artık ZORUNLU — dashboard bir endpoint
    # kartına tıklayıp o cihazın process listesini/sistem bilgisini istediği
    # için hedef client'ı açıkça belirtiyor. Eski (tek client varsayan) bir
    # çağrı client_id göndermezse en son görülen client'a düşülüyor (bkz.
    # queue_command) — geriye dönük uyumluluk için, ama artık önerilmiyor.
    client_id: Optional[int] = None


class KillRequestCreate(BaseModel):
    pid: int
    process_name: str = ""
    reason: str = ""
    requested_by: str = "dashboard-admin"
    # Day 7: kill isteğinin hangi endpoint'i hedeflediği — PID'ler cihazlar
    # arasında çakışabileceği için (aynı PID, iki farklı makinede iki farklı
    # process olabilir) bu artık zorunlu bilgi.
    client_id: Optional[int] = None


class KillRequestApprove(BaseModel):
    passcode: str
    approved_by: str = "dashboard-admin"


@app.on_event("startup")
def on_startup():
    db.init_db(config["db_path"])
    log_event(
        EVENTS_LOG, "SERVER-START",
        {"db_path": config["db_path"], "host": config["host"], "port": str(config["port"])},
        {"status": "ready", "retention_days": str(config["heartbeat_retention_days"])},
        f"veritabanı hazır, retention {config['heartbeat_retention_days']} gün",
    )
    _start_cleanup_thread()


def _cleanup_loop():
    """
    Arka plan thread'i: hemen bir kez temizlik yapar (sunucu uzun süre
    kapalı kalmışsa birikmiş eski kayıtları geciktirmeden temizlemek için),
    sonra her cleanup_interval_hours'ta bir tekrarlar. Daemon thread —
    ana süreç kapanınca o da kapanır, ayrıca durdurmaya gerek yok.
    """
    retention_days = config["heartbeat_retention_days"]
    interval_seconds = config["cleanup_interval_hours"] * 3600

    while True:
        deleted = db.cleanup_old_heartbeats(retention_days)
        if deleted:
            log_event(
                EVENTS_LOG, "CLEANUP",
                {"retention_days": str(retention_days)},
                {"status": "ok", "deleted": str(deleted)},
                f"{deleted} eski heartbeat kaydı silindi",
            )
        time.sleep(interval_seconds)


def _start_cleanup_thread():
    thread = threading.Thread(target=_cleanup_loop, daemon=True)
    thread.start()


@app.post("/heartbeat")
def heartbeat(hb: Heartbeat):
    """
    Bu, agent'ın her 3 saniyede bir gönderdiği TELEMETRİ'dir.
    Bir komutun sonucu DEĞİLDİR — /command_result ile karıştırma.
    SQLite'a kalıcı olarak yazılıyor (clients + heartbeats tabloları).
    """
    payload = hb.model_dump()
    client_id = db.upsert_client(
        payload.get("endpoint_id"), payload["hostname"],
        payload.get("computer_name"), payload.get("local_ip"),
    )
    db.insert_heartbeat(client_id, payload)

    # Day 7: SADECE bu client_id'ye kuyruklanmış komutlar bu heartbeat'in
    # cevabında gönderiliyor — başka bir endpoint için istenen bir komut
    # buraya asla karışmaz (bkz. _pending_commands'in yukarıdaki tanımı).
    to_send = _pop_pending_for_client(client_id)

    log_event(
        EVENTS_LOG, "HEARTBEAT",
        {"endpoint_id": hb.endpoint_id or "-", "hostname": hb.hostname,
         "cpu": str(hb.system_info.get("cpu_percent")), "ram": str(hb.system_info.get("ram_percent"))},
        {"status": "stored", "pending_delivered": str(len(to_send))},
        f"heartbeat stored, {len(to_send)} pending command(s) delivered",
    )

    # Kill onay zincirinin 3. adımı: "Agent'a ulaştı". Kuyruktaki komutlardan
    # kill_process olanları bu heartbeat cevabıyla agent'a teslim ediliyor —
    # ilgili kill_request kaydını "delivered" durumuna çekip ayrı bir
    # RFC5424 satırıyla logluyoruz, böylece audit zincirinde "sunucudan
    # çıktı" ile "agent'a ulaştı" adımları birbirinden ayırt edilebiliyor.
    for cmd in to_send:
        if cmd.get("command") == "kill_process":
            pid = cmd.get("params", {}).get("pid")
            for req_id, req in _kill_requests.items():
                if req["pid"] == pid and req["status"] == "dispatched" and req.get("client_id") == client_id:
                    req["status"] = "delivered"
                    req["delivered_at"] = _now_iso()
                    log_event(
                        KILL_LOG, "KILL-DELIVERED",
                        {"request_id": req_id, "pid": str(pid)},
                        {"status": "delivered"},
                        f"kill_process(pid={pid}) agent'a ulaştı (request {req_id})",
                    )
                    break

    return {"status": "ack", "pending_commands": to_send}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@app.post("/command_result")
def command_result(result: dict):
    """
    Bu, agent'ın çalıştırdığı BİR komutun sonucudur (heartbeat telemetrisi
    değil). Her sonuç {"command": "...", "status": "...", "data": {...}}
    şeklinde geliyor. SQLite'a kalıcı olarak yazılıyor.
    """
    client_id = _resolve_result_client_id(result)
    if client_id is not None:
        db.insert_command_result(client_id, result)

    log_event(
        EVENTS_LOG, "CMD-RESULT",
        {"command": result.get("command", "?")},
        {"status": result.get("status", "unknown")},
        f"{result.get('command')} -> {result.get('status')} stored",
    )
    _mark_kill_result_if_applicable(result)
    return {"status": "received"}


def _mark_kill_result_if_applicable(result: dict) -> None:
    """
    Kill onay zincirinin son adımı: "Agent cevap verdi". kill_process
    sonucu geldiğinde, PID (VE client_id) eşleşen en son "delivered"
    durumundaki kill_request'i done/error olarak kapatır.

    Day 7: dispatcher.py artık her sonuca kendi endpoint_id'sini eklediği
    için (bkz. dispatcher.py _resolve_endpoint_id), burada da client_id'yi
    çözüp eşleştirmeye dahil ediyoruz — aksi halde iki farklı cihazda AYNI
    PID numarasına sahip iki ayrı process kill edilirse, yanlış cihazın
    kill isteği "done" olarak işaretlenebilirdi. endpoint_id henüz
    gelmiyorsa (eski agent) client_id None olur ve eşleştirme eskisi gibi
    sadece PID+"delivered" durumuna göre yapılır (geriye dönük uyumluluk).
    """
    if result.get("command") != "kill_process":
        return
    pid = (result.get("data") or {}).get("pid")
    status = result.get("status", "unknown")
    result_client_id = _resolve_result_client_id(result)
    for req_id, req in _kill_requests.items():
        if req["status"] != "delivered" or (pid is not None and req["pid"] != pid):
            continue
        if result_client_id is not None and req.get("client_id") != result_client_id:
            continue
        req["status"] = "done" if status == "ok" else "error"
        req["result_at"] = _now_iso()
        req["result_detail"] = (result.get("data") or {}).get("detail", "")
        log_event(
            KILL_LOG, "KILL-RESULT",
            {"request_id": req_id, "pid": str(req["pid"])},
            {"status": req["status"], "detail": req["result_detail"][:120]},
            f"kill_process(pid={req['pid']}) sonucu: {status} (request {req_id})",
            severity=SEVERITY_INFO if status == "ok" else SEVERITY_ERROR,
        )
        break


@app.post("/command_results/batch")
def command_results_batch(payload: dict):
    """
    Agent, ağ kesintisi sırasında gönderemediği komut sonuçlarını yerel
    bir kuyrukta biriktirir ve bağlantı geri geldiğinde hepsini tek
    istekte buraya gönderir. Hepsi SQLite'a kalıcı olarak yazılıyor.
    """
    results = payload.get("results", [])
    # payload düzeyinde bir endpoint_id varsa (tüm batch aynı agent'tan
    # geldiği için tipik durum) her result için fallback olarak kullanılır;
    # result'un kendi içinde endpoint_id varsa o önceliklidir.
    batch_endpoint_id = payload.get("endpoint_id")
    for result in results:
        client_id = _resolve_result_client_id(
            result if result.get("endpoint_id") else {**result, "endpoint_id": batch_endpoint_id}
        )
        if client_id is not None:
            db.insert_command_result(client_id, result)

    log_event(
        EVENTS_LOG, "CMD-RESULT-BATCH",
        {"count": str(len(results))},
        {"status": "stored"},
        f"{len(results)} sonuç toplu olarak alındı",
    )
    for result in results:
        _mark_kill_result_if_applicable(result)
    return {"status": "received", "count": len(results)}


# ---------------------------------------------------------------------------
# Kill onay iş akışı — Endpoints (Day 4)
#
# Akış: dashboard PID için /kill/request ile bir onay talebi açar (adım 1:
# "kill komutu talep edildi", henüz agent'a hiçbir şey gitmedi) -> operatör
# admin parolasını girip /kill/{id}/approve çağırır (adım 2: yetkilendirme
# — başarısızsa reddedilir ve WARNING loglanır, hiçbir komut kuyruğa
# girmez) -> onaylanınca kill_process komutu normal _pending_commands
# kuyruğuna eklenir (adım 3: "gönderildi") -> bir sonraki heartbeat'te
# agent'a teslim edilir (adım 4: "agent'a ulaştı", bkz. heartbeat()) ->
# agent'ın sonucu /command_result ile geri gelir (adım 5: "agent cevap
# verdi", bkz. _mark_kill_result_if_applicable). Her adım logger.py
# üzerinden ayrı bir RFC5424 satırı olarak diske yazılır.
# ---------------------------------------------------------------------------
@app.post("/kill/request")
def kill_request(req: KillRequestCreate):
    # client_id verilmezse (eski çağrı) en son görülen client'a düşülür —
    # ama artık dashboard her zaman seçili endpoint'in client_id'sini gönderiyor.
    client_id = req.client_id if req.client_id is not None else _resolve_single_client_id()
    client = db.get_client_by_id(client_id) if client_id is not None else None

    request_id = uuid.uuid4().hex[:12]
    _kill_requests[request_id] = {
        "request_id": request_id,
        "pid": req.pid,
        "process_name": req.process_name,
        "reason": req.reason,
        "requested_by": req.requested_by,
        "status": "pending_approval",
        "requested_at": _now_iso(),
        "client_id": client_id,
        "endpoint_label": (client.get("computer_name") or client.get("hostname")) if client else "?",
    }
    log_event(
        KILL_LOG, "KILL-REQUEST",
        {"request_id": request_id, "pid": str(req.pid), "process": req.process_name,
         "requested_by": req.requested_by, "client_id": str(client_id)},
        {"status": "pending_approval"},
        f"kill isteği açıldı: pid={req.pid} ({req.process_name}) endpoint={_kill_requests[request_id]['endpoint_label']} by {req.requested_by}",
    )
    return {"status": "pending_approval", "request_id": request_id}


@app.post("/kill/{request_id}/approve")
def kill_approve(request_id: str, body: KillRequestApprove):
    req = _kill_requests.get(request_id)
    if req is None:
        return {"status": "not_found"}

    if body.passcode != config.get("admin_passcode"):
        req["status"] = "denied"
        req["approved_by"] = body.approved_by
        req["approved_at"] = _now_iso()
        log_event(
            KILL_LOG, "KILL-DENY",
            {"request_id": request_id, "pid": str(req["pid"]), "attempted_by": body.approved_by},
            {"status": "denied", "reason": "invalid_passcode"},
            f"kill onayı reddedildi: pid={req['pid']} — geçersiz parola ({body.approved_by})",
            severity=SEVERITY_WARNING,
        )
        return {"status": "denied", "detail": "invalid passcode"}

    req["status"] = "approved"
    req["approved_by"] = body.approved_by
    req["approved_at"] = _now_iso()
    log_event(
        KILL_LOG, "KILL-APPROVE",
        {"request_id": request_id, "pid": str(req["pid"]), "approved_by": body.approved_by},
        {"status": "approved"},
        f"kill onaylandı: pid={req['pid']} ({req['process_name']}) by {body.approved_by}",
    )

    if req.get("client_id") is None:
        req["status"] = "error"
        log_event(
            KILL_LOG, "KILL-DISPATCH",
            {"request_id": request_id, "pid": str(req["pid"])},
            {"status": "error", "detail": "no target client_id"},
            f"kill_process(pid={req['pid']}) kuyruğa eklenemedi — hedef endpoint bilinmiyor",
            severity=SEVERITY_ERROR,
        )
        return {"status": "error", "request_id": request_id, "detail": "target endpoint unknown"}

    depth = _queue_for_client(req["client_id"], {"command": "kill_process", "params": {"pid": req["pid"]}})
    req["status"] = "dispatched"
    req["dispatched_at"] = _now_iso()
    log_event(
        KILL_LOG, "KILL-DISPATCH",
        {"request_id": request_id, "pid": str(req["pid"]), "client_id": str(req["client_id"])},
        {"status": "queued", "queue_depth": str(depth)},
        f"kill_process(pid={req['pid']}) endpoint={req.get('endpoint_label', '?')} kuyruğa eklendi, bir sonraki heartbeat'te teslim edilecek",
    )
    return {"status": "dispatched", "request_id": request_id}


@app.get("/kill/requests")
def kill_requests_list():
    """Dashboard'un audit görünümü için tüm kill isteklerinin geçmişi (en yeni önce)."""
    return {"requests": sorted(_kill_requests.values(), key=lambda r: r["requested_at"], reverse=True)}


def _resolve_single_client_id() -> int | None:
    """
    GERİYE DÖNÜK UYUMLULUK İÇİN DEĞİŞMEDEN KALDI (Day 3'ten): en son
    heartbeat atan client'ın id'sine düşer. Day 7'den itibaren SADECE
    /command_result ve /command_results/batch içinde, agent henüz
    endpoint_id göndermiyorsa (dispatcher.py güncellenmediyse) bir
    fallback olarak kullanılıyor — bkz. _resolve_result_client_id.
    Birden fazla agent AYNI ANDA aktifse bu fallback yanlış client'a
    yazabilir (sonucu en son heartbeat atan cihaza yazar); doğru çözüm
    agent'ın command_result gövdesine kendi endpoint_id'sini eklemesi
    (bkz. dispatcher.py — henüz görmediğimiz bir dosya, eklenmesi gerekiyor).
    """
    latest_hb = db.get_latest_heartbeat()
    if latest_hb is None:
        return None
    return db.upsert_client(
        latest_hb.get("endpoint_id"), latest_hb["hostname"],
        latest_hb.get("computer_name"), latest_hb.get("local_ip"),
    )


def _resolve_result_client_id(result: dict) -> int | None:
    """
    /command_result ve /command_results/batch için client çözümü.
    ÖNCELİK: gövdede bir endpoint_id varsa (agent tarafı güncellenip
    eklerse) ONU kullan — çoklu client altında tek doğru yol bu, çünkü
    birden fazla agent aynı anda komut sonucu gönderebilir ve "en son
    heartbeat atan" artık güvenilir bir varsayım değil. endpoint_id yoksa
    (eski/güncellenmemiş agent) eski tek-client fallback'ine düşülür —
    yanlış olabileceği yukarıda açıklandı, ama hiç kaydetmemekten iyidir.
    """
    endpoint_id = result.get("endpoint_id")
    if endpoint_id:
        client = db.get_client_by_endpoint_id(endpoint_id)
        if client:
            return client["id"]
    return _resolve_single_client_id()


@app.post("/queue_command")
def queue_command(req: CommandQueueRequest):
    """
    Agent'ın bir sonraki heartbeat'inde alacağı komutu kuyruğa ekler.
    Day 7: client_id ZORUNLU olarak belirtilmeli (dashboard artık her
    zaman "hangi endpoint kartına tıklandıysa" o client_id'yi gönderiyor).
    Verilmezse (eski çağrı) en son görülen client'a düşülür — geriye dönük
    uyumluluk, ama birden fazla endpoint aktifken yanlış cihazı hedefleyebilir.
    """
    client_id = req.client_id if req.client_id is not None else _resolve_single_client_id()
    if client_id is None:
        return {"status": "error", "detail": "no known client (henüz hiç heartbeat gelmedi)"}

    cmd = {"command": req.command, "params": req.params}
    depth = _queue_for_client(client_id, cmd)
    log_event(
        EVENTS_LOG, "CMD-QUEUE",
        {"command": req.command, "params": str(req.params), "client_id": str(client_id)},
        {"status": "queued", "queue_depth": str(depth)},
        f"{req.command} client_id={client_id} kuyruğa eklendi, bir sonraki heartbeat'te teslim edilecek",
    )
    return {"status": "queued", "command": cmd, "client_id": client_id}


@app.get("/clients")
def clients_list():
    """
    Çoklu-endpoint desteği (Day 7): dashboard'un Endpoints listesi
    (kartlar) için — görülmüş TÜM client'ları, her birinin en son
    heartbeat'inden türetilen özet bilgiyle (online/offline, CPU/RAM,
    kullanıcı, vb.) döner. Bir endpoint kartına tıklamak, dashboard'un
    sonraki tüm isteklerine (/state, /processes/latest, /kill/request...)
    bu listedeki "id" alanını client_id olarak eklemesi anlamına gelir.
    """
    rows = db.get_all_clients()
    now = datetime.now(timezone.utc)
    clients = []
    for r in rows:
        online = False
        if r.get("hb_timestamp"):
            try:
                age = (now - datetime.fromisoformat(r["hb_timestamp"])).total_seconds()
                online = age < 12
            except ValueError:
                online = False
        clients.append({
            "client_id": r["id"],
            "endpoint_id": r["endpoint_id"],
            "hostname": r["hostname"],
            "computer_name": r["computer_name"],
            "local_ip": r["local_ip"],
            "first_seen": r["first_seen"],
            "last_seen": r["last_seen"],
            "online": online,
            "user": r["hb_user"],
            "platform": r["platform"],
            "cpu_percent": r["cpu_percent"],
            "ram_percent": r["ram_percent"],
            "process_count": r["process_count"],
        })
    return {"clients": clients}


@app.get("/state")
def state(client_id: Optional[int] = None):
    """
    Genel durum görünümü — dashboard'un ihtiyaç duyacağı her şeyi tek
    seferde verir: agent yaşıyor mu (telemetri), en son komut ne oldu,
    bekleyen komut var mı. Telemetri ve komut sonucu artık SQLite'tan
    okunuyor (kalıcı); pending_commands hâlâ bellekte (Day 3 kararı).

    Day 7: client_id verilirse SADECE o endpoint'in verisi döner (dashboard
    bir endpoint kartına tıkladığında bunu kullanır). Verilmezse eski
    tek-client davranışına (en son görülen client) düşülür — geriye dönük
    uyumluluk için.
    """
    if client_id is not None:
        return {
            "latest_telemetry": db.get_latest_heartbeat_for_client(client_id),
            "latest_command_result": db.get_latest_command_result_for_client(client_id),
            "pending_commands": _pending_commands.get(client_id, []),
        }
    return {
        "latest_telemetry": db.get_latest_heartbeat(),
        "latest_command_result": db.get_latest_command_result(),
        "pending_commands": [c for cmds in _pending_commands.values() for c in cmds],
    }


@app.get("/latest_result")
def latest_result():
    """
    SADECE en son komutun sonucunu döner — telemetri yok, pending_commands
    yok. Artık SQLite'tan okunuyor, sunucu yeniden başlasa bile kaybolmaz.
    """
    return {"result": db.get_latest_command_result()}


@app.get("/processes/latest")
def processes_latest(client_id: Optional[int] = None):
    """
    Client'ın en son bildirdiği process listesini döner — PID, isim,
    kullanıcı (user + real_user), çalıştırma parametreleri (cmdline),
    ve aktif ağ bağlantıları (connections) her process için mevcut.

    `/latest_result`'tan farkı: araya başka bir komut (örn.
    get_system_info) girmiş olsa bile, bu endpoint SADECE
    get_process_list'in en son sonucunu arar ve onu döner — process
    listesi hiçbir zaman başka bir komutun sonucu tarafından
    "gölgelenmez". Bu, sunucu tarafında process listesini güvenilir
    şekilde görüntüleyebilme gereksinimini karşılıyor.

    Day 7: client_id verilirse SADECE o endpoint'in process listesi döner
    (dashboard bir endpoint kartı seçiliyken bunu kullanır) — aksi halde
    çoklu client'ta hangi cihazın listesine baktığın belirsiz olurdu.

    Henüz hiç get_process_list çalıştırılmadıysa result: null döner —
    bu durumda `curl -X POST /queue_command -d '{"command":
    "get_process_list", "client_id": <id>}'` ile bir kez tetiklemen gerekir.
    """
    if client_id is not None:
        return {"result": db.get_latest_result_by_command_for_client(client_id, "get_process_list")}
    return {"result": db.get_latest_result_by_command("get_process_list")}


@app.get("/system_info/latest")
def system_info_latest(client_id: Optional[int] = None):
    """
    Client'ın en son bildirdiği sistem bilgisini (CPU/RAM/disk) döner —
    processes/latest ile aynı mantık: araya başka komut girse bile
    kaybolmaz. Dashboard'un "sistem bilgisi" paneli için. client_id
    verilirse SADECE o endpoint'in sonucu döner (bkz. /processes/latest).
    """
    if client_id is not None:
        return {"result": db.get_latest_result_by_command_for_client(client_id, "get_system_info")}
    return {"result": db.get_latest_result_by_command("get_system_info")}


@app.get("/heartbeats/recent")
def heartbeats_recent(limit: int = 50):
    """
    Dashboard'un Logs sayfası için: en son N heartbeat kaydını döner
    (en yeni önce). Bu GERÇEK telemetri verisi — agent'ın gerçekten
    gönderdiği heartbeat'lerin veritabanı kaydı, uydurma log satırları
    değil. `limit` query param ile kaç kayıt istediğini belirleyebilirsin
    (örn. /heartbeats/recent?limit=100).
    """
    return {"heartbeats": db.get_recent_heartbeats(limit)}


_RFC5424_RE = re.compile(
    r"^<(\d+)>(\d+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+((?:\[[^\]]*\])*)\s*(.*)$"
)


def _log_line_msgid(line: str) -> str | None:
    """
    Bir RFC5424 satırından MSGID alanını (7. alan) çıkarır. dashboard.html'deki
    parseRfc5424() ile aynı regex — ikisi de aynı satır formatını ayrıştırıyor,
    biri arama/filtreleme için sunucuda, diğeri görüntüleme için tarayıcıda.
    Satır beklenen formatta değilse None döner (filtrelemede satır elenmez,
    sadece msgid eşleşmesi arıyorsak dahil edilmez).
    """
    m = _RFC5424_RE.match(line)
    return m.group(7) if m else None


@app.get("/logs/files")
def logs_files():
    """
    Dashboard'un Logs sayfasındaki dosya sekmeleri için: LOG_DIR (server/logs/)
    altındaki GERÇEK log dosyalarını listeler — isim, boyut, son değişim zamanı.

    Not (mimari sınır): burada SADECE server'ın kendi ürettiği loglar
    listelenir (argus_server_events.log, kill_audit.log). Agent'ın kendi
    logları (argus_heartbeat.log, argus_commands.log) agent'ın KENDİ
    diskinde duruyor — agent ve server şu an aynı makinede olsa bile, VM'e
    taşındığında server'ın dosya sistemi agent'ınkine hiç erişemeyecek.
    Bu yüzden agent loglarını server'ın diskinden okumaya çalışmak yanlış
    bir varsayım olurdu; agent logları göstermek istenirse agent'ın bunları
    ayrı bir uç noktayla periyodik olarak server'a göndermesi gerekir
    (henüz eklenmedi — bilinçli olarak sonraki bir adıma bırakıldı).
    """
    files = []
    for p in sorted(LOG_DIR.glob("*.log")):
        stat = p.stat()
        files.append({
            "name": p.name,
            "size_bytes": stat.st_size,
            "modified_at": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
        })
    return {"files": files}


@app.get("/logs/{filename}/msgids")
def logs_msgids(filename: str):
    """
    Dosyadaki BENZERSİZ MSGID değerlerini (dosyanın tamamından, en sık
    görülenden en aza sıralı) döner — dashboard'un MsgID filtre dropdown'ı
    bunu kullanıyor. Sadece o an ekranda görünen (zaten filtrelenmiş)
    satırlardan türetmek yerine ayrı bir uç nokta olması, kullanıcı bir
    filtre uygulamışken bile listenin dosyadaki TÜM olası değerleri
    göstermeye devam etmesini sağlıyor.
    """
    if "/" in filename or "\\" in filename or ".." in filename or not filename.endswith(".log"):
        return {"error": "invalid filename", "msgids": []}

    path = LOG_DIR / filename
    if not path.exists() or not path.is_file():
        return {"error": "not found", "msgids": []}

    with open(path, "r", errors="replace") as f:
        counts: dict[str, int] = {}
        for line in f:
            if not line.strip():
                continue
            mid = _log_line_msgid(line.rstrip("\n"))
            if mid:
                counts[mid] = counts.get(mid, 0) + 1

    ordered = sorted(counts.items(), key=lambda kv: kv[1], reverse=True)
    return {"msgids": [{"msgid": mid, "count": c} for mid, c in ordered]}


@app.get("/logs/{filename}")
def logs_tail(filename: str, limit: int = 300, q: Optional[str] = None, msgid: Optional[str] = None):
    """
    Belirtilen log dosyasının son `limit` satırını, EN YENİ EN ÜSTTE olacak
    şekilde ham RFC5424 metni olarak döner (dashboard bunu doğrudan terminal
    görünümünde basıyor, ayrıca parse etmiyor).

    Arama/filtreleme (q, msgid) DOSYANIN TAMAMI üzerinde yapılır, sadece son
    `limit` satır üzerinde değil — yoksa örneğin dün sabah 09:38'i aratan biri,
    o satır dosyanın daha eski bir bölümündeyse hiçbir sonuç bulamazdı.
    Filtreden sonra en fazla `limit` eşleşme döner (yine en yeni en üstte).

    q: serbest metin arama, satırın HAM haliyle (timestamp, hostname,
       msgid, structured-data, mesaj — hepsi) büyük/küçük harf duyarsız alt
       dize eşleşmesi. Saat aramalarında ("09.38" gibi) kullanıcı ayıracı
       nokta/virgül ile yazabilir; ISO 8601 zaman damgaları ":" kullandığı
       için burada "." ve "," -> ":" olarak da denenir, ikisi de eşleşmezse
       hiçbir şey elenmez diye q aynen de denenir.
    msgid: MSGID alanına (7. RFC5424 alanı) göre TAM eşleşme, büyük/küçük
       harf duyarsız. Satır beklenmedik formattaysa (msgid çıkarılamıyorsa)
       bu filtreden hiç geçmez.

    Güvenlik: path traversal'a karşı filename sıkı doğrulanıyor — sadece
    "/" ya da ".." içermeyen, ".log" ile biten dosya adlarına izin verilir,
    ve dosya her zaman LOG_DIR içinden okunur (dışarıdan keyfi bir mutlak
    yol verilemez).
    """
    if "/" in filename or "\\" in filename or ".." in filename or not filename.endswith(".log"):
        return {"error": "invalid filename", "lines": [], "total_lines": 0}

    path = LOG_DIR / filename
    if not path.exists() or not path.is_file():
        return {"error": "not found", "lines": [], "total_lines": 0}

    with open(path, "r", errors="replace") as f:
        all_lines = [line.rstrip("\n") for line in f if line.strip()]

    matched = all_lines

    if q and q.strip():
        q_norm = q.strip().lower()
        q_variants = {q_norm, q_norm.replace(".", ":"), q_norm.replace(",", ":")}
        matched = [l for l in matched if any(v in l.lower() for v in q_variants)]

    if msgid and msgid.strip():
        msgid_norm = msgid.strip().lower()
        matched = [l for l in matched if (_log_line_msgid(l) or "").lower() == msgid_norm]

    tail = matched[-limit:]
    tail.reverse()
    return {
        "name": filename,
        "lines": tail,
        "total_lines": len(all_lines),
        "matched_lines": len(matched),
        "filtered": bool((q and q.strip()) or (msgid and msgid.strip())),
    }


@app.get("/config")
def get_config():
    """
    Sunucunun kendi yapılandırmasını salt-okunur olarak döner (Settings
    sayfası için). Bu bir düzenleme endpoint'i DEĞİL — sadece görüntüleme.
    Ayarları değiştirmek istersen server_config.json dosyasını düzenleyip
    sunucuyu yeniden başlatman gerekiyor (Day 3 tasarımı, bkz. docs/05_day3_log.md).

    Güvenlik notu: admin_passcode burada BİLEREK dışlanıyor —
    bu endpoint salt-okunur ve yetkisiz herhangi bir istemci tarafından
    çağrılabiliyor; parolayı olduğu gibi döndürmek kill onay mekanizmasını
    anlamsız kılardı.
    """
    safe_config = {k: v for k, v in config.items() if k != "admin_passcode"}
    return {"config": safe_config}
