"""
db.py — Argus Server kalıcı depolama katmanı (SQLite).

Şema (3 normalize tablo, tek client için de doğru tasarım):
  clients          — endpoint_id başına tek satır (id, endpoint_id, hostname,
                      computer_name, local_ip, first_seen, last_seen). Eşleştirme
                      anahtarı endpoint_id (agent_config.json'da kalıcı, ağ
                      değişse bile sabit) — hostname/computer_name/local_ip
                      sadece bilgi amaçlı, her heartbeat'te güncellenir.
  heartbeats       — her heartbeat'in kaydı, client_id ile clients'a bağlı
  command_results  — her komut sonucunun kaydı, client_id ile clients'a bağlı

Her fonksiyon kendi kısa ömürlü SQLite bağlantısını açıp kapatıyor —
FastAPI'nin event loop'u ile arka plan temizlik thread'i aynı anda
veritabanına erişebileceği için, bağlantıları paylaşmak yerine her
işlemde yeni bağlantı açmak eşzamanlılık sorunlarından kaçınmanın en
basit yolu. SQLite yazma kilitlemesini kendi içinde hallediyor.
"""

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

_DB_PATH: Path | None = None

# ---------------------------------------------------------------------------
# Aktif / Pasif komut ayrımı (Day 6)
# ---------------------------------------------------------------------------
# AKTİF komutlar sistemde GERÇEK bir değişiklik yapar (bir process'i
# öldürmek, DNS cache temizlemek, çöp kutusunu boşaltmak, log rotate etmek,
# vb.). Bunların kaydı denetim (audit) açısından hayati — durumu ne olursa
# olsun (ok/error/rejected) cleanup_old_command_results tarafından ASLA
# silinmez, ne kadar eski olursa olsun kalıcı tutulur.
#
# PASİF (salt-okunur/telemetri) komutlar sistemde hiçbir şeyi değiştirmez
# (get_process_list, get_system_info, check_disk_space,
# get_network_interfaces, ve run_predefined_task'ın salt-okunur alt
# görevleri: run_disk_cleanup_report, run_quick_diagnostics). Bunların
# geçmişe dönük denetim değeri düşük — command_results tablosunu şişirmemesi
# için retention_days'ten eski olanlar silinebilir.
#
# Şu an dashboard'dan fiilen SADECE kill_process tetiklenebiliyor; diğer
# run_predefined_task alt görevleri (flush_dns_cache, empty_trash, vb.)
# dashboard'a henüz bağlanmadı ama COMMAND_REGISTRY'de hazır duruyorlar —
# ileride bağlanırlarsa aşağıdaki ACTIVE_TASK_NAMES seti de otomatik olarak
# onları koruma altına alıyor, ayrıca bir kod değişikliği gerekmiyor.
ACTIVE_COMMANDS = {"kill_process"}
ACTIVE_TASK_NAMES = {"flush_dns_cache", "empty_trash", "lock_screen", "rotate_local_logs", "restart_agent"}


def _is_active_command_result(command: str, data_json: str | None) -> bool:
    """Bir command_results satırının AKTİF (asla silinmeyecek) olup olmadığını belirler."""
    if command in ACTIVE_COMMANDS:
        return True
    if command == "run_predefined_task" and data_json:
        try:
            data = json.loads(data_json)
        except (TypeError, ValueError):
            # data_json bozuksa güvenli tarafta kal: silme, aktif say.
            return True
        return data.get("task_name") in ACTIVE_TASK_NAMES
    return False


def init_db(db_path: str) -> None:
    """Veritabanı dosyasını ve şemayı (yoksa) oluşturur. Sunucu başlarken bir kez çağrılır."""
    global _DB_PATH
    _DB_PATH = Path(db_path)

    with sqlite3.connect(_DB_PATH) as conn:
        # Not (Day 5): clients artık hostname yerine endpoint_id ile
        # eşleştiriliyor — hostname ağ değişince değişebiliyor (farklı ağda
        # farklı FQDN), endpoint_id ise agent_config.json'da kalıcı ve sabit.
        #
        # Migrasyon (düzeltme): eski (Day 3) şemada "hostname TEXT UNIQUE NOT
        # NULL" kısıtlaması CREATE TABLE'ın içine gömülüydü. İlk sürümde bu
        # durumu sadece ALTER TABLE ... ADD COLUMN ile yeni kolon eklemeye
        # çalışıyorduk — ama SQLite, ALTER TABLE ile VAR OLAN bir UNIQUE
        # kısıtlamasını KALDIRMAYI desteklemiyor. Sonuç: endpoint_id kolonu
        # eklenmiş olsa bile hostname üzerindeki eski UNIQUE kısıtlaması
        # duruyor kalıyordu, ve aynı hostname'den farklı endpoint_id ile ikinci
        # bir heartbeat gelince "UNIQUE constraint failed: clients.hostname"
        # hatası veriyordu. Doğru çözüm: kısıtlama hâlâ varsa tabloyu id'leri
        # KORUYARAK yeniden kurmak (rename -> yeni şemayla yeniden yarat ->
        # veriyi taşı -> eskisini sil) — heartbeats/command_results'taki
        # client_id referansları bu sayede bozulmuyor.
        legacy_sql_row = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='clients'"
        ).fetchone()
        needs_rebuild = legacy_sql_row is not None and "UNIQUE" in (legacy_sql_row[0] or "") and "hostname" in (legacy_sql_row[0] or "")

        if needs_rebuild:
            legacy_cols = {row[1] for row in conn.execute("PRAGMA table_info(clients)")}
            conn.execute("ALTER TABLE clients RENAME TO clients_legacy")
            conn.execute("""
                CREATE TABLE clients (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    endpoint_id TEXT,
                    hostname TEXT NOT NULL,
                    computer_name TEXT,
                    local_ip TEXT,
                    first_seen TEXT NOT NULL,
                    last_seen TEXT NOT NULL
                )
            """)
            target_cols = ("id", "endpoint_id", "hostname", "computer_name", "local_ip", "first_seen", "last_seen")
            select_list = ", ".join(c if c in legacy_cols else f"NULL AS {c}" for c in target_cols)
            conn.execute(f"INSERT INTO clients ({', '.join(target_cols)}) SELECT {select_list} FROM clients_legacy")
            conn.execute("DROP TABLE clients_legacy")
        else:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS clients (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    endpoint_id TEXT,
                    hostname TEXT NOT NULL,
                    computer_name TEXT,
                    local_ip TEXT,
                    first_seen TEXT NOT NULL,
                    last_seen TEXT NOT NULL
                )
            """)
            existing_cols = {row[1] for row in conn.execute("PRAGMA table_info(clients)")}
            for col in ("endpoint_id", "computer_name", "local_ip"):
                if col not in existing_cols:
                    conn.execute(f"ALTER TABLE clients ADD COLUMN {col} TEXT")

        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_clients_endpoint_id ON clients(endpoint_id)")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS heartbeats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id INTEGER NOT NULL REFERENCES clients(id),
                timestamp TEXT NOT NULL,
                user TEXT,
                cpu_percent REAL,
                ram_percent REAL,
                ram_used_mb REAL,
                disk_read_mb REAL,
                disk_write_mb REAL,
                process_count INTEGER,
                platform TEXT,
                raw_json TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS command_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id INTEGER NOT NULL REFERENCES clients(id),
                command TEXT NOT NULL,
                status TEXT NOT NULL,
                data_json TEXT,
                at TEXT NOT NULL
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_heartbeats_client_ts ON heartbeats(client_id, timestamp)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_command_results_client_at ON command_results(client_id, at)")
        conn.commit()


def _connect() -> sqlite3.Connection:
    if _DB_PATH is None:
        raise RuntimeError("init_db() çağrılmadan db.py fonksiyonları kullanılamaz")
    conn = sqlite3.connect(_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def upsert_client(endpoint_id: str, hostname: str = "", computer_name: str | None = None, local_ip: str | None = None) -> int:
    """
    endpoint_id'yi (agent_config.json'daki sabit kimlik) clients tablosunda
    bulur/oluşturur, last_seen'i ve o anki hostname/computer_name/local_ip'i
    günceller, client_id döner.

    Not: hostname/computer_name/local_ip her çağrıda güncellenir çünkü bunlar
    ağa göre değişebilir (VM'e taşınma, Wi-Fi değişimi) — sadece endpoint_id
    sabit kalıp eşleştirme anahtarı olarak kullanılıyor.

    Geriye dönük uyumluluk: eski çağrı biçimi upsert_client(hostname) idi.
    endpoint_id boş/None gelirse (örn. henüz endpoint_id göndermeyen eski bir
    heartbeat kaydı), hostname'e düşülür — böylece migrasyon sırasında hiçbir
    şey patlamaz, sadece o client eski usul hostname ile eşleşmeye devam eder.
    """
    now = datetime.now(timezone.utc).isoformat()
    with _connect() as conn:
        if endpoint_id:
            row = conn.execute("SELECT id FROM clients WHERE endpoint_id = ?", (endpoint_id,)).fetchone()
        else:
            # endpoint_id yok (eski/geriye dönük çağrı) -> tek çare hostname'e
            # bakmak, ama SADECE henüz endpoint_id kazanmamış eski satırlarda.
            row = conn.execute(
                "SELECT id FROM clients WHERE endpoint_id IS NULL AND hostname = ?", (hostname,)
            ).fetchone()

        if row:
            conn.execute(
                "UPDATE clients SET endpoint_id = COALESCE(?, endpoint_id), hostname = ?, computer_name = ?, local_ip = ?, last_seen = ? WHERE id = ?",
                (endpoint_id, hostname or "?", computer_name, local_ip, now, row["id"]),
            )
            conn.commit()
            return row["id"]

        cursor = conn.execute(
            "INSERT INTO clients (endpoint_id, hostname, computer_name, local_ip, first_seen, last_seen) VALUES (?, ?, ?, ?, ?, ?)",
            (endpoint_id, hostname or "?", computer_name, local_ip, now, now),
        )
        conn.commit()
        return cursor.lastrowid


def insert_heartbeat(client_id: int, payload: dict) -> None:
    """
    payload: agent'ın /heartbeat'e gönderdiği ham gövde
    ({"hostname", "user", "timestamp", "system_info": {...}, "process_count"}).
    Sık sorgulanacak alanlar (cpu, ram, process_count) ayrı kolonlara
    da yazılıyor — ham JSON da saklanıyor, hiçbir bilgi kaybolmuyor.
    """
    system_info = payload.get("system_info", {})
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO heartbeats
                (client_id, timestamp, user, cpu_percent, ram_percent, ram_used_mb,
                 disk_read_mb, disk_write_mb, process_count, platform, raw_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                client_id,
                payload.get("timestamp"),
                payload.get("user"),
                system_info.get("cpu_percent"),
                system_info.get("ram_percent"),
                system_info.get("ram_used_mb"),
                system_info.get("disk_read_mb"),
                system_info.get("disk_write_mb"),
                payload.get("process_count"),
                system_info.get("platform"),
                json.dumps(payload),
            ),
        )
        conn.commit()


def insert_command_result(client_id: int, result: dict) -> None:
    """result: {"command": ..., "status": ..., "data": {...}, "at": ...} (at yoksa şimdi eklenir)."""
    at = result.get("at") or datetime.now(timezone.utc).isoformat()
    with _connect() as conn:
        conn.execute(
            "INSERT INTO command_results (client_id, command, status, data_json, at) VALUES (?, ?, ?, ?, ?)",
            (client_id, result.get("command"), result.get("status"), json.dumps(result.get("data", {})), at),
        )
        conn.commit()


def get_latest_heartbeat() -> dict | None:
    """Tek client varsayımıyla: veritabanındaki en son heartbeat'i döner."""
    with _connect() as conn:
        row = conn.execute("SELECT raw_json FROM heartbeats ORDER BY id DESC LIMIT 1").fetchone()
        return json.loads(row["raw_json"]) if row else None


def get_latest_command_result() -> dict | None:
    """Tek client varsayımıyla: veritabanındaki en son komut sonucunu döner."""
    with _connect() as conn:
        row = conn.execute(
            "SELECT command, status, data_json, at FROM command_results ORDER BY id DESC LIMIT 1"
        ).fetchone()
        if not row:
            return None
        return {
            "command": row["command"],
            "status": row["status"],
            "data": json.loads(row["data_json"]) if row["data_json"] else {},
            "at": row["at"],
        }


def get_latest_result_by_command(command_name: str) -> dict | None:
    """
    Belirli bir komut türünün EN SON sonucunu döner — araya başka
    komutlar girmiş olsa bile. `get_latest_command_result()`'ın aksine
    (o sadece genel olarak en son çalışan komutu döner, ne olursa olsun),
    bu fonksiyon "get_process_list'in en son sonucu ne?" gibi sorulara
    güvenilir cevap verir; process listesi dashboard'da her zaman
    görüntülenebilir olsun diye gerekli.
    """
    with _connect() as conn:
        row = conn.execute(
            "SELECT command, status, data_json, at FROM command_results "
            "WHERE command = ? ORDER BY id DESC LIMIT 1",
            (command_name,),
        ).fetchone()
        if not row:
            return None
        return {
            "command": row["command"],
            "status": row["status"],
            "data": json.loads(row["data_json"]) if row["data_json"] else {},
            "at": row["at"],
        }


def get_all_clients() -> list[dict]:
    """
    Çoklu-endpoint desteği (Day 7): TÜM client'ları, her birinin en son
    heartbeat'iyle birlikte döner — dashboard'daki Endpoints listesi
    (kartlar) bunu kullanır. clients LEFT JOIN heartbeats ile o client'ın
    en son heartbeat satırı eşleştirilir; hiç heartbeat atmamış bir client
    (teorik olarak olmaz ama savunmacı olsun diye) hb_* alanları None döner.
    En son görülen en üstte (last_seen DESC).
    """
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT c.id, c.endpoint_id, c.hostname, c.computer_name, c.local_ip,
                   c.first_seen, c.last_seen,
                   h.timestamp AS hb_timestamp, h.user AS hb_user,
                   h.cpu_percent, h.ram_percent, h.ram_used_mb,
                   h.disk_read_mb, h.disk_write_mb, h.process_count, h.platform
            FROM clients c
            LEFT JOIN heartbeats h ON h.id = (
                SELECT id FROM heartbeats WHERE client_id = c.id ORDER BY id DESC LIMIT 1
            )
            ORDER BY c.last_seen DESC
            """
        ).fetchall()
        return [dict(r) for r in rows]


def get_client_by_id(client_id: int) -> dict | None:
    with _connect() as conn:
        row = conn.execute("SELECT * FROM clients WHERE id = ?", (client_id,)).fetchone()
        return dict(row) if row else None


def get_client_by_endpoint_id(endpoint_id: str) -> dict | None:
    """command_result gibi endpoint_id taşıyan (ya da taşıyacak) isteklerde
    client_id'ye çözmek için — bkz. server.py /command_result."""
    if not endpoint_id:
        return None
    with _connect() as conn:
        row = conn.execute("SELECT * FROM clients WHERE endpoint_id = ?", (endpoint_id,)).fetchone()
        return dict(row) if row else None


def get_latest_heartbeat_for_client(client_id: int) -> dict | None:
    """get_latest_heartbeat()'in çoklu-client sürümü — belirli bir client_id'nin en son heartbeat'i."""
    with _connect() as conn:
        row = conn.execute(
            "SELECT raw_json FROM heartbeats WHERE client_id = ? ORDER BY id DESC LIMIT 1",
            (client_id,),
        ).fetchone()
        return json.loads(row["raw_json"]) if row else None


def get_latest_command_result_for_client(client_id: int) -> dict | None:
    """get_latest_command_result()'ın çoklu-client sürümü."""
    with _connect() as conn:
        row = conn.execute(
            "SELECT command, status, data_json, at FROM command_results WHERE client_id = ? ORDER BY id DESC LIMIT 1",
            (client_id,),
        ).fetchone()
        if not row:
            return None
        return {
            "command": row["command"], "status": row["status"],
            "data": json.loads(row["data_json"]) if row["data_json"] else {},
            "at": row["at"],
        }


def get_latest_result_by_command_for_client(client_id: int, command_name: str) -> dict | None:
    """get_latest_result_by_command()'ın çoklu-client sürümü — process/system_info panellerinin
    seçili endpoint'e göre filtrelenmesi için (bkz. server.py /processes/latest, /system_info/latest)."""
    with _connect() as conn:
        row = conn.execute(
            "SELECT command, status, data_json, at FROM command_results "
            "WHERE client_id = ? AND command = ? ORDER BY id DESC LIMIT 1",
            (client_id, command_name),
        ).fetchone()
        if not row:
            return None
        return {
            "command": row["command"], "status": row["status"],
            "data": json.loads(row["data_json"]) if row["data_json"] else {},
            "at": row["at"],
        }


def cleanup_old_heartbeats(retention_days: int) -> int:
    """
    retention_days'ten eski heartbeat kayıtlarını siler. Silinen satır
    sayısını döner (loglamak için). command_results ayrı bir politikayla
    (bkz. cleanup_old_command_results) temizleniyor — Day 3'te "sınırsız
    tutulsun" kararı verilmişti, Day 6'da aktif/pasif ayrımıyla revize edildi.
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(days=retention_days)).isoformat()
    with _connect() as conn:
        cursor = conn.execute("DELETE FROM heartbeats WHERE timestamp < ?", (cutoff,))
        conn.commit()
        return cursor.rowcount


def cleanup_old_command_results(retention_days: int) -> int:
    """
    command_results tablosunu şişiren PASİF (salt-okunur/telemetri) komut
    sonuçlarını temizler — get_process_list/get_system_info gibi sistemde
    hiçbir şey değiştirmeyen komutların `retention_days`'ten eski kayıtları
    silinir.

    AKTİF (sistemi değiştiren: kill_process ve run_predefined_task'ın aktif
    alt görevleri) komutlar durum (ok/error/rejected) fark etmeksizin ASLA
    silinmez — bunlar denetim izidir, ne kadar eski olursa olsun kalıcı
    tutulur (bkz. ACTIVE_COMMANDS / ACTIVE_TASK_NAMES yukarıda).

    Silinen satır sayısını döner (loglamak için).
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(days=retention_days)).isoformat()
    with _connect() as conn:
        rows = conn.execute(
            "SELECT id, command, data_json FROM command_results WHERE at < ?", (cutoff,)
        ).fetchall()
        to_delete = [r["id"] for r in rows if not _is_active_command_result(r["command"], r["data_json"])]
        if not to_delete:
            return 0
        placeholders = ",".join("?" * len(to_delete))
        conn.execute(f"DELETE FROM command_results WHERE id IN ({placeholders})", to_delete)
        conn.commit()
        return len(to_delete)


def get_recent_heartbeats(limit: int = 50) -> list[dict]:
    """
    Dashboard'un Logs sayfası için: en son N heartbeat kaydını, en yeni
    üstte olacak şekilde döner. Bu, gerçek telemetri verisi — uydurma
    log satırları değil, agent'ın gerçekten gönderdiği heartbeat'ler.
    clients tablosuyla join edilerek hostname de dahil ediliyor.
    """
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT h.timestamp, h.cpu_percent, h.ram_percent, h.process_count, c.hostname
            FROM heartbeats h
            JOIN clients c ON h.client_id = c.id
            ORDER BY h.id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [
            {
                "timestamp": r["timestamp"],
                "hostname": r["hostname"],
                "cpu_percent": r["cpu_percent"],
                "ram_percent": r["ram_percent"],
                "process_count": r["process_count"],
            }
            for r in rows
        ]
