"""
agent_config.py — Argus Agent config yönetimi.

agent_config.json bulunamazsa varsayılan olarak localhost'a (127.0.0.1:8000)
bağlanır — Day 1-2'deki gibi tek Mac üzerinde test ederken hiçbir şey
değişmez. Agent bir VM'e taşındığında (Day 7), tek yapman gereken
agent_config.json içindeki "server_url"ü ana Mac'in VM'den erişilebilir
IP'sine çevirmek — kod hiç değişmiyor.

Endpoint ID (Day 5): dashboard'un aynı cihazı ağ değişse bile ("hostname"
FQDN'i farklı ağlarda farklı görünebiliyor, örn. "be34.wls.metu.edu.tr" ev
ağında tamamen farklı bir şey olur) tutarlı biçimde tanıyabilmesi için,
agent ilk çalıştığında SABİT bir kimlik (endpoint_id) üretir ve
agent_config.json içine kalıcı olarak yazar. Bir daha asla değişmez — dosya
silinmediği sürece, agent'ı VM'e taşısan, ağ değiştirsen bile aynı ID
kullanılmaya devam eder. Sunucu tarafında client eşleştirmesi artık
hostname yerine bu ID'ye dayanıyor (bkz. server/db.py upsert_client).
"""

import json
import uuid
from pathlib import Path

CONFIG_PATH = Path(__file__).resolve().parent / "agent_config.json"

DEFAULT_CONFIG = {
    "server_url": "http://127.0.0.1:8000",
    # Heartbeat gönderme sıklığı (saniye). Sunucu tarafındaki online/offline
    # eşiği (isOnline(): son heartbeat 12sn'den eskiyse offline, bkz.
    # dashboard.html) bu değere göre ayarlı — bunu büyütürsen dashboard'da
    # da o eşiği güncellemen gerekir, aksi halde agent hâlâ çalışırken
    # "offline" görünebilir.
    "heartbeat_interval_seconds": 3,
}


def _generate_endpoint_id() -> str:
    return f"ARGUS-{uuid.uuid4().hex[:12].upper()}"


def load_agent_config() -> dict:
    config = DEFAULT_CONFIG.copy()
    user_config: dict = {}
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH) as f:
            user_config = json.load(f)
        config.update(user_config)

    # endpoint_id yoksa (ilk çalıştırma ya da eski bir agent_config.json'dan
    # geliyorsa) bir kere üret ve dosyaya geri yaz — sonraki her çalıştırmada
    # aynı ID okunur, kod tarafında ayrıca bir "ilk kurulum" adımı gerekmez.
    if not config.get("endpoint_id"):
        config["endpoint_id"] = _generate_endpoint_id()
        user_config["endpoint_id"] = config["endpoint_id"]
        with open(CONFIG_PATH, "w") as f:
            json.dump(user_config, f, indent=2)
            f.write("\n")

    return config
