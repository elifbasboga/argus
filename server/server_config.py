"""
server_config.py — Argus Server config yönetimi.

server_config.json bulunamazsa ya da eksik alan varsa, DEFAULT_CONFIG'teki
değerler kullanılır — hiçbir zaman config dosyası zorunlu değil, "varsa
override et" mantığı. Bu, sunucuyu Mac'te (0.0.0.0'da dinleyerek, VM'den
gelen bağlantıları kabul edecek şekilde) hiç config dosyası olmadan bile
makul varsayılanlarla çalıştırabilmeni sağlıyor.
"""

import json
from pathlib import Path

CONFIG_PATH = Path(__file__).resolve().parent / "server_config.json"

DEFAULT_CONFIG = {
    "host": "0.0.0.0",  # VM'den gelen bağlantıları kabul edebilmesi için 127.0.0.1 DEĞİL
    "port": 8000,
    "db_path": "argus_server.db",
    "heartbeat_retention_days": 7,
    # PASİF komut sonuçları (get_process_list/get_system_info gibi) için ayrı
    # bir saklama süresi — AKTİF komutlar (kill_process vb.) bu süreden
    # bağımsız olarak HİÇBİR ZAMAN silinmez. bkz. server/db.py ACTIVE_COMMANDS.
    "command_result_retention_days": 7,
    "cleanup_interval_hours": 1,
}


def load_server_config() -> dict:
    config = DEFAULT_CONFIG.copy()
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH) as f:
            user_config = json.load(f)
        config.update(user_config)
    return config
