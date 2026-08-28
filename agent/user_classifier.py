"""
user_classifier.py — Argus gerçek kullanıcı / sistem hesabı ayrımı.

macOS'te process listesi düzinelerce servis hesabı gösterir
(_modelmanagerd, _biome, _rmd, _securityagent, _oahd, nobody, vb.).
Bu modül, bir kullanıcı adının "gerçek" (insan tarafından kullanılan,
dashboard'da öne çıkarılacak) mı yoksa bir sistem/servis hesabı mı
olduğuna karar verir.

Kural:
  - "root" her zaman gerçek kabul edilir (UID 0, admin işlemleri için gerekli).
  - Adı alt çizgi (_) ile başlayan hesaplar sistem hesabıdır (macOS kuralı).
  - "nobody", "daemon", "guest" gibi bilinen sistem hesapları elenir.
  - Geri kalanlar için UID eşiği kullanılır: UID >= 500 olan hesaplar
    gerçek (insan) kullanıcı kabul edilir — bu, macOS'in normal kullanıcı
    hesaplarına UID atama kuralına dayanır.
"""

import pwd

KNOWN_NON_HUMAN_USERNAMES = {"nobody", "daemon", "guest"}
REAL_USER_UID_THRESHOLD = 500


def is_real_user(username: str) -> bool:
    if not username:
        return False

    if username == "root":
        return True

    if username.startswith("_"):
        return False

    if username in KNOWN_NON_HUMAN_USERNAMES:
        return False

    try:
        uid = pwd.getpwnam(username).pw_uid
    except KeyError:
        # Sistem pwd veritabanında olmayan / silinmiş kullanıcı -> güvenli taraf: sistem hesabı say
        return False

    return uid >= REAL_USER_UID_THRESHOLD
