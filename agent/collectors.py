"""
collectors.py — Argus passive data collectors.

Tümü salt okunur: sistemi/process'leri gözlemler, hiçbir şeyi değiştirmez.
"""

import platform
import shutil
import socket as socket_module
import subprocess

import psutil

# psutil 6.1.0'da connections() deprecated edildi, yerine net_connections()
# eklendi — ama net_connections() SADECE 6.1.0+ sürümlerinde var, daha eski
# psutil'lerde (bu projenin venv'inde olduğu gibi) hiç YOK ve çağrılırsa
# AttributeError fırlatıyor. "Hangisi mevcutsa onu kullan" şeklinde tek
# seferlik bir kontrolle ikisini de destekliyoruz — hem eski hem yeni psutil
# sürümünde çalışır, sürüm yükseltilince kod değişikliği gerekmez.
_CONNECTIONS_METHOD = "net_connections" if hasattr(psutil.Process, "net_connections") else "connections"


def _process_connections(proc, kind="inet"):
    return getattr(proc, _CONNECTIONS_METHOD)(kind=kind)

from logger import HOSTNAME
from user_classifier import is_real_user
from process_classifier import classify_process

# Not (Day 4): connectivity/ping özelliği tamamen kaldırıldı — bkz.
# dispatcher.py ve server.py'daki ilgili değişiklikler. socket_module
# importu hâlâ burada duruyor çünkü process bağlantılarının protokolünü
# (TCP/UDP) psutil'in SOCK_STREAM/SOCK_DGRAM sabitleriyle çözmek için
# kullanılıyor (bkz. collect_process_list).

# Endpoint kimliği için: ComputerName tek seferlik bir subprocess çağrısı
# gerektiriyor (scutil), her heartbeat'te tekrar çağırmamak için modül
# seviyesinde cache'leniyor. Cihaz açıkken adı değiştirse bile agent zaten
# yeniden başlatılınca güncel değeri alır — bu, 3sn'lik heartbeat döngüsünde
# gereksiz subprocess çağrısından kaçınmakla makul bir denge.
_computer_name_cache: str | None = None


def get_computer_name() -> str:
    """
    macOS'ta Sistem Ayarları > Genel > Paylaşım'da görünen "ComputerName" —
    kullanıcının cihaza verdiği, Wi-Fi ağı değiştiğinde DEĞİŞMEYEN sabit yerel
    ad. HOSTNAME (socket.gethostname()) ağ/DHCP'ye göre değişebildiği için
    (örn. "be34.wls.metu.edu.tr" -> farklı ağda farklı bir FQDN) endpoint
    kimliği için güvenilir değil; ComputerName daha stabil bir alternatif.
    macOS dışında (scutil yoksa) HOSTNAME'e düşer.
    """
    global _computer_name_cache
    if _computer_name_cache is not None:
        return _computer_name_cache

    try:
        result = subprocess.run(
            ["scutil", "--get", "ComputerName"],
            capture_output=True, text=True, timeout=3,
        )
        name = result.stdout.strip()
        _computer_name_cache = name if (result.returncode == 0 and name) else HOSTNAME
    except Exception:
        _computer_name_cache = HOSTNAME

    return _computer_name_cache


def get_local_ip() -> str | None:
    """
    O anki ağdaki local IPv4 adresi — outbound bir UDP socket açıp (hiçbir
    paket göndermeden, connect() sadece kernel'e "bu hedefe gitmek için hangi
    arayüzü kullanırdın" sorusunu sorduruyor) hangi arayüzün kullanılacağını
    öğrenme tekniği. Sadece bilgi amaçlı gösterim için; endpoint kimliği bu
    değere DAYANMIYOR (o iş agent_config.json'daki sabit endpoint_id'de).
    """
    try:
        with socket_module.socket(socket_module.AF_INET, socket_module.SOCK_DGRAM) as s:
            s.settimeout(1)
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
    except Exception:
        return None


def collect_process_list() -> list[dict]:
    """
    Tüm process'lerin metadata'sı: PID, isim, kullanıcı, cmdline,
    network connection'lar, real_user, ve category.

    real_user=True  -> root ya da UID>=500 insan kullanıcı hesabı
    real_user=False -> _modelmanagerd, _biome, nobody, _rmd gibi sistem/servis hesapları

    category -> "system_service" / "user_application" / "background_process"
    (process_classifier.py'daki path + parent process heuristics ile hesaplanır)

    Tek psutil.process_iter() geçişi yapılır; parent process isimleri
    ek bir sistem çağrısı yapmadan, aynı geçişte toplanan verilerden
    (pid -> name eşlemesi) çözülür.
    """
    raw = []
    for proc in psutil.process_iter(["pid", "name", "username", "cmdline", "exe", "ppid"]):
        try:
            info = proc.info
            try:
                # Not: connections() psutil 6.1.0'da deprecated edildi, yerine
                # net_connections() eklendi — ama net_connections() daha eski
                # psutil sürümlerinde hiç yok. _process_connections() ikisini
                # de destekleyen bir uyumluluk katmanı (yukarıda tanımlı).
                conns = _process_connections(proc, kind="inet")
                connections = [
                    {
                        "laddr": f"{c.laddr.ip}:{c.laddr.port}" if c.laddr else None,
                        "raddr": f"{c.raddr.ip}:{c.raddr.port}" if c.raddr else None,
                        "status": c.status,
                        # Dashboard'un canlı ağ bağlantı tablosu (Protokol sütunu)
                        # için: psutil socket.type üzerinden TCP/UDP ayrımı.
                        "protocol": (
                            "TCP" if c.type == socket_module.SOCK_STREAM
                            else "UDP" if c.type == socket_module.SOCK_DGRAM
                            else str(c.type)
                        ),
                    }
                    for c in conns
                ]
            except (psutil.AccessDenied, psutil.NoSuchProcess, OSError, RuntimeError):
                # RuntimeError özellikle: macOS'ta bazı process'ler için (örn.
                # kernel_task/PID 0 gibi gerçek bir "process" olmayan özel
                # PID'ler) proc_pidinfo(PROC_PIDLISTFDS) syscall'ı başarısız
                # oluyor VE psutil bunu OSError değil, düz bir RuntimeError
                # olarak fırlatıyor (mesaj: "proc_pidinfo(PROC_PIDLISTFDS)
                # 2/2 syscall failed" — sende bunu gerçek makinende
                # doğrulandı). Bu TEK process'in bağlantı bilgisini atlamalı,
                # tüm get_process_list komutunu çökertmemeli — bilinmeyen/
                # beklenmeyen hata sınıflarını (TypeError, AttributeError
                # gibi gerçek bug'ları) hâlâ yakalamıyoruz.
                connections = []

            raw.append(
                {
                    "pid": info["pid"],
                    "name": info["name"],
                    "user": info["username"],
                    # Not (düzeltme): DİZİ olarak taşınıyor, string'e join edilmiyor.
                    # Daha önce " ".join(...) ile tek string yapılıyordu — bu, boşluk
                    # içeren argümanları (örn. "Visual Studio Code.app" gibi yol
                    # parçalarını) geri dönüşü olmayan şekilde birbirine karıştırıyordu
                    # ve dashboard'da "her parametre kendi satırında" gösterimini
                    # imkansız kılıyordu. Dizi olarak taşımak hem doğru hem de
                    # dashboard'un tek tek renklendirip satır satır göstermesine izin veriyor.
                    "cmdline": info["cmdline"] or [],
                    "exe": info.get("exe") or "",
                    "ppid": info.get("ppid"),
                    "connections": connections,
                }
            )
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            continue

    # Ek psutil çağrısı yapmadan ppid -> parent adı çözümü için,
    # zaten topladığımız listeden bir pid->name haritası kuruyoruz.
    pid_to_name = {p["pid"]: p["name"] for p in raw}

    processes = []
    for p in raw:
        real_user = is_real_user(p["user"])
        parent_name = pid_to_name.get(p["ppid"], "")
        category = classify_process(
            {
                "exe": p["exe"],
                "name": p["name"],
                "real_user": real_user,
                "ppid": p["ppid"],
                "parent_name": parent_name,
            }
        )
        processes.append(
            {
                "pid": p["pid"],
                "name": p["name"],
                "user": p["user"],
                "real_user": real_user,
                "category": category,
                "cmdline": p["cmdline"],
                "connections": p["connections"],
            }
        )
    return processes


def collect_system_info() -> dict:
    """CPU kullanımı, RAM kullanımı, disk I/O."""
    disk_io = psutil.disk_io_counters()
    return {
        "cpu_percent": psutil.cpu_percent(interval=0.5),
        "ram_percent": psutil.virtual_memory().percent,
        "ram_used_mb": round(psutil.virtual_memory().used / (1024 * 1024), 1),
        "disk_read_mb": round(disk_io.read_bytes / (1024 * 1024), 1) if disk_io else None,
        "disk_write_mb": round(disk_io.write_bytes / (1024 * 1024), 1) if disk_io else None,
        "hostname": HOSTNAME,
        "platform": platform.platform(),
    }


def check_disk_space() -> dict:
    usage = shutil.disk_usage("/")
    return {
        "total_gb": round(usage.total / (1024**3), 1),
        "used_gb": round(usage.used / (1024**3), 1),
        "free_gb": round(usage.free / (1024**3), 1),
    }


def get_network_interfaces() -> dict:
    interfaces = {}
    for name, addrs in psutil.net_if_addrs().items():
        interfaces[name] = [a.address for a in addrs if a.family.name in ("AF_INET", "AF_INET6")]
    return interfaces



