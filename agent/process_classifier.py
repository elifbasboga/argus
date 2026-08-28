"""
process_classifier.py — Argus process kategorileme.

Her process'i üç kategoriden birine ayırır:
  - system_service:     macOS/launchd tarafından yönetilen sistem servisleri
  - user_application:   kullanıcının Finder/Dock üzerinden başlattığı uygulamalar
  - background_process: yukarıdakilerin hiçbirine net uymayan, geri planda
                          çalışan process'ler (CLI araçları, script'ler, vb.)

Sınıflandırma sırayla şu sinyallere bakar (öncelik sırasıyla):
  1. .app bundle mı ("/Applications/" veya "/System/Applications/" altında)
     — sistem path kontrolünden ÖNCE bakılır, çünkü macOS Catalina'dan beri
     Apple'ın yerleşik uygulamaları (Preview, Mail, TextEdit, Photos, vb.)
     "/System/Applications/" altında yaşıyor; bu yol "/System/" ile başladığı
     için genel sistem path prefix kontrolü bunları YANLIŞ şekilde
     system_service sanabilir. .app bundle kontrolü önce çalışırsa bu
     yanlış pozitif oluşmaz.
  2. Yürütülebilir dosyanın diğer path'leri (system path'ler mi)
  3. Parent process ilişkisi (launchd'nin doğrudan çocuğu mu, Dock/Finder'ın
     başlattığı bir şey mi)
  4. Kullanıcı tipi (real_user — user_classifier.py'dan)

Path + isim + parent kombinasyonu tek başına path'ten daha doğru sonuç
verir çünkü örneğin bir sistem path'inde olmayan ama launchd tarafından
doğrudan başlatılan bir servisi de yakalar.
"""

CATEGORY_SYSTEM_SERVICE = "system_service"
CATEGORY_USER_APPLICATION = "user_application"
CATEGORY_BACKGROUND = "background_process"

SYSTEM_PATH_PREFIXES = (
    "/System/",
    "/usr/libexec/",
    "/usr/sbin/",
    "/Library/Apple/",
)

# .app bundle'ların yaşayabileceği "Applications" kökleri. "/System/Applications/"
# dahil edilmezse, Catalina+ sürümlerinde buraya taşınmış yerleşik Apple
# uygulamaları (Preview, Mail, TextEdit...) "/System/" prefix'ine takılıp
# system_service sanılır — bkz. modül docstring'i.
APPLICATIONS_ROOTS = ("/Applications/", "/System/Applications/")

# Kullanıcı etkileşimiyle uygulama başlatan tipik parent process'ler
INTERACTIVE_LAUNCHER_NAMES = {"Dock", "Finder", "loginwindow", "SystemUIServer"}


def classify_process(info: dict) -> str:
    """
    info dict şu alanları taşımalı:
      exe (str), name (str), real_user (bool), ppid (int|None), parent_name (str)
    """
    exe = info.get("exe") or ""
    real_user = info.get("real_user", False)
    ppid = info.get("ppid")
    parent_name = info.get("parent_name") or ""

    # 1. .app bundle içinden çalışıyor (Applications altında, kullanıcının kendi
    #    Applications klasörü ve /System/Applications/ dahil) -> user_application
    #    Sistem path kontrolünden ÖNCE bakılıyor (bkz. docstring).
    if ".app/Contents/MacOS/" in exe and any(root in exe for root in APPLICATIONS_ROOTS):
        return CATEGORY_USER_APPLICATION

    # 2. Bilinen sistem path'lerinden çalışıyor -> system_service
    if any(exe.startswith(prefix) for prefix in SYSTEM_PATH_PREFIXES):
        return CATEGORY_SYSTEM_SERVICE

    # 2b. /usr/bin altında ama gerçek kullanıcı tarafından çalıştırılmıyorsa -> system_service
    if exe.startswith("/usr/bin/") and not real_user:
        return CATEGORY_SYSTEM_SERVICE

    # 3. launchd'nin (PID 1) doğrudan çocuğu ve gerçek kullanıcı değilse -> system_service
    #    (macOS'te sistem daemon'ları tipik olarak doğrudan launchd'den başlar)
    if ppid == 1 and not real_user:
        return CATEGORY_SYSTEM_SERVICE

    # 4. Dock/Finder/loginwindow tarafından başlatılmışsa -> user_application
    #    (kullanıcı bir uygulamayı tıkladığında bu şekilde başlar)
    if parent_name in INTERACTIVE_LAUNCHER_NAMES:
        return CATEGORY_USER_APPLICATION

    # 5. Gerçek kullanıcı ve path içinde .app geçiyorsa -> user_application
    if real_user and ".app/" in exe:
        return CATEGORY_USER_APPLICATION

    # 6. Hiçbiri net değilse -> background_process
    return CATEGORY_BACKGROUND
