# Argus

Argus, macOS cihazlarını merkezi olarak izlemek ve yönetmek için geliştirilmiş,
istemci-sunucu (agent-server) mimarisine sahip bir uç nokta (endpoint)
yönetim sistemidir. Ağdaki her cihaza kurulan hafif bir agent, düzenli
aralıklarla merkezi sunucuya sistem durumunu (CPU, RAM, süreç sayısı, ağ
bilgisi vb.) raporlar; sunucu bu verileri saklar ve canlı bir web dashboard
üzerinden görselleştirir.

## Özellikler

- **Heartbeat tabanlı izleme** — her agent belirli aralıklarla (varsayılan
  3 saniye) sunucuya durum bildirir, sunucu cihazın online/offline
  durumunu buna göre belirler.
- **Komut dağıtımı** — sunucu üzerinden agent'lara komut gönderilebilir,
  agent tarafında bir allowlist (izin verilen komutlar listesi) ile
  güvenli şekilde çalıştırılır ve sonuç sunucuya geri raporlanır.
- **Kalıcı kimlik (`endpoint_id`)** — her cihaz, ağ veya IP değişse bile
  sabit kalan bir kimlikle tanınır; dashboard'da cihaz kaybolmaz.
- **Dayanıklı iletişim** — ağ kesintilerinde komut sonuçları kaybolmaz,
  bağlantı kurulana kadar kuyrukta tutulup toplu olarak gönderilir.
- **RFC 5424 uyumlu loglama** — hem agent hem sunucu tarafında yapılandırılmış,
  aranabilir log kayıtları tutulur.
- **Canlı web dashboard** — bağlı cihazları, heartbeat geçmişini ve log
  kayıtlarını tarayıcı üzerinden izleme.
- **launchd servis desteği** — hem agent hem sunucu, macOS'ta arka planda
  kalıcı olarak çalışacak şekilde (`launchd`) yapılandırılabilir.

## Klasör yapısı

- `agent/` — İzlenen cihazlarda çalışan istemci
  - `agent.py` — Ana çalışma döngüsü (config + heartbeat)
  - `agent_config.py` — Config yükleme mantığı
  - `collectors.py` — Sistem bilgisi toplama (CPU, RAM, IP vb.)
  - `dispatcher.py` — Komut çalıştırma ve allowlist kontrolü
  - `logger.py` — RFC5424 loglama
  - `process_classifier.py` — Süreç sınıflandırma
  - `user_classifier.py` — Kullanıcı sınıflandırma
  - `agent_config.example.json`
- `server/` — Merkezi sunucu
  - `server.py` — FastAPI uygulaması, API endpoint'leri
  - `db.py` — SQLite veritabanı katmanı
  - `logger.py` — RFC5424 loglama
  - `server_config.example.json`
- `dashboard/` — Statik web dashboard
  - `dashboard.html`
- `venv/` — (git'e dahil değil) Python sanal ortamı


## Kurulum

### Gereksinimler
- Python 3.10+
- macOS (agent, `psutil` ile sistem bilgisi toplamak için macOS'a özgü
  bazı komutlar kullanıyor; sunucu platformdan bağımsız çalışabilir)

### 1) Depoyu klonla
```bash
git clone https://github.com/elifbasboga/argus.git
cd argus
```

### 2) Sunucu kurulumu
```bash
cd server
python3 -m venv ../venv
source ../venv/bin/activate
pip install fastapi uvicorn pydantic
cp server_config.example.json server_config.json
```
`server_config.json` içindeki `admin_passcode` alanını kendi güçlü bir
parolanızla değiştirin — bu, agent'lara kill/kritik komut gönderirken
kullanılan onay parolasıdır.

Sunucuyu başlat:
```bash
uvicorn server:app --host 0.0.0.0 --port 8000
```

### 3) Agent kurulumu (izlenecek her cihazda)
```bash
cd agent
python3 -m venv ../venv
source ../venv/bin/activate
pip install psutil requests
cp agent_config.example.json agent_config.json
```
`agent_config.json` içinde:
- `server_url`: sunucunun erişilebilir adresi (örn. `http://<sunucu-ip>:8000`)
- `endpoint_id`: bu cihaz için benzersiz bir kimlik (birden fazla cihaz
  kuracaksanız her birine **farklı** bir `endpoint_id` verin)

Agent'ı başlat (bazı sistem bilgilerini toplayabilmesi için root gerekir):
```bash
sudo python3 agent.py
```

### 4) Dashboard'u aç
`dashboard/dashboard.html` dosyasını bir tarayıcıda açıp sunucu adresini
girerek bağlı cihazları, heartbeat akışını ve logları izleyebilirsiniz.

## Arka plan servisi olarak çalıştırma

macOS'ta hem agent'ı hem sunucuyu `launchd` ile kalıcı bir arka plan
servisi haline getirmek mümkündür (`launchctl load/unload`). Geliştirme
sürecinde test ortamı olarak ikinci bir fiziksel Mac yerine bir UTM sanal
makinesi kullanılmış, agent VM içinde, sunucu ise host makinede servis
olarak çalıştırılmıştır.

## Güvenlik notları

- `server_config.json` ve `agent_config.json` dosyaları **gerçek**
  yapılandırma bilgilerinizi (parola, sunucu adresi, cihaz kimliği)
  içerdiği için `.gitignore` ile depo dışında tutulur — sadece
  `.example.json` şablonları depoda yer alır.
- `admin_passcode` API yanıtlarından her zaman dışlanır, hiçbir
  endpoint'te açığa çıkmaz.
- Üretim ortamında sunucuyu genel internete açmadan önce mutlaka
  bir ters proxy / TLS / kimlik doğrulama katmanı eklemeniz önerilir.

## Lisans

Bu proje [MIT Lisansı](LICENSE) altında yayınlanmaktadır.

