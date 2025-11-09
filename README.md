# RugBase

## Kurulum
1. Depoyu klonladıktan sonra proje kök dizininde `pip install -r requirements.txt` komutunu çalıştırarak Google Drive/Sheets bağımlılıklarını yükleyin.
2. Masaüstü uygulamasını geliştirme ortamında çalıştıracaksanız `python app.py` komutu yeterlidir.

## Google Sheets Senkronizasyonu
1. Google Sheet'i `rugbase-sync@rugbase-sync.iam.gserviceaccount.com` hizmet hesabıyla **Editor** yetkisi vererek paylaşın.
2. Uygulamayı başlatın ve menüden **Sync Settings** penceresini açın.
3. "Service Account JSON" alanından `service_account.json` dosyasını seçin. Dosya seçildiğinde hizmet hesabı e-postası ve Private Key ID alanları otomatik doldurulur.
4. "Bağımlılık Testi" butonu tüm Google kütüphanelerinin paketlemede yer aldığını doğrulamalıdır. Eksik paket uyarısı alırsanız `pip install -r requirements.txt` komutuyla ortamı güncelleyin ve PyInstaller ile yeniden paketleyin.
5. "Erişimi Doğrula" butonu hem Google Drive hem de Google Sheets erişimini kontrol eder. Sheet paylaşımı eksikse pencere gerekli yönergeyi gösterir.
6. Testler başarıyla tamamlandıktan sonra "Kaydet" diyerek ayarları kalıcı hale getirin ve ana pencereden senkronizasyonu başlatın.

## PyInstaller ile Paketleme
Uygulamayı paketlemeden önce bağımlılıkların import edilebildiğini doğrulayan kontrol, `build_exe.py` tarafından otomatik yapılır. Manuel olarak çalıştırmak için:

```
python build_exe.py
```

Script şu PyInstaller çağrısını üretir:

```
pyinstaller -y --name RugBase --noconsole \
  --hidden-import googleapiclient.discovery --hidden-import googleapiclient.http \
  --hidden-import googleapiclient._helpers --hidden-import google.oauth2.service_account \
  --hidden-import google.auth.transport.requests --hidden-import httplib2 \
  --hidden-import oauthlib.oauth2 --collect-submodules googleapiclient \
  --collect-submodules google --collect-submodules google.oauth2 app.py
```

Servis hesabı, token ve log dosyaları Windows'ta `C:\Users\<USER>\AppData\Local\RugBase\` dizini altında tutulur; masaüstüne dosya bırakılmaz.
