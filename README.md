# RugBase

## Installation
1. After cloning the repository, run `pip install -r requirements.txt` in the project root to install the Google Drive/Sheets dependencies.
2. To run the desktop application in a development environment, execute `python app.py`.

## Google Sheets Synchronization
1. Share the Google Sheet with the service account `rugbase-sync@rugbase-sync.iam.gserviceaccount.com` and grant **Editor** access.
2. Launch the application and open the **Sync Settings** window from the menu.
3. Choose the `service_account.json` file in the “Service Account JSON” field. The service account email and Private Key ID fields are populated automatically when the file is selected.
4. The “Dependency Test” button verifies that all Google libraries are included in the package. If any packages are missing, run `pip install -r requirements.txt` to update the environment and rebuild with PyInstaller.
5. The “Verify Access” button checks both Google Drive and Google Sheets access. If the Sheet is not shared correctly, the window provides the necessary instructions.
6. After the tests finish successfully, click “Save” to persist the settings and start synchronization from the main window.

## Packaging with PyInstaller
`build_exe.py` automatically verifies that the dependencies can be imported before packaging. To run it manually:

```
python build_exe.py
```

The script produces the following PyInstaller invocation:

```
pyinstaller -y --name RugBase --noconsole \
  --hidden-import googleapiclient.discovery --hidden-import googleapiclient.http \
  --hidden-import googleapiclient._helpers --hidden-import google.oauth2.service_account \
  --hidden-import google.auth.transport.requests --hidden-import httplib2 \
  --hidden-import oauthlib.oauth2 --collect-submodules googleapiclient \
  --collect-submodules google --collect-submodules google.oauth2 app.py
```

Service account, token, and log files are stored under `C:\Users\<USER>\AppData\Local\RugBase\` on Windows; nothing is saved on the desktop.
