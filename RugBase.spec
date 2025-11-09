# -*- mode: python ; coding: utf-8 -*-

import certifi
from PyInstaller.utils.hooks import collect_submodules

block_cipher = None

google_hidden = collect_submodules('googleapiclient') + collect_submodules('google')


a = Analysis(
    ['app.py'],
    pathex=['.'],
    binaries=[],
    datas=[
        ('core', 'core'),
        ('ui_item_card.py', '.'),
        ('ui_main.py', '.'),
        ('resources/wheels', 'resources/wheels'),
        (certifi.where(), 'certifi'),
    ],
    hiddenimports=[
        'googleapiclient',
        'googleapiclient.discovery',
        'googleapiclient.http',
        'googleapiclient._auth',
        'google.auth',
        'google_auth_oauthlib.flow',
        'google.oauth2.service_account',
        'google.auth.transport.requests',
        'httplib2',
        *google_hidden,
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)
pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)
exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    name='RugBase',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
)
