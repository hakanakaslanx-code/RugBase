# -*- mode: python ; coding: utf-8 -*-

block_cipher = None


a = Analysis(
    ['app.py'],
    pathex=['.'],
    binaries=[],
    datas=[
        ('core', 'core'),
        ('ui_item_card.py', '.'),
        ('ui_main.py', '.'),
    ],
    hiddenimports=[
        'googleapiclient.discovery',
        'googleapiclient.errors',
        'googleapiclient.http',
        'google.oauth2.credentials',
        'google.auth.transport.requests',
        'google_auth_oauthlib.flow',
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
