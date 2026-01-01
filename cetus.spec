# -*- mode: python ; coding: utf-8 -*-
# PyInstaller spec file for cetus CLI
# Build with: pyinstaller cetus.spec

from PyInstaller.utils.hooks import collect_data_files, collect_submodules

block_cipher = None

# Collect all submodules for packages that use dynamic imports
hiddenimports = [
    'tomllib',  # Python 3.11+ built-in TOML
    'tomli',    # Fallback for Python < 3.11
    'platformdirs',
    'click',
    'rich',
    'rich.console',
    'rich.table',
    'rich.progress',
    'rich.markup',
    'httpx',
    'httpx._transports',
    'httpx._transports.default',
]

# Add all rich submodules for terminal rendering
hiddenimports += collect_submodules('rich')

a = Analysis(
    ['src/cetus/cli.py'],
    pathex=[],
    binaries=[],
    datas=[],
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        # Exclude test/dev dependencies
        'pytest',
        'pytest_httpx',
        'ruff',
        # Exclude unused stdlib modules
        'tkinter',
        'unittest',
    ],
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
    [],
    name='cetus',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
