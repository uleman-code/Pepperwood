Write-Output "Making sure pyinstaller is up to date:"
Write-Output "  uv lock --upgrade-package pyinstaller"
uv lock --upgrade-package pyinstaller
Write-Output "Using pyinstaller to build the distribution:"
Write-Output "  uv run pyinstaller ingest.spec"
uv run pyinstaller .\ingest.spec 2| Tee-Object -FilePath logs\pyinstaller.log
Write-Output "Creating setup package ..."
Write-Output "  'C:\Program Files (x86)\Inno Setup 6\ISCC.exe' ..\ingest_installer.iss"
& 'C:\Program Files (x86)\Inno Setup 6\ISCC.exe' ..\ingest_installer.iss 2| Tee-Object -FilePath logs\inno.log