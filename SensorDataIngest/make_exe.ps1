Write-Output "Making sure pyinstaller is up to date:"
Write-Output "  pip.exe install --upgrade pyinstaller"
pip.exe install --upgrade pyinstaller
Write-Output "Using pyinstaller to build the distribution:"
Write-Output "  pyinstaller.exe ingest.spec"
pyinstaller.exe ingest.spec
Write-Output "Creating setup package ..."
Write-Output "  'C:\Program Files (x86)\Inno Setup 6\ISCC.exe' ..\ingest_installer.iss"
& 'C:\Program Files (x86)\Inno Setup 6\ISCC.exe' ..\ingest_installer.iss