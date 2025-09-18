Write-Output "Making sure pyinstaller is up to date:"
Write-Output "  pip.exe install --upgrade pyinstaller"
pip.exe install --upgrade pyinstaller
Write-Output "Using pyinstaller to build the distribution:"
Write-Output "  pyinstaller.exe ingest.spec"
pyinstaller.exe ingest.spec
Write-Output "Creating compressed archive ..."
Write-Output "  Compress-Archive -Force -Path .\ingest.cfg, .\dist\ingest.exe -DestinationPath .\ingest.zip"
Compress-Archive -Force -Path .\ingest.cfg, .\dist\ingest.exe -DestinationPath .\ingest.zip