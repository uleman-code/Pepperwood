pip install --upgrade pyinstaller
pyinstaller ingest.spec
@REM copy dist\ingest.exe .
tar cvzf ingest.zip -C dist ingest.exe -C .. ingest.cfg