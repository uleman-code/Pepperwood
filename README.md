# Sensor Data Ingest
An interactive application for:
- Loading files produced by Campbell Scientific data loggers
- Performing several automated sanity checks
- Visually inspecting the various time series contained in the files
- Saving the data, along with column and site metadata and notes about data anomalies, in Excel workbooks

## Installation (Windows ONLY)
1. Download the installer by clicking [this link](https://github.com/uleman-code/Pepperwood/raw/refs/heads/main/ingest_installer.exe).
1. Run the downloaded installer (a Setup Wizard), `ingest_installer.exe`.
    - By default, it installs into `C:\Users\<your-username>\AppData\Local\Programs\Ingest`; you can override this.
    - A shortcut is automatically added to the Start menu. In addition, you can choose (by filling a checkbox) to create a desktop shortcut.
    - Before finishing and closing the Setup Wizard, you can choose to launch the program (by leaving the checkbox filled). Since the application is intended to run in the background at all times, you might as well.
1. As part of the installation process, an uninstaller called `unins000.exe` is added to the same folder where the program is installed (see above). Uninstalling will remove anything created by the installer, including the program itself, any shortcuts, and the configuration settings file. Anything created later by the application (such as the `logs` and `file_system_backend` folders and any additional configuration files) is left alone.

## Running the application (Windows ONLY)
If not already launched by the installer, you can start the application in the usual ways of clicking "Ingest" in the Start menu or double-clicking the desktop shortcut. Or you can launch it from an existing Command or PowerShell window by typing the full path and filename:  `C:\Users\<your-username>\AppData\Local\Programs\Ingest\ingest.exe`.

A command window will open, showing the tail end of the current configuration settings (most of it will have scrolled off-screen).
Keep this window open (minimizing is okay); as you use the app, further informational messages will appear.

Point your browser at http://localhost:8050/.

You can leave this program running indefinitely; it's only providing a simple web server, which should not interfere with anything when idle.

To shut down, simply close the command window.

------
## Non-Windows installation
**WARNING: THIS PROCEDURE IS NOT TESTED; RESULTS NOT GUARANTEED**
### Prerequisite: Python 3.12 or later

### Installation (MacOS, Linux)
1. Download the zip file from https://github.com/uleman-code/Pepperwood/archive/refs/heads/main.zip
    - The downloaded file is `Pepperwood-main.zip`.
1. Unzip the downloaded file in a directory of your choice.
    - This creates `…/your-directory/Pepperwood-main`, with files and a subdirectory.
        - NOTE: if the directory and files already exist, this will overwrite the earlier contents.
1. Open a terminal window.
    1. Change directory to `…/Pepperwood-main`.
    1. (**SKIP IF UPDATING AN EXISTING INSTALLATION**) Create a virtual environment:<br/>
        `python -m venv .venv`<br/>
    1. Activate the virtual environment:<br/>
        `source .venv\bin\activate`<br/>
    1. Install the required packages:<br/>
        `pip install -r requirements.txt`

## Running the application (MacOS, Linux)
Either:
1. In a terminal window, change directory to `…\Pepperwood-main\SensorDataIngest`.
1. Issue this command:<br/>
   `python ingest.py`<br/>
   This prints out something like this:
```
Dash is running on http://127.0.0.1:8050/

* Serving Flask app 'ingest'
* Debug mode: on
Ingest|INFO Interactive ingest application started.
Ingest|INFO Logging directory is C:\...\Pepperwood\SensorDataIngest\logs.
```

and then:

3. Point your browser at http://localhost:8050/.
    - Note: `localhost` is a synonym for `127.0.0.1`, shown in the output snippet above.
