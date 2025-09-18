# Sensor Data Ingest
An interactive application for:
- Loading files produced by Campbell Scientific data loggers
- Performing several automated sanity checks
- Visually inspecting the various time series contained in the files
- Saving the data, along with column and site metadata and notes about data anomalies, in Excel workbooks

## Installation (Windows ONLY)
1. Download the zip archive `ingest.zip` by clicking [this link](https://github.com/uleman-code/Pepperwood/raw/refs/heads/main/SensorDataIngest/ingest.zip).
1. Extract the zip archive into a folder of your choice.
    - For example, `C:\Users\<your-user-name>\AppData\Local`. But any location where you can easily find it will do.
    - You will get a binary executable, `ingest.exe`, along with a readable configuration file, `ingest.cfg`.
1. (Optional) Create a shortcut to the executable and put it on the taskbar and/or the desktop.
    - Tip: Give it a more descriptive name, like "Ingest Sensor Data".

## Running the application (Windows ONLY)
Double-click on the `ingest.exe` filename or <img width="32" height="32" alt="image" src="https://github.com/user-attachments/assets/1523a0ff-3153-4274-b94c-9d5f09c30125" />
icon or shortcut (single-click in the taskbar). A command window will open:

<img width="1116" height="225" alt="image" src="https://github.com/user-attachments/assets/2d99acc9-7523-4d5c-99ef-21aaf49c8fcf" />
(The directory shown in this screenshot depends on where the executable happens to be stored, and will be different for you.)

Keep this window open (minimizing is okay); as you use the app, further informational messages will appear.

Point your browser at http://localhost:8050/.
- Note: `localhost` is a synonym for `127.0.0.1`, shown in the screen shot above.

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
