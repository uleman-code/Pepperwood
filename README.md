# Sensor Data Ingest
An interactive application for:
- Loading files produced by Campbell Scientific data loggers,
- Visually inspecting the various time series contained in the files,
- And saving the data, along with column metadata and site information, in Excel workbooks.

## Installation
### Prerequisite: Python 3.12.2
Due to the use of libraries that do not yet support the latest versions of Python, this application requires version 3.12.2.
1. Get the Windows web installer for Python 3.12.2 at https://www.python.org/ftp/python/3.12.2/python-3.12.2-amd64.exe
1. Go through the Windows installer process:
    1. On the first screen,
        - Check the box **Add Python 3.12 to PATH**
        - Choose **Install Now**
    - Beware of long paths (deep hierarchies of directories with long names; may require a policy or registry change).
1. Verify that the Python launcher was installed: Enter the following command in a command window (the C-prompt is only shown for context).<br>
```C:\> py --list```<br>
This should show the currently installed version.

### Installing the application itself
1. Download the zip file from https://github.com/uleman-code/Pepperwood/archive/refs/heads/main.zip
    - The downloaded file is `Pepperwood-main.zip`.
1. Unzip the downloaded file in a directory of your choice.
    - This creates `…\your-directory\Pepperwood-main`, with files and a subdirectory.
1. Open a command window (or PowerShell).
    1. Change directory to `…\Pepperwood-main`.
    1. Create a virtual environment in either type of window (CMD = Command, PS = PowerShell):
        - **CMD** `python -m venv .`
        - **PS** `Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser`
    1. Activate the virtual environment (again, CMD or PS):
        - **CMD** `venv\Scripts\activate.bat`
        - **PS** `venv\Scripts\Activate.ps1`
    1. Install the required packages:
        - `pip install -r requirements.txt`
