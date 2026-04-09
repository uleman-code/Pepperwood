"""All callbacks for the SensorDataIngest/ingest Dash app.

The Dash layout is defined in a separate module, layout.py.
The callbacks rely on functions from another module, helpers.py, for operations that
do not depend on or affect Dash elements.
"""

import logging
from datetime import datetime
from pathlib import Path
from dataclasses import asdict
from typing import Any, Callable, TypedDict, Required, NotRequired
from enum import StrEnum, auto
import decorator

import humanfriendly as hf

import dash_mantine_components as dmc
from dash import (  # A few definitions are not yet surfaced by dash-extensions
    Patch,
    set_props,
)
from dash.exceptions import PreventUpdate
from dash_extensions.enrich import (
    ALL,
    DashBlueprint,
    Input,
    Output,
    Serverside,
    State,
    Trigger,
    callback_context,
    dcc,
    no_update,
)

from . import config as cfg
from . import helpers
from . import layout
from .helpers import Frames, clear_file_cache

class QA_Status(StrEnum):
    READY = auto()
    COMPLETE = auto()

class Status(TypedDict):
    files: Required[list[str]]
    status: Required[bool]
    qa_status: NotRequired[QA_Status]
    qa_range: NotRequired[list[str]]
    no_save: NotRequired[bool]

logger: logging.Logger = logging.getLogger(f'{cfg.program_name}.{__name__}')
ee_logger: logging.Logger = logging.getLogger(f'{cfg.program_name}_ee.{__name__}')


@decorator.decorator
def log_func(fn: Callable, *args, **kwargs) -> Callable:
    """Function entry and exit logger, capturing exceptions as well.

    Very simplistic; no argument logging or execution timing.
    """

    ee_logger.debug('>>> Enter.', extra={'fname': fn.__name__})

    try:
        out = fn(*args, **kwargs)
    except PreventUpdate:  # Totally normal signal from a callback
        ee_logger.debug('<<< Exit.', extra={'fname': fn.__name__})
        raise
    except Exception as ex:
        ee_logger.debug('<<< Exception: %s', ex, exc_info=True, extra={'fname': fn.__name__})
        raise

    ee_logger.debug('<<< Exit.', extra={'fname': fn.__name__})
    return out

@decorator.decorator
def log_batch_func(fn: Callable, *args, **kwargs) -> Callable:
    """Function entry and exit logger for batch processing functions, capturing exceptions as well.

    Very simplistic; no general argument logging or execution timing. Specifically logs the file
    counter argument, which is always the first parameter and must be of type int.
    """

    ee_logger.debug('>>> (%s) Enter.', args[0], extra={'fname': fn.__name__})

    try:
        out = fn(*args, **kwargs)
    except PreventUpdate:  # Totally normal signal from a callback
        ee_logger.debug('<<< (%s) Exit.', args[0], extra={'fname': fn.__name__})
        raise
    except Exception as ex:
        ee_logger.debug('<<< (%s) Exception: %s', args[0], ex, exc_info=True, extra={'fname': fn.__name__})
        raise

    ee_logger.debug('<<< (%s) Exit.', args[0], extra={'fname': fn.__name__})
    return out

timestamp_column: str = cfg.config.metadata.timestamp_column
seqno_column: str = cfg.config.metadata.sequence_number_column
file_cache: Path = Path(cfg.config.application.file_cache_root)

blueprint: DashBlueprint = layout.blueprint


@blueprint.callback(
    Output('frame-store', 'data'),
    Output('read-error', 'opened'),
    Output('error-title', 'children'),
    Output('error-text', 'children'),
    Input('files-status', 'data'),
)
@log_func
def load_file(files_status: Status) -> tuple:
    """If one file was opened, load it.

    Triggered by a change in files-status after the user selects a single file, read the uploaded file 
    into a DataFrame, along with separate DataFrames for metadata (column descriptions) and site data,
    and possibly a notes table (if reading an Excel file).
    Persist the three DataFrames in the server-side frame-store.

    Parameters:
        files_status     File path(s) and (un)saved status

    Returns:
        frame-store/data     (dict[DataFrame]) The three DataFrames (data, meta, site) for one file
        read-error/opened    (bool) True in case of error (show error modal), otherwise False
        error-title/children (str)  In case of error, title for error modal; otherwise an empty string
        error-text/children  (str)  In case of error, error text for the modal dialog; otherwise an empty string

    """

    # This callback is triggered by any change to files-status. But action is only needed if there is one
    # new file to be loaded, in which case file is a single non-empty string, and the unsaved flag is True.
    # This only happens when a single new file is selected by the user:
    # - Another callback populates the filename and sets the unsaved flag to True.
    # - If the user selects multiple files, file is a list of strings, not a single string.
    # - After a Clear, filename is an empty string.
    # - After a Save, the unsaved flag is False.
    # - In Append mode, the unsaved flag is True but a qa_status element is added.
    if len(files_status['files']) != 1 or 'qa_status' in files_status:
        logger.debug('Zero or multiple files, or in Append mode; nothing to show interactively.')
        raise PreventUpdate

    if not files_status['unsaved']:
        logger.debug('File was just saved; no change to uploaded data.')
        raise PreventUpdate

    file: str = files_status['files'][0]
    try:
        frames: Frames = helpers.load_data(file)
        logger.debug('Data initialized.')
    except (helpers.BadFileError, helpers.UnsupportedFileTypeError) as err:
        logger.error('File Read Error.')
        return (
            no_update,
            True,
            'Error Reading File',
            f'We could not process the file "{file}": {err}',
        )

    try:
        helpers.merge_metadata(frames)
    except helpers.SiteIdNotFoundError as err:
        logger.info('Continuing with incomplete metadata: %s', err)
        return (
            Serverside(asdict(frames), key='Frames'),
            True,
            'Continuing with incomplete metadata:',
            str(err)
        )

    # Keep the DataFrames store on the server. This avoids potentially large transfers between
    # server and browser, along with all the associated conversions (JSON) and encodings
    # (base64). Instead, we can store a DataFrame as-is. This simplifies code and improves
    # performance in plot rendering and file saving, sometimes dramatically.
    # Providing a key to the Serverside constructor makes the serverside cache use and reuse a
    # single file, preventing unlimited storage growth.
    return Serverside(asdict(frames), key='Frames'), False, '', ''


@blueprint.callback(
    Output('save-xlsx', 'data'),
    Output('files-status', 'data', allow_duplicate=True),
    Trigger('save-button', 'n_clicks'),
    Input('files-status', 'data'),
    State('frame-store', 'data'),
    running=[(Output('wait-please', 'visible'), True, False)],  # Show busy indicator while saving
)
@log_func
def save_file(files_status: Status, frame_store: dict[str, Any] | None) -> tuple:
    """Save the data currently in memory in an Excel (.XLSX) file.

    In response to a button click, take the data from the serverside frame store, download it to the browser, and have
    the browser write it to a file, of the same name as the original but with a ".xlsx" extension. Depending
    on the user's browser settings, this either silently saves the file in a pre-designated folder (e.g., Downloads)
    or opens the OS-native Save File dialog, allowing the user to choose any folder (and change the filename, too).

    NOTE: If the Save File dialog is opened, it's possible that the user clicks Cancel, in which case the file is not
          saved. There seems to be no way for the program to detect this. For now, this app assumes that the file
          is always saved.

    This callback is also triggered by completion of the QA/sanity checks in Append mode. We do not require the user
    to click Save again after an Append operation.

    Parameters:
        files_status    Server-side file paths and (un)saved status
        frames          The four DataFrames (data, meta, site, notes) for one file

    Returns:
        save-xlsx/data       (dict) Content and filename to be downloaded to the browser
        files-status/data    (str)  Same as files_status parameter but with the unsaved flag set to False
    """

    if callback_context.triggered_id == 'files-status' and not (
        'qa_status' in files_status and files_status['qa_status'] == QA_Status.COMPLETE
    ):
        logger.debug('Data is not ready to be saved.')
        raise PreventUpdate

    frames: Frames = Frames(**frame_store) if frame_store else None

    if frames and not frames.data.empty:
        outfile: str = Path(files_status['files'][0]).stem + '.xlsx'
        # frames: Frames = Frames(**frame_store)
        outfile: str = Path(files_status['files'][0]).stem + '.xlsx'

        # Dash provides a convenience function to create the required dictionary. That function in turn
        # relies on a writer (e.g., DataFrame.to_excel) to produce the content. In this case, that writer
        # is a custom function specific to this app.
        contents: dict[str, Any | None] = dcc.send_bytes(helpers.multi_df_to_excel(frames), outfile)
        files_status['unsaved'] = False

        # Remove artifacts, if any, of an Append process, so the combined data looks as if it was read directly from
        # a single file (except for the displayed sanity check results). You could repeat the Append action to chain
        # any number of files together, not just two.
        files_status = {k: v for k, v in files_status.items() if k in ['files', 'unsaved']}

        logger.debug('File saved.')
        return contents, files_status
    else:
        logger.debug('Nothing to save.')
        raise PreventUpdate


@blueprint.callback(
    Output('files-status', 'data', allow_duplicate=True),
    Output('frame-store', 'clear_data'),
    Output('show-data', 'children', allow_duplicate=True),
    Output('file-name', 'children', allow_duplicate=True),
    Output('file-attributes', 'children', allow_duplicate=True),
    Trigger('clear-button', 'n_clicks'),
    State('show-data', 'children'),
)
@log_func
def clear(show_data: list[dmc.CardSection]) -> tuple:
    """Clear all data in memory and on the screen, triggered by the Clear button.

    Set the filename and unsaved flag in files-status to blank and False, respectively, which in turn
    triggers all the follow-on chain of callbacks (clear the UI, clear the DataFrame store, etc.).

    Parameters:
        show_data       The layout of the main app area

    Returns:
        files-status/data      (str)  Empty list of server-side file paths, unsaved flag False
        frame-store/clear_data (bool) Delete the contents of the serverside DataFrame store
        show-data/children     (list[dmc.CardSection]) Truncated contents of the main app area:
                               remove batch processing output, if any
        file-name/children     (str)  Empty string to clear
        file-attributes/children (str)  Empty string to clear

    """

    logger.debug('Responding to Clear button click. Reset files-status.')

    status: Status = Status(files=[], unsaved=False)

    # Always clear the DataFrame store, truncate the main app area, and clear the filename/file-attributes text.
    return status, True, show_data[:3], None, None


@blueprint.callback(
    Output('files-status', 'data', allow_duplicate=True),
    Output('show-data', 'children', allow_duplicate=True),
    Input('select-file' , 'uploadedFiles'),
    State('show-data', 'children'),
    # TODO: Use failedFiles for error processing.
)
@log_func
def files_uploaded(uploaded_files: list[dict[str, str | int | dict[str, str | int]]],
                   show_data: list[dmc.CardSection]) -> dict[str, list[str] | bool]:
    """One or more files were selected and uploaded to the server.
    
    Parameters:
        uploaded_files      Filenames and other info of the uploaded files
        show_data           The layout of the main app area

    Returns:
        files-status/data   (dict) Server-side file paths and unsaved status
        show-data/children  (list[dmc.CardSection]) Truncated contents of the main app area:
                            remove batch processing output, if any
    """

    files: list[Path] = [str(file_cache / file['response']['filename']) for file in uploaded_files]
    if not files:
        logger.debug('Files were selected but none were uploaded.')
        raise PreventUpdate
    
    logger.debug('%s file(s) uploaded: %s', len(uploaded_files), ', '.join([f['name'] for f in uploaded_files]))

    status: dict[str, str | list[str] | bool] = dict(files=files, unsaved=True)

    return status, show_data[:3]


@blueprint.callback(
    Output('select-file', 'disabled'),
    Output('select-file', 'uploadedFiles'),
    Output('load-label', 'c'),
    Input('files-status', 'data'),
)
@log_func
def toggle_loaddata(status: Status) -> tuple:
    """Disable the Load Data element when new data is loaded and not (yet) saved; re-enable when data is cleared or saved.

    This includes graying out the label of the Load Data area; the rest is governed by the Upload component
    and grayed out automatically.

    If data is cleared or saved, empty the upload-file cache, since it means we no longer need those files. Also
    clear the uploaded-files list, to ensure that a callback always happens when the user selects a file, even
    if it's the same file again.

    Parameters:
        status (Status) Filename(s) and (un)saved status

    Returns:
        select-file/disabled      (bool) True if unsaved data in memory; False otherwise (no data or data was saved)
        select-file/uploadedFiles (list) If not unsaved, empty list to clear the uploaded-files list
        load-label/c              (str)  Color for the Load Data area label: dimmed for disabled, black for enabled
    """

    unsaved: bool = status['unsaved']
    logger.debug(
        'Data %s saved%s; %sable Load Data.',
        'not' if unsaved else 'is', '' if unsaved else ' or cleared', 'dis' if unsaved else 'en'
    )

    if not unsaved:
        helpers.clear_file_cache()

    return unsaved, no_update if unsaved else [], 'dimmed' if unsaved else 'black'


@blueprint.callback(
    Output('inspect-data', 'display'),
    Output('data-columns', 'children'),
    Output('select-columns', 'value'),
    Input('frame-store', 'data'),
    State('files-status', 'data'),
)
@log_func
def show_columns(frame_store: dict[str, Any] | None, status: Status) -> tuple:
    """When data is loaded, populate the column selection element with checkboxes for all data columns (variables).

    When there is no data (for example, after a Clear), clear the column list, delete the checkboxes,
    and hide that part of the Navbar.

    When there's data but the unsaved flag is False, do nothing. That is because this callback is invoked every time
    anything at all changes in the data in memory. If the unsaved flag is False, it means that we got here because
    that flag just changed (because the user clicked Save XLSX), which in turn means that the columns were already
    populated when that data set was loaded, when the unsaved flag was still True, and nothing in the data itself
    has changed since then. This also avoids clearing the selected columns (checked boxes) just because the user
    did a Save.

    Parameters:
        frame_store  The four DataFrames (data, meta, site, notes) for one file
        status  Filename(s) and (un)saved status

    Returns:
        inspect-data/display  (str)  Show the column selection part of the Navbar if there's data; otherwise blank it
        data-columns/children (list) A list of dmc.Checkbox elements, one for each data column
        select-columns/value  (list) Reset the current selection (uncheck all boxes)
    """

    frames: Frames = Frames(**frame_store) if frame_store else None

    if (frames and not frames.data.empty):
        if status['unsaved']:
            logger.debug('DataFrame found. Populating variable selection list.')
            data = frames.data

            # Skip the timestamp and sequence number columns; these are not data columns.
            checkboxes: list[dmc.Checkbox] = [
                dmc.Checkbox(
                    label=c,
                    value=c,
                    size='sm',
                )
                for c in data.columns
                if c not in [timestamp_column, seqno_column]
            ]
            return 'flex', checkboxes, []
        else:
            raise PreventUpdate
    else:
        logger.debug('No data; clear column selection element.')
        return 'none', '', []


@blueprint.callback(
    Output('stacked-graphs', 'figure'),
    Output('plot-area', 'display'),
    Input('select-columns', 'value'),
    Input('single-plot', 'checked'),
    State('frame-store', 'data'),
)
@log_func
def draw_plots(showcols: list[str], single_plot: bool, frame_store: dict[str, Any] | None) -> tuple:
    """Draw plots, one below the other, for each of the selected columns.

    Redraw the entire stacked plot each time the selection changes.

    Parameters:
        showcols    Column names, in the order in which they were selected
        frames      The four DataFrames (data, meta, site, notes) for one file
        single_plot If true draw a single multivariable plot, otherwise multiple single-variable plots

    Returns:
        stacked-graphs/figure (Figure) Plotly figure of all graphs in one stacked plot
        plot-area/display     (str)    Hide the plot area if there are no graphs to show
                                       (otherwise you see an empty set of axes)
    """

    frames = Frames(**frame_store) if frame_store else None

    if showcols:
        logger.debug('Columns selected; generating graphs.')
        fig = helpers.render_graphs(frames.data, frames.meta, showcols, single_plot)
        return fig, 'contents'
    else:
        logger.debug('No columns selected; clear the graphs.')
        return {}, 'none'


@blueprint.callback(
    Output('saved-badge', 'display'),
    Output('save-xlsx', 'data', allow_duplicate=True),
    Input('files-status', 'data'),
)
@log_func
def show_badge(files_status: Status) -> tuple:
    """Respond to a Save action by showing a SAVED badge.

    Because this is triggered after every single-file save action, also use this callback to clear the data
    in dcc.Download. Not clearing the data may result in the download action continuing to be triggered by
    every UI interaction.

    Parameters:
        files_status    Server-side file paths and (un)saved status

    Returns
        saved-badge/display (str) Show ('inline') the SAVED badge if data was saved; otherwise hide it ('none')
        save-xlsx/data      (obj) None, to clear the data
    """

    # To show the badge, there must be a single file loaded and it must be saved.
    # (The unsaved flag is also False if there nothing loaded.)
    files: list[str] = files_status['files']
    retval: str

    if len(files) == 1:
        unsaved: bool = files_status['unsaved']
        logger.debug('Single file loaded, file %ssaved; %s Saved badge.',
                     'not ' if unsaved else '', 'hide' if unsaved else 'show')
        retval = 'none' if unsaved else 'inline'
    else:
        logger.debug('Zero or multiple files loaded; hide Saved badge.')
        retval = 'none'

    return retval, None


@blueprint.callback(
    Output('save-button', 'disabled'),
    Output('append-button', 'disabled'),
    Output('clear-button', 'disabled'),
    Input('files-status', 'data'),
)
@log_func
def toggle_save_clear(files_status: Status) -> tuple:
    """If there's one file loaded, enable the Save, Clear, and Append buttons; otherwise, disable them.

    Batches have their own logic and use of these buttons.

    Parameters:
        files_status    Filename(s) and (un)saved status

    Returns:
        save-button/disabled   True to disable (no data); False to enable (data in memory)
        append-button/disabled True to disable (no data); False to enable (data in memory)
        clear-button/disabled  True to disable (no data); False to enable (data in memory)
    """

    files: str = files_status['files']
    have_file: bool = len(files) == 1
    do_not_save: bool = 'no_save' in files_status and files_status['no_save']

    if have_file:
        if do_not_save:
            logger.debug(
                'One file in memory, but a data problem makes it unwise to save. Enable Clear but disable Save and Append buttons.'
            )
            return True, True, False
        else:
            logger.debug('One file in memory. Enable Save, Append, and Clear buttons.')
            return (False,) * 3
    else:
        logger.debug('Zero or multiple files in memory. Disable Save, Append and Clear buttons.')
        return (True,) * 3


@blueprint.callback(
    Output('file-name', 'children'),
    Output('file-attributes', 'children'),
    Input('files-status', 'data'),
    State('select-file', 'uploadedFiles'),
)
@log_func
def show_file_info(files_status: Status, uploaded_files: list[dict[str, str | int | dict[str, str | int]]]):
    """If there's data in memory, show information (filename, file-attributes) about the file that was loaded.

    Parameters:
        files_status    File paths and (un)saved status
        uploaded_files  File names and other info for the uploaded files

    Returns:
        file-name/children      (str) The name of the currently loaded file (no path)
        file-attributes/children  (str) Friendly-formatted size of the currently loaded file
    """

    files: list[str] = files_status['files']

    # Make sure there's only one file and we're not appending.
    match len(files):
        case 1:
            if 'qa_status' not in files_status:
                name: str = uploaded_files[0]['name']
                info: str = f'Size: {hf.format_size(uploaded_files[0]["size"])}'
                logger.debug('Data in memory; show file information.')
                return name, info
            else:
                logger.debug('Append in process; nothing to do.')
                raise PreventUpdate
        case 0:
            logger.debug('In-memory data was cleared. Clear file information.')
            return '', ''
        case _:
            logger.debug('Multiple files loaded; nothing to do.')
            raise PreventUpdate


@log_func
def run_sanity_checks(frames: Frames, qa_range: list[str] | None = None) -> list[dmc.Text]:
    """Callback helper function: run the sanity/QA checks.

    Collect the results and create the messages to be displayed in the UI in case irregularities were found.
    In principle, any number of checks can be reported on in the top part of the main app area or alongside each
    filename in a batch. In practice, if the list grows too long, a page layout redesign may be needed.

    Parameters:
        frames (In/Out pandas.DataFrame)  The three or four DataFrames (data, meta, site, possibly notes) for one file
        data  (In/Out pandas.DataFrame) The DataFrame containing the sensor data time series; may be updated to fix dropouts
        notes (In/Out pandas.DataFrame) If not None, the notes previously generated from the new data set (in Append mode)
        qa_range                          If not None, the range of timestamps [start, end] to be sanity-checked (in Append mode)

    Returns:
        A list of Text objects, each element representing a simple sanity check result
        A DataFrame with rows describing occurrences of missing values or samples
        The original data DataFrame, possibly with additional rows to complete the time series
    """

    report: list[dmc.Text] = []
    missing_values: bool
    missing_samples: bool

    duplicate_samples, missing_values, missing_samples = helpers.run_qa(frames, qa_range)

    if duplicate_samples:
        report.append(
            dmc.Text('Duplicate samples were found and dropped.', c='red', ta='right')
        )

    if missing_values:
        report.append(
            dmc.Text('One or more variables have data dropouts.', c='red', ta='right')
        )

    if missing_samples:
        report.append(
            dmc.Text(
                'There are gaps in the time series; placeholder samples were inserted.',
                c='red',
               
                ta='right',
            )
        )

    return report


@blueprint.callback(
    Output('sanity-checks', 'children'),
    Output('files-status', 'data', allow_duplicate=True),
    Output('frame-store', 'data', allow_duplicate=True),
    State('sanity-checks', 'children'),
    Input('files-status', 'data'),
    Input('frame-store', 'data'),
)
@log_func
def report_sanity_checks(
    current_report: list[dmc.Text] | None, status: Status, frame_store: dict[str, Any] | None
) -> tuple[list[dmc.Text], dict, Serverside[dict]]:
    """Perform sanity checks/QA on the data and report the results in a separate area of the app shell.

    If anything like data dropouts is found, record that in a separate DataFrame, which will become
    the Notes worksheet in the output file upon save. In some cases (such as missing rows/samples),
    the data may be altered (for example, by adding NaN-filled rows to fill gaps in the time series).

    This is triggered simply by the availability of new sensor data.

    Parameters:
        current_report  Previously shown sanity check results, if any
        status          Filename(s) and (un)saved status
        frames          The three or four DataFrames (data, meta, site, possibly notes) for one file

    Returns:
        sanity-checks/children  The results of a few simple sanity checks
        frame-store/data        The four DataFrames (data, meta, site, notes) for one file
    """

    frames: Frames = Frames(**frame_store) if frame_store else None

    if not frames or frames.data.empty:
        logger.debug('No data loaded. Clear the sanity check reports.')
        return [], no_update, no_update

    if not frames.notes.empty and 'qa_status' not in status:
        logger.debug('Notes worksheet already populated. Do nothing.')
        raise PreventUpdate

    # At callback level, we don't know about pandas, but we can still apply python functions like len() to DataFrames.
    data = frames.data

    report: list[dmc.Text]
    qa_range: list[str] | None

    if 'qa_status' in status:
        # Append mode: an existing Excel file was loaded and concatenated with the new data.
        # Sanity-test only the part of the time series indicated by qa_range, and append the results
        # to whatever is already reported (but remove duplicates).
        logger.debug('New data appended to an existing file. Run and report sanity check on what is new.')
        assert current_report is not None

        # For some reason (JSON, I presume), the children of a Stack are returned as dicts, not Text objects. Turn them back into objects.
        current_report = [dmc.Text(**c['props']) for c in current_report]
        report = current_report + [dmc.Text(f'{len(data):,} total samples after appending.', ta='right')]
        qa_range = status['qa_range']
        status['qa_status'] = QA_Status.COMPLETE
    else:
        logger.debug('New data found. Running and reporting sanity checks.')
        report = [dmc.Text(f'{len(data):,} samples; {len(data.columns) - 2} variables.', ta='right')]
        qa_range = None

    qa_report: list[dmc.Text]

    try:
        qa_report = run_sanity_checks(frames, qa_range)
    except (helpers.DuplicateTimestampError, helpers.TimestampColumnNotFoundError) as err:
        qa_report = [dmc.Text(str(err), c='red', ta='right')]
        status['no_save'] = True  # Do not save; requires manual intervention

    report += qa_report
    report = list({t.children: t for t in report}.values())  # Remove duplicates while maintaining order (only needed in Append mode)

    return report, status, Serverside(asdict(frames), key='Frames')


@blueprint.callback(
    Output('file-name'    , 'children'     , allow_duplicate=True),
    Output('file-attributes', 'children'     , allow_duplicate=True),
    Output('next-file'    , 'data'         , allow_duplicate=True),
    Input('files-status'  , 'data'         ),
    State('select-file'   , 'uploadedFiles'),
)
@log_func
def setup_batch(files_status: Status, uploaded_files: list[dict[str, str | int | dict[str, str | int]]]) -> tuple:
    """Set up for batch operation by starting the loop counter. Show a batch operation header.

    Looping over a batch of multiple files works as follows:
        Multiple files selected (select-file -> clear_load -> files-status)
        ==> 1) next-file := 0 (setup_batch)
            ==> 2) file-counter := next-file (next_in_batch)
                ==> 3a) next-file += 1 (increment_file_counter)
                ==> 3b) process one file (process_batch)
                    --> REPEAT from step 2)
    This has the effect of walking quickly through the batch index from 0 to len(batch)-1,
    while queueing the long-running processing step for each file.

    Parameters:
        files_status    Filename(s) and (un)saved status
        uploaded_files  Filenames and other info of the uploaded files

    Returns:
        file-name/children          (str) Reuse for Batch mode operation header
        file-attributes/children    (list[str]) Reuse for start and completion time of batch operation
        next-file/data              (int) Set the next value for the loop counter
        next-file/data              (int) Set the next value for the loop counter

    """

    if len(uploaded_files) <= 1:
        logger.debug('No file or a single filename: not a batch.')
        raise PreventUpdate

    # When the batch is complete, the unsaved flag gets set to False.
    if not files_status['unsaved']:
        logger.debug('Batch already done. Do not start again.')
        raise PreventUpdate

   # files_status has the server-side full paths, which may have sanitized (safe) versions of the original filenames.
    # Use the client-side filenames provided by the Upload component instead.
    filenames: list[str] = [f['name'] for f in uploaded_files]
    logger.debug('Files loaded: %s.', ", ".join(filenames))
    start_time: str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    next_file: int = 0  # This triggers the start of the loop over file_counter
    return 'Batch mode operation', [f'{len(filenames)} files. Started at {start_time}'], next_file


@blueprint.callback(
    Output( 'show-data'   , 'children'          ),
    Output( 'file-counter', 'data'              ),
    Trigger('next-file'   , 'modified_timestamp'),
    State(  'next-file'   , 'data'              ),
    State(  'select-file' , 'uploadedFiles'     ),
)
@log_batch_func
def next_in_batch(next_file: int, uploaded_files: list[dict[str, str | int | dict[str, str | int]]]) -> tuple:
    """Show file information and a busy indicator for the current item in the batch.

    Parameters:
        next_file       The current file's index in the batch list
        uploaded_files  Filenames and other info of the uploaded files

    Returns:
        show-data/children (object) Patch object to add another CardSection to the main app area
        file-counter/data  (int)    The file counter value for the current batch item
    """

    # files_status has the server-side full paths, which may have sanitized (safe) versions of the original filenames.
    # Use the client-side filenames provided by the Upload component instead.
    this_file = uploaded_files[next_file]

    # Construct a whole new CardSection element, to be appended to the show-data area
    this_file_info: dmc.CardSection = layout.make_file_info(next_file)              # file-info-n
    this_file_info.children.children[1].children[0].children = this_file['name']    # file-name-n
    this_file_info.children.children[1].children[1].children[0].children = f'Size: {hf.format_size(this_file["size"])}' # file-attributes-n

    showdata: Patch = Patch()
    showdata.append(this_file_info)

    return showdata, next_file


@blueprint.callback(
    Output( 'next-file'   , 'data'              , allow_duplicate=True),
    Trigger('file-counter', 'modified_timestamp'),
    State(  'file-counter', 'data'              ),
    State(  'select-file' , 'uploadedFiles'     ),
)
@log_batch_func
def increment_file_counter(file_counter: int, uploaded_files: list[dict[str, str | int | dict[str, str | int]]]) -> int:
    """Set the next value for the batch loop index (file counter). Stop at the end of the batch.

    The reason for incrementing the loop index in a separate callback rather than in process_batch()
    is that callbacks can run in parallel, so we don't want to wait for the previous file to be fully
    processed (a long-running callback) before kicking off the next one. This way, the entire batch
    can be iterated over quickly, with the long-running processing callbacks queued up and run in
    multithreaded fashion in whatever way Dash has to take advantage of multiple available CPU cores.

    Parameters:
        file_counter    The index of the current file in the list of files (the batch)
        filenames       The selected filenames (here only used to get the length of the batch)
    
    Returns:
        next-file/data  The next value for the file counter.
        
    Raises
        PreventUpdate when the end of the batch is reached.
    """

    next_file: int = file_counter + 1

    if next_file >= len(uploaded_files):
        logger.debug('Reached the end of the batch; stop operation.')
        raise PreventUpdate

    return next_file

def done_no_save(file_counter: int) -> None:
    """If the current file cannot be saved, stop the Loader and display a NOT SAVED badge.

    Parameters:
        file_counter    The index of the current file in the list of files (the batch)
    """

    set_props(f'wait-please-{file_counter}', {'visible': False})
    set_props(
        {'type': 'saved-badge', 'index': file_counter},
        {'children': 'NOT SAVED', 'display': 'inline', 'color': 'red'},
    )
    logger.info('(%s) File not saved.', file_counter)

@blueprint.callback(
    Trigger('file-counter', 'modified_timestamp'),
    State(  'file-counter', 'data'              ),
    State(  'files-status', 'data'              ),
    # background=True,
)
@log_batch_func
def process_batch(file_counter: int, status: Status) -> tuple:
    """Process one file in the batch, without user involvement.

    Read the file contents into DataFrames, perform sanity checks, and save the DataFrames to an Excel file.
    When done, display the results of the sanity checks, hide the busy indicator, and show the Saved badge.

    NOTE: This callback uses set_props() to directly manipulate UI elements, instead of relying on return
        values to do so. This is the only way I could think of to address dynamically created elements,
        with generated names ending in a suffix indicating the batch index (file counter), such as
        sanity-checks-0, saved-badge-3, etc.

    Parameters:
        file_counter    The index of the current file in the list of files (the batch)
        status          Filename(s) and (un)saved status
   """

    files: list[str] = status['files']
    if (len(files) <= file_counter):  # We got here because there's a batch, so this should not happen
        logger.error(
            '(%s) Something is wrong. Processing file %s but there are only %s files in the batch.',
            file_counter, file_counter, len(files)
        )
        logger.debug('(%s) Exit.', file_counter)
        return (
            True,
            'System Error:',
            f'Processing file number {file_counter} but there are only {len(files)} in the batch.'
        )

    file: str = files[file_counter]
    outfile: str = Path(file).stem + '.xlsx'

    logger.debug('(%s) Processing %s.', file_counter, Path(file).name)

    # Read the file contents into DataFrames.
    try:
        frames = helpers.load_data(file)
        logger.debug('(%s) Data initialized.', file_counter)
    except (helpers.BadFileError, helpers.UnsupportedFileTypeError) as err:
        logger.error('(%s) File Read Error:\n%s', file_counter, err)
        set_props(f'sanity-checks-{file_counter}', {'children': [dmc.Text(f'Error reading file {Path(file).name}:', c='red', ta='right'),
                                                                 dmc.Text(str(err), c='red', ta='right')]})
        done_no_save(file_counter)
        return

    data = frames.data
    report: list[dmc.Text] = [dmc.Text(f'{len(data):,} samples; {len(data.columns) - 2} variables.', ta='right')]

    no_save = False

    try:
        helpers.merge_metadata(frames)
    except helpers.SiteIdNotFoundError as err:
        logger.info('Continuing with incomplete metadata: %s', err)
        report += [dmc.Text(f'Incomplete metadata: {err}', c='red', ta='right')]

    # Perform sanity/QA checks and report the results, except:
    #   If there already is a Notes DataFrame, then it was read in from a previously saved, and possibly edited, Excel file.
    #   In that case, neither make corrections to the data nor generate a new Notes worksheet.
    if frames.notes.empty:
        try:
            qa_report: list[dmc.Text] = run_sanity_checks(frames, None)
        except (helpers.DuplicateTimestampError, helpers.TimestampColumnNotFoundError) as err:
            no_save = True
            qa_report = [dmc.Text(str(err), c='red', ta='right')]

        report += qa_report

    set_props(f'sanity-checks-{file_counter}', {'children': report})

    # If duplicate timestamps were found (an unrecoverable error), skip the saving.
    # But we stil need to turn off the Loader and show a Badge, because the batch process looks
    # for badges to know when it's complete. Instead of the usual "SAVED", though, let the user
    # know that no file was saved.
    if no_save:
        done_no_save(file_counter)
        return

    # Save the file.
    # Dash provides a convenience function to create the required dictionary. That function in turn
    # relies on a writer (e.g., DataFrame.to_excel) to produce the content. In this case, that writer
    # is a custom function specific to this app.
    data_for_download: dict[str, Any | None] = dcc.send_bytes(
        helpers.multi_df_to_excel(frames), outfile
    )
    logger.debug('(%s) Got byte string for Download.', file_counter)
    set_props(f'save-xlsx-{file_counter}', {'data': data_for_download})
    logger.debug('(%s) Download complete. Clean up.', file_counter)

    set_props(f'wait-please-{file_counter}', {'visible': False})
    set_props({'type': 'saved-badge', 'index': file_counter}, {'display': 'inline'})

    return


@blueprint.callback(
    Output('files-status', 'data'),
    Output('file-attributes', 'children', allow_duplicate=True),
    State('files-status', 'data'),
    Input({'type': 'saved-badge', 'index': ALL}, 'display'),
)
@log_func
def batch_done(files_status: Status, badges: list[str]) -> tuple:
    """Keep track of batch progress; set file unsaved flag to re-enable new file selection when all files are processed.

    Look for changes to the Saved badges: setup_batch() creates one, invisible (display='none'), for each file in the
    batch, and process_batch() makes it visible (display='inline') as it finishes with each file. When all badges are
    visible, the batch is complete.

    When the batch is complete, empty the upload-file cache.

    Parameters:
        files_status    Server-side file paths and unsaved status
        badges          The display attribute for all batch-related Saved badges
                        NOTE: This takes advantage of pattern-matching callback inputs.

    Returns:
        files-status/data      (str)  Same as files_status parameter but with the unsaved flag set to False
        file-attributes/children (str)  Add the completion time of the batch operation (start time was added by setup_batch())
    """

    if badges:  # This also gets triggered when all batch-related badges disappear
        logger.debug('There are %s files in progress: %s', len(badges), badges)

        if all(display == 'inline' for display in badges):
            logger.debug('Batch complete.')
            files_status['unsaved'] = False
            clear_file_cache()
            end_time: Patch = Patch()
            end_time.append(f' \N{EM DASH} Complete at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
            return files_status, end_time
        else:
            logger.debug(
                f'Batch not complete; so far only {len([d for d in badges if d == "inline"])} files.'
            )
            raise PreventUpdate
    else:
        logger.debug('No batch in progress.')
        raise PreventUpdate


@blueprint.callback(
    Output('frame-store' , 'data'    , allow_duplicate=True),
    Output('files-status', 'data'    , allow_duplicate=True),
    Output('append-file' , 'contents'),
    Output('read-error'  , 'opened'  , allow_duplicate=True),
    Output('error-title' , 'children', allow_duplicate=True),
    Output('error-text'  , 'children', allow_duplicate=True),
    State( 'frame-store' , 'data'    ),
    State( 'files-status', 'data'    ),
    State( 'append-file' , 'filename'),
    Input( 'append-file' , 'contents'),
)
@log_func
def append_file(
    frame_store: dict[str, Any], status: Status, filename: str, contents: str
) -> tuple:
    """An existing Excel file was opened, to be appended to. Append the current data and update the frame store.

    Copy the metadata and site data from the new set; if they're not the same as the existing set, assume that the
    descriptions were edited since last saved and don't treat it as an error.

    Copy the notes, if any, from the existing file. Let the QA code know where to start its analysis: at the start
    of the time series (if there were no previously saved notes), or at the transition to the new data (if there
    already were notes).

    Parameters:
        new_frames       The three or four DataFrames (data, meta, site, possibly notes) from the currently loaded new file
        status           Server-side file path (of the new data) and (un)saved status
        filename         Filename of the newly loaded existing Excel file
        contents         Base64-encoded file contents of the newly loaded existing Excel file

    Returns:
        frame-store/data     (dict) The three or four DataFrames of the combined set
        files-status/data    (dict) Original files_status, with new members, the timestamp range on which to perform QA analysis
        append-file/contents (list) Empty to reset, so a new Append file upload always results in a contents change
        read-error/opened    (bool) True in case of error (show error modal), otherwise False
        error-title/children (str)  In case of error, title for error modal; otherwise an empty string
        error-text/children  (str)  In case of error, error text for the modal dialog; otherwise an empty string

    """

    new_frames: Frames = Frames(**frame_store)

    try:
        base_frames: Frames = helpers.load_data(filename, contents)
        logger.debug('Existing file data initialized.')
        combined_frames: Frames
        combined_frames, status['qa_range'] = helpers.append(base_frames, new_frames)
        status['qa_status'] = QA_Status.READY
        logger.info(
            f'Base {len(base_frames.data)}; New {len(new_frames.data)}; Combined {len(combined_frames.data)}'
        )

        return Serverside(combined_frames, key='Frames'), status, [], False, '', ''
    except helpers.UnmatchedColumnsError as e:
        logger.error(e)
        return no_update, no_update, no_update, True, 'Unmatched files', str(e)
    except Exception as e:
        logger.error(e)
        logger.error(f'File Read Error:\n{e}')
        return (
            no_update,
            no_update,
            no_update,
            True,
            'Error Reading File',
            f'We could not process the file "{filename}": {e}',
        )
