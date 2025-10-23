"""All callbacks for the SensorDataIngest/ingest Dash app.

The Dash layout is defined in a separate module, layout.py.
The callbacks rely on functions from another module, helpers.py, for operations that
do not depend on or affect Dash elements.
"""

import logging
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

import dash_mantine_components as dmc
import decorator
import helpers  # Local module implementing Dash-independent actions
import layout
from config import capitalized_program_name, config
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
    ServersideOutputTransform,
    State,
    Trigger,
    TriggerTransform,
    callback_context,
    dcc,
    no_update,
)

logger: logging.Logger = logging.getLogger(
    f'{capitalized_program_name}.{__name__}'
)  # Child logger inherits root logger settings
frame_store: dict = {}


@decorator.decorator
def log_func(fn: Callable, *args, **kwargs) -> Callable:
    """Function entry and exit logger, capturing exceptions as well.

    Very simplistic; no argument logging or execution timing.
    """

    ee_logger: logging.Logger = logging.getLogger(f'{capitalized_program_name}.{__name__}')
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


timestamp_column: str = config['metadata']['timestamp_column']
seqno_column: str = config['metadata']['sequence_number_column']

blueprint: DashBlueprint = DashBlueprint(
    transforms=[ServersideOutputTransform(), TriggerTransform()]
)
blueprint.layout = dmc.MantineProvider(layout.layout)


@blueprint.callback(
    Output('frame-store', 'data'),
    Output('read-error', 'opened'),
    Output('error-title', 'children'),
    Output('error-text', 'children'),
    Input('files-status', 'data'),
    State('select-file', 'contents'),
)
@log_func
def load_file(files_status: dict[str, str | bool], all_contents: list[str]) -> tuple:
    """If one file was opened, load it.

    Triggered by a change in files-status after the user selects a single file, read the base64-encoded file contents
    provided by the Upload component into a DataFrame, along with separate DataFrames for metadata (column descriptions)
    and site data. Persist the three DataFrames in the server-side frame-store.

    Parameters:
        files_status     Filename(s) and (un)saved status
        all_contents     Base64-encoded file contents

    Returns:
        frame-store/data     (dict[DataFrame]) The three DataFrames (data, meta, site) for one file
        read-error/opened    (bool) True in case of error (show error modal), otherwise False
        error-title/children (str)  In case of error, title for error modal; otherwise an empty string
        error-text/children  (str)  In case of error, error text for the modal dialog; otherwise an empty string

    """
    filename: str = files_status['filename']
    unsaved: str = files_status['unsaved']

    # This callback is triggered by any change to files-status. But action is only needed if there is one
    # new file to be loaded, in which case filename is a single non-empty string, and the unsaved flag is True.
    # This only happens when a single new file is selected by the user:
    # - Another callback populates the filename and sets the unsaved flag to True.
    # - If the user selects multiple files, filename is a list of strings, not a single string.
    # - After a Clear, filename is an empty string.
    # - After a Save, the unsaved flag is False.
    if isinstance(filename, list) or not filename or 'qa_status' in files_status:
        logger.debug('Zero or multiple files, or in Append mode; nothing to show interactively.')
        raise PreventUpdate

    if not unsaved:
        logger.debug('File was just saved; no change to loaded data.')
        raise PreventUpdate

    contents: str = all_contents[0]  # We got here, so there is exactly one file

    try:
        frames: dict[str, Any] = helpers.load_data(contents, filename)

        logger.debug('Data initialized.')

        # Keep the DataFrames store on the server. This avoids potentially large transfers between
        # server and browser, along with all the associated conversions (JSON) and encodings
        # (base64). Instead, we can store a DataFrame as-is. This simplifies code and improves
        # performance in plot rendering and file saving, sometimes dramatically.
        # Providing a key to the Serverside constructor makes the serverside cache use and reuse a
        # single file, preventing unlimited storage growth.
        return Serverside(frames, key='Frames'), False, '', ''
    except Exception as e:
        logger.exception('File Read Error.')
        return (
            no_update,
            True,
            'Error Reading File',
            f'We could not process the file "{filename}": {e}',
        )


@blueprint.callback(
    Output('save-xlsx', 'data'),
    Output('files-status', 'data', allow_duplicate=True),
    Output('select-file', 'contents', allow_duplicate=True),
    Trigger('save-button', 'n_clicks'),
    Input('files-status', 'data'),
    State('frame-store', 'data'),
    running=[
        (Output('wait-please', 'display'), 'flex', 'none')
    ],  # Show busy indicator while saving
)
@log_func
def save_file(files_status: dict[str, str | bool], frames: dict[str, Any]) -> tuple:
    """Save the data currently in memory in an Excel (.XLSX) file.

    In response to a button click, take the data from the serverside frame store, download it to the browser, and have
    the browser write it to a file, of the same name as the original but with a ".xlsx" extension. Depending
    on the user's browser settings, this either silently saves the file in a pre-designated folder (e.g., Downloads)
    or opens the OS-native Save File dialog, allowing the user to choose any folder (and change the filename, too).

    NOTE: If the Save File dialog is opened, it's possible that the user clicks Cancel, in which case the file is not
        saved. There seems to be no way for the program to detect this. For now, this app assumes that the file
        is always saved.

    Parameters:
        files_status    Filename(s) and (un)saved status
        frames          The four DataFrames (data, meta, site, notes) for one file

    Returns:
        save-xlsx/data       (dict) Content and filename to be downloaded to the browser
        files-status/data    (str)  Same as files_status parameter but with the unsaved flag set to False
        select-file/contents (list) Empty to reset, so a new file upload always results in a contents change, even if
                                    it's the same file(s) as previously loaded
    """

    if callback_context.triggered_id == 'files-status' and not (
        'qa_status' in files_status and files_status['qa_status'] == 'Complete'
    ):
        logger.debug('Data is not ready to be saved.')
        raise PreventUpdate

    if (
        frames and 'data' in frames
    ):  # If not initialized, it's not a dict; so check first if there's anything there
        outfile: str = str(Path(files_status['filename']).with_suffix('.xlsx'))

        # Dash provides a convenience function to create the required dictionary. That function in turn
        # relies on a writer (e.g., DataFrame.to_excel) to produce the content. In this case, that writer
        # is a custom function specific to this app.
        contents: dict[str, Any | None] = dcc.send_bytes(helpers.multi_df_to_excel(frames), outfile)
        files_status['unsaved'] = False

        # Remove artifacts, if any, of an Append process, so the combined data looks as if it was read directly from
        # a single file (except for the displayed sanity check results). You could repeat the Append action to chain
        # any number of files together, not just two.
        files_status = {k: v for k, v in files_status.items() if k in ['filename', 'unsaved']}

        logger.debug('File saved.')
        return contents, files_status, []
    else:
        logger.debug('Nothing to save.')
        raise PreventUpdate


@blueprint.callback(
    Output('files-status', 'data', allow_duplicate=True),
    Output('select-file', 'contents', allow_duplicate=True),
    Output('frame-store', 'clear_data'),
    Output('show-data', 'children', allow_duplicate=True),
    Output('file-name', 'children', allow_duplicate=True),
    Output('last-modified', 'children', allow_duplicate=True),
    Trigger('clear-button', 'n_clicks'),
    Input('select-file', 'contents'),
    State('select-file', 'filename'),
    State('show-data', 'children'),
)
@log_func
def clear_load(all_contents: list[str], filenames: list[str], show_data: list[Any]) -> tuple:
    """Clear all data in memory and on the screen, triggered by the Clear button or the loading of (a) new file(s) in the Upload component.

    If new file(s), set the filename and unsaved flag in files-status, which in turn triggers all the follow-on chain
    of callbacks (load the file or process the batch, along with the UI expressions of the process); and clear all UI
    elements that may be populated by the current file, in preparation for the new information.

    NOTE: The clearing of some UI components (file-name, last-modified) must happen here, because relying on another callback
        to do it makes it difficult to guarantee that the clearing happens before the new information is shown. There is no
        way to guarantee the execution order of callbacks triggered by the same trigger.
        The exception is the Saved badge: since it is not reused for other purposes, showing or hiding it can be handled
        by its own callback.

    Parameters:
        all_contents    Base64-encoded file contents for all files in the batch
        filenames       The selected filename(s), provided by the Upload component
        show_data       The layout of the main app area, consisting of a list of CardSections

    Returns:
        files-status/data      (str)  JSON representation of a cleared data store: filename blank, unsaved flag False
        select-file/contents   (list) Empty to reset, so a new file upload always results in a contents change, even if
                                    it's the same file(s) as previously loaded
        frame-store/clear_data (bool) Delete the contents of the serverside DataFrame store
        show-data/children     (list[objects]) Truncated contents of the main app area: remove batch processing output, if any
        file-name/children     (str)  Empty string to clear
        last-modified/children (str)  Empty string to clear
    """

    status: dict[str, str | bool]
    if callback_context.triggered_id == 'clear-button':
        logger.debug(
            'Responding to Clear button click. Reset files-status and select-file contents.'
        )
        status = dict(filename='', unsaved=False)
        contents = []
    elif all_contents:
        fn: str | list[str]
        fntext: str
        if len(filenames) == 1:
            fntext = fn = filenames[0]  # Make life easier for callbacks processing a single file
        else:
            fn = filenames
            fntext = ', '.join(filenames)

        logger.debug(f'File(s) loaded: {fntext}.')
        status = dict(filename=fn, unsaved=True)
        contents = no_update  # This was triggered by a new file, so don't mess with the contents
    else:
        raise PreventUpdate  # Contents cleared by another callback; nothing to do

    # Always clear the DataFrame store, truncate the main app area, and clear the filename/last-modified text.
    return status, contents, True, show_data[:3], None, None


@blueprint.callback(
    Output('select-file', 'disabled'),
    Output('load-label', 'c'),
    Input('files-status', 'data'),
)
@log_func
def toggle_loaddata(status: dict[str, str | bool]) -> tuple:
    """Disable the Load Data element when new data is loaded and not (yet) saved; re-enable when data is cleared or saved.

    This includes graying out the label of the Load Data area; the rest is governed by the Upload component
    and grayed out automatically.

    Parameters:
        status (dict) Filename(s) and (un)saved status

    Returns:
        select-file/disabled (bool) True if unsaved data in memory; False otherwise (no data or data was saved)
        load-label/c         (str)  Color for the Load Data area label: dimmed for disabled, black for enabled
    """

    unsaved: bool = status['unsaved']
    logger.debug(
        f'Data {"not" if unsaved else "is"} saved{"" if unsaved else " or cleared"}; {"dis" if unsaved else "en"}able Load Data.'
    )
    return unsaved, 'dimmed' if unsaved else 'black'


@blueprint.callback(
    Output('inspect-data', 'display'),
    Output('data-columns', 'children'),
    Output('select-columns', 'value'),
    Input('frame-store', 'data'),
    State('files-status', 'data'),
)
@log_func
def show_columns(frames: dict, status: dict) -> tuple:
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
        frames  The four DataFrames (data, meta, site, notes) for one file
        status  Filename(s) and (un)saved status

    Returns:
        inspect-data/display  (str)  Show the column selection part of the Navbar if there's data; otherwise blank it
        data-columns/children (list) A list of dmc.Checkbox elements, one for each data column
        select-columns/value  (list) Reset the current selection (uncheck all boxes)
    """

    if (
        frames and 'data' in frames
    ):  # Make sure frames is a dict before checking for presence of data
        if status['unsaved']:
            logger.debug('DataFrame found. Populating variable selection list.')
            data = frames['data']

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
def draw_plots(showcols: list[str], single_plot: bool, frames: dict) -> tuple:
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

    if showcols:
        logger.debug('Columns selected; generating graphs.')
        data = frames['data']
        fig = helpers.render_graphs(data, showcols, single_plot)

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
def show_badge(files_status: dict) -> tuple:
    """Respond to a Save action by showing a SAVED badge.

    Because this is triggered after every single-file save action, also use this callback to clear the data
    in dcc.Download. Not clearing the data may result in the download action continuing to be triggered by
    every UI interaction.

    Parameters:
        files_status    Filename(s) and (un)saved status

    Returns
        saved-badge/display (str) Show ('inline') the SAVED badge if data was saved; otherwise hide it ('none')
        save-xlsx/data      (obj) None, to clear the data
    """

    # To show the badge, there must be a single file loaded and it must be saved.
    # (The unsaved flag is also False if there nothing loaded.)
    filename: str = files_status['filename']
    retval: str
    if filename and isinstance(filename, str):
        unsaved: bool = files_status['unsaved']
        logger.debug(
            f'Single file loaded, file {"not " if unsaved else ""}saved; {"hide" if unsaved else "show"} Saved badge.'
        )
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
def toggle_save_clear(files_status: dict) -> tuple:
    """If there's one file loaded, enable the Save and Clear buttons; otherwise, disable them.

    Batches have their own logic and use of these buttons.

    Parameters:
        files_status    Filename(s) and (un)saved status

    Returns:
        save-button/disabled   True to disable (no data); False to enable (data in memory)
        append-button/disabled True to disable (no data); False to enable (data in memory)
        clear-button/disabled  True to disable (no data); False to enable (data in memory)
    """

    filename: str = files_status['filename']
    have_file = bool(
        filename and isinstance(filename, str)
    )  # Must coerce type to avoid non-boolean result if filename is empty
    do_not_save = 'no_save' in files_status and files_status['no_save']

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
    Output('last-modified', 'children'),
    Input('files-status', 'data'),
    State('select-file', 'last_modified'),
)
@log_func
def show_file_info(files_status: dict[str, str | bool], last_modified: list[int]):
    """If there's data in memory, show information (filename, last-modified) about the file that was loaded.

    Parameters:
        files_status    Filename(s) and (un)saved status
        last_modified   File last-modified timestamps (there should only be one element in the list)

    Returns:
        file-name/children      (str) The name of the currently loaded file (no path)
        last-modified/children  (str) Formatted last-modified timestamp of the currently loaded file
    """

    filename: str = files_status['filename']

    # Make sure there's only one file and we're not appending.
    if isinstance(filename, str) and filename and 'qa_status' not in files_status:
        modified: str = f'Last modified: {datetime.fromtimestamp(last_modified[0]).strftime("%Y-%m-%d %H:%M:%S")}'
        logger.debug('Data in memory; show file information.')
        return filename, modified
    else:
        logger.debug('Zero or multiple files loaded, or in Append process; nothing to do.')
        raise PreventUpdate


@log_func
def run_sanity_checks(
    data, notes, qa_range: list[str] | None = None
) -> tuple[list[dmc.Text], Any, Any]:
    """Callback helper function: run the sanity/QA checks.

    Collect the results and create the messages to be displayed in the UI in case irregularities were found.
    In principle, any number of checks can be reported on in the top part of the main app area or alongside each
    filename in a batch. In practice, if the list grows too long, a page layout redesign may be needed.

    Parameters:
        data  (In/Out pandas.DataFrame) The DataFrame containing the sensor data time series; may be updated to fix dropouts
        notes (In/Out pandas.DataFrame) If not None, the notes previously generated from the new data set (in Append mode)
        qa_range                        If not None, the range of timestamps [start, end] to be sanity-checked (in Append mode)

    Returns:
        A list of Text objects, each element representing a simple sanity check result
        A DataFrame with rows describing occurrences of missing values or samples
        The original data DataFrame, possibly with additional rows to complete the time series
    """

    report: list[dmc.Text] = []
    missing_values: bool
    missing_samples: bool

    try:
        duplicate_samples, missing_values, missing_samples, notes, fixed = helpers.run_qa(
            data, notes, qa_range
        )
    except ValueError:
        raise

    if duplicate_samples:
        report.append(
            dmc.Text('Duplicate samples were found and dropped.', c='red', h='sm', ta='right')
        )

    if missing_values:
        report.append(
            dmc.Text('One or more variables have data dropouts.', c='red', h='sm', ta='right')
        )

    if missing_samples:
        report.append(
            dmc.Text(
                'There are gaps in the time series; placeholder samples were inserted.',
                c='red',
                h='sm',
                ta='right',
            )
        )

    return report, notes, fixed


@blueprint.callback(
    Output('sanity-checks', 'children'),
    # Output( 'save-button'  , 'disabled'          , allow_duplicate=True),
    # Output( 'append-button', 'disabled'          , allow_duplicate=True),
    Output('files-status', 'data', allow_duplicate=True),
    Output('frame-store', 'data', allow_duplicate=True),
    State('sanity-checks', 'children'),
    Input('files-status', 'data'),
    Input('frame-store', 'data'),
)
@log_func
def report_sanity_checks(
    current_report: list[dmc.Text] | None, status: dict, frames: dict
) -> tuple[list[dmc.Text], dict, Serverside[dict]]:
    """Perform sanity checks/QA on the data and report the results in a separate area of the app shell.

    If anything like data dropouts is found, record that in a separate DataFrame, which will become
    the Notes worksheet in the output file upon save. In some cases (such as missing rows/samples),
    the data may be altered (for example, by adding NaN-filled rows to fill gaps in the time series).

    This is triggered simply by the availability of new sensor data.

    Parameters:
        frames  The three or four DataFrames (data, meta, site, possibly notes) for one file

    Returns:
        sanity-checks/children  The results of a few simple sanity checks
        frame-store/data        The four DataFrames (data, meta, site, notes) for one file
    """

    if not frames or 'data' not in frames:
        logger.debug('No data loaded. Clear the sanity check reports.')
        return [], no_update, no_update

    if 'notes' in frames and 'qa_status' not in status:
        logger.debug('Notes worksheet already populated . Do nothing.')
        raise PreventUpdate

    data = frames['data']
    notes = frames['notes'] if 'notes' in frames else None

    report: list[dmc.Text]
    qa_range: list[str] | None

    if 'qa_status' in status:
        # Append mode: an existing Excel file was loaded and concatenated with the new data.
        # Sanity-test only the part of the time series indicated by qa_range, and append the results
        # to whatever is already reported (but remove duplicates).
        logger.debug(
            'New data appended to an existing file. Run and report sanity check on what is new.'
        )
        assert current_report is not None

        # For some reason (JSON, I presume), the children of a Stack are returned as dicts, not Text objects. Turn them back into objects.
        current_report = [dmc.Text(**c['props']) for c in current_report]
        report = current_report + [
            dmc.Text(f'{len(data):,} total samples after appending.', h='sm', ta='right')
        ]
        qa_range = status['qa_range']
        status['qa_status'] = 'Complete'
    else:
        logger.debug('New data found. Running and reporting sanity checks.')
        report = [
            dmc.Text(
                f'{len(data):,} samples; {len(data.columns) - 2} variables.', h='sm', ta='right'
            )
        ]
        qa_range = None

    qa_report: list[dmc.Text]

    try:
        qa_report, frames['notes'], frames['data'] = run_sanity_checks(data, notes, qa_range)
    except ValueError as err:  # Duplicate timestamp with distinct variable values found
        qa_report = [dmc.Text(str(err), c='red', h='sm', ta='right')]
        status['no_save'] = True  # Do not save; requires manual intervention

    report += qa_report
    report = list(
        {t.children: t for t in report}.values()
    )  # Remove duplicates while maintaining order (only needed in Append mode)

    return report, status, Serverside(frames, key='Frames')


@blueprint.callback(
    Output('file-name', 'children', allow_duplicate=True),
    Output('last-modified', 'children', allow_duplicate=True),
    Output('next-file', 'data', allow_duplicate=True),
    Input('files-status', 'data'),
)
@log_func
def setup_batch(files_status: dict) -> tuple:
    """Set up for batch operation by starting the loop counter. Show a batch operation header.

    Looping over a batch of multiple files works as follows:
        Multiple files selected (select-file -> clear_load -> files-status)
        ==> 1) next-file := 0 (setup_batch)
            ==> 2) file-counter := next-file (next_in_batch)
                ==> 3a) next-file += 1 (increment_file_counter)
                    --> REPEAT from step 2)
                ==> 3b) process one file (process_batch)
    This has the effect of walking quickly through the batch index from 0 to len(batch)-1,
    while queueing the long-running processing step for each file.

    Parameters:
        files_status    Filename(s) and (un)saved status

    Returns:
        file-name/children     (str) Reuse for Batch mode operation header
        last-modified/children (str) Reuse for start and completion time of batch operation
        next-file/data         (int) Set the next value for the loop counter

    """

    filenames: str | list[str] = files_status['filename']
    if isinstance(filenames, str):
        logger.debug('No file or a single filename: not a batch.')
        raise PreventUpdate

    # When the batch is complete, the unsaved flag gets set to False.
    if not files_status['unsaved']:
        logger.debug('Batch already done. Do not start again.')
        raise PreventUpdate

    logger.debug(f'Files loaded: {", ".join(filenames)}.')
    start_time: str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    next_file: int = 0  # This triggers the start of the loop over file_counter
    return 'Batch mode operation', [f'Started at {start_time}'], next_file


@blueprint.callback(
    Output('show-data', 'children'),
    Output('file-counter', 'data'),
    Trigger('next-file', 'modified_timestamp'),
    State('next-file', 'data'),
    State('select-file', 'filename'),
    State('select-file', 'last_modified'),
)
@log_func
def next_in_batch(next_file: int, filenames: list[str], last_modified: list[int]) -> tuple:
    """Increment the file counter if there are more files. This triggers the next batch item.

    Also, display file information and a busy indicator for the current item in the batch.

    Parameters:
        next_file     (int)       The current file's index in the batch list
        filenames     (list[str]) The selected filenames, provided by the Upload component
        last_modified (list[int]) File last-modified timestamps

    Returns:
        show-data/children (object) Patch object to add another CardSection to the main app area
        file-counter/data  (int)    The file counter value for the current batch item
    """

    logger.debug(f'Next file index is {next_file}.')

    # Construct a whole new CardSection element, to be appended to the show-data area
    this_file_info: dmc.CardSection = layout.make_file_info(next_file)
    this_file_info.children.children[0].children[0].children = filenames[next_file]
    this_file_info.children.children[0].children[1].children[
        0
    ].children = f'Last modified: {datetime.fromtimestamp(last_modified[next_file]).strftime("%Y-%m-%d %H:%M:%S")}'
    this_file_info.children.children[1].display = 'flex'
    this_file_info.children.children[1].type = 'dots'

    showdata: Patch = Patch()
    showdata.append(this_file_info)

    return showdata, next_file


@blueprint.callback(
    Output('next-file', 'data', allow_duplicate=True),
    Trigger('file-counter', 'modified_timestamp'),
    State('file-counter', 'data'),
    State('select-file', 'filename'),
)
@log_func
def increment_file_counter(file_counter: int, filenames: list[str]) -> int:
    """Set the next value for the batch loop index (file counter). Stop at the end of the batch.

    The reason for incrementing the loop index in a separate callback rather than in process_batch()
    is that callbacks can run in parallel, so we don't want to wait for the previous file to be fully
    processed (a long-running callback) before kicking off the next one. This way, the entire batch
    can be iterated over quickly, with the long-running processing callbacks queued up and run in
    multithreaded fashion in whatever way Dash has to take advantage of multiple available CPU cores.

    Parameters:
        file_counter    The index of the next file in the list of files (the batch)
        filenames       The selected filenames (here only used to get the length of the batch)
    """

    logger.debug(f'File counter is {file_counter}.')
    next_file: int = file_counter + 1

    if next_file >= len(filenames):
        logger.debug('Reached the end of the batch; stop operation.')
        raise PreventUpdate

    return next_file


@blueprint.callback(
    Output('read-error', 'opened', allow_duplicate=True),
    Output('error-title', 'children', allow_duplicate=True),
    Output('error-text', 'children', allow_duplicate=True),
    Trigger('file-counter', 'modified_timestamp'),
    State('file-counter', 'data'),
    State('select-file', 'filename'),
    State('select-file', 'contents'),
    # background=True,
)
@log_func
def process_batch(file_counter: int, filenames: list[str], all_contents: list[str]) -> tuple:
    """Process one file in the batch, without user involvement.

    Read the file contents into DataFrames, perform sanity checks, and save the DataFrames to an Excel file.
    When done, display the results of the sanity checks, hide the busy indicator, and show the Saved badge.

    NOTE: This callback uses set_props() to directly manipulate UI elements, instead of relying on return
        values to do so. This is the only way I could think of to address dynamically created elements,
        with generated names ending in a suffix indicating the batch index (file counter), such as
        sanity-checks-0, saved-badge-3, etc.

    Parameters:
        file_counter    The index of the next file in the list of files (the batch)
        filenames       The selected filenames (here only used to get the length of the batch)
        all_contents    Base64-encoded file contents for all files in the batch

    Returns:
        read-error/opened    (bool) True in case of error (show error modal), otherwise False
        error-title/children (str)  In case of error, title for error modal; otherwise an empty string
        error-text/children  (str)  In case of error, error text for the modal dialog; otherwise an empty string
    """

    logger.debug(f'({file_counter}) Enter.')

    if (
        len(filenames) <= file_counter
    ):  # We got here because there's a batch, so this should not happen
        logger.error(
            f'({file_counter}) Something is wrong. Processing file {file_counter} but there are only {len(filenames)} files in the batch.'
        )
        logger.debug(f'({file_counter}) Exit.')
        return (
            True,
            'System Error:',
            f'Processing file number {file_counter} but there are only {len(filenames)} in the batch.',
        )

    logger.debug(f'({file_counter}) Processing {filenames[file_counter]}.')
    # time.sleep(3 + random.uniform(-2, 2))

    contents: str = all_contents[file_counter]
    filename: str = filenames[file_counter]
    outfile: str = str(Path(filename).with_suffix('.xlsx'))

    # Read the file contents into DataFrames.
    try:
        frames = helpers.load_data(contents, filename)
        logger.debug(f'({file_counter}) Data initialized.')
    except Exception as e:
        logger.error(f'({file_counter}) File Read Error:\n{e}')
        logger.debug(f'({file_counter}) Exit.')
        return True, 'Error Reading File', f'We could not process the file "{filename}": {e}'

    data = frames['data']
    report: list[dmc.Text] = [
        dmc.Text(f'{len(data):,} samples; {len(data.columns) - 2} variables.', h='sm', ta='right')
    ]

    no_save = False

    # Perform sanity/QA checks and report the results, except:
    #   If there already is a Notes DataFrame, then it was read in from a previously saved, and possibly edited, Excel file.
    #   In that case, neither make corrections to the data nor generate a new Notes worksheet.
    if 'notes' not in frames:
        qa_report: list[dmc.Text]

        try:
            qa_report, frames['notes'], frames['data'] = run_sanity_checks(frames['data'], None)
        except ValueError as err:
            no_save = True
            qa_report = [dmc.Text(str(err), c='red', h='sm', ta='right')]

        report += qa_report

    set_props(f'sanity-checks-{file_counter}', {'children': report})

    # If duplicate timestamps were found (an unrecoverable error), skip the saving.
    # But we stil need to turn off the Loader and show a Badge, because the batch process looks
    # for badges to know when it's complete. Instead of the usual "SAVED", though, let the user
    # know that no file was saved.
    if no_save:
        set_props(f'wait-please-{file_counter}', {'display': 'none'})
        set_props(
            {'type': 'saved-badge', 'index': file_counter},
            {'children': 'NOT SAVED', 'display': 'inline', 'color': 'red'},
        )

        logger.debug(f'({file_counter}) Exit.')
        return False, '', ''

    # Save the file.
    # Dash provides a convenience function to create the required dictionary. That function in turn
    # relies on a writer (e.g., DataFrame.to_excel) to produce the content. In this case, that writer
    # is a custom function specific to this app.
    data_for_download: dict[str, Any | None] = dcc.send_bytes(
        helpers.multi_df_to_excel(frames), outfile
    )
    logger.debug(f'({file_counter}) Got byte string for Download.')
    set_props(f'save-xlsx-{file_counter}', {'data': data_for_download})
    logger.debug(f'({file_counter}) Download complete. Clean up.')

    set_props(f'wait-please-{file_counter}', {'display': 'none'})
    set_props({'type': 'saved-badge', 'index': file_counter}, {'display': 'inline'})

    logger.debug(f'({file_counter}) Exit.')
    return False, '', ''


@blueprint.callback(
    Output('files-status', 'data'),
    Output('last-modified', 'children', allow_duplicate=True),
    Output('select-file', 'contents', allow_duplicate=True),
    State('files-status', 'data'),
    Input({'type': 'saved-badge', 'index': ALL}, 'display'),
)
@log_func
def batch_done(files_status: dict, displays: list[str]) -> tuple:
    """Keep track of batch progress; set file unsaved flag to re-enable new file selection when all files are processed.

    Look for changes to the Saved badges: setup_batch() creates one, invisible (display='none'), for each file in the
    batch, and process_batch() makes it visible (display='inline') as it finishes with each file. When all badges are
    visible, the batch is complete.

    Parameters:
        files_status    Filename(s) and (un)saved status
        displays        The display attribute for all batch-related Saved badges
                        NOTE: This takes advantage of pattern-matching callback inputs.

    Returns:
        files-status/data      (str)  Same as files_status parameter but with the unsaved flag set to False
        last-modified/children (str)  Add the completion time of the batch operation (start time was added by setup_batch())
        select-file/contents   (list) Empty to reset, so a new file upload always results in a contents change, even if
                                    it's the same file(s) as previously loaded
    """

    if displays:  # This also gets triggered when all batch-related badges disappear
        logger.debug(f'There are {len(displays)} files in progress: {displays}')

        if all(d == 'inline' for d in displays):
            logger.debug('Batch complete.')
            files_status['unsaved'] = False
            end_time: Patch = Patch()
            end_time.append(f' -- Complete at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
            return files_status, end_time, []
        else:
            logger.debug(
                f'Batch not complete; so far only {len([d for d in displays if d == "inline"])} files.'
            )
            raise PreventUpdate
    else:
        logger.debug('No batch in progress.')
        raise PreventUpdate


@blueprint.callback(
    Output('frame-store', 'data', allow_duplicate=True),
    Output('files-status', 'data', allow_duplicate=True),
    Output('append-file', 'contents'),
    Output('read-error', 'opened', allow_duplicate=True),
    Output('error-title', 'children', allow_duplicate=True),
    Output('error-text', 'children', allow_duplicate=True),
    State('frame-store', 'data'),
    State('files-status', 'data'),
    State('append-file', 'filename'),
    Input('append-file', 'contents'),
)
@log_func
def append_file(
    new_frames: dict[str, Any], status: dict[str, str | bool], filename: str, contents: str
) -> tuple:
    """An existing Excel file was opened, to be appended to. Append the current data and update the frame store.

    Copy the metadata and site data from the new set; if they're not the same as the existing set, assume that the
    descriptions were edited since last saved and don't treat it as an error.

    Copy the notes, if any, from the existing file. Let the QA code know where to start its analysis: at the start
    of the time series (if there were no previously saved notes), or at the transition to the new data (if there
    already were notes).

    Parameters:
        new_frames       The three or four DataFrames (data, meta, site, possibly notes) from the currently loaded new file
        status           Filename (of the new data) and (un)saved status
        filename         Filename of the newly loaded existing Excel file
        all_contents     Base64-encoded file contents of the newly loaded existing Excel file

    Returns:
        frame-store/data     (dict) The three or four DataFrames of the combined set
        files-status/data    (dict) Original files_status, with new members, the timestamp range on which to perform QA analysis
        append-file/contents (list) Empty to reset, so a new Append file upload always results in a contents change
        read-error/opened    (bool) True in case of error (show error modal), otherwise False
        error-title/children (str)  In case of error, title for error modal; otherwise an empty string
        error-text/children  (str)  In case of error, error text for the modal dialog; otherwise an empty string

    """

    try:
        base_frames: dict[str, Any] = helpers.load_data(contents, filename)
        logger.debug('Existing file data initialized.')
        combined_frames: dict[str, Any]
        combined_frames, status['qa_range'] = helpers.append(base_frames, new_frames)
        status['qa_status'] = 'Ready'
        logger.info(
            f'Base {len(base_frames["data"])}; New {len(new_frames["data"])}; Combined {len(combined_frames["data"])}'
        )

        return Serverside(combined_frames, key='Frames'), status, [], False, '', ''
    except helpers.UnmatchedColumnsError as e:
        logger.error(e)
        return no_update, no_update, no_update, True, 'Unmatched files', str(e)
    except Exception as e:
        logger.exception(e)
        logger.error(f'File Read Error:\n{e}')
        return (
            no_update,
            no_update,
            no_update,
            True,
            'Error Reading File',
            f'We could not process the file "{filename}": {e}',
        )
