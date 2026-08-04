"""All callbacks for the SensorDataIngest/ingest Dash app.

The Dash layout is defined in a separate module, layout.py.
The callbacks rely on functions from another module, helpers.py, for operations that
do not depend on or affect Dash elements.
"""

import logging
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass, field, asdict
from typing import Any, Callable, Final
from enum import StrEnum, auto
import itertools
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
    html,
    no_update,
)

from . import config as cfg
from . import helpers
from . import layout
from .helpers import Frames

append_arrow: Final[str] = r'$\xrightarrow{+}$'     # Use Markdown with Latex to render arrow with plus sign

class QA_Status(StrEnum):
    NONE = auto()
    APPEND_READY = auto()
    APPEND_COMPLETE = auto()
    COMPLETE = auto()

@dataclass
class Context:
    files: list[str] = field(default_factory=list)
    unsaved: bool = False
    upload_id: str = ''
    qa_status: QA_Status = QA_Status.NONE
    qa_range: list[str] = field(default_factory=list)
    no_save: bool = False
    start_batch: bool = False

logger: logging.Logger = logging.getLogger(f'{cfg.program_name}.{__name__}')
ee_logger: logging.Logger = logging.getLogger(f'{cfg.program_name}_ee.{__name__}')


@decorator.decorator
def log_func(fn: Callable, *args, **kwargs) -> Any:
    """Function entry and exit logger, capturing exceptions as well.

    Very simplistic; no argument logging or execution timing.
    Differs from the definition in helpers.py in that it logs PreventUpdate exceptions
    as normal exits, rather than as exceptions.
    """

    context_value = ''
    for arg in list(args) + list(kwargs.values()):
        if isinstance(arg, Context):
            context_value = asdict(arg)
            context_value['files'] = [Path(f).name for f in context_value.get('files', [])]
            del context_value['upload_id']      # Keep it short; this never changes.
            break

    ee_logger.debug('>>> Enter.', extra={'fname': fn.__name__, 'context': context_value})

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
    Input('files-context', 'data'),
)
@log_func
def load_file(context: Context) -> tuple:
    """If one file was opened, load it.

    Triggered by a change in files-context after the user selects a single file, read the uploaded file 
    into a DataFrame, along with separate DataFrames for metadata (column descriptions) and site data,
    and possibly a notes table (if reading an Excel file).
    Persist the three DataFrames in the server-side frame-store.

    Parameters:
        context    File path(s) and (un)saved status

    Returns:
        frame-store/data     (dict[DataFrame]) The three DataFrames (data, meta, site) for one file
        read-error/opened    (bool) True in case of error (show error modal), otherwise False
        error-title/children (str)  In case of error, title for error modal; otherwise an empty string
        error-text/children  (str)  In case of error, error text for the modal dialog; otherwise an empty string

    """

    # This callback is triggered by any change to files-context. But action is only needed if there is one
    # new file to be loaded, in which case file is a single non-empty string, and the unsaved flag is True.
    # This only happens when a single new file is selected by the user:
    # - Another callback populates the filename and sets the unsaved flag to True.
    # - If the user selects multiple files, file is a list of strings, not a single string.
    # - After a Clear, filename is an empty string.
    # - After a Save, the unsaved flag is False.
    # - In Append mode, the unsaved flag is True but a qa_status element is added.

    if len(context.files) != 1 or context.qa_status != QA_Status.NONE:
        logger.debug('Zero or multiple files, or in Append mode; nothing to show interactively.')
        raise PreventUpdate

    if not context.unsaved:
        logger.debug('File was just saved; no change to uploaded data.')
        raise PreventUpdate

    file: str = context.files[0]
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
            Serverside(frames, key='Frames'),
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
    return Serverside(frames, key='Frames'), False, '', ''


@blueprint.callback(
    Output('save-xlsx', 'data'),
    Output('files-context', 'data', allow_duplicate=True),
    Trigger('save-button', 'n_clicks'),
    Input('files-context', 'data'),
    State('frame-store', 'data'),
    running=[(Output('wait-please', 'visible'), True, False)],  # Show busy indicator while saving
)
@log_func
def save_file(context: Context, frames: Frames | None) -> tuple:
    """If a single file is loaded save the data in an Excel (.XLSX) file. If multiple, trigger batch processing instead.

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
        context_dict   Server-side file paths and (un)saved status
        frames         The four DataFrames (data, meta, site, notes) for one file

    Returns:
        save-xlsx/data       (dict) Content and filename to be downloaded to the browser
        files-context/data   (str)  Same as context parameter but with the unsaved flag set to False
    """

    files: list[str] = context.files

    if callback_context.triggered_id == 'save-button' and len(files) > 1:
        logger.debug('Save requested for multiple files; kick off batch process.')
        context.start_batch = True
        return no_update, context

    # The only time we want to respond to a context-change trigger is when we're in append mode and the QA checks have completed.
    if callback_context.triggered_id == 'files-context' and context.qa_status != QA_Status.APPEND_COMPLETE:
        logger.debug('Not in append mode and context changed for unrelated reason, or appended file not ready to be saved (QA pending): skip saving.')
        raise PreventUpdate

    # Guard against a save requested while QA checks are still in process. The window of vulnerabiity is very small, but not zero.
    # Note that QA can only be completed if there is data in memory, so there is no need to check for that here.
    if context.qa_status in (QA_Status.APPEND_COMPLETE, QA_Status.COMPLETE):
        outfile: str = Path(files[0]).with_suffix('.xlsx').name

        # Dash provides a convenience function to create the required dictionary. That function in turn
        # relies on a writer (e.g., DataFrame.to_excel) to produce the content. In this case, that writer
        # is a custom function specific to this app.
        contents: dict[str, Any | None] = dcc.send_bytes(helpers.multi_df_to_excel(frames), outfile)
        context.unsaved = False

        # Remove artifacts, if any, of an Append process, so the combined data looks as if it was read directly from
        # a single file (except for the displayed sanity check results). You could repeat the Append action to chain
        # any number of files together, not just two.
        context.qa_status = QA_Status.NONE
        context.qa_range = []
        context.no_save = False

        logger.debug(f'File "{outfile}" saved.')
        return contents, context
    elif not frames or frames.data.empty:
        logger.debug('No data in memory; nothing to save.')
        raise PreventUpdate
    else:
        logger.debug('File not ready to be saved (QA pending); skip saving.')
        raise PreventUpdate


@blueprint.callback(
    Output('files-context', 'data', allow_duplicate=True),
    Output('frame-store', 'clear_data'),
    Output('show-data', 'children', allow_duplicate=True),
    Output('file-name', 'children', allow_duplicate=True),
    Output('file-attributes', 'children', allow_duplicate=True),
    Trigger('clear-button', 'n_clicks'),
    State('files-context', 'data'),
    State('show-data', 'children'),
)
@log_func
def clear(old_context: Context, show_data: list[dmc.CardSection]) -> tuple:
    """Clear all data in memory and on the screen, triggered by the Clear button.

    Set the filename and unsaved flag in files-context to blank and False, respectively, which in turn
    triggers all the follow-on chain of callbacks (clear the UI, clear the DataFrame store, etc.).

    Parameters:
        old_context    Server-side file paths and (un)saved status
        show_data      The layout of the main app area

    Returns:
        files-context/data       (str)  Empty list of server-side file paths, unsaved flag False
        frame-store/clear_data   (bool) Delete the contents of the serverside DataFrame store
        show-data/children       (list[dmc.CardSection]) Truncated contents of the main app area:
                                                         remove batch processing output, if any
        file-name/children       (str)  Empty string to clear
        file-attributes/children (str)  Empty string to clear

    """

    logger.debug('Responding to Clear button click. Reset files-context.')

    upload_id: str = old_context.upload_id
    context: Context = Context(upload_id=upload_id)  # Keep the upload ID so we can clear the file cache

    # Always clear the DataFrame store, truncate the main app area, and clear the filename/file-attributes text.
    return context, True, show_data[:3], None, None


@blueprint.callback(
    Output('files-context', 'data', allow_duplicate=True),
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
        files-context/data  (dict) Server-side file paths and unsaved status
        show-data/children  (list[dmc.CardSection]) Truncated contents of the main app area:
                            remove batch processing output, if any
    """

    files: list[str] = [str(file_cache / file['upload_id'] / file['response']['filename'])
                        for file in uploaded_files]
    if not files:
        logger.debug('No new files uploaded.')
        raise PreventUpdate
    
    logger.debug('%s file(s) uploaded: %s', len(uploaded_files), ', '.join([f['name'] for f in uploaded_files]))
    context: Context = Context(files=files, unsaved=True, upload_id=uploaded_files[0]['upload_id'])

    logger.debug('File cache upload ID is %s', context.upload_id)
    return context, show_data[:3]


@blueprint.callback(
    Output('files-context', 'data', allow_duplicate=True),
    Output('append-pairs', 'data', allow_duplicate=True),
    Input('select-append-batch', 'uploadedFiles'),
    State('files-context', 'data'),
)
@log_func
def start_append_batch(uploaded_files: list[dict[str, str | int | dict[str, str | int]]],
                       context: Context) -> tuple[Context, list[list[str | None]]]:
    """Match newly selected append files to the currently loaded files and start the append batch.
    
    Parameters:
        uploaded_files      Filenames and other info of the uploaded files
        context             The current application context

    Returns:
        files-context/data  Updated context with start_batch set to True
        append-pairs/data   List of matched pairs of files to be appended together
    """

    if not uploaded_files or len(context.files) <= 1:
        logger.debug('No append files uploaded or not in multi-file mode; ignore.')
        raise PreventUpdate

    append_files: list[str] = [
        str(file_cache / file['upload_id'] / file['response']['filename'])
        for file in uploaded_files
    ]
    append_pairs: list[tuple[str, str | None]] = helpers.pair_files_by_prefix(context.files, append_files)
    unmatched: list[str] = [Path(left).name for left, right in append_pairs if right is None]
    if len(unmatched) == len(append_pairs):
        logger.warning('No unique matches found for append batch. Files: %s vs %s',
                       [Path(p).name for p in context.files],
                       [Path(p).name for p in append_files])
        raise PreventUpdate

    if unmatched:
        logger.warning('No unique append match for %s file(s): %s', len(unmatched), unmatched)

    context.start_batch = True
    logger.debug('Append batch matched %s pair(s): %s',
                 len(append_pairs),
                 [f'{Path(left).name} <- {Path(right).name}' for left, right in append_pairs])
    return context, append_pairs


@blueprint.callback(
    Output('select-file', 'disabled'),
    Output('select-file', 'uploadedFiles'),
    Output('load-label', 'c'),
    Input('files-context', 'data'),
)
@log_func
def toggle_loaddata(context: Context) -> tuple:
    """Disable the Load Data element when new data is loaded and not (yet) saved; re-enable when data is cleared or saved.

    This includes graying out the label of the Load Data area; the rest is governed by the Upload component
    and grayed out automatically.

    If data is cleared or saved, empty the upload-file cache, since it means we no longer need those files. Also
    clear the uploaded-files list, to ensure that a callback always happens when the user selects a file, even
    if it's the same file again.

    Parameters:
        context (Context) Filename(s) and (un)saved status

    Returns:
        select-file/disabled      (bool) True if unsaved data in memory; False otherwise (no data or data was saved)
        select-file/uploadedFiles (list) If not unsaved, empty list to clear the uploaded-files list
        load-label/c              (str)  Color for the Load Data area label: dimmed for disabled, black for enabled
    """

    unsaved: bool = context.unsaved

    if unsaved:
        logger.debug('Data not saved; disable Load Data.')
        return unsaved, no_update, 'dimmed'
    else:
        logger.debug('Data saved or cleared; enable Load Data and clear upload cache.')
        helpers.clear_file_cache(context.upload_id)
        return unsaved, [], 'black'


@blueprint.callback(
    Output('append-batch', 'display'),
    Trigger('append-button', 'n_clicks'),
    Input('files-context', 'data'),
)
@log_func
def show_append_batch(context: Context) -> str:
    """Toggle the display of the Append Batch element based on the current context.

    Parameters:
        context Filename(s) and (un)saved status

    Returns:
        append-batch/display  (str)  Show the Append Batch element if the Append button was clicked;
                                     hide it if there's no data.
    """

    match callback_context.triggered_id:
        case 'append-button':
            if len(context.files) > 1:
                logger.debug('Append button clicked in batch; show Append Batch element.')
                return 'block'
            else:
                logger.debug('Append button clicked but not a batch; hide Append Batch element.')
                return 'none'
        case 'files-context':
            logger.debug('Context changed%s', ': data cleared, so hide Append Batch element' if not context.files 
                         else ' for unrelated reason: ignore.')
            return 'none' if not context.files else no_update
        case _:
            logger.debug('Unexpected trigger: %s; hide Append Batch element.', callback_context.triggered_id)
            return 'none'

@blueprint.callback(
    Output('inspect-data', 'display'),
    Output('data-columns', 'children'),
    Output('select-columns', 'value'),
    Input('frame-store', 'data'),
    State('files-context', 'data'),
)
@log_func
def show_columns(frames: Frames | None, context: Context) -> tuple:
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
        frames       The four DataFrames (data, meta, site, notes) for one file
        context_dict Filename(s) and (un)saved status

    Returns:
        inspect-data/display  (str)  Show the column selection part of the Navbar if there's data; otherwise blank it
        data-columns/children (list) A list of dmc.Checkbox elements, one for each data column
        select-columns/value  (list) Reset the current selection (uncheck all boxes)
    """

    if (frames and not frames.data.empty):
        if context.unsaved:
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
def draw_plots(showcols: list[str], single_plot: bool, frames: Frames | None) -> tuple:
    """Draw plots, one below the other, for each of the selected columns.

    Redraw the entire stacked plot each time the selection changes.

    Parameters:
        showcols        Column names, in the order in which they were selected
        frames          The four DataFrames (data, meta, site, notes) for one file
        single_plot     If true draw a single multivariable plot, otherwise multiple single-variable plots

    Returns:
        stacked-graphs/figure (Figure) Plotly figure of all graphs in one stacked plot
        plot-area/display     (str)    Hide the plot area if there are no graphs to show
                                       (otherwise you see an empty set of axes)
    """

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
    Input('files-context', 'data'),
)
@log_func
def show_badge(context: Context) -> tuple:
    """Respond to a Save action by showing a SAVED badge.

    Because this is triggered after every single-file save action, also use this callback to clear the data
    in dcc.Download. Not clearing the data may result in the download action continuing to be triggered by
    every UI interaction.

    Parameters:
        context   Server-side file paths and (un)saved status

    Returns
        saved-badge/display (str) Show ('inline') the SAVED badge if data was saved; otherwise hide it ('none')
        save-xlsx/data      (obj) None, to clear the data
    """

    # To show the badge, there must be a single file loaded and it must be saved.
    # (The unsaved flag is also False if there nothing loaded.)
    files: list[str] = context.files
    retval: str

    if len(files) == 1:
        unsaved: bool = context.unsaved
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
    Output('append-file', 'disabled'),
    Output('clear-button', 'disabled'),
    Input('files-context', 'data'),
)
@log_func
def toggle_save_clear(context: Context) -> tuple:
    """If one or more files are loaded, enable the Save, Clear, and Append buttons; otherwise, disable them.

    For a single file, do not enable Save and Append if the no_save flag is set, which happens when 
    the sanity checks find problems that require manual intervention.
    
    For a batch, enable the Append button but not the dcc.Upload component that contains it. Ignore
    the no_save flag.

    Parameters:
        context   Filename(s) and (un)saved status

    Returns:
        save-button/disabled        True to disable; False to enable
        append-button/disabled      True to disable; False to enable
        append-file/disabled        True to disable; False to enable
        clear-button/disabled       True to disable; False to enable
    """

    files: list[str] = list(context.files)  # A single file comes in as a string.
    num_files = len(files)
    do_not_save: bool = context.no_save

    # The Append button works differently for single file versus batch.
    # Single file: Append is an active dcc.Upload component that allows dropping a file and upon click immediately
    #              opens the browser's file-open dialog.
    # Batch: Append is a regular button (no drag-drop) that exposes a separate du.Upload component, which takes
    #        the file selection from there.
    disable_save = num_files < 1 or do_not_save             # Enable unless nothing loaded or single bad file
    disable_append_button = num_files < 1 or do_not_save    # Enable unless nothing loaded or single bad file
    disable_append_file = num_files != 1 or do_not_save     # Enable only for a single file, unless that file is bad
    disable_clear = num_files < 1                           # Enable for any number of files

    return disable_save, disable_append_button, disable_append_file, disable_clear

@blueprint.callback(
    Output('file-name', 'children'),
    Output('file-attributes', 'children'),
    Input('files-context', 'data'),
    State('select-file', 'uploadedFiles'),
)
@log_func
def show_file_info(context: Context, uploaded_files: list[dict[str, str | int | dict[str, str | int]]]):
    """If there's data in memory, show information (filename, file-attributes) about the file that was loaded.

    Parameters:
        context   File paths and (un)saved status
        uploaded_files  File names and other info for the uploaded files

    Returns:
        file-name/children        (str) The name of the currently loaded file (no path)
        file-attributes/children  (str) Friendly-formatted size of the currently loaded file
    """

    files: list[str] = context.files

    # Make sure there's only one file and we're not appending.
    match len(files):
        case 1:
            if context.unsaved and context.qa_status != QA_Status.APPEND_READY:
                name: str = uploaded_files[0]['name']
                info: str = f'Size: {hf.format_size(uploaded_files[0]["size"])}'
                logger.debug('Data in memory; show file information.')
                return name, info
            else:
                logger.debug('Append in process or finished; nothing to do.')
                raise PreventUpdate
        case 0:
            logger.debug('In-memory data was cleared. Clear file information.')
            return '', ''
        case _:
            logger.debug('Multiple files loaded; nothing to do.')
            raise PreventUpdate


@log_func
def _run_sanity_checks(frames: Frames, qa_range: list[str] | None = None) -> list[dmc.Text]:
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

    def warning_text(msg: str) -> dmc.Text:
        return dmc.Text(msg, c='red', ta='right')

    duplicate_samples, missing_values, missing_samples = helpers.run_qa(frames, qa_range)

    if duplicate_samples:
        report.append(
            warning_text('Duplicate samples were found and dropped.')
        )

    if missing_values:
        report.append(
            warning_text('One or more variables have data dropouts.')
        )

    if missing_samples:
        report.append(
            warning_text('There are gaps in the time series; placeholder samples were inserted.')
        )

    return report


@blueprint.callback(
    Output('sanity-checks', 'children'),
    Output('files-context', 'data', allow_duplicate=True),
    Output('frame-store', 'data', allow_duplicate=True),
    State('sanity-checks', 'children'),
    Input('files-context', 'data'),
    Input('frame-store', 'data'),
)
@log_func
def report_sanity_checks(
    current_report: list[dmc.Text] | None, context: Context, frames: Frames
) -> tuple[list[dmc.Text], dict, Serverside[dict]]:
    """Perform sanity checks/QA on the data and report the results in a separate area of the app shell.

    If anything like data dropouts is found, record that in a separate DataFrame, which will become
    the Notes worksheet in the output file upon save. In some cases (such as missing rows/samples),
    the data may be altered (for example, by adding NaN-filled rows to fill gaps in the time series).

    This is triggered simply by the availability of new sensor data.

    Parameters:
        current_report  Previously shown sanity check results, if any
        context_dict    Filename(s) and (un)saved status
        frames          The three or four DataFrames (data, meta, site, possibly notes) for one file

    Returns:
        sanity-checks/children  The results of a few simple sanity checks
        frame-store/data        The four DataFrames (data, meta, site, notes) for one file
    """

    if not frames or frames.data.empty:
        logger.debug('No data loaded. Clear the sanity check reports.')
        return [], no_update, no_update

    if not frames.notes.empty and context.qa_status == QA_Status.NONE:
        logger.debug('Notes worksheet already populated. Do nothing.')
        raise PreventUpdate

    # At callback level, we don't know about pandas, but we can still apply python functions like len() to DataFrames.
    data = frames.data

    report: list[dmc.Text]
    qa_range: list[str] | None

    if context.qa_status == QA_Status.APPEND_READY:
        # Append mode: an existing Excel file was loaded and concatenated with the new data.
        # Sanity-test only the part of the time series indicated by qa_range, and append the results
        # to whatever is already reported (but remove duplicates).
        logger.debug('New data appended to an existing file. Run and report sanity check on what is new.')
        assert current_report is not None

        # For some reason (JSON, I presume), the children of a Stack are returned as dicts, not Text objects. Turn them back into objects.
        current_report = [dmc.Text(**c['props']) for c in current_report]
        report = current_report + [dmc.Text(f'{len(data):,} total samples after appending.', ta='right')]
        qa_range = context.qa_range
        context.qa_status = QA_Status.APPEND_COMPLETE
    else:
        logger.debug('New data found. Running and reporting sanity checks.')
        report = [dmc.Text(f'{len(data):,} samples; {len(data.columns) - 2} variables.', ta='right')]
        qa_range = None
        context.qa_status = QA_Status.COMPLETE

    qa_report: list[dmc.Text]

    try:
        qa_report = _run_sanity_checks(frames, qa_range)
    except (helpers.DuplicateTimestampError, helpers.TimestampColumnNotFoundError) as err:
        qa_report = [dmc.Text(str(err), c='red', ta='right')]
        context.no_save = True  # Do not save; requires manual intervention

    report += qa_report
    report = list({t.children: t for t in report}.values())  # Remove duplicates while maintaining order (only needed in Append mode)

    return report, context, Serverside(frames, key='Frames')


@blueprint.callback(
    Output('file-name', 'children', allow_duplicate=True),
    Output('file-attributes', 'children', allow_duplicate=True),
    Output('append-batch', 'display', allow_duplicate=True),
    Output('next-file', 'data', allow_duplicate=True),
    Input('files-context', 'data'),
    State('select-file', 'uploadedFiles'),
    State('append-pairs', 'data'),
)
@log_func
def setup_batch(context: Context, uploaded_files: list[dict[str, str | int | dict[str, str | int]]], append_pairs: list[list[str]]) -> tuple:
    """Set up for batch operation by starting the loop counter. Show a batch operation header.

    Looping over a batch of multiple files works as follows:
        Multiple files selected (select-file -> clear_load -> files-context)
        ==> 1) next-file := 0 (setup_batch)
            ==> 2) file-counter := next-file (next_in_batch)
                ==> 3a) next-file += 1 (increment_file_counter)
                ==> 3b) process one file (process_batch)
                    --> REPEAT from step 2)
    This has the effect of walking quickly through the batch index from 0 to len(batch)-1,
    while queueing the long-running processing step for each file.

    Parameters:
        context   Filename(s) and (un)saved status
        uploaded_files  Filenames and other info of the uploaded files
        append_pairs    List of matched pairs of files for Batch Append mode

    Returns:
        file-name/children          (str) Reuse for Batch mode operation header
        file-attributes/children    (list[str]) Reuse for start and completion time of batch operation
        append-batch/display        (str) Hide the Append Batch element once batch processing starts
        next-file/data              (int) Set the next value for the loop counter
    """

    if len(uploaded_files) <= 1:
        logger.debug('No file or a single filename: not a batch.')
        raise PreventUpdate
    
    # When the batch is complete, the unsaved flag gets set to False.
    if not context.unsaved:
        logger.debug('Batch already done. Do not start again.')
        raise PreventUpdate

    if context.start_batch:
        logger.debug('Batch triggered by user; start operation.')
        start_time: str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        next_file: int = 0  # This triggers the start of the loop over file_counter
        return 'Batch mode operation', [f'{len(uploaded_files)} files. Started at {start_time}'], 'none', next_file
    else:
        # context has the server-side full paths, which may have sanitized (safe) versions of the original filenames.
        # Show the client-side filenames provided by the Upload component instead.
        filenames: list[str] = [f['name'] for f in uploaded_files]
        logger.debug('Waiting for user to click Save to start batch processing.')
        files_list: list[str | html.Br] = list(itertools.chain.from_iterable(zip([html.Br()] * len(filenames),
                                                                                 [f'\N{BULLET} {name}' for name in filenames])))
        return ('Batch mode operation', [f'{len(filenames)} files loaded:'] + files_list, no_update, no_update)        


@blueprint.callback(
    Output( 'show-data'   , 'children'          ),
    Output( 'file-counter', 'data'              ),
    Trigger('next-file'   , 'modified_timestamp'),
    State(  'next-file'   , 'data'              ),
    State(  'select-file' , 'uploadedFiles'     ),
    State(  'append-pairs', 'data'              ),
)
@log_batch_func
def next_in_batch(
    next_file: int,
    uploaded_files: list[dict[str, str | int | dict[str, str | int]]],
    append_pairs: list[list[str]],
) -> tuple:
    """Show file information and a busy indicator for the current item in the batch.

    Parameters:
        next_file       The current file's index in the batch list
        uploaded_files  Filenames and other info of the uploaded files

    Returns:
        show-data/children (object) Patch object to add another CardSection to the main app area
        file-counter/data  (int)    The file counter value for the current batch item
    """
    # context has the server-side full paths, which may have sanitized (safe) versions of the original filenames.
    # Use the client-side filenames provided by the Upload component instead.
    this_file = uploaded_files[next_file]

    # Construct a whole new CardSection element, to be appended to the show-data area
    this_file_info: dmc.CardSection = layout.make_file_info(next_file)              # file-info-n
    this_file_info.children.children[1].children[0].children = this_file['name']    # file-name-n
    if append_pairs and append_pairs[next_file][1]:
        this_file_info.children.children[1].children[0].children += f' {append_arrow} {Path(append_pairs[next_file][1]).name}'
    this_file_info.children.children[1].children[1].children[0].children = f'Size: {hf.format_size(this_file["size"])}' # file-attributes-n

    showdata: Patch = Patch()
    showdata.append(this_file_info)

    return showdata, next_file


@blueprint.callback(
    Output( 'next-file', 'data', allow_duplicate=True),
    Trigger('file-counter', 'modified_timestamp'),
    State('file-counter', 'data'),
    State('select-file' , 'uploadedFiles'),
)
@log_batch_func
def increment_file_counter(
    file_counter: int,
    uploaded_files: list[dict[str, str | int | dict[str, str | int]]],
) -> int:
    """Set the next value for the batch loop index (file counter). Stop at the end of the batch.

    The reason for incrementing the loop index in a separate callback rather than in process_batch()
    is that callbacks can run in parallel, so we don't want to wait for the previous file to be fully
    processed (a long-running callback) before kicking off the next one. This way, the entire batch
    can be iterated over quickly, with the long-running processing callbacks queued up and run in
    multithreaded fashion in whatever way Dash has to take advantage of multiple available CPU cores.

    Parameters:
        file_counter    The index of the current file in the list of files (the batch)
        uploaded_files  The selected filenames (here only used to get the length of the batch)
        append_pairs    List of matched pairs of files for Batch Append mode

    Returns:
        next-file/data  The next value for the file counter.
        
    Raises
        PreventUpdate when the end of the batch is reached.
    """
    next_file: int = file_counter + 1
    batch_size = len(uploaded_files)

    if next_file >= batch_size:
        logger.debug('Reached the end of the batch; stop operation.')
        raise PreventUpdate

    return next_file

def _done_no_save(file_counter: int) -> None:
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
    State('file-counter', 'data'),
    State('files-context', 'data'),
    State('append-pairs', 'data'),
    # background=True,
)
@log_batch_func
def process_batch(file_counter: int, context: Context, append_pairs: list[list[str]]) -> tuple:
    """Process one file in the batch, without user involvement.

    Read the file contents into DataFrames, perform sanity checks, and save the DataFrames to an Excel file.
    When done, display the results of the sanity checks, hide the busy indicator, and show the Saved badge.

    NOTE: This callback uses set_props() to directly manipulate UI elements, instead of relying on return
        values to do so. This is the only way I could think of to address dynamically created elements,
        with generated names ending in a suffix indicating the batch index (file counter), such as
        sanity-checks-0, saved-badge-3, etc.

    Parameters:
        file_counter    The index of the current file in the list of files (the batch)
        context         Filename(s) and (un)saved status
        append_pairs    List of matched pairs of files for Batch Append mode
   """

    is_append_batch: bool = bool(append_pairs)
    files: list[str] = context.files

    if (len(files) <= file_counter):  # We got here because there's a batch, so this should not happen
        logger.error(
            '(%s) Something is wrong. Processing file %s but there are only %s files in the batch.',
            file_counter, file_counter, len(files)
        )

    file: str = files[file_counter]
    outfile: str = Path(file).with_suffix('.xlsx').name
    qa_range: list[str] | None = None

        frames: Frames = helpers.load_data(file)

    # Read the (original) file contents into DataFrames.
    try:
        frames: list[Any] = helpers.load_data(file)
        logger.debug('(%s) Data initialized.', file_counter)
    except (helpers.BadFileError, helpers.UnsupportedFileTypeError) as err:
        logger.error('(%s) File Read Error:\n%s', file_counter, err)
        set_props(f'sanity-checks-{file_counter}', {'children': [dmc.Text(f'Error reading file {Path(file).name}:', c='red', ta='right'),
                                                                 dmc.Text(str(err), c='red', ta='right')]})
        _done_no_save(file_counter)
        return

    # If in Append mode, do the same with the base file and merge the two sets of DataFrames. Then run the sanity checks on the combined data.
    if is_append_batch and append_pairs[file_counter][1]:
        base_file: str = append_pairs[file_counter][1]
        try:
            base_frames = helpers.load_data(base_file)
            logger.debug('(%s) Base file data initialized.', file_counter)
        except (helpers.BadFileError, helpers.UnsupportedFileTypeError) as err:
            logger.error('(%s) Base file Read Error:\n%s', file_counter, err)
            set_props(f'sanity-checks-{file_counter}', {'children': [dmc.Text(f'Error reading base file {Path(base_file).name}:', c='red', ta='right'),
                                                                     dmc.Text(str(err), c='red', ta='right')]})
            _done_no_save(file_counter)
            return

        try:
            frames, qa_range = helpers.append(base_frames, frames)
        except helpers.UnmatchedColumnsError as e:
            logger.error('(%s) Append error: %s', file_counter, e)
            set_props(f'sanity-checks-{file_counter}', {'children': [dmc.Text(str(e), c='red', ta='right')]})
            _done_no_save(file_counter)
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
            qa_report: list[dmc.Text] = _run_sanity_checks(frames, None)
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
        _done_no_save(file_counter)
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
    Output('files-context', 'data'),
    Output('file-attributes', 'children', allow_duplicate=True),
    Output('append-pairs', 'data', allow_duplicate=True),
    State('files-context', 'data'),
    Input({'type': 'saved-badge', 'index': ALL}, 'display'),
)
@log_func
def batch_done(context: Context, badges: list[str]) -> tuple:
    """Keep track of batch progress; set file unsaved flag to re-enable new file selection when all files are processed.

    Look for changes to the Saved badges: setup_batch() creates one, invisible (display='none'), for each file in the
    batch, and process_batch() makes it visible (display='inline') as it finishes with each file. When all badges are
    visible, the batch is complete.

    When the batch is complete, empty the upload-file cache.

    Parameters:
        context   Server-side file paths and unsaved status
        badges          The display attribute for all batch-related Saved badges
                        NOTE: This takes advantage of pattern-matching callback inputs.

    Returns:
        files-context/data       (str)  Same as context parameter but with the unsaved flag set to False
        file-attributes/children (str)  Add the completion time of the batch operation (start time was added by setup_batch())
        append-pairs/data        (None) Clear the list of file pairs for appending
    """

    if badges:  # This also gets triggered when all batch-related badges disappear
        logger.debug('There are %s files in progress: %s', len(badges), badges)

        if all(display == 'inline' for display in badges):
            logger.debug('Batch complete.')
            context.unsaved = False
            context.start_batch = False
            append_pairs = None
            upload_id: str = context.upload_id
            helpers.clear_file_cache(upload_id)
            end_time: Patch = Patch()
            end_time.append(f' \N{EM DASH} Complete at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
            return context, end_time, append_pairs
        else:
            logger.debug(
                f'Batch not complete; so far only {len([d for d in badges if d == "inline"])} files.'
            )
            raise PreventUpdate
    else:
        logger.debug('No batch in progress.')
        raise PreventUpdate


@blueprint.callback(
    Output('frame-store'  , 'data'    , allow_duplicate=True),
    Output('files-context', 'data'    , allow_duplicate=True),
    Output('file-name'    , 'children', allow_duplicate=True),
    Output('append-file'  , 'contents'),
    Output('read-error'   , 'opened'  , allow_duplicate=True),
    Output('error-title'  , 'children', allow_duplicate=True),
    Output('error-text'   , 'children', allow_duplicate=True),
    State( 'frame-store'  , 'data'    ),
    State( 'files-context', 'data'    ),
    State( 'append-file'  , 'filename'),
    Input( 'append-file'  , 'contents'),
)
@log_func
def append_file(
    new_frames: Frames, context: Context, base_filename: str, contents: str
) -> tuple:
    """An existing Excel file was opened, to be appended to. Append the current data and update the frame store.

    Copy the metadata and site data from the new set; if they're not the same as the existing set, assume that the
    descriptions were edited since last saved and don't treat it as an error.

    Copy the notes, if any, from the existing file. Let the QA code know where to start its analysis: at the start
    of the time series (if there were no previously saved notes), or at the transition to the new data (if there
    already were notes).

    Parameters:
        new_frames        The three or four DataFrames (data, meta, site, possibly notes) from the currently loaded new file
        context           Server-side file path (of the new data) and (un)saved status
        base_filename     Filename of the newly loaded existing Excel file
        contents          Base64-encoded file contents of the newly loaded existing Excel file

    Returns:
        frame-store/data     (dict) The three or four DataFrames of the combined set
        files-context/data   (dict) Original context, with new members, the timestamp range on which to perform QA analysis
        file-name/children   (str)  The names the new and existing files, linked by a compound arrow-plus, indicating appending
        append-file/contents (list) Empty to reset, so a new Append file upload always results in a contents change
        read-error/opened    (bool) True in case of error (show error modal), otherwise False
        error-title/children (str)  In case of error, title for error modal; otherwise an empty string
        error-text/children  (str)  In case of error, error text for the modal dialog; otherwise an empty string

    """

    try:
        new_filename: str = Path(context.files[0]).name
        file_names: str = f"{new_filename} {append_arrow} {base_filename}"
        base_frames: Frames = helpers.load_data(base_filename, contents)
        logger.debug('Existing file data initialized.')
        combined_frames: Frames
        combined_frames, context.qa_range = helpers.append(base_frames, new_frames)
        context.qa_status = QA_Status.APPEND_READY
        logger.info(
            f'Rows: Base rows {len(base_frames.data)}; New rows {len(new_frames.data)}; Combined rows {len(combined_frames.data)}'
        )

        return Serverside(combined_frames, key='Frames'), context, file_names, [], False, '', ''
    except helpers.UnmatchedColumnsError as e:
        logger.error(e)
        return no_update, no_update, no_update, no_update, True, 'Unmatched files', str(e)
    except Exception as e:
        logger.error(f'File Read Error:\n{e}')
        return (
            no_update,
            no_update,
            no_update,
            no_update,
            True,
            'Error Reading File',
            f'We could not process the file "{base_filename}": {e}',
        )
        )
