'''All callbacks for the SensorDataIngest/ingest Dash app.'''

from dash                      import (
                                   dcc, 
                                   Input, 
                                   State, 
                                   Output, 
                                   Patch,
                                   callback,
                                   no_update,
                                )
from   dash.exceptions         import PreventUpdate
from   dash_iconify            import DashIconify
import dash_mantine_components as     dmc

from   datetime import datetime
from   pathlib  import Path
import pandas   as     pd

import logging
import json
import io
import uuid

import layout                           # Dash layout including one or more functions for dynamic component creation
import helpers                          # Local module implementing Dash-independent actions

frame_store = {}

logger = logging.getLogger(f'Ingest.{__name__.capitalize()}')        # Child logger inherits root logger settings

@callback(
    Output('memory-store'  , 'data'    , allow_duplicate=True),
    Output('read-error'    , 'opened'),
    Output('error-title'   , 'children'),
    Output('error-text'    , 'children'),
    Input( 'select-file'   , 'contents'),
    State( 'select-file'   , 'filename'),
    State( 'select-file'   , 'last_modified'),
    prevent_initial_call=True,
)
def load_file(all_contents, filenames, last_modified):
    '''If one file was opened, load it. If multiple files, load, check, and save the entire batch.
    
    Triggered by the Upload component providing the base64-encoded file contents, read the sensor data into a DataFrame,
    along with separate DataFrames for metadata (column descriptions) and site data. Persist the three DataFrames, along
    with the filename, in the memory store. Set the unsaved flag to True (the freshly loaded data has not been saved as
    an Excel file).

    Parameters:
        all_contents    (list[str])   base64-encoded file contents for all files
        filenames       (list[str])   filenames, with extension, without path
        last_modified   (list[float]) UNIX-style timestamps (seconds since 01-01-1970 00:00:00) 

    Returns:
        memory-store/data    (str)  JSON representation of a dict containing filename, a reference to the three DataFrames, and the "unsaved" flag
        read-error/opened    (bool) True in case of error (show error modal), otherwise False
        error-title/children (str)  In case of error, title for error modal; otherwise an empty string
        error-text/children  (str)  In case of error, error text for the modal dialog; otherwise an empty string

    '''
    
    logger.debug('Enter.')
    logger.debug(f'Files loaded: {", ".join(filenames)}.' if all_contents else 'No files loaded.')
    
    if len(all_contents) != 1:
        logger.debug('No contents or multiple files; nothing to show interactively.')
        logger.debug('Exit.')
        raise PreventUpdate

    contents = all_contents[0]
    filename = filenames[0]
    modified = last_modified[0]

    try:
        id = str(uuid.uuid4())
        frame_store[id] = {}
        frames          = frame_store[id]
        frames['data'], frames['meta'], frames['site'] = helpers.load_data(contents, filename) 

        # Build up the dict structure to be stored from scratch, regardless of what may have been stored before.
        _data                  = {}
        _data['filename']      = filename
        _data['last_modified'] = modified
        _data['frame_id']      = id
        _data['unsaved']       = True

        logger.debug('Data initialized.')
        logger.debug('Exit.')
        return json.dumps(_data), False, '', ''
    except Exception as e:
        logger.error(f'File Read Error:\n{e}')
        return no_update, True, 'Error Reading File', f'We could not process the file "{filename}": {e}'

@callback(
    Output('file-name'     , 'children', allow_duplicate=True),
    Output('last-modified' , 'children', allow_duplicate=True),
    Output('file-counter'  , 'n_clicks'),
    Input('select-file'    , 'filename'),
    prevent_initial_call=True,
)
def prepare_batch(filenames):
    '''Set up for batch operation.
    '''

    logger.debug('Enter.')
    if len(filenames) <= 1:
        logger.debug('Not a batch.')
        logger.debug('Exit.')
        raise PreventUpdate
    
    logger.debug(f'Files loaded: {", ".join(filenames)}.')
    start_time = datetime.now()
    logger.debug('Exit.')
    return 'Batch mode operation', f'Started at {start_time}', 0

@callback(
    Output('file-counter', 'n_clicks'     , allow_duplicate=True),
    Output('show-data'   , 'children'     ),
    Input( 'file-counter', 'n_clicks'     ),
    State( 'select-file' , 'filename'     ),
    State( 'select-file' , 'last_modified'),
    prevent_initial_call=True,
)
def next_in_batch(current_n, filenames, last_modified):
    '''Increment the file counter if there are more files. This triggers the next batch item.
    '''

    logger.debug('Enter.')
    logger.debug(f'Current file counter is {current_n}.')

    if current_n + 1 >= len(filenames):
        logger.debug('Reached the end of the batch; stop operation.')
        logger.debug('Exit.')
        raise PreventUpdate
    
    this_file_info                                                          = layout.file_info(current_n)
    this_file_info.children[0].children[0].children[1].children             = filenames[current_n]
    this_file_info.children[0].children[0].children[2].children[0].children = last_modified[current_n]

    return Patch().append(this_file_info), current_n + 1

@callback(
    Output('read-error'    , 'opened'  , allow_duplicate=True),
    Output('error-title'   , 'children', allow_duplicate=True),
    Output('error-text'    , 'children', allow_duplicate=True),
    Input( 'file-counter'  , 'n_clicks'),
    State( 'select-file'   , 'contents'),
    State( 'select-file'   , 'filename'),
    State( 'select-file'   , 'last_modified'),
    prevent_initial_call=True,
)
def process_batch(current_n, all_contents, filenames, last_modified):
    '''Process all files in batch, without user involvement.
    '''

    logger.debug('Enter.')
    logger.debug(f'Current file counter is {current_n}.')

    if len(all_contents) <= 1:
        logger.debug('Only one file or none at all; not a batch.')
        logger.debug('Exit.')
        raise PreventUpdate

    logger.debug(f'Processing file {filenames[current_n]}.')    
    
    return no_update,
    raise PreventUpdate

@callback(
    Output('save-xlsx'   , 'data'    ),
    Output('memory-store', 'data'    , allow_duplicate=True),
    # Input( 'wait-please' , 'visible' ),
    Input( 'save-button' , 'n_clicks'),
    State( 'memory-store', 'data'    ),
    prevent_initial_call=True,
    running=[(Output('wait-please', 'visible'), True, False)]       # Show the busy indicator (LoadingOverlay)
)
def save_file(n_clicks, data):
    '''Save the data currently in memory in an Excel (.XLSX) file.

    In response to a button click, take the data from the in-memory frame store, download it to the browser, and have
    the browser write it to a file, of the same name as the original but with a ".xlsx" extension. Depending
    on the user's browser settings, this either silently saves the file in a pre-designated folder (e.g., Downloads)
    or opens the OS-native Save File dialog, allowing the user to choose any folder (and change the filename, too).

    NOTE: If the Save File dialog is opened, it's possible that the user clicks Cancel, in which case the file is not
          saved. There seems to be no way for the program to detect this. For now, this app assumes that the file
          is always saved.
    
    Parameters:
        n_clicks (int) The number of times the button has been pressed
        data     (str) JSON representation of the data currently loaded
    
    Returns:
        save-xlsx/data    (dict) Content and filename to be downloaded to the browser
        memory-store/data (str)  Same as data parameter but with the unsaved flag set to False
    '''

    logger.debug('Enter.')
    _data   = json.loads(data)

    if 'frame_id' in _data:
        id = _data['frame_id']
        df_data = frame_store[id]['data']
        df_meta = frame_store[id]['meta']
        df_site = frame_store[id]['site']
        
        outfile = str(Path(_data['filename']).with_suffix('.xlsx'))

        # Dash provides a convenience function to create the required dictionary. That function in turn
        # relies on a writer (e.g., DataFrame.to_excel) to produce the content. In this case, that writer
        # is a custom function specific to this app.
        contents         = dcc.send_bytes(helpers.multi_df_to_excel(df_data, df_meta, df_site), outfile)
        _data['unsaved'] = False

        logger.debug('File saved.')
        logger.debug('Exit.')
        return contents, json.dumps(_data)
    else:
        logger.debug('Nothing to save.')
        logger.debug('Exit.')
        raise PreventUpdate

@callback(
    Output('memory-store', 'data'    , allow_duplicate=True),
    Output('select-file' , 'contents', allow_duplicate=True),
    Input( 'clear-button', 'n_clicks'),
    State( 'memory-store', 'data'),
    prevent_initial_call=True,
)
def clear_data(n_clicks, data):
    '''Clear all data in memory.
    
    Triggered by the Clear button.

    Parameters:
        n_clicks (int) Number of times the Clear button was pressed
        data     (str) JSON representation of the data currently loaded

    Returns:
        memory-store/data    (str)  JSON representation of a cleared data store: no DataFrames, filename blank, unsaved flag False
        select-file/contents (list) Empty to reset, so a new file upload always results in a contents change, even if
                                    it's the same file(s) as currently loaded
    '''

    logger.debug('Enter.')
    _data = json.loads(data)

    # In addition to clearing out the in-memory store, must also clear the dataframes.
    id    = _data['frame_id']
    del frame_store[id]
    _data = dict(filename='', unsaved=False)
    logger.debug('Exit.')

    return json.dumps(_data), []

@callback(
    Output('select-file' , 'disabled'),
    Input( 'memory-store', 'data'),
    prevent_initial_call=True,
)
def disable_load_data(data):
    '''Disable the Load Data element when new data is loaded; re-enable when data is cleared or saved.

    Parameters:
        data (str) JSON representation of the data currently loaded

    Returns:
        select-file/disabled (bool) True if unsaved data in memory; False otherwise (no data or data was saved).
    '''

    logger.debug('Enter.')
    _data = json.loads(data)
    uns   = _data['unsaved']
    logger.debug(f'Data {"not" if uns else "is"} saved; {"dis" if uns else "en"}able Load Data.')
    logger.debug('Exit.')
    return uns

@callback(
    Output('inspect-data'  , 'display'),
    Output('select-columns', 'label'),
    Output('data-columns'  , 'children'),
    Output('select-columns', 'value'),
    Input( 'memory-store'  , 'data'),
    prevent_initial_call=True,
)
def show_columns(data):
    ''' When data is loaded, populate the column selection element with checkboxes for all data columns in the df_data DataFrame.

    When there is no data (for example, after a Clear), clear the column list, delete the checkboxes,
    and hide that part of the Navbar.

    When there's data but the unsaved flag is False, do nothing. That is because this callback is invoked every time
    anything at all changes in the data in memory. If the unsaved flag is False, it means that we got here because
    that flag just changed (because the user clicked Save XLSX), which in turn means that the columns were already
    populated when that data set was loaded, when the unsaved flag was still True, and nothing in the data itself
    has changed since then. This also avoids clearing the selected columns (checked boxes) just because the user
    did a Save.

    Parameters:
        data (str) JSON representation of the data currently loaded
        
    Returns:
        inspect-data/display  (str)  Show the column selection part of the Navbar if there's data; otherwise blank it 
        select-columns/label  (str)  Use the label (title) to show how many columns are available
        data-columns/children (list) A list of dmc.Checkbox elements, one for each data column
        select-columns/value  (list) Reset the current selection (uncheck all boxes)
    '''

    logger.debug('Enter.')
    _data   = json.loads(data)

    if 'frame_id' in _data:
        if _data['unsaved']:
            logger.debug('DataFrame found. Populating column selection list.')
            id      = _data['frame_id']
            df_data = frame_store[id]['data']

            # Skip the timestamp and record number columns; these are not data columns.
            # TODO: Try to find a way to get these names from the metadata, in case they're not
            #       the same across data loggers or across time.
            checkboxes = [dmc.Checkbox(label=c, value=c, size='sm',) for c in df_data.columns
                        if c not in ['TIMESTAMP', 'RECORD']] 
            logger.debug('Exit.')
            return 'flex', f'{len(checkboxes)} Available Columns', checkboxes, []
        else:
            raise PreventUpdate
    else:
        logger.debug('No data; clear column selection element.')
        logger.debug('Exit.')
        return 'none', '', [], []

@callback(
    Output('stacked-graphs', 'figure'),
    Output('plot-area'     , 'display'),
    Input( 'select-columns', 'value'),
    State( 'memory-store'  , 'data'),
    prevent_initial_call=True,
)
def draw_plots(showcols, data):
    '''Draw plots, one below the other, for each of the selected columns.

    Redraw the entire stacked plot each time the selection changes.

    Parameters:
        showcols (list) Column names, in the order in which they were selected
        data     (str)  JSON representation of the data currently loaded

    Returns:
        stacked-graphs/figure (Figure) Plotly figure of all graphs in one stacked plot
        plot-area/display     (str)    Hide the plot area if there are no graphs to show
                                       (otherwise you see an empty set of axes)
    '''
    
    logger.debug('Enter.')
    if showcols:
        logger.debug('Columns selected; generating graphs.')
        _data   = json.loads(data)
        id      = _data['frame_id']
        df_data = frame_store[id]['data']
        fig     = helpers.render_graphs(df_data, showcols)

        logger.debug('Exit.')
        return fig, 'contents'
    else:
        logger.debug('No columns selected; clear the graphs.')
        logger.debug('Exit.')
        return {}, 'none'

@callback(
    Output('saved-badge', 'display'),
    # Output('wait-please', 'visible', allow_duplicate=True,),
    Output('select-file', 'contents'),
    Input('memory-store', 'data'),
    prevent_initial_call=True,
)
def process_saved(data):
    '''Respond to a Save action by showing a SAVED badge.

    Also clear the contents of the Upload element, so any new file load results in a data refresh,
    even if it's the same file as before.

    Parameters:
        data (str) JSON representation of the data currently loaded

    Returns
        saved-badge/display  (str)  Show ('contents') the SAVED badge if data was saved; otherwise hide it ('none')
        select-file/contents (str)  Clear ('') the Upload contents if data was saved; otherwise no change
    '''
    
    logger.debug('Enter.')
    _data = json.loads(data)

    if _data['filename'] and not _data['unsaved']:
        logger.debug('Data in memory, file saved; show Saved badge.')
        logger.debug('Exit.')
        return 'inline', ''
    else:
        logger.debug('No data or data unsaved; hide Saved badge.')
        logger.debug('Exit.')
        return 'none', no_update

@callback(
    Output('save-button' , 'disabled'),
    Output('clear-button', 'disabled'),
    Input('memory-store' , 'data'),
    prevent_initial_call=True,
)
def toggle_save_clear(data):
    '''If there's data in memory, enable the Save and Clear buttons; otherwise, disable them.

    Parameters:
        data (str) JSON representation of the data currently loaded (this function only cares about the filename member)

    Returns:
        save-button/disabled   True to disable (no data); False to enable (data in memory)
        clear-button/disabled  True to disable (no data); False to enable (data in memory)
    '''
    
    logger.debug('Enter.')
    _data = json.loads(data)

    have_file = bool(_data['filename'])
    logger.debug(f'{"D" if have_file else "No d"}ata in memory. {"En" if have_file else "Dis"}able Save and Clear buttons.')
    logger.debug('Exit.')

    return (False, False) if have_file else (True, True)

@callback(
    Output('file-name'     , 'children'),
    Output('last-modified' , 'children'),
    Input('memory-store'   , 'data'),
    prevent_initial_call=True,
)
def show_file_info(data):
    '''If there's data in memory, show information (filename, last-modified) about the file that was loaded.

    Parameters:
        data (str) JSON representation of the data currently loaded (this function only cares about the filename member)

    Returns:
        file-name/children      (str) The name of the currently loaded file (no path)
        last-modified/children  (str) A report of the last-modified timestamp of the currently loaded file
    '''

    logger.debug('Enter.')
    _data = json.loads(data)

    if _data['filename']:
        filename      = _data['filename']
        last_modified = f'Last modified: {datetime.fromtimestamp(_data['last_modified'])}'
        
        logger.debug('Data in memory; report file information.')
        logger.debug('Exit.')
        return filename, last_modified
    else:
        logger.debug('No data in memory; clear file information.')
        logger.debug('Exit.')
        return '', ''
    
@callback(
    Output('sanity-checks', 'children'),
    State( 'file-name'    , 'children'),
    Input( 'memory-store' , 'data'),
    prevent_initial_call=True,
)
def report_sanity_checks(previous_filename, data):
    '''Perform sanity checks on the data and report the results in a separate area of the app shell.

    Currently only checking for drop-outs or irregularities in the time and record sequences; other checks to be added.
    In principle, any number of checks can be reported on in the top part of the main app area. In practice if the list
    grows too long, A page layour redesign may be needed.

    Parameters:
        previous-filename (str) The filename from before the change that triggered this call
        data              (str) JSON representation of the data currently loaded

    Returns:
        sanity-checks/children  The results of a few simple sanity checks
        # memory-store/data
    '''

    logger.debug('Enter.')
    _data = json.loads(data)

    # Get the names of the timestamp and sequence-number coluns.
    # TODO: find a way to get the names of these columns from the metadata.
    ts_col    = 'TIMESTAMP'
    seqno_col = 'RECORD'
    filename  = _data['filename']
    report    = []

    if filename:
        if filename != previous_filename:     # New data is different from the old data, so rerun the checks and update the report
            logger.debug('New data found. Running and reporting sanity checks.')
            id               = _data['frame_id']
            df_data          = frame_store[id]['data']
            interval_minutes = 15             # TODO: Get this from the metadata
            
            # Fill in text elements
            if helpers.ts_is_regular(df_data[ts_col], interval_minutes):
                report.append(dmc.Text(f'{ts_col} is monotonically increasing by {interval_minutes} minutes from each row to the next.', h='sm'))
            else:
                report.append(dmc.Text(f'{ts_col} is not monotonically and regularly increasing.', c='red', h='sm'))
            
            if helpers.seqno_is_regular(df_data[seqno_col]):
                report.append(dmc.Text(f'{seqno_col} is monotonically increasing by one from each row to the next.', h='sm'))
            else:
                report.append(dmc.Text(f'{seqno_col} sequence is not monotonic or has gaps; column was renumbered, starting at 0.', c='red', h='sm'))

                # The automatically generated index is a row-number sequence (starting at 0). Use that to "renumber" the sequence-number column.
                df_data[seqno_col]  = df_data.index
        else:
            logger.debug('No change in data. Skipping sanity checks, leaving current reports unchanged.')
            logger.debug('Exit.')
            raise PreventUpdate
    else:                                     
        logger.debug('No data loaded. Clear the sanity check reports.')
    
    logger.debug('Exit.')
    return report    