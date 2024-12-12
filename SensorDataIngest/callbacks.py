'''All callbacks for the SensorDataIngest/ingest Dash app.'''

from dash                      import (
                                #    dcc, 
                                #    Input, 
                                #    State, 
                                #    Output, 
                                   Patch,
                                #    callback,
                                #    no_update,
                                   set_props,
                                #    callback_context,
                                )
from dash_extensions.enrich    import (
                                   dcc,
                                   Input,
                                   State,
                                   Output,
                                   callback,
                                   no_update,
                                   callback_context,
                                   Serverside,
                                )
from   dash.exceptions         import PreventUpdate
from   dash_iconify            import DashIconify
import dash_mantine_components as     dmc

from   layout   import make_file_info
from   datetime import datetime
from   pathlib  import Path
import pandas   as     pd

import logging
import json
import os
import uuid
import time

import helpers                          # Local module implementing Dash-independent actions

logger      = logging.getLogger(f'Ingest.{__name__.capitalize()}')        # Child logger inherits root logger settings
frame_store = {}

@callback(
    # Output('files-status'  , 'data'    , allow_duplicate=True),
    Output('frame-store'   , 'data'    ),
    Output('read-error'    , 'opened'  ),
    Output('error-title'   , 'children'),
    Output('error-text'    , 'children'),
    Input( 'files-status'  , 'data'    ),
    State( 'select-file'   , 'contents'),
    # State( 'select-file'   , 'last_modified'),
    prevent_initial_call=True,
)
# def load_file(all_contents, filenames, last_modified):
def load_file(files_status, all_contents):
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
        files-status/data    (str)  JSON representation of a dict containing filename, a reference to the three DataFrames, and the "unsaved" flag
        read-error/opened    (bool) True in case of error (show error modal), otherwise False
        error-title/children (str)  In case of error, title for error modal; otherwise an empty string
        error-text/children  (str)  In case of error, error text for the modal dialog; otherwise an empty string

    '''
    
    logger.debug('Enter.')
    # filenames = files_status['filename']
    # logger.debug(f'Files loaded: {", ".join(filenames)}.' if all_contents else 'None.')
    
    # if len(all_contents) != 1:
    filename = files_status['filename']
    unsaved  = files_status['unsaved']

    if isinstance(filename, list) or not filename:
        logger.debug('Zero or multiple files; nothing to show interactively.')
        logger.debug('Exit.')
        raise PreventUpdate

    if not unsaved:
        logger.debug('File was just saved; no change to loaded data.')
        logger.debug('Exit.')
        raise PreventUpdate

    logger.debug(f'Current contents: {type(all_contents)}, {len(all_contents)} elements or characters.')
    contents = all_contents[0]
    # modified = last_modified[0]

    try:
        # id = str(uuid.uuid4())
        # frame_store[id] = {}
        frames = {}
        frames['data'], frames['meta'], frames['site'] = helpers.load_data(contents, filename) 

        # Build up the dict structure to be stored from scratch, regardless of what may have been stored before.
        # data                  = {}
        # data['filename']      = filename
        # data['last_modified'] = modified
        # files_status['frame_id'] = id
        # files_status['unsaved']  = True

        logger.debug('Data initialized.')
        logger.debug('Exit.')
        return Serverside(frames, key='Frames'), False, '', ''
    except Exception as e:
        logger.error(f'File Read Error:\n{e}')
        return no_update, True, 'Error Reading File', f'We could not process the file "{filename}": {e}'

@callback(
    Output('save-xlsx'   , 'data'    ),
    Output('files-status', 'data'    , allow_duplicate=True),
    # Input( 'wait-please' , 'visible' ),
    Input( 'save-button' , 'n_clicks'),
    State( 'files-status', 'data'    ),
    State( 'frame-store' , 'data'    ),
    prevent_initial_call=True,
    running=[(Output('wait-please', 'display'), 'flex', 'none')]       # Show the busy indicator (LoadingOverlay)
)
def save_file(n_clicks, files_status, frames):
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
        files-status/data (str)  Same as data parameter but with the unsaved flag set to False
    '''

    logger.debug('Enter.')
    # data   = json.loads(data)

    if frames and 'data' in frames:
        # id = files_status['frame_id']
        df_data = frames['data']
        df_meta = frames['meta']
        df_site = frames['site']
        
        outfile = str(Path(files_status['filename']).with_suffix('.xlsx'))

        # Dash provides a convenience function to create the required dictionary. That function in turn
        # relies on a writer (e.g., DataFrame.to_excel) to produce the content. In this case, that writer
        # is a custom function specific to this app.
        contents                = dcc.send_bytes(helpers.multi_df_to_excel(df_data, df_meta, df_site), outfile)
        files_status['unsaved'] = False

        logger.debug('File saved.')
        logger.debug('Exit.')
        return contents, files_status
    else:
        logger.debug('Nothing to save.')
        logger.debug('Exit.')
        raise PreventUpdate

@callback(
    Output('files-status' , 'data'      , allow_duplicate=True),
    Output('frame-store'  , 'clear_data'),
    Output('show-data'    , 'children'  , allow_duplicate=True),
    Output('file-name'    , 'children'  , allow_duplicate=True),
    Output('last-modified', 'children'  , allow_duplicate=True),
    Output('sanity-checks', 'children'  , allow_duplicate=True),
    Output('select-file'  , 'contents'  , allow_duplicate=True),
    Input( 'clear-button' , 'n_clicks'  ),
    Input( 'select-file'  , 'contents'  ),
    State( 'select-file'  , 'filename'  ),
    # State( 'files-status' , 'data'      ),
    # State( 'frame-store'  , 'data'    ),
    State( 'show-data'    , 'children'  ),
    prevent_initial_call=True,
)
def clear_data(n_clicks, in_contents, filenames, show_data):
    '''Clear all data in memory and on the screen, triggered by the Clear button or the loading of (a) new file(s).
    
    Parameters:
        n_clicks (int) Number of times the Clear button was pressed
        data     (str) JSON representation of the data currently loaded

    Returns:
        files-status/data    (str)  JSON representation of a cleared data store: no DataFrames, filename blank, unsaved flag False
        select-file/contents (list) Empty to reset, so a new file upload always results in a contents change, even if
                                    it's the same file(s) as currently loaded
    '''

    logger.debug('Enter.')
    # data = json.loads(data)

    # # In addition to clearing out the in-memory store, must also clear the dataframes.
    # # if 'Frames' in frames:
    # logger.debug('Clearing the frame store.')
    # frame_store = {}
        # id = files_status['frame_id']
        # del frame_store[id]

    if callback_context.triggered_id == 'clear-button':
        logger.debug('Responding to Clear button click. Reset files-status and select-file contents.')
        status   = dict(filename='', unsaved=False)
        contents = ['']
    else:
        if len(filenames) == 1:
            fntext = fn = filenames[0]
        else:
            fn     = filenames
            fntext = ', '.join(filenames)
        
        logger.debug(f'File(s) loaded: {fntext}.')
        status   = dict(filename=fn, unsaved=True)
        contents = no_update

    logger.debug('Exit.')

    # return data, []
    return status, True, show_data[:3], None, None, None, contents

@callback(
    Output('select-file' , 'disabled'),
    Output('load-label'  , 'c'),
    Input( 'files-status', 'data'),
    prevent_initial_call=True,
)
def disable_loaddata(data):
    '''Disable the Load Data element when new data is loaded; re-enable when data is cleared or saved.

    Parameters:
        data (str) JSON representation of the data currently loaded

    Returns:
        select-file/disabled (bool) True if unsaved data in memory; False otherwise (no data or data was saved).
    '''

    logger.debug('Enter.')
    # data = json.loads(data)
    uns = data['unsaved']
    logger.debug(f'Data {"not" if uns else "is"} saved{"" if uns else " or cleared"}; {"dis" if uns else "en"}able Load Data.')
    logger.debug('Exit.')
    return uns, 'gray' if uns else 'black'

@callback(
    Output('inspect-data'  , 'display' ),
    Output('select-columns', 'label'   ),
    Output('data-columns'  , 'children'),
    Output('select-columns', 'value'   ),
    Input( 'frame-store'   , 'data'    ),
    State( 'files-status'  , 'data'    ),
    prevent_initial_call=True,
)
def show_columns(frames, status):
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
    # data   = json.loads(data)

    if frames and 'data' in frames:
        if status['unsaved']:
            logger.debug('DataFrame found. Populating column selection list.')
            # id      = data['frame_id']
            df_data = frames['data']

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
    Output('stacked-graphs', 'figure' ),
    Output('plot-area'     , 'display'),
    Input( 'select-columns', 'value'  ),
    # State( 'files-status'  , 'data'),
    State( 'frame-store'   , 'data'   ),
    prevent_initial_call=True,
)
def draw_plots(showcols, frames):
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
        # data   = json.loads(data)
        # id      = data['frame_id']
        df_data = frames['data']
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
    # Output('select-file', 'contents'),
    Input('files-status', 'data'),
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
    # data = json.loads(data)

    if data['filename'] and not data['unsaved']:
        logger.debug('Data in memory, file saved; show Saved badge.')
        logger.debug('Exit.')
        return 'inline'     # , ''
    else:
        logger.debug('No data or data unsaved; hide Saved badge.')
        logger.debug('Exit.')
        return 'none'       # , no_update

@callback(
    Output('save-button' , 'disabled'),
    Output('clear-button', 'disabled'),
    Input('files-status' , 'data'),
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
    # data = json.loads(data)

    have_file = bool(data['filename'])
    logger.debug(f'{"D" if have_file else "No d"}ata in memory. {"En" if have_file else "Dis"}able Save and Clear buttons.')
    logger.debug('Exit.')

    return (False, False) if have_file else (True, True)

@callback(
    Output('file-name'    , 'children'),
    Output('last-modified', 'children'),
    Input( 'files-status' , 'data'    ),
    # State( 'file-name'    , 'children'),
    State( 'select-file'  , 'last_modified'),
    prevent_initial_call=True,
)
def show_file_info(files_status, last_modified):
    '''If there's data in memory, show information (filename, last-modified) about the file that was loaded.

    Parameters:
        data (str) JSON representation of the data currently loaded (this function only cares about the filename member)

    Returns:
        file-name/children      (str) The name of the currently loaded file (no path)
        last-modified/children  (str) A report of the last-modified timestamp of the currently loaded file
    '''

    logger.debug('Enter.')
    # data = json.loads(data)

    filename = files_status['filename']
    if isinstance(filename, str) and filename:
        modified = f'Last modified: {datetime.fromtimestamp(last_modified[0])}'
        
        logger.debug('Data in memory; report file information.')
        logger.debug('Exit.')
        return filename, modified
    else:
        logger.debug('No data in memory; nothing to do.')
        logger.debug('Exit.')
        raise PreventUpdate
    
@callback(
    Output('sanity-checks', 'children'),
    # State( 'file-name'    , 'children'),
    # Input( 'files-status' , 'data'),
    Input( 'frame-store'  , 'data'    ),
    prevent_initial_call=True,
)
def report_sanity_checks(frames):
    '''Perform sanity checks on the data and report the results in a separate area of the app shell.

    Currently only checking for drop-outs or irregularities in the time and record sequences; other checks to be added.
    In principle, any number of checks can be reported on in the top part of the main app area. In practice if the list
    grows too long, A page layour redesign may be needed.

    Parameters:
        previous-filename (str) The filename from before the change that triggered this call
        data              (str) JSON representation of the data currently loaded

    Returns:
        sanity-checks/children  The results of a few simple sanity checks
        # files-status/data
    '''

    logger.debug('Enter.')
    # data = json.loads(data)

    # Get the names of the timestamp and sequence-number coluns.
    # TODO: find a way to get the names of these columns from the metadata.
    ts_col    = 'TIMESTAMP'
    seqno_col = 'RECORD'
    # filename  = files_status['filename']
    report    = []

    if frames and 'data' in frames:
        # if filename != previous_filename:     # New data is different from the old data, so rerun the checks and update the report
        logger.debug('New data found. Running and reporting sanity checks.')
        # id               = files_status['frame_id']
        df_data          = frames['data']
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
        # else:
        #     logger.debug('No change in data. Skipping sanity checks, leaving current reports unchanged.')
        #     logger.debug('Exit.')
        #     raise PreventUpdate
    else:                                     
        logger.debug('No data loaded. Clear the sanity check reports.')
    
    logger.debug('Exit.')
    return report

@callback(
    Output('file-name'    , 'children', allow_duplicate=True),
    Output('last-modified', 'children', allow_duplicate=True),
    Output('next-file'    , 'data'    , allow_duplicate=True),
    Input( 'files-status' , 'data'    ),
    prevent_initial_call=True,
)
def batch_header(files_status):
    '''Set up for batch operation.
    '''

    logger.debug('Enter.')
    filenames = files_status['filename']
    if isinstance(filenames, str):
        logger.debug('No file or a single filename: not a batch.')
        logger.debug('Exit.')
        raise PreventUpdate
    
    logger.debug(f'Files loaded: {", ".join(filenames)}.')
    start_time   = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    next_file = 0
    logger.debug('Exit.')
    return 'Batch mode operation', f'Started at {start_time}', next_file

@callback(
    Output('show-data'   , 'children'          ),
    Output('file-counter', 'data'              ),
    Input( 'next-file'   , 'modified_timestamp'),
    State( 'next-file'   , 'data'              ),
    State( 'select-file' , 'filename'          ),
    State( 'select-file' , 'last_modified'     ),
    prevent_initial_call=True,
)
def next_in_batch(mod_ts, next_file, filenames, last_modified):
    '''Increment the file counter if there are more files. This triggers the next batch item.
    '''

    logger.debug('Enter.')
    ts = datetime.fromtimestamp(0.001*mod_ts).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]     # To milliseconds
    logger.debug(f'Next file index is {next_file}, last incremented at {ts}.')

    this_file_info = make_file_info(next_file)
    this_file_info.children.children[0].children[0].children             = filenames[next_file]
    this_file_info.children.children[0].children[1].children[0].children = f'Last modified: {datetime.fromtimestamp(last_modified[next_file])}'
    this_file_info.children.children[1].display                          = 'flex'
    this_file_info.children.children[1].type                             = 'dots'

    showdata = Patch()
    showdata.append(this_file_info)

    logger.debug('Exit.')
    return showdata, next_file

@callback(
    # Output('next-file'   , 'n_clicks', allow_duplicate=True),
    # Input( 'file-counter', 'n_clicks'),
    Output('next-file'   , 'data'              , allow_duplicate=True),
    Input( 'file-counter', 'modified_timestamp'),
    State( 'file-counter', 'data'              ),
    State( 'select-file',  'filename'          ),
    prevent_initial_call=True,
    background=False,
)
def increment_file_counter(mod_ts, file_counter, filenames):
    logger.debug('Enter.')
    ts = datetime.fromtimestamp(0.001*mod_ts).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]     # To milliseconds
    logger.debug(f'File counter is {file_counter}, last incremented at {ts}')
    next_file = file_counter + 1

    if next_file >= len(filenames):
        logger.debug('Reached the end of the batch; stop operation.')
        logger.debug('Exit.')
        raise PreventUpdate
    
    logger.debug('Exit.')
    return next_file

# @callback(
#     Output('show-data'   , 'children'     ),
#     Output('next-file'   , 'data'         , allow_duplicate=True),
#     Input( 'select-file' , 'filename'     ),
#     State( 'select-file' , 'last_modified'),
#     prevent_initial_call=True,
# )
# def setup_batch(filenames, last_modified):
#     '''Increment the file counter if there are more files. This triggers the next batch item.
#     '''

#     logger.debug('Enter.')
#     if len(filenames) < 2:
#         logger.debug('Not a batch.')
#         logger.debug('Exit.')
#         raise PreventUpdate
 
#     current_n = json.loads(file_counter)['val']
#     logger.debug(f'Current file counter is {current_n}.')

#     if current_n + 1 >= len(filenames):
#         logger.debug('Reached the end of the batch; stop operation.')
#         logger.debug('Exit.')
#         raise PreventUpdate
    
#     this_file_info = make_file_info(current_n)
#     this_file_info.children.children[0].children[1].children             = filenames[current_n]
#     this_file_info.children.children[0].children[2].children[0].children = f'Last modified: {datetime.fromtimestamp(last_modified[current_n])}'

#     showdata = Patch()
#     showdata.append(this_file_info)

#     next_file = dict(val=current_n + 1)

#     logger.debug('Exit.')
#     return showdata, json.dumps(next_file)

@callback(
    # Output('read-error'  , 'opened'            , allow_duplicate=True),
    # Output('error-title' , 'children'          , allow_duplicate=True),
    # Output('error-text'  , 'children'          , allow_duplicate=True),
    # Output('show-data'   , 'children'          , allow_duplicate=True),
    # Output('last-modified', 'children'           , allow_duplicate=True),
    Input( 'file-counter', 'modified_timestamp'),
    State( 'file-counter', 'data'              ),
    # State( 'select-file' , 'contents'          ),
    State( 'select-file' , 'filename'          ),
    State( 'show-data'   , 'children'          ),
    prevent_initial_call=True,
)
def process_batch(mod_ts, file_counter, filenames, showdata):     #, all_contents, showdata):
    '''Process all files in batch, without user involvement.
    '''

    logger.debug('Enter.')
    logger.debug(f'Processing file {file_counter}: {filenames[file_counter]}.')
    # logger.debug(showdata)
    time.sleep(1)
    # newdata = Patch()
    # newdata[file_counter+3].children.children[1].display                         = 'none'
    # newdata[file_counter+3].children.children[0].children[1].children[1].display = 'inline'
    # newdata[file_counter+3].children = dmc.Text(f'   File counter is {file_counter}.')
    # newdata.append(f'   File counter is {file_counter}.')
    set_props(f'wait-please-{file_counter}', {'display': 'none'})
    set_props(f'saved-badge-{file_counter}', {'display': 'inline'})
    logger.debug('Exit.')
    
    # return newdata
    # return f'File counter is {file_counter}.'
    # return no_update
    # raise PreventUpdate

