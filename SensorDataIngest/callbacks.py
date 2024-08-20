'''All callbacks for the SensorDataIngest/ingest Dash app.'''

from dash                      import (
                                   dcc, 
                                   Input, 
                                   State, 
                                   Output, 
                                   callback,
                                   no_update,
                                )
from   dash.exceptions         import PreventUpdate
from   dash_iconify            import DashIconify
import dash_mantine_components as     dmc

from   pathlib import Path
import pandas  as     pd

import logging
import datetime
import json
import io

import helpers                          # Local module implementing Dash-independent actions

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
def load_file(contents, filename, last_modified):
    '''Take the results of opening a data file and store the contents in the memory store.
    
    Triggered by the Upload component providing the base64-encoded file contents, read the sensor data into a DataFrame,
    along with separate DataFrames for metadata (column descriptions) and site data. Persist the three DataFrames, along
    with the filename, in the memory store. Set the unsaved flag to True (the freshly loaded data has not been saved as
    an Excel file).

    Parameters:
        contents        (str)   base64-encoded file contents
        filename        (str)   filename, with extension, without path
        last_modified   (float) UNIX-style timestamp (seconds since 01-01-1970) 

    Returns:
        memory-store/data    (str)  JSON representation of a dict containing filename, three DataFrames, and the "unsaved" flag
        read-error/opened    (bool) True in case of error (show error modal), otherwise False
        error-title/children (str)  In case of error, title for error modal; otherwise an empty string
        error-text/children  (str)  In case of error, error text for the modal dialog; otherwise an empty string

    '''
    
    logger.debug(f'Entered load_file(). contents{" not" if contents else ""} empty. filename={filename}')
    if contents:
        try:
            df_data, df_meta, df_site = helpers.load_data(contents, filename)

            # Build up the dict structure to be stored from scratch, regardless of what may have been stored before.
            # NOTE: there are several ways of JSONifying a DataFrame (orient parameter); "split" is lossless and efficient.
            _data                  = {}
            _data['filename']      = filename
            _data['last_modified'] = last_modified
            _data['df_data']       = df_data.to_json(orient='split', date_format='iso')
            _data['df_meta']       = df_meta.to_json(orient='split')
            _data['df_site']       = df_site.to_json(orient='split')
            _data['unsaved']       = True

            logger.debug('Data initialized. Exiting load_file().')
            return json.dumps(_data), False, '', ''
        except Exception as e:
            logger.error(f'File Read Error:\n{e}')
            return no_update, True, 'Error Reading File', f'We could not process the file "{filename}": {e}'
    else:
        logger.debug('No contents to process. Exiting load_file().')
        raise PreventUpdate                 # No file contents: do nothing

@callback(
    Output('save-xlsx'   , 'data'    ),
    Output('memory-store', 'data'    , allow_duplicate=True),
    Input( 'save-button' , 'n_clicks'),
    State( 'memory-store', 'data'    ),
    prevent_initial_call=True,
)
def save_file(n_clicks, data):
    '''Save the data currently in memory in an Excel (.XLSX) file.

    In response to a button click, take the data from the memory store, download it to the browser, and have
    the browser write it to a file, of the same name as the original but with a ".xlsx" extension. Depending
    on the user's browser settings, this either silently saves the file in a pre-designated folder (e.g., Downloads)
    or opens the OS-native Save File dialog, allowing the user to choose any folder (and change the filename, too).

    NOTE: If the Save File dialog is opened, it's possible that the user clicks Cancel, in which case the file is not
          saved. There seems to be no way for the program to detect this. For now, this app assumes that the file
          is always saved.
    
    Parameters:
        n_clicks  (int) The number of times the button has been pressed
        data      (str) JSON representation of the DataFrames and other data
    
    Returns:
        save-xlsx/data    (dict) Content and filename to be downloaded to the browser
        memory-store/data (str)  Same as data parameter but with the unsaved flag set to False
    '''

    logger.debug('Entered save_file().')
    _data   = json.loads(data)

    if 'df_data' in _data:
        df_data = pd.read_json(io.StringIO(_data['df_data']), orient='split') 
        df_meta = pd.read_json(io.StringIO(_data['df_meta']), orient='split')
        df_site = pd.read_json(io.StringIO(_data['df_site']), orient='split')
        
        outfile = str(Path(_data['filename']).with_suffix('.xlsx'))

        # Dash provides a convenience function to create the required dictionary. That function in turn
        # relies on a writer (e.g., DataFrame.to_excel) to produce the content. In this case, that writer
        # is a custom function specific to this app.
        contents         = dcc.send_bytes(helpers.multi_df_to_excel(df_data, df_meta, df_site), outfile)
        _data['unsaved'] = False

        logger.debug('File saved. Exiting save_file().')
        return contents, json.dumps(_data)
    else:
        logger.debug('Nothing to save. Exiting save_file()')
        raise PreventUpdate

@callback(
    Output('memory-store'  , 'data'    , allow_duplicate=True),
    Output('select-file'   , 'contents', allow_duplicate=True),
    Input( 'clear-button'  , 'n_clicks'),
    prevent_initial_call=True,
)
def clear_data(n_clicks):
    '''Clear all data in memory.
    
    Triggered by the Clear button.

    Parameters:
        n_clicks (int) Number of times the Clear button was pressed

    Returns:
        memory-store/data    (str) JSON representation of a cleared data store: no DataFrames, filename blank, unsaved flag False
        select-file/contents (str) Empty string to reset, so a new file upload always results in a contents change, even if
                                   it's the same file as currently loaded
    '''

    logger.debug('Called clear_data().')
    _data = dict(filename='', unsaved=False)

    return json.dumps(_data), []

@callback(
    Output('select-file' , 'disabled', allow_duplicate=True),
    Input( 'memory-store', 'data'),
    prevent_initial_call=True,
)
def disable_load_data(data):
    '''Disable the Load Data element when new data is loaded; re-enable when data is cleared or saved.

    Parameters:
        data (str) JSON representation of the DataFrames and other data; this function only looks at the unsaved flag.

    Returns:
        select-file/disabled (bool) True if unsaved data in memory; False otherwise (no data or data was saved).
    '''

    logger.debug('Entered disable_load_data().')
    _data = json.loads(data)

    # This could just be "return _data['unsaved']" but with logging an explicit if-else is clearer.
    if _data['unsaved']:
        logger.debug('Data not saved; disable Load Data. Exiting disable_load_data().')
        return True
    else:
        logger.debug('Data is saved; enable Load Data. Exiting disable_load_data().')
        return False

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
        data      (str) JSON representation of the DataFrames and other data
        
    Returns:
        inspect-data/display  (str)  Show the column selection part of the Navbar if there's data; otherwise blank it 
        select-columns/label  (str)  Use the label (title) to show how many columns are available
        data-columns/children (list) A list of dmc.Checkbox elements, one for each data column
        select-columns/value  (list) Reset the current selection (uncheck all boxes)
    '''

    logger.debug('Entered show_columns().')
    _data   = json.loads(data)

    if 'df_data' in _data:
        if _data['unsaved']:
            logger.debug('DataFrame found. Populating column selection list.')
            df_data    = pd.read_json(io.StringIO(_data['df_data']), orient='split')

            # Skip the timestamp and record number columns; these are not data columns.
            # TODO: Try to find a way to get these names from the metadata, in case they're not
            #       the same across data loggers or across time.
            checkboxes = [dmc.Checkbox(label=c, value=c, size='sm',) for c in df_data.columns
                        if c not in ['TIMESTAMP', 'RECORD']] 
            logger.debug('Exiting show_columns().')
            return 'contents', f'{len(checkboxes)} Available Columns', checkboxes, []
        else:
            raise PreventUpdate
    else:
        logger.debug('No data; clear column selection element. Exiting show_columns().')
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
        data     (str) JSON representation of the DataFrames and other data

    Returns:
        stacked-graphs/figure (Figure) Plotly figure of all graphs in one stacked plot
        plot-area/display     (str)    Hide the plot area if there are no graphs to show
                                       (otherwise you see an empty set of axes)
    '''
    
    logger.debug('Entering draw_plots().')
    if showcols:
        logger.debug('Columns selected; generating graphs.')
        _data   = json.loads(data)
        df_data = pd.read_json(io.StringIO(_data['df_data']), orient='split')
        fig     = helpers.render_graphs(df_data, showcols)

        logger.debug('Exiting draw_plots().')
        return fig, 'contents'
    else:
        logger.debug('No columns selected; clear the graphs. Exiting draw_plots().')
        return {}, 'none'

@callback(
    Output('saved-badge', 'display'),
    Output('select-file', 'contents'),
    Input('memory-store', 'data'),
    prevent_initial_call=True,
)
def process_saved(data):
    '''Respond to a Save action by showing a SAVED badge.

    Also clear the contents of the Upload element, so any new file load results in a data refresh,
    even if it's the same file as before.

    Parameters:
        data      (str) JSON representation of the DataFrames and other data

    Returns
        saved-badge/display  (str) Show ('contents') the SAVED badge if data was saved; otherwise hide it ('none')
        select-file/contents (str) Clear ('') the Upload contents if data was saved; otherwise no change
    '''
    
    logger.debug('Entering process_saved().')
    _data = json.loads(data)

    if _data['filename'] and not _data['unsaved']:
        logger.debug('Data in memory, file saved; show Saved badge. Exiting process_saved().')
        return 'inline', ''
    else:
        logger.debug('No data or data unsaved; hide Saved badge. Exiting process_saved().')
        return 'none', no_update

@callback(
    Output('save-button' , 'disabled'),
    Output('clear-button', 'disabled'),
    Input('memory-store' , 'data'),
)
def toggle_save_clear(data):
    '''If there's data in memory, enable the Save and Clear buttons; otherwise, disable them.

    Parameters:
        data (str) JSON representation of the DataFrames and other data (this function only cares about the filename member)

    Returns:
        save-button/disabled   True to disable (no data); False to enable (data in memory)
        clear-button/disabled  True to disable (no data); False to enable (data in memory)
    '''
    
    logger.debug('Called toggle_save_clear().')
    _data = json.loads(data)

    have_file = bool(_data['filename'])
    logger.debug(f'{"D" if have_file else "No d"}ata in memory. {"En" if have_file else "Dis"}able Save and Clear buttons.')

    return (False, False) if have_file else (True, True)

@callback(
    Output('file-name'     , 'children'),
    Output('last-modified' , 'children'),
    Input('memory-store'   , 'data'),
)
def show_file_info(data):
    '''If there's data in memory, show information (filename, last-modified) about the file that was loaded.

    Parameters:
        data (str) JSON representation of the DataFrames and other data (this function only cares about the filename member)

    Returns:
        file-name/children      (str) The name of the currently loaded file (no path)
        last-modified/children  (str) A report of the last-modified timestamp of the currently loaded file
    '''

    logger.debug('Entered show_file_info().')
    _data = json.loads(data)

    if _data['filename']:
        filename      = _data['filename']
        last_modified = f'Last modified: {datetime.datetime.fromtimestamp(_data['last_modified'])}'
        
        logger.debug('Data in memory; report file information. Exiting show_file_info().')
        return filename, last_modified
    else:
        logger.debug('No data in memory; clear file information. Exiting show_file_info().')
        return '', ''
    
@callback(
    Output('sanity-checks', 'children'),
    Output('memory-store' , 'data'),
    State( 'file-name'    , 'children'),
    Input( 'memory-store' , 'data'),
)
def report_sanity_checks(previous_filename, data):
    '''Perform sanity checks on the data and report the results in a separate area of the app shell.

    Currently only checking for drop-outs or irregularities in the time and record sequences; other checks to be added.
    In principle, any number of checks can be reported on in the top part of the main app area. In practice if the list
    grows too long, A page layour redesign may be needed.

    Parameters:
        previous-filename (str) The filename from before the change that triggered this call
        data              (str) JSON representation of the DataFrames and other data

    Returns:
    '''
    _data = json.loads(data)

    # Get the names of the timestamp and sequence-number coluns.
    # TODO: find a way to get the names of these columns from the metadata.
    ts_col    = 'TIMESTAMP'
    seqno_col = 'RECORD'
    filename  = _data['filename']
    new_data  = no_update

    if filename:
        if filename != previous_filename:     # New data is different from the old data, so rerun the checks and update the report
            df_data          = pd.read_json(io.StringIO(_data['df_data']), orient='split') 
            interval_minutes = 15             # TODO: Get this from the metadata
            report           = []
            
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
                new_data            = _data
                new_data['df_data'] = df_data.to_json(orient='split', date_format='iso')
        else:                                 # New data is the same as the old data, so do nothing
            raise PreventUpdate
    else:                                     # No data, so clear the report
        report = []
    
    return report, new_data
    