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
    # Output('select-columns', 'value'),
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
        select-columns/value (list) Empty list to clear the multiselect component
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
    logger.debug('Entered disable_load_data().')

    _data = json.loads(data)

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
    logger.debug('Entered show_columns().')
    _data   = json.loads(data)

    if 'df_data' in _data:
        if _data['unsaved']:
            logger.debug('DataFrame found. Populating column selection list.')
            df_data    = pd.read_json(io.StringIO(_data['df_data']), orient='split')
            checkboxes = [dmc.Checkbox(label=c, value=c, size='sm',) for c in df_data.columns
                        if c not in ['TIMESTAMP', 'RECORD']]
            logger.debug('Exiting show_columns().')
            return 'inherit', f'{len(checkboxes)} Available Columns', checkboxes, []
        else:
            raise PreventUpdate
    else:
        logger.debug('No data; clear column selection element. Exiting show_columns().')
        return 'none', '', [], []

@callback(
    # Output('stacked-graphs', 'children', allow_duplicate=True),
    Output('stacked-graphs', 'figure'),
    Output('plot-area'     , 'display'),
    Input( 'select-columns', 'value'),
    State( 'memory-store'  , 'data'),
    prevent_initial_call=True,
)
def draw_plots(showcols, data):
    logger.debug('Entering draw_plots().')
    if showcols:
        logger.debug('Columns selected; generating graphs.')
        _data   = json.loads(data)
        df_data = pd.read_json(io.StringIO(_data['df_data']), orient='split')
        fig     = helpers.render_graphs(df_data, showcols)

        logger.debug('Exiting draw_plots().')
        # return dcc.Graph(figure=fig)
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
    logger.debug('Entering process_saved().')
    _data = json.loads(data)

    if _data['filename'] and not _data['unsaved']:
        logger.debug('Data in memory, file saved; show Saved badge. Exiting process_saved().')
        return 'inherit', ''
    else:
        logger.debug('No data or data unsaved; hide Saved badge. Exiting process_saved().')
        return 'none', no_update

@callback(
    Output('save-button' , 'disabled'),
    Output('clear-button', 'disabled'),
    Input('memory-store' , 'data'),
)
def toggle_save_clear(data):
    logger.debug('Called toggle_save_clear().')
    _data = json.loads(data)

    have_file = bool(_data['filename'])
    logger.debug(f'{"D" if have_file else "No d"}ata in memory. {"En" if have_file else "Dis"}able Save and Clear buttons.')
    return (False, False) if _data['filename'] else (True, True)

@callback(
    Output('file-name'     , 'children'),
    Output('last-modified' , 'children'),
    Input('memory-store'   , 'data'),
)
def show_file_info(data):
    _data = json.loads(data)

    if _data['filename']:
        filename      = _data['filename']
        last_modified = f'Last modified: {datetime.datetime.fromtimestamp(_data['last_modified'])}'
        
        return filename, last_modified
    else:
        return '', ''
    
@callback(
    Output('sanity-checks', 'children'),
    Output('memory-store' , 'data'),
    State( 'file-name'    , 'children'),
    Input( 'memory-store' , 'data'),
)
def report_sanity_checks(previous_filename, data):
    _data = json.loads(data)

    ts_col    = 'TIMESTAMP'
    seqno_col = 'RECORD'
    filename  = _data['filename']
    new_data  = no_update

    if filename:
        if filename != previous_filename:
            df_data          = pd.read_json(io.StringIO(_data['df_data']), orient='split') 
            interval_minutes = 15
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
                df_data[seqno_col]  = df_data.index
                new_data            = _data
                new_data['df_data'] = df_data.to_json(orient='split', date_format='iso')
        else:
            # Do nothing
            raise PreventUpdate
    else:
        report = []
    
    return report, new_data
    