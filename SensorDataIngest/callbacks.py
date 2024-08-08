'''All callbacks for the sensor_data_ingest Dash app.'''

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

import helpers                          # Local module implementing dash-independent actions

logger = logging.getLogger(f'Ingest.{__name__.capitalize()}')

@callback(
    Output('memory-store'  , 'data'    , allow_duplicate=True),
    Output('select-columns', 'value'),
    Output('read-error'    , 'opened'),
    Output('error-title'   , 'children'),
    Output('error-text'    , 'children'),
    Input( 'select-file'   , 'contents'),
    State( 'select-file'   , 'filename'),
    State( 'select-file'   , 'last_modified'),
    prevent_initial_call=True,
)
def load_file(contents, filename, last_modified):
    '''Load data file and store contents in memory store.
    
    Triggered by the Upload component providing the base64-encoded file contents, read the sensor data into a DataFrame,
    along with separate DataFrames for metadata (column descriptions) and site data. Persist the three DataFrames, along
    with the filename, in the memory store. Set the unsaved flag to True (the freshly loaded data has not been saved as
    an Excel file).

    Show the filename and last-modified datetime in the main app area. In addition, enable the Save and Clear buttons and
    disable the data load area.

    Parameters:
        contents        (str) base64-encoded file contents
        filename        (str) filename, with extension, without path
        last_modified   (float) UNIX-style timestamp (seconds since 1970) 

    Returns:
        data         (str) JSON representation of a dict containing filename, three DataFrames, and the "unsaved" flag
        file_info    (list) Text components with filename and last-modified datetime, plus a "Saved" badge

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
            return json.dumps(_data), [], False, '', ''
        except Exception as e:
            logger.error(f'File Read Error:\n{e}')
            return no_update, no_update, True, 'Error Reading File', f'We could not process the file "{filename}": {e}'
    else:
        logger.debug('No contents to process.')
        raise PreventUpdate                 # No file contents: do nothing

@callback(
    Output('save-xlsx'   , 'data'    ),
    Output('memory-store', 'data'    , allow_duplicate=True),
    Input( 'save-button' , 'n_clicks'),
    State( 'memory-store', 'data'    ),
    prevent_initial_call=True,
)
def save_file(n_clicks, data):
    _data   = json.loads(data)
    if 'df_data' in _data:
        df_data = pd.read_json(io.StringIO(_data['df_data']), orient='split') 
        df_meta = pd.read_json(io.StringIO(_data['df_meta']), orient='split')
        df_site = pd.read_json(io.StringIO(_data['df_site']), orient='split')
        
        outfile = str(Path(_data['filename']).with_suffix('.xlsx'))

        get_contents     = dcc.send_bytes(helpers.multi_df_to_excel(df_data, df_meta, df_site), outfile)
        _data['unsaved'] = False

        return get_contents, json.dumps(_data)
    else:
        raise PreventUpdate

@callback(
    Output('memory-store'  , 'data'    , allow_duplicate=True),
    Output('select-file'   , 'contents', allow_duplicate=True),
    Output('select-columns', 'value'   , allow_duplicate=True),
    Input( 'clear-button'  , 'n_clicks'),
    prevent_initial_call=True,
)
def clear_data(n_clicks):
    _data=dict(filename='', unsaved=False)

    return json.dumps(_data), [], []

@callback(
    Output('select-file' , 'disabled', allow_duplicate=True),
    Input( 'memory-store', 'data'),
    prevent_initial_call=True,
)
def disable_load_data(data):
    _data = json.loads(data)

    if _data['unsaved']:
        return True
    else:
        return False

@callback(
    Output('inspect-data'  , 'display'),
    Output('select-columns', 'label'),
    Output('data-columns'  , 'children'),
    Input( 'memory-store'  , 'data'),
    prevent_initial_call=True,
)
def show_columns(data):
    _data   = json.loads(data)

    if 'df_data' in _data:
        df_data    = pd.read_json(io.StringIO(_data['df_data']), orient='split')
        checkboxes = [dmc.Checkbox(label=c, value=c, size='sm',) for c in df_data.columns
                      if c not in ['TIMESTAMP', 'RECORD']]
        return 'inherit', f'{len(checkboxes)} Available Columns', checkboxes
    else:
        return 'none', '', []

@callback(
    Output('stacked-graphs', 'children', allow_duplicate=True),
    Input( 'select-columns', 'value'),
    State( 'memory-store'  , 'data'),
    prevent_initial_call=True,
)
def draw_plots(showcols, data):
    if showcols:
        _data   = json.loads(data)
        df_data = pd.read_json(io.StringIO(_data['df_data']), orient='split')
        fig     = helpers.render_graphs(df_data, showcols)

        return dcc.Graph(figure=fig)
    else:
        return []

@callback(
    Output('saved-badge', 'display'),
    Output('select-file', 'contents'),
    Input('memory-store', 'data'),
    prevent_initial_call=True,
)
def process_saved(data):
    _data = json.loads(data)

    if _data['filename'] and not _data['unsaved']:
        return 'inherit', ''
    else:
        return 'none', no_update

@callback(
    Output('save-button' , 'disabled'),
    Output('clear-button', 'disabled'),
    Input('memory-store' , 'data'),
)
def toggle_save_clear(data):
    _data = json.loads(data)

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
    