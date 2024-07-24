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

import datetime
import json
import io

import helpers

@callback(
    Output('memory-store'  , 'data'    , allow_duplicate=True),
    Output('file-info'     , 'children', allow_duplicate=True),
    Output('select-file'   , 'disabled', allow_duplicate=True),
    Output('save-button'   , 'disabled'),
    Output('clear-button'  , 'disabled'),
    Output('select-columns', 'value'),
    Input( 'select-file'   , 'contents'),
    State( 'select-file'   , 'filename'),
    State( 'select-file'   , 'last_modified'),
    State( 'memory-store'  , 'data'),
    prevent_initial_call=True,
)
def load_file(contents, filename, last_modified, data):
    if contents:
        try:
            df_data, df_meta, df_site = helpers.load_data(contents, filename)
            _data             = {}
            _data['filename'] = filename
            _data['df_data']  = df_data.to_json(orient='split', date_format='iso')
            _data['df_meta']  = df_meta.to_json(orient='split')
            _data['df_site']  = df_site.to_json(orient='split')
            _data['unsaved']  = True
            children = dmc.Stack(
                children=[
                    dmc.Text(filename, size='lg', fw=700, h='sm'),
                    dmc.Group(
                        children=[
                            dmc.Text(f'Last modified: {datetime.datetime.fromtimestamp(last_modified)}'),
                            dmc.Badge('Saved', id='saved-badge', ml='sm', display='none'),
                        ]
                    )
                ],
            )
            return json.dumps(_data), children, True, False, False, []
        except Exception as e:
            print('File Read Error', e)
            children = dmc.Alert(
                dcc.Markdown(f'We could not process the file **{filename}**: {e}'),
                title='File Read Error',
                withCloseButton=True,
                color='red',
            )
            return no_update, children, no_update, no_update, no_update, no_update
    else:
        raise PreventUpdate

@callback(
    Output('save-xlsx'   , 'data'    ),
    Output('select-file' , 'contents', allow_duplicate=True),
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

        _data['unsaved'] = False
        get_contents = dcc.send_bytes(helpers.multi_df_to_excel(df_data, df_meta, df_site), outfile)

        return get_contents, '', json.dumps(_data)
    else:
        raise PreventUpdate

@callback(
    Output('select-file'   , 'contents', allow_duplicate=True),
    Output('memory-store'  , 'data'    , allow_duplicate=True),
    Output('file-info'     , 'children', allow_duplicate=True),
    Output('select-columns', 'value'   , allow_duplicate=True),
    Input( 'clear-button'  , 'n_clicks'),
    prevent_initial_call=True,
)
def clear_data(n_clicks):
    _data=dict(filename='', unsaved=False)

    return '', json.dumps(_data), [], []

@callback(
    Output('select-file' , 'disabled', allow_duplicate=True),
    Input( 'memory-store', 'data'),
    prevent_initial_call=True,
)
def toggle_load_data(data):
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
    Input('memory-store', 'data'),
    prevent_initial_call=True,
)
def add_saved_badge(data):
    _data    = json.loads(data)

    return 'none' if _data['unsaved'] else 'inherit'