from dash                      import (
                                   Dash,
                                   dcc, 
                                   html, 
                                   dash_table, 
                                   Input, 
                                   State, 
                                   Output, 
                                   callback,
                                   callback_context,
                                   no_update,
                                   _dash_renderer,
                                )
from   dash.exceptions         import PreventUpdate
from   dash_iconify            import DashIconify
import dash_mantine_components as     dmc

import base64
import datetime
import io
import json

from   pathlib import Path
import pandas  as     pd

_dash_renderer._set_react_version('18.2.0')
pd.set_option('plotting.backend', 'plotly')

stylesheets = [
    'https://unpkg.com/@mantine/dates@7/styles.css',
    'https://unpkg.com/@mantine/code-highlight@7/styles.css',
    'https://unpkg.com/@mantine/charts@7/styles.css',
    'https://unpkg.com/@mantine/carousel@7/styles.css',
    'https://unpkg.com/@mantine/notifications@7/styles.css',
    'https://unpkg.com/@mantine/nprogress@7/styles.css',
]

header = dmc.Group(
    [
        dmc.Burger(id='burger-button', opened=False, hiddenFrom='md'),
        dmc.Title('Sensor Data Ingest'),
    ],
    justify='flex-start',
)

load_save_columns = [
    dmc.CardSection(
        dmc.Text('Load Data', size='xl', fw=700),
        ta = 'center',
    ),
    dmc.CardSection(
        dcc.Upload(
            id='select-file',
            children=[
                dmc.Stack(
                    children=[
                        dmc.Text('Drag and drop', h='xs'),
                        dmc.Text('or', h='sm'),
                        dmc.Button('Select File'),
                    ],
                    align='center',
                )
            ],
            multiple=False,
            accept='.dat,.csv,.xlsx,.xls',      # NOTE: a string, not a list of strings
        ),
        withBorder=True,
        py='xs',
        mt=-10,
    ),
    dmc.CardSection(
        dmc.Group(
            children=[
                dmc.Tooltip(
                    dmc.Button('Save XLSX', id='save-button', disabled=True),
                    label='Save current data as an Excel file',
                ),
                dmc.Tooltip(
                    dmc.Button('Clear', id='clear-button', disabled=True, color='red'),
                    label='Clear all data from memory',
                ),
            ],
        ),
        withBorder=True,
        inheritPadding=True,
        py='xs',
    ),
    dmc.CardSection(
        id='inspect-data',
        children=[
            dmc.CheckboxGroup(
                id='select-columns',
                description=
                [
                    dmc.Text('Graphs are shown in the order', size='xs',),
                    dmc.Text('in which you you select the columns.', size='xs',),
                ],
                children=[
                    dmc.Space(h='sm'),
                    dmc.ScrollArea(
                        dmc.Stack(
                            id='data-columns',
                            gap='xs',
                        ),
                        type='hover',
                        h=570,
                    ),
                ],
            ),
        ],
        display='none',
        withBorder=True,
        # w=250,
        p='lg',
    )
]

navbar = dmc.Card(
    load_save_columns,
    p='xl',
)

page_main = dmc.Card(
    id='show-data',
    children=[
        dmc.CardSection(
            id='file-info',
            # withBorder=False,
        ),
        dmc.CardSection(
            id='stacked-graphs',
            # config={'autosizable': True}, 
        ),
    ],
    # withBorder=True,
    # radius='md'
),

layout =dmc.AppShell(
    children=[
        dmc.AppShellHeader(header, px=25),
        dmc.AppShellNavbar(navbar,),
        dmc.AppShellMain(page_main),
        dcc.Store(
            id='memory-store',
            storage_type='memory',
            data=json.dumps(dict(filename='', unsaved=False)),
        ),
        dcc.Download(id='save-xlsx'),
    ],
    header={'height': 50},
    padding='xl',
    navbar={
        'width': 275,
        'breakpoint': 'md',
        'collapsed': {'mobile': True},
    },
)

def load_data(contents, filename):
    _, content_string = contents.split(',')     # File contents are preceded by a file type string
    meta_columns = 'Name Alias Sample/Average'.split()
    site_columns = 'Unkown01 SiteId DataLoggerModel Unknown02 DataLoggerOsVersion Unknown03 Unknnown04 SamplingInterval'.split()

    decoded = io.StringIO(base64.b64decode(content_string).decode('utf-8'))
    try:
        if Path(filename).suffix in ['.dat', '.csv']:
            # Assume that the user uploaded a CSV file
            df_data = pd.read_csv(decoded, skiprows=[0,2,3], parse_dates=['TIMESTAMP'])
            decoded.seek(0)
            df_meta = pd.read_csv(decoded, header=None, skiprows=[0], nrows=3).T
            decoded.seek(0)
            df_site = pd.read_csv(decoded, header=None, nrows=1)
            df_meta.columns = meta_columns
            df_site.columns = site_columns
            
        elif Path(filename).suffix in ['.xlsx', '.xls']:
            # Assume that the user uploaded an excel file
            df_data = pd.read_excel(io.BytesIO(decoded), sheet_name='Data')
            df_meta = pd.read_excel(io.BytesIO(decoded), sheet_name='Columns')
            df_site = pd.read_excel(io.BytesIO(decoded), sheet_name='Site')
        else:
            # Log it
            raise ValueError(f'We do not support the **{Path(filename).suffix}** file type.')
    except Exception as e:
        # Log it
        raise

    return df_data, df_meta, df_site

def multi_df_to_excel(df_data, df_meta, df_site):
    buffer = io.BytesIO()
    sheets = {'Data': df_data, 'Columns': df_meta, 'Site': df_site}

    with pd.ExcelWriter(buffer) as xl:
        for sheet, df in sheets.items():
            df.to_excel(xl, index=False, sheet_name=sheet)

            # Automatically adjust column widths to fit all text
            # NOTE: this may be an expensive operation. Beware of large files!
            for column in df:
                column_width = max(df[column].astype(str).str.len().max(), len(column))
                col_idx      = df.columns.get_loc(column)
                xl.sheets[sheet].set_column(col_idx, col_idx, column_width)

    return buffer.getvalue()

def render_graphs(df_data, showcols):
    df_show = df_data.set_index('TIMESTAMP')[showcols]                                 # TIMESTAMP is the independent (X-axis) variable for all plots
    
    fig = (df_show.plot.line(facet_row='variable', height=200 + 200*len(showcols))           # Simplistic attempt at calculating the height depending on number of graphs
        .update_yaxes(matches=None, title_text='')                                     # Each graph has its own value range; don't show the axis title 'value'
        .update_xaxes(showticklabels=True)                                             # Repeat the time scale under each graph
        .for_each_annotation(lambda a: a.update(text=a.text.replace('variable=', ''))) # Just print the variable (column) name
        .update_layout(legend_title_text='Variable')
        # .update_traces(visible='legendonly')
        )
    
    return fig

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
            df_data, df_meta, df_site = load_data(contents, filename)
            _data             = json.loads(data)
            _data['filename'] = filename
            _data['df_data']  = df_data.to_json(orient='split', date_format='iso')
            _data['df_meta']  = df_meta.to_json(orient='split')
            _data['df_site']  = df_site.to_json(orient='split')
            _data['unsaved']  = True
            children = dmc.Stack(
                children=[
                    dmc.Text(filename, size='lg', fw=700, h='sm'),
                    dmc.Text(f'Last modified: {datetime.datetime.fromtimestamp(last_modified)}'),
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
    df_data = pd.read_json(io.StringIO(_data['df_data']), orient='split') 
    df_meta = pd.read_json(io.StringIO(_data['df_meta']), orient='split')
    df_site = pd.read_json(io.StringIO(_data['df_site']), orient='split')
    
    outfile = str(Path(_data['filename']).with_suffix('.xlsx'))

    _data['unsaved'] = False
    download = dcc.send_bytes(multi_df_to_excel(df_data, df_meta, df_site), outfile)
    # download = dcc.send_bytes(multi_df_to_excel, outfile, df_data=df_data, df_meta=df_meta, df_site=df_site)

    return download, '', json.dumps(_data)

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
        fig     = render_graphs(df_data, showcols)

        return dcc.Graph(figure=fig)
    else:
        return []

app = Dash(external_stylesheets=stylesheets)
app.layout = dmc.MantineProvider(layout)

if __name__ == '__main__':
    app.run(debug=True)
