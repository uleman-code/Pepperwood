'''Static Dash layout: app shell with header, navigation bar, and main area.'''

from   dash                    import dcc
import dash_mantine_components as     dmc

import json

header = dmc.Group(
    [
        dmc.Burger(id='burger-button', opened=False, hiddenFrom='md'),      # Won't matter unless on a mobile device
        dmc.Title('Sensor Data Ingest'),
    ],
    justify='center',
)

load_save = [
    dmc.CardSection(
        children=
        [
            dmc.Text('Load Data', size='lg', fw=700),
            dcc.Upload(
                dmc.Stack(                          # The entire Stack serves as the drag-and-drop area
                    children=[
                        dmc.Text('Drag and drop', h='xs'),
                        dmc.Text('or', h='sm', mt=-5),
                        dmc.Button('Select File'),
                    ],
                    align='center',
                ),
                id='select-file',
                multiple=True,
                accept='.dat,.csv,.xlsx,.xls',      # NOTE: a string, not a list of strings
            ),
        ],
        withBorder=True,
        ta = 'center',
        py='xs',
        mt=-18,
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
            justify='center',
        ),
        withBorder=True,
        inheritPadding=True,
        py='xs',
    ),
]

columns = [
    dmc.ScrollArea(
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
                        dmc.Stack(
                            id='data-columns',
                            gap='xs',
                        ),
                    ],
                    pt='lg',           # For some reason, without this the ScrollArea hides the top of the text
                ),
            ],
            display='none',
            inheritPadding=True,
        ),
        scrollbars='y',                # This may be too rigid in case very long column names are possible
    )
]

navbar = dmc.Card(load_save + columns, withBorder=True, h='100dvh')

def file_info(n=None):
    suffix = '' if n is None else '-' + str(n)
    return  dmc.CardSection(
                dmc.Group(
                    children=[
                        dmc.Stack(
                            children=[
                                dmc.Text(id=f'file-name{suffix}', size='lg', fw=700, h='sm'),  # Filename in bold
                                dmc.Group(
                                    children=[
                                        dmc.LoadingOverlay(id=f'wait-please{suffix}', visible=False),
                                        dmc.Text(id=f'last-modified{suffix}'),
                                        dmc.Badge('Saved', id=f'saved-badge{suffix}', ml='sm', display='none'),
                                    ]
                                )
                            ],
                            py='xs',
                            mt=25,
                        ),
                        dmc.Stack(
                            id=f'sanity-checks{suffix}',
                            py='xs',
                            mt=25,
                            mr=20,
                        ),
                    ],
                    justify='space-between',
                ),
                id=f'file-info{suffix}',
                withBorder=True,
            )

page_main = dmc.Card(
    id='show-data',
    children=[
        dmc.Modal(
            dmc.Text(id='error-text'),
            title=dmc.Text(id='error-title', fw=700),
            id='read-error',
            zIndex=10000,
        ),
        file_info(),
        dmc.CardSection(
            dcc.Graph(
                id='stacked-graphs',
            ),
            id='plot-area',
            py='xs',
            display='none',
        ),
    ],
)

layout =dmc.AppShell(
    children=[
        dmc.AppShellHeader(header, px=25),
        dmc.AppShellNavbar(navbar,),
        dmc.AppShellMain(page_main,
                         pt=17,
                         ml=10,
                        ),
        dcc.Store(
            id='memory-store',
            storage_type='memory',                                      # Contents are cleared with every page load
            data=json.dumps(dict(filename='', unsaved=False)),
        ),
        dcc.Download(id='save-xlsx'),
    ],
    header={'height': 50},
    navbar={
        'width': 270,
        'breakpoint': 'md',
        'collapsed': {'mobile': True},
    },
)
