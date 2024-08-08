'''Static Dash layout: app shell with header, navigation bar, and main area.'''

from   dash                    import dcc
import dash_mantine_components as     dmc

import json

header = dmc.Group(
    [
        dmc.Burger(id='burger-button', opened=False, hiddenFrom='md'),      # Won't matter unless on a mobile device
        dmc.Title('Sensor Data Ingest'),
    ],
    justify='flex-start',
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
                multiple=False,
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
                        h=570,                      # Can fiddle with height based on user feedback
                    ),
                ],
            ),
        ],
        display='none',
        withBorder=True,
        pl='xl',
    )
]

navbar = dmc.Card(load_save + columns)

page_main = dmc.Card(
    id='show-data',
    children=[
        dmc.Modal(
            dmc.Text(id='error-text'),
            title=dmc.Text(id='error-title', fw=700),
            id='read-error',
            zIndex=10000,
        ),
        dmc.CardSection(
            dmc.Group(
                children=[
                    dmc.Stack(
                        children=[
                            dmc.Text(id='file-name', size='lg', fw=700, h='sm'),  # Filename in bold
                            dmc.Group(
                                children=[
                                    dmc.Text(id='last-modified'),
                                    dmc.Badge('Saved', id='saved-badge', ml='sm', display='none'),
                                ]
                            )
                        ],
                        py='xs',
                        mt=25,
                    ),
                    dmc.Stack(
                        id='sanity-checks',
                        py='xs',
                        mt=25,
                        mr=20,
                    ),
                ],
                justify='space-between',
            ),
            withBorder=True,
        ),
        dmc.CardSection(
            id='stacked-graphs',
            py='xs',
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
