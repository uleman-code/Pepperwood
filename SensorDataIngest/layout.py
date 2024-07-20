from   dash                    import dcc
import dash_mantine_components as     dmc

import json

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
