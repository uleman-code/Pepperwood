"""Static Dash layout: app shell with header, navigation bar, and main area."""

import json
import uuid

import dash_mantine_components as dmc
from dash_extensions.enrich import dcc
import dash_uploader as du

UPLOADER_TEXT = 'Drop file(s) here or click to select'

header = dmc.Group(
    [
        dmc.Burger(id='burger-button', opened=False, hiddenFrom='md'),      # Mobile only?
        dmc.Title('Sensor Data Ingest'),
    ],
    justify='center',
)

def make_uploader(id: str) -> du.Upload:
    """Create a Dash Uploader component from dash_uploader.

    NOTE: The dash_uploader project is no longer actively maintained, so there is a risk
    that future versions of Dash may break its functionality. The same thing applies to
    Flow.js, which dash_uploader uses internally, and which is also not being maintained.

    There are several limitations to dash_uploader / Flow.js:
    - It does not support folder uploads when uploads are limited to certain file types
      (filetypes argument).
    - It does not pass the selected filetypes to the file selection dialog; it only uses them
      to filter files after the fact.
    - It does not behave the same as dcc.Upload in that it does not have a "children" property;
      this means that it is inflexible in terms of customizing its appearance (text only, no buttons).
    - It requires a wrapper around the Dash app and a special callback decorator, so seems to live
      outside the normal Dash paradigm.
    - Some text properties (text_disabled) do not work; others (text_completed) are awkward in that
      you cannot control the entire string.

    Parameters:
        id  The component ID.

    Returns:
        The Upload component.
    """

    return du.Upload(
        id=id,
        text=UPLOADER_TEXT,
        max_file_size=1024,             # 1 GB (default)  TODO: make configurable?
        max_files=100,                  # TODO: make configurable?
        chunk_size=1,                   # 1 MB (default), a reasonable guess?
        filetypes=['dat', 'csv', 'xlsx', 'xls'],   # TODO: make configurable? Other synonyms for CSV?
        upload_id=uuid.uuid1(),
        pause_button=False,
        cancel_button=False,
        disabled=False,
        default_style={
            'width': '100%',
            'height': '25px',
            'lineHeight': '10px',
            'borderStyle': 'none',
            'textAlign': 'center',
        },
    )

load_save = [
    dmc.CardSection(
        children=
        [
            dmc.Text('Load Data', id='load-label', size='lg', fw='bold'),
            make_uploader(id='select-file'),
            # dcc.Upload(
            #     dmc.Stack(                          # The entire Stack is the drag-and-drop area
            #         children=[
            #             dmc.Text('Drag and drop, or', h='xs'),
            #             dmc.Button('Select File(s)'),
            #         ],
            #         align='center',
            #     ),
            #     id='select-file',
            #     multiple=True,
            #     accept='.dat,.csv,.xlsx,.xls',      # NOTE: a string, not a list of strings
            # ),
        ],
        withBorder=True,
        ta = 'center',
        h='80px',
    ),
    dmc.CardSection(
        dmc.Group(
            children=[
                dmc.Tooltip(
                    dmc.Button('Save', id='save-button', disabled=True),
                    label='Save current data as an Excel file',
                ),
                dcc.Upload(
                    dmc.Tooltip(
                        dmc.Button('Append', id='append-button', disabled=True),
                        label='Select file(s) to append current data to',
                    ),
                    id='append-file',
                    multiple=False,
                    accept='.xlsx',
                ),
                dmc.Tooltip(
                    dmc.Button('Clear', id='clear-button', disabled=True, color='red'),
                    label='Clear all data from memory',
                ),
            ],
            justify='center',
            gap='xs',
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
                    label='Select variables for plotting.',
                    children=[
                        dmc.Switch('Single plot', id='single-plot'),
                        dmc.Space(h='sm'),
                        dmc.Stack(
                            id='data-columns',
                            gap='xs',
                        ),
                    ],
                    pt='lg',           # Without this the ScrollArea hides the top of the text???
                ),
            ],
            display='none',
            inheritPadding=True,
        ),
        scrollbars='y',                # May be too rigid in case of very long column names
    ),
]

navbar = dmc.Card(load_save + columns, withBorder=True, h='100dvh')

def make_file_info(n: int | None = None) -> dmc.CardSection:
    """Create a card section for one file's file info, progress, and QA results.

    In a batch process, one of these is created for each file, with the suffix n (1, 2, 3, ...)
    ensuring unique IDs. In case of a single file, no suffix is applied to the ID.

    Parameters:
        n  Numeric suffix for the element ID (batch case). If single file, None means no suffix.

    Returns:
        dmc.CardSection: This file's info/progress/QA area
    """
    suffix   = '' if n is None else '-' + str(n)
    badge_id = 'saved-badge' if n is None else {'type': 'saved-badge', 'index': n}
    return  dmc.CardSection(
                dmc.Group(
                    children=[
                        dmc.Stack(
                            children=[
                                dmc.Text(id=f'file-name{suffix}', size='lg', fw='bold', h='sm'),
                                dmc.Group(
                                    children=[
                                        dmc.Text(id=f'last-modified{suffix}'),
                                        dmc.Badge(
                                            'Saved',
                                            id=badge_id,
                                            ml='sm',
                                            display='none',
                                        ),
                                    ],
                                ),
                            ],
                            py='xs',
                            mt=25,
                        ),
                        dmc.Loader(
                            id=f'wait-please{suffix}',
                            display='none',
                            mt=25,
                            styles={'justify-content': 'start'},
                        ),
                        dmc.Stack(
                            id=f'sanity-checks{suffix}',
                            py='xs',
                            mt=25,
                            mr=20,
                            gap='xs',
                        ),
                        dcc.Download(id=f'save-xlsx{suffix}'),
                    ],
                    justify='space-between',
                ),
                id=f'file-info{suffix}',
                withBorder=True,
                mt=-25 if n is not None else None,
            )

page_main = dmc.Card(
    id='show-data',
    children=[
        dmc.Modal(
            dmc.Text(id='error-text'),
            title=dmc.Text(id='error-title', fw='bold'),
            id='read-error',
            zIndex=10000,
        ),
        make_file_info(),
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

def get_layout() -> dmc.AppShell:
    """Get the Dash layout for the app.

    By making this a function, we ensure that a new layout is created
    each time the app is reloaded, with a unique component ID for the uploader.

    Returns:
        The app layout
    """

    return dmc.AppShell(
                children=[
                    dmc.AppShellHeader(header, px=25),
                    dmc.AppShellNavbar(navbar),
                    dmc.AppShellMain(page_main,
                                        pt=17,
                                        ml=10,
                                    ),
                    dcc.Store(
                        id='files-status',
                        data=json.dumps({'filename': '', 'unsaved': False}),
                    ),
                    dcc.Store(id='frame-store'),
                    dcc.Store(id='file-counter'),
                    dcc.Store(id='next-file'),
                ],
                header={'height': 50},
                navbar={
                    'width': 270,
                    'breakpoint': 'md',
                    'collapsed': {'mobile': True},
                },
           )
