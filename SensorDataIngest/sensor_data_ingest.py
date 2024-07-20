from dash import Dash, _dash_renderer

import dash_mantine_components as dmc

from layout    import layout
from callbacks import *

_dash_renderer._set_react_version('18.2.0')

stylesheets = [
    'https://unpkg.com/@mantine/dates@7/styles.css',
    'https://unpkg.com/@mantine/code-highlight@7/styles.css',
    'https://unpkg.com/@mantine/charts@7/styles.css',
    'https://unpkg.com/@mantine/carousel@7/styles.css',
    'https://unpkg.com/@mantine/notifications@7/styles.css',
    'https://unpkg.com/@mantine/nprogress@7/styles.css',
]

app = Dash(external_stylesheets=stylesheets)
app.layout = dmc.MantineProvider(layout)

if __name__ == '__main__':
    app.run(debug=True)
