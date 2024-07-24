'''Main module for the SensorDataIngest package for Pepperwood.'''

from dash import Dash, _dash_renderer

import dash_mantine_components as dmc

from layout    import layout
from callbacks import *           # In this case, a star import is acceptable: we want to instantiate all callbacks.

_dash_renderer._set_react_version('18.2.0')     # Required by Dash Mantine Components 0.14.3; the need should go away in a future release.

# Explicitly identifying the needed stylesheets is mandatory in dash 3.14.3. But this app doesn't use any of them.
stylesheets = [
    # 'https://unpkg.com/@mantine/dates@7/styles.css',
    # 'https://unpkg.com/@mantine/code-highlight@7/styles.css',
    # 'https://unpkg.com/@mantine/charts@7/styles.css',
    # 'https://unpkg.com/@mantine/carousel@7/styles.css',
    # 'https://unpkg.com/@mantine/notifications@7/styles.css',
    # 'https://unpkg.com/@mantine/nprogress@7/styles.css',
]

# Standard creation of a Dash app with Dash Mantine Components.
# Suppress callback exceptions because one of the callbacks refers to a component created by another callback
# (not part of the static layout). This normally causes an "id is not part of the layout" error.
app = Dash(external_stylesheets=stylesheets, suppress_callback_exceptions=True)
app.layout = dmc.MantineProvider(layout)

if __name__ == '__main__':
    app.run(debug=False)
