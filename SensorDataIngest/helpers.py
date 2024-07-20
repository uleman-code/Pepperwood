import base64
import io

from   pathlib import Path
import pandas  as     pd

pd.set_option('plotting.backend', 'plotly')

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
    
    fig = (df_show.plot.line(facet_row='variable', height=120 + 200*len(showcols))           # Simplistic attempt at calculating the height depending on number of graphs
        .update_yaxes(matches=None, title_text='')                                     # Each graph has its own value range; don't show the axis title 'value'
        .update_xaxes(showticklabels=True)                                             # Repeat the time scale under each graph
        .for_each_annotation(lambda a: a.update(text=a.text.replace('variable=', ''))) # Just print the variable (column) name
        .update_layout(legend_title_text='Variable')
        # .update_traces(visible='legendonly')
        )
    
    return fig
