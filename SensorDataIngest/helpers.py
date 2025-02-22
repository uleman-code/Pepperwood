'''Functions that implement logic independent of the environment: Dash (interactive) or command-line (batch).'''

import base64
import io
import logging

from   pathlib import Path
from   typing  import Any
import pandas  as     pd

logger = logging.getLogger(f'Ingest.{__name__.capitalize()}')        # Child logger inherits root logger settings
pd.set_option('plotting.backend', 'plotly')

def load_data(contents: str, filename: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]: 
    '''Load a data file (CSV, .dat or .csv suffix; or Excel) and return three DataFrames: data, metadata, and site data.

    Files from data loggers have additional information in the first few lines, including column names and descriptions and
    site information. Turn these into separate DataFrames for metadata and site data, respectively; skip them when loading
    the main data DataFrame.

    Excel files are expected to have been saved by this same app, in which case they have three worksheets, named
    Data, Columns, and Site.

    Limitations:
        File type derived from filename suffix, not contents; this is fragile. In case the contents are not as expected,
            the error message is likely to be cryptic.
        No recovery in the case of an Excel file if meta- or site data are missing but valid data is there.
        No recovery if the case of a CSV file if the top rows are not as expected.
            (In both cases, the result is a file reading exception.)

    Parameters:
        contents    Base64-encoded string of the file contents
        filename    The name of the file; only needed to get the extension

    Returns:
        Three DataFrames: data, metadata, site data
    '''

    logger.debug('Enter.')
    logger.debug(f'Uploaded contents: {contents[:80]}')

    content_string: str
    _, content_string = contents.split(',')                  # File contents are preceded by a file type string

    # TODO: Get these column names from a (configuration?) file.
    # TODO: Figure out what the "UnknownNN" columns are for and give them real names.
    meta_columns: list[str] = 'Name Alias Sample/Average'.split()
    site_columns: list[str] = 'Unknown01 SiteId DataLoggerModel Unknown02 DataLoggerOsVersion Unknown03 Unknnown04 SamplingInterval'.split()
    b64decoded:   bytes     = base64.b64decode(content_string)
    logger.debug('Got decoded file contents.')

    try:
        if Path(filename).suffix in ['.dat', '.csv']:
            # Assume that the user uploaded a CSV file
            logger.info('Reading CSV data file. Expect additional info in the first four rows.')
            decoded = io.StringIO(b64decoded.decode('utf-8'))
            df_data: pd.DataFrame = pd.read_csv(decoded, skiprows=[0,2,3], parse_dates=['TIMESTAMP'], na_values='NAN')    # First pass to read the real data
            decoded.seek(0)
            df_meta: pd.DataFrame = pd.read_csv(decoded, header=None, skiprows=[0], nrows=3).T                            # Second pass to read the column metadata
            decoded.seek(0)
            df_site: pd.DataFrame = pd.read_csv(decoded, header=None, nrows=1).iloc[:,:len(site_columns)]                 # Third pass to read the site data
            df_meta.columns = meta_columns
            df_site.columns = site_columns
            
        elif Path(filename).suffix in ['.xlsx', '.xls']:
            # Assume that the user uploaded an excel file
            logger.info('Reading Excel workbook. Expect three worksheets.')
            buffer  = io.BytesIO(b64decoded)
            df_data: pd.DataFrame = pd.read_excel(buffer, sheet_name='Data', na_values='NAN')
            df_meta: pd.DataFrame = pd.read_excel(buffer, sheet_name='Columns')
            df_site: pd.DataFrame = pd.read_excel(buffer, sheet_name='Site')
        else:
            # This should not happen: the Upload element limits the supported filename extensions.
            logger.error(f'Unsupported file type: {Path(filename).suffix}.')
            raise ValueError(f'We do not support the **{Path(filename).suffix}** file type.')
    except Exception as e:
        logger.error(f'Error reading file {filename}. {e}')
        raise

    logger.debug('DataFrames for data, metadata, and site data populated.')
    logger.debug('Exit.')
    return df_data, df_meta, df_site

def multi_df_to_excel(df_data: pd.DataFrame, df_meta: pd.DataFrame, df_site: pd.DataFrame) -> bytes:
    '''Save three DataFrames to a single Excel file buffer: data, (column) metadata, and site data.

    Each of the DataFrames becomes a separate worksheet in the file, named Data, Columns, and Site.

    Parameters:
        df_data     Time sequence of multiple columns of sensor data
        df_meta     Column names and descriptions
        df_site     Site information

    Returns:
        The full Excel file contents (per specification of the dcc.send_bytes() convenience function)
    '''

    logger.debug('Enter.')
    buffer = io.BytesIO()

    # TODO: Should these worksheet names be data-driven (e.g., from a configuration file)?
    sheets: dict[str, pd.DataFrame] = {'Data': df_data, 'Columns': df_meta, 'Site': df_site}

    # Writing multiple worksheets is a little tricky and requires an ExcelWriter context manager.
    # Setting column widths is even trickier.
    with pd.ExcelWriter(buffer) as xl:
        for sheet, df in sheets.items():
            df.to_excel(xl, index=False, sheet_name=sheet)

            # Automatically adjust column widths to fit all text
            # NOTE: this may be an expensive operation. Beware of large files!
            for column in df.columns:
                column_width: int = max(df[column].astype(str).str.len().max(), len(column))
                col_idx     : int = df.columns.get_loc(column)
                xl.sheets[sheet].set_column(col_idx, col_idx, column_width)

    logger.debug('Excel file buffer written.')
    logger.debug('Exit.')
    return buffer.getvalue()            # Must return a byte string, not the IO buffer itself

def render_graphs(df_data: pd.DataFrame, showcols: list[str]) -> Any:
    '''For each of the selected columns, generate a Plotly graph.

    The graphs are stacked vertically and rendered from the top down in the order in which the columns appear in the list.

    Parameters:
        df_data  (DataFrame) The multicolumn time sequence of sensor data
        showcols (list)      Names of the selected columns

    Returns:
        A Plotly Figure containing all graphs
    '''

    logger.debug('Enter.')
    df_show: pd.DataFrame = df_data.set_index('TIMESTAMP')[showcols]            # TIMESTAMP is the independent (X-axis) variable for all plots
    
    # Using Plotly facet graphing convenience: multiple graphs in one figure (facet_row='variable' makes it that way).
    # This is very convenient, but makes it somewhat inflexible with respect to individual plot titles/annotations. For now, good enough.
    fig = (df_show.plot.line(facet_row='variable', height=120 + 200*len(showcols))           # Simplistic attempt at calculating the height depending on number of graphs
        .update_yaxes(matches=None, title_text='')                                     # Each graph has its own value range; don't show the axis title 'value'
        .update_xaxes(showticklabels=True)                                             # Repeat the time scale under each graph
        .for_each_annotation(lambda a: a.update(text=a.text.replace('variable=', ''))) # Just print the variable (column) name
        .update_layout(legend_title_text='Variable')
        )
    
    logger.debug('Plot generated.')
    logger.debug('Exit.')
    return fig

def ts_is_regular(timeseries: pd.Series, interval_minutes: int = 15) -> bool:
    '''Check if the time-sequence column is "regular", meaning monotonically increasing at a fixed interval.

    Parameters:
        timeseries          Sequence of Datetime values
        interval_minutes    Expected interval, in minutes, between consecutive values

    Returns:
        True if interval between all pairs of consecutive values equals interval_minutes; otherwise False
    '''

    logger.debug('Enter.')
    is_regular: bool = (timeseries[1:] - timeseries.shift()[1:] == pd.Timedelta(seconds=interval_minutes*60)).all()
    logger.debug('Exit.')

    return is_regular

def seqno_is_regular(seqno_series: pd.Series) -> bool:
    '''Check if the record sequence-number column is "regular", meaning monotonically increasing by one, with no gaps.

    Parameters:
        seqno_series    Sequence of integers

    Returns
        True if difference between all pairs of consecutive values is one; otherwise False
    '''

    logger.debug('Enter.')
    is_regular: bool = (seqno_series[1:] - seqno_series.shift()[1:].astype('int') == 1).all()
    logger.debug('Exit.')

    return is_regular
