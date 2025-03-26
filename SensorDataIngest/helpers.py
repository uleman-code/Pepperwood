'''Functions that implement logic independent of the environment: Dash (interactive) or command-line (batch).'''

import base64
import io
import logging
import typing

from   pathlib import Path
from   typing  import Any
import pandas  as     pd

from pandas.core.groupby.generic import SeriesGroupBy       # Just for type hinting

# The following are subject to revision. They may be used by more than one function.

# TODO: Should these worksheet names be data-driven (e.g., from a configuration file)?
worksheet_names   = 'Data Columns Meta Notes'.split()
qa_report_columns = ['Start of issue', 'End of issue', 'Sensor or Data Field', 'Data Omitted', 'Nature of problem']

logger = logging.getLogger(f'Ingest.{__name__.capitalize()}')        # Child logger inherits root logger settings
pd.set_option('plotting.backend', 'plotly')

def load_data(contents: str, filename: str) -> dict[str, pd.DataFrame]: 
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
    meta_columns: list[typing.LiteralString] = 'Name Alias Sample/Average'.split()
    site_columns: list[typing.LiteralString] = 'Unknown01 SiteId DataLoggerModel Unknown02 DataLoggerOsVersion Unknown03 Unknnown04 SamplingInterval'.split()
    b64decoded:   bytes     = base64.b64decode(content_string)
    logger.debug('Got decoded file contents.')

    frames: dict[str, pd.DataFrame] = {}
    try:
        if Path(filename).suffix in ['.dat', '.csv']:
            # Assume that the user uploaded a raw-data CSV file
            logger.info('Reading CSV data file. Expect additional info in the first four rows.')
            decoded = io.StringIO(b64decoded.decode('utf-8'))

            # First pass to read the real data
            frames['data'] = pd.read_csv(decoded, skiprows=[0,2,3], parse_dates=['TIMESTAMP'], na_values='NAN')
            
            # Second pass to read the column metadata
            decoded.seek(0)
            frames['meta']         = pd.read_csv(decoded, header=None, skiprows=[0], nrows=3).T
            frames['meta'].columns = meta_columns
            
            # Third pass to read the site data
            # NOTE: Limit the columns to avoid problems in case the raw-data file was edited in Excel
            decoded.seek(0)
            frames['site']         = pd.read_csv(decoded, header=None, nrows=1, usecols=list(range(len(site_columns))))
            frames['site'].columns = site_columns
            
        elif Path(filename).suffix in ['.xlsx', '.xls']:
            # Assume that the user uploaded an Excel file
            logger.info('Reading Excel workbook. Expect three or four worksheets.')
            buffer = io.BytesIO(b64decoded)

            frames['data']  = pd.read_excel(buffer, sheet_name=worksheet_names[0], na_values='NAN')
            frames['meta']  = pd.read_excel(buffer, sheet_name=worksheet_names[1])
            frames['site']  = pd.read_excel(buffer, sheet_name=worksheet_names[2])

            try:
                frames['notes'] = pd.read_excel(buffer, sheet_name=worksheet_names[3])
                logger.info('Notes worksheet found. Will not perform QA; will copy worksheets unaltered upon save.')
            except ValueError as e:                 # Worksheet named 'Notes' not found
                logger.info('No Notes worksheet in this file. Will perform QA and write new worksheet upon save.')
        else:
            # This should not happen: the Upload element limits the supported filename extensions.
            logger.error(f'Unsupported file type: {Path(filename).suffix}.')
            raise ValueError(f'We do not support the **{Path(filename).suffix}** file type.')
    except Exception as e:
        logger.error(f'Error reading file {filename}. {e}')
        raise

    logger.debug('DataFrames for data, metadata, and site data populated.')
    logger.debug('Exit.')
    return frames

def multi_df_to_excel(frames: dict[str, pd.DataFrame], na_rep: str = '#N/A') -> bytes:
    '''Save three DataFrames to a single Excel file buffer: data, (column) metadata, and site data.

    Each of the DataFrames becomes a separate worksheet in the file, named Data, Columns, and Site.

    Parameters:
        frames      The four DataFrames (data, meta, site, notes) for one file
        na_rep      Representation of NaNs in the Excel file; empty string becomes a blank cell.
                    Only applies to the "data" worksheet.

    Returns:
        The full Excel file contents (per specification of the dcc.send_bytes() convenience function)
    '''

    logger.debug('Enter.')
    buffer: io.BytesIO                               = io.BytesIO()
    sheets: dict[typing.LiteralString, pd.DataFrame] = dict(zip(worksheet_names, frames.values()))

    # Writing multiple worksheets is a little tricky and requires an ExcelWriter context manager.
    # Setting column widths is even trickier.
    with pd.ExcelWriter(buffer) as xl:
        for sheet, df in sheets.items():
            logger.debug(f'Writing {type(df)} to sheet {sheet}.')
            df.to_excel(xl, index=False, sheet_name=sheet, na_rep=na_rep if sheet == 'Data' else '')

            # Automatically adjust column widths to fit all text, including the column header
            # NOTE: this may be an expensive operation. Beware of large files!
            for column in df.columns:
                column_width: int = pd.concat([df[column], pd.Series([column])]).astype(str).str.len().max()
                column_index: int = df.columns.get_loc(column)  # type: ignore
                xl.sheets[sheet].set_column(column_index, column_index, column_width)

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
    fig = (df_show.plot.line(facet_row='variable', height=120 + 200*len(showcols))        # Simplistic attempt at calculating the height depending on number of graphs
           .update_yaxes(matches=None, title_text='')   # type: ignore                    # Each graph has its own value range; don't show the axis title 'value'
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

def report_missing_column_values(df: pd.DataFrame, column: str, timestamp_column: str = 'TIMESTAMP') -> pd.DataFrame:
    '''Construct a missing-value report for each occurrence or range of missing values in a given column.
    
    The report is intended to be included in the "Data Notes" worksheet of the output Excel file.

    In the input raw-data file, a dropout in an individual variable (column), presumably caused by a problem with a
    specific sensor, is represented by some placeholder, like the string "NAN", which in the DataFrame becomes a NaN.
    (Entire missing samples/rows due to a data logger problem are handled elsewhere.)

    The report is a DataFrame with zero or more rows and the following columns:
    - The first and last timestamps of the run of missing data (equal in the case of a singleton missing value);
    - The column name;
    - Whether or not it involves missing data (always "yes");
    - A description of the cause of the data problem (always "unknown", since we cannot guess from the data itself).

    Parameters:
        df                  Input DataFrame
        column              The column to be examined
        timestamp_column    (optional) Normally "TIMESTAMP" but may be overridden

    Returns:
        A DataFrame (possibly empty) with the rows representing the report
    '''

    logger.debug('Enter.')
    nan_mask: pd.Series = df[column].isna()

    grouper: pd.Series = (nan_mask[nan_mask]      # Only keep the rows with NaNs
                          .index                  # We only need the indices
                          .to_series()            # But we need them as a Series (otherwise diff() does not apply)
                          .diff()                 # This compares each index label with its predecessor
                          .bfill()                # The first one has no predecessor so becomes NaN; copy the first diff value (this actually works)
                          .ne(1)                  # Only the ones after a gap become True/1
                          .cumsum()               # This increments after each gap
                         )
    grouped: SeriesGroupBy = (df
                              .loc[nan_mask, timestamp_column]
                              .astype(str)
                              .groupby(grouper)
                             )
    report: pd.DataFrame = pd.DataFrame(dict(zip(qa_report_columns, [grouped.first(), grouped.last(), column, 'Yes', 'Unknown'])),
                                        columns=qa_report_columns)
    
    if not report.empty:
        logger.info(f'Missing values found in column {column}.')

    logger.debug('Exit.')
    return report

def fill_missing_rows(df: pd.DataFrame, timestamp_column: str = 'TIMESTAMP',
                      seqno_column: str = 'RECORD', interval: str | pd.DateOffset = '15min') -> pd.DataFrame:
    '''Complete a regular time series by inserting NaN-valued records wherever there are gaps.
    
    A dropout in the time series means that there simply is no record for the expected timestamp. There may be a single
    missing row/timestamp somewhere, or an entire missing period (sequence of timestamps). Either way, insert as
    many new rows as needed to fill the gap, with all column values (except the sequence-number column and, of course,
    the timestamp column) set to NaN. These will become empty cells in the output Excel file.
    
    Renumber the sequence-number column, starting at zero. This eliminates any gaps or NaNs in this column.

    Assumptions:
        Input data represents a regular timeseries, with a constant, predictable time interval between samples, and the
        timestamps show no subtle drift or change. For example, if they fall generally on exactly 0, 15, 30, or 45
        minutes after the hour, they are never at 15-and-a-few-seconds, 30-minus-a-few-seconds, or some such variation.
    
    Parameters:
        df      Input DataFrame
        timestamp_column    Name of the column containing the datetime values constituting the time series index
        seqno_column        Name of the column containing row sequence numbers
        interval            The time series sampling period. Must be a valid datetime offset or string representation thereof.

    Returns:
        The input DataFrame, with zero or more new rows inserted
    '''

    logger.debug('Enter.')
    df_fixed: pd.DataFrame = (df
                              .set_index(timestamp_column, drop=True)
                              .drop(columns=seqno_column)               # Will reconstruct later
                              .asfreq(freq=interval)
                              .reset_index()                            # Bring TIMESTAMP back as column
                              .reset_index()                            # Copy row sequence numbers into column ...
                              .rename(columns={'index': seqno_column})  # ... and call it by its original name.
                              .loc[:, df.columns]                       # Restore the original column order
                             ) # type: ignore

    logger.debug('Exit.')
    return df_fixed

def report_missing_samples(old_dt_index: pd.DatetimeIndex, new_dt_index: pd.DatetimeIndex,
                           seqno_column: str = 'RECORD', interval: str | pd.DateOffset = '15min') -> pd.DataFrame:
    '''Given the datetime index before and after insertion of missing rows, report what was found and changed.
    
    The report is intended to be included in the "Data Notes" worksheet of the output Excel file.

    In the input raw-data file, a missing sample/row, presumably caused by a problem with a data logger,
    causes a break in the regular sequence of timestamps. This sequence 
    (Entire missing samples/rows due to a data logger problem are handled elsewhere.)

    The report is a DataFrame with zero or more rows and the following columns:
    - The first and last timestamps of the run of missing data (equal in the case of a singleton missing value);
    - The column name (always "all");
    - Whether or not it involves missing data (always "yes");
    - A description of the cause of the data problem (always "unknown", since we cannot guess from the data itself).

    Parameters:
        old_dt_index    The datetime index of the time series before restoration (with gaps)
        new_dt_index    The datetime index of the time series after restoration (gaps filled in)
        seqno_column    The name of the column containing row sequence numbers

    Returns:
        A DataFrame (possibly empty) with the rows representing the report
    '''

    logger.debug('Enter.')
    inserted_timestamps = new_dt_index.difference(old_dt_index).to_series(name='')
    grouped             = (inserted_timestamps
                           .astype(str)                                               # May be inefficient: for a range, we only care about first and last
                           .groupby(inserted_timestamps
                                    .diff()                                           # Find each timestamp's increment from its predecessor
                                    .bfill()                                          # Fill in the first one (which does not have a predecessor)
                                    .ne(pd.Timedelta(interval))  # type: ignore       # Mark (True) if it's bigger than expected (that is, a gap)
                                    .cumsum()                                         # This increments after each gap
                                   )
                          )

    report: pd.DataFrame = pd.DataFrame(dict(zip(qa_report_columns,
                                                 [grouped.first(), grouped.last(), 'All', 'Yes',
                                                  f'Unknown; NA-filled records inserted and {seqno_column} renumbered.'])))

    if not report.empty:
        logger.info('Missing samples found.')

    logger.debug('Exit.')
    return report

def run_qa(df: pd.DataFrame, timestamp_column: str = 'TIMESTAMP', seqno_column: str = 'RECORD',
           interval: str | pd.DateOffset = '15min') -> tuple[bool, bool, pd.DataFrame, pd.DataFrame]:
    '''Run data integrity checks, make any corrections possible, and report results for both data notes in the output and interactive display.

    Test for two kinds of data dropout:
        - Missing values in individual columns/variables
        - Missing samples/rows, constituding a gap in the regular time series
    
    In the case of missing samples, restore the regular time series by inserting rows with the expected timestamps and sequence numbers
    --the latter will be renumbered--, and with NaNs for all the other columns. 

    The report is a DataFrame with zero or more rows and the following columns:
    - The first and last timestamps of the run of missing data (equal in the case of a singleton missing value);
    - The column name in the case of missing values, "all" if missing samples;
    - Whether or not it involves missing data (always "yes");
    - A description of the cause of the data problem (always "unknown", since we cannot guess from the data itself, with, in case of missing
      samples, an indication that rows were inserted).

    Parameters:
        df                  Input/Output DataFrame: any restoration (inserted rows) is applied in place
        timestamp_column    Name of the column containing the datetime values constituting the time series index
        seqno_column        Name of the column containing row sequence numbers
        interval            The time series sampling period. Must be a valid datetime offset or string representation thereof.

    Returns:
        Missing values found?
        Missing samples found?
        A DataFrame (possibly empty) with the rows representing the report
     '''

    logger.debug('Enter')
    missing_values_report: pd.DataFrame  = pd.concat([report_missing_column_values(df, col, timestamp_column) for col in df.columns])
    missing_values_found:  bool          = bool(len(missing_values_report))

    original_index:         pd.DatetimeIndex = df.set_index(timestamp_column).index # type: ignore
    df_fixed:               pd.DataFrame     = fill_missing_rows(df, timestamp_column, seqno_column, interval)
    new_index:              pd.DatetimeIndex = df_fixed.set_index(timestamp_column).index # type: ignore
    missing_samples_report: pd.DataFrame     = report_missing_samples(original_index, new_index, seqno_column, interval)
    missing_samples_found:  bool             = bool(len(missing_samples_report))

    report = pd.concat([missing_values_report, missing_samples_report])

    logger.debug('Exit.')
    return missing_values_found, missing_samples_found, report, df_fixed