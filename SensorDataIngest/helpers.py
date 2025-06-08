'''Functions that implement logic independent of the environment: Dash (interactive) or command-line (batch).'''

import base64
import io
import logging
import typing

from   pathlib import Path
from   typing  import Any
import pandas  as     pd

from pandas.core.groupby.generic import SeriesGroupBy, DataFrameGroupBy       # Just for type hinting

class UnmatchedColumnsError(ValueError):
    pass

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

    Excel files are expected to have been saved by this same app, in which case they have four worksheets, named
    Data, Columns, Site, and Notes. Or, if saved by an earlier version, only three because there would be no Notes.

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
        Four or three DataFrames: data, metadata, site data, and (unless from an early version) QA notes
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
                logger.info('Notes worksheet found. Unless in Append mode, will copy worksheets unaltered upon save.')
            except ValueError as e:                 # Worksheet named 'Notes' not found
                logger.info('No Notes worksheet in this file. Will perform QA and write new worksheet upon save.')
        else:
            # This should not happen: the Upload element limits the supported filename extensions.
            logger.error(f'Unsupported file type: {Path(filename).suffix}.')
            raise ValueError(f'We do not support the **{Path(filename).suffix}** file type.')
    except (UnicodeDecodeError, ValueError) as e:
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
            logger.debug(f'Writing {type(df).__name__} to sheet {sheet}.')
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

# def ts_is_regular(timeseries: pd.Series, interval_minutes: int = 15) -> bool:
#     '''Check if the time-sequence column is "regular", meaning monotonically increasing at a fixed interval.

#     Parameters:
#         timeseries          Sequence of Datetime values
#         interval_minutes    Expected interval, in minutes, between consecutive values

#     Returns:
#         True if interval between all pairs of consecutive values equals interval_minutes; otherwise False
#     '''

#     logger.debug('Enter.')
#     is_regular: bool = (timeseries[1:] - timeseries.shift()[1:] == pd.Timedelta(seconds=interval_minutes*60)).all()
#     logger.debug('Exit.')

#     return is_regular

# def seqno_is_regular(seqno_series: pd.Series) -> bool:
#     '''Check if the record sequence-number column is "regular", meaning monotonically increasing by one, with no gaps.

#     Parameters:
#         seqno_series    Sequence of integers

#     Returns
#         True if difference between all pairs of consecutive values is one; otherwise False
#     '''

#     logger.debug('Enter.')
#     is_regular: bool = (seqno_series[1:] - seqno_series.shift()[1:].astype('int') == 1).all()
#     logger.debug('Exit.')

#     return is_regular

def report_duplicates(df: pd.DataFrame, timestamp_column: str = 'TIMESTAMP', seqno_column: str = 'RECORD') -> pd.DataFrame:
    '''Construct a report listing each occurrence of duplicated rows or duplicate timestamps with otherwise distinct values.

    There are three distinct cases:
        1. No duplicates: do nothing. Return an empty report. This is the normal, expected case.
        2. Duplicate samples (rows): all values in the duplicates are identical, so the extras can be dropped without
           loss. Remove the duplicates and record it in the report.
        3. Duplicate timestamps, but some or all of the variable values are distinct. As it is not clear what to do about
           this, raise an exception so the calling function can take action to prevent saving to Excel and report this
           in the UI. It will require manual intervention to deal with the problem.
    
    Assumptions:
        - The input DataFrame is sorted by timestamp, so any duplicates occur together, as repeated rows/timestamps.
        - Duplicate occurrences are somewhat rare. While it's easy enough to keep track of fully duplicated samples and
          where they occur (case 2), no attempt is made to report any further duplicates if any instance of case 3 is found.
        - Actual duplicate removal in the sensor data occurs elsewhere.
    
    Parameters:
        df                  Input DataFrame
        timestamp_column    Name of the column containing the datetime values constituting the time series index
        seqno_column        Name of the column containing row sequence numbers

    Returns:
        A DataFrame (possibly empty) with the rows representing the report

    Raises:
        ValueError, if an instance of case 3 is found.
    '''

    logger.debug('Enter.')

    # Get just the rows with repeated timestamps. Usually empty.
    # In what follows, ignore the sequence number column. It is not a sensor data variable and we don't care about it.
    df_repeat: pd.DataFrame = df[df[timestamp_column].duplicated(keep=False)].drop(columns=seqno_column)

    # Just the locations in the time series, for reporting purposes. Usually empty.
    ts_repeat: pd.Series = df_repeat[timestamp_column].drop_duplicates()

    # If they are just repeated whole samples, nunique for each occurrence should be one. But if there are differences in the
    # variable values, then it'll be greater than one. Empty if no duplicates.
    nunique: pd.Series = df_repeat.groupby(timestamp_column, as_index=True).nunique().max(axis='columns')

    if len(nunique) and max(nunique) > 1:           # max() doesn't deal with an empty argument, so test for content
        logger.info('Duplicate timestamps found.')
        logger.debug('Exit.')
        raise ValueError(f'Repeated timestamp found at {", ".join(list((ts_repeat.astype(str))))}. Do not save to Excel.')
    else:
        if len(ts_repeat):
            logger.info('Duplicate samples found.')
        logger.debug('Exit.')
        return pd.DataFrame(dict(zip(qa_report_columns,
                                     [ts_repeat, ts_repeat, 'All', 'No', 
                                      f'Repeated samples; {len(df_repeat) - len(ts_repeat)} duplicate(s) removed.'])),
                            columns=qa_report_columns)

def report_missing_column_values(df: pd.DataFrame, column: str, qa_range: pd.Series | slice, timestamp_column: str = 'TIMESTAMP') -> pd.DataFrame:
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
        qa_range            If not slice(None), a boolean Series indicating the range of timestamps to be sanity-checked (in Append mode)
        timestamp_column    (optional) Normally "TIMESTAMP" but may be overridden

    Returns:
        A DataFrame (possibly empty) with the rows representing the report
    '''

    logger.debug('Enter.')
    
    nan_mask: pd.Series[bool]

    # In Append mode, limit the part of the time series analyzed.
    # But the mask that drives the analysis must have the same length as the DataFrame, initialized to False.
    if isinstance(qa_range, pd.Series):
        nan_mask                  = pd.Series(False, index=df.index)
        nan_mask[qa_range]        = df.loc[qa_range, column].isna()
    else:
        nan_mask = df[column].isna()

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
                              .groupby(grouper)
                             )
    report: pd.DataFrame = pd.DataFrame(dict(zip(qa_report_columns, [grouped.first(), grouped.last(), column, 'Yes', 'Unknown'])),
                                        columns=qa_report_columns)
    
    if not report.empty:
        logger.info(f'Missing values found in column {column}.')

    logger.debug('Exit.')
    return report

def fill_missing_rows(df: pd.DataFrame, timestamp_column: str = 'TIMESTAMP',
                      seqno_column: str = 'RECORD', interval: str = '15min') -> pd.DataFrame:
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
        df                  Input DataFrame
        timestamp_column    Name of the column containing the datetime values constituting the time series index
        seqno_column        Name of the column containing row sequence numbers
        interval            The time series sampling period. Must be a valid frequency string.

    Returns:
        The input DataFrame, with zero or more new rows inserted
    '''

    logger.debug('Enter.')
    df_fixed: pd.DataFrame = (df
                              .set_index(timestamp_column, drop=True)
                              .drop(columns=seqno_column)               # Will reconstruct later
                              .asfreq(freq=interval)
                              .reset_index()                            # Bring TIMESTAMP back as column
                              .reset_index(names=seqno_column)          # Copy row sequence numbers into the sequence number column
                              .loc[:, df.columns]                       # Restore the original column order
                             ) # type: ignore

    logger.debug('Exit.')
    return df_fixed

def report_missing_samples(old_dt_index: pd.DatetimeIndex, new_dt_index: pd.DatetimeIndex,
                           seqno_column: str = 'RECORD', interval: str = '15min') -> pd.DataFrame:
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
        interval        The period (time between samples) of the regular time series

    Returns:
        A DataFrame (possibly empty) with the rows representing the report
    '''

    logger.debug('Enter.')
    inserted_timestamps: pd.Series     = new_dt_index.difference(old_dt_index).to_series(name='')
    grouped:             SeriesGroupBy = (inserted_timestamps
                                          .groupby(inserted_timestamps
                                                   .diff()                                     # Find each timestamp's increment from its predecessor
                                                   .bfill()                                    # Fill in the first one (which does not have a predecessor)
                                                   .ne(pd.Timedelta(interval)) # type: ignore  # Mark (True) if it's bigger than expected (that is, a gap)
                                                   .cumsum()                                   # This increments after each gap
                                                  )
                                         )

    first = grouped.first()
    last  = grouped.last()

    make_text = lambda x: f'Unknown; {x} NA-filled record{"s" if x > 1 else ""} inserted and {seqno_column} renumbered.'
    report: pd.DataFrame = pd.DataFrame(dict(zip(qa_report_columns, [first, last, 'All', 'Yes',
            (((last - first)/pd.Timedelta(interval)).astype(int) + 1).apply(make_text)])))      # type: ignore

    if not report.empty:
        logger.info('Missing samples found.')

    logger.debug('Exit.')
    return report

def run_qa(df_data: pd.DataFrame, df_notes: pd.DataFrame | None, qa_range: list[str] | None,
           timestamp_column: str = 'TIMESTAMP', seqno_column: str = 'RECORD', interval: str = '15min'
          ) -> tuple[bool, bool, bool, pd.DataFrame, pd.DataFrame]:
    '''Run data integrity checks, make any corrections possible, and report results for both data notes in the output and interactive display.

    1. Test for duplicates, both repeated whole samples and samples with repeated timestamps but distinct variable values.

    2. Test for two kinds of data dropout:
        - Missing values in individual columns/variables
        - Missing samples/rows, constituding a gap in the regular time series
    
       In the case of missing samples, restore the regular time series by inserting rows with the expected timestamps and sequence numbers
       --the latter will be renumbered--, and with NaNs for all the other columns. 

    3. In the test for duplicates, the sequential-numbering column (seqno_column, usually "RECORD") is ignored. This column is fairly meaningless
       and gets reassigned at the end, erasing any effects of concatenation, duplicate removal, and gap filling.

    3. In Append mode, analyzing the combined dataset could potentially find the same issues found previously, leading to redundant report entries.
       However, since missing samples and duplicate whole samples result in corrected data, and duplicate timestamps prevent saving and appending
       altogether, the only test for which it is important to limit the analysis to only the QA range indicated is the one for missing values
       in individual variables.

    The report is a DataFrame with zero or more rows and the following columns:
    - The timestamps of any duplicates;
    - The first and last timestamps of the run of missing data (equal in the case of a singleton missing value);
    - The column name in the case of missing values, "all" if duplicate or missing samples;
    - Whether or not it involves missing data ("no" for duplicates, otherwise "yes");
    - A description of the cause of the data problem (always "unknown", since we cannot guess from the data itself, with, in case of duplicate
      or missing samples, an indication that rows were dropped/inserted).

    Limitations:
        In Append mode, it is theoretically possible (but probably rare) that a run of missing variable values, missing values, or
        duplicates extends from the end of previously saved data through the beginning of the current set. This function would report on
        each part separately and make no attempt to consolidate the two parts into a single run.

    Parameters:
        df_data             Input/Output DataFrame: any restoration (inserted rows) is applied in place
        notes               If not None, the notes previously generated from the new data set (in Append mode)
        qa_range            If not None, the range of timestamps [start, end] to be sanity-checked (in Append mode)
        timestamp_column    Name of the column containing the datetime values constituting the time series index
        seqno_column        Name of the column containing row sequence numbers
        interval            The time series sampling period. Must be a valid frequency string.

    Returns:
        Duplicate samples found?
        Missing values found?
        Missing samples found?
        A DataFrame (possibly empty) with the rows representing the report
        The original data DataFrame, possibly with duplicates dropped or gaps filled
     '''

    logger.debug('Enter')

    try:
        duplicates_report: pd.DataFrame = report_duplicates(df_data, timestamp_column)
        duplicates_found:  bool         = bool(len(duplicates_report))
        df_fixed:          pd.DataFrame = df_data.drop_duplicates(subset=df_data.columns.drop(seqno_column), ignore_index=True) if duplicates_found else df_data
        if duplicates_found:
            logger.info(f'Duplicates found. Original size: {len(df_data)}. Deduplicated size: {len(df_fixed)}.')
    except ValueError as err:
        # Duplicate timestamps with distinct variable values. Just pass the exception on to the caller.
        logger.info(err)
        logger.debug('Exit.')
        raise

    variable_columns:      pd.Index[str] = df_fixed.columns.drop([timestamp_column, seqno_column])
    s_qa_range:            pd.Series[bool] | slice = df_fixed[timestamp_column].between(qa_range[0], qa_range[1]) if qa_range else slice(None)
    missing_value_columns: pd.Series     = df_fixed.loc[s_qa_range, variable_columns].isna().sum()        # type: ignore
    missing_values_found:  bool          = bool(missing_value_columns.sum())

    missing_values_report: pd.DataFrame
    if not missing_values_found:
        logger.info('No missing values found. Moving on to look for missing samples.')
        missing_values_report = pd.DataFrame([])
    else:
        # Note that this is the one test that needs to be limited to data not previously analyzed, to avoid double reporting.
        missing_values_report = pd.concat([report_missing_column_values(df_fixed, col, s_qa_range, timestamp_column)
                                           for col in variable_columns[missing_value_columns.astype(bool)]])

    original_index:         pd.DatetimeIndex = pd.DatetimeIndex(df_fixed[timestamp_column])           # type: ignore
    df_fixed                                 = fill_missing_rows(df_fixed, timestamp_column, seqno_column, interval)
    new_index:              pd.DatetimeIndex = pd.DatetimeIndex(df_fixed[timestamp_column])           # type: ignore
    missing_samples_report: pd.DataFrame     = report_missing_samples(original_index, new_index, seqno_column, interval)
    missing_samples_found:  bool             = bool(len(missing_samples_report))

    report = pd.concat([df_notes, duplicates_report, missing_values_report, missing_samples_report]).sort_values(['Start of issue', 'End of issue'])

    logger.debug('Exit.')
    return duplicates_found, missing_values_found, missing_samples_found, report, df_fixed

def append(base_frames: dict[str, pd.DataFrame], new_frames: dict[str, pd.DataFrame], timestamp_column: str = 'TIMESTAMP',
           seqno_column: str = 'RECORD') -> tuple[dict[str, pd.DataFrame], list[str]]:
    '''Append new sensor data to an existing set.
    
    The data already in memory is considered the new file, with the newly loaded data the base file to which the new file is
    to be appended.

    Concatenate the sensor data; use the metadata and site data from the new file, and keep the notes from the base file. New
    QA sanity checks will be performed on any part of the combined data that has not been QA-ed before. Determine the time
    range of the samples that still need to be QA-ed.

    Limitations:
        This assumes that the new file is newer than the base file, meaning that the timestamps in the base file are older
        and the new file's timestamps neatly follow after those of the base. This function handles exceptions to this
        assumption by sorting the time series after concatenation and by adjusting the way the QA range is determined.
        But for the meta- and site data, the new file's versions always replace the base file's. This could be fixed by
        comparing file creation/modification times, but this may not be reliable due, among other things, to the possibility
        of the base file (an Excel file) having been edited.

    Parameters:
        base_frames         The three or four DataFrames (data, meta, site, possibly notes) from the newly loaded base file
        new_frames          The three or four DataFrames (data, meta, site, possibly notes) from the previously loaded new file
        timestamp_column    Name of the column containing the datetime values constituting the time series index
        seqno_column        Name of the column containing row sequence numbers
    
    Returns:
        The four DataFrames for the combined files
        The time range, as a tuple (start, end), of the combined time series that still needs to be QA-ed

    Raises:
        UnmatchedColumnsError   if the lists of variables (columns other than timestamp and sequence number)
                                do not match between the two files
    '''

    logger.debug('Enter.')

    if not base_frames['data'].columns.drop(seqno_column).equals(new_frames['data'].columns.drop(seqno_column)):
        raise UnmatchedColumnsError('The two files are not compatible because their column lists do not match.')
    
    # Be sure to keep the ordering of the frames dict the same by assigning the frames in order: data, meta, site, notes.
    # Concatenate the actual data; put the time series in order (just in case the files were loaded in the "wrong" order);
    # reset the index (leave renumbering the sequence number column till after sanity checks).
    combined_frames: dict[str, pd.DataFrame] = {}
    combined_frames['data']                  = (pd.concat([base_frames['data'], new_frames['data']])
                                                .sort_values(timestamp_column)
                                                .reset_index(drop=True)
                                               )

    # Use the newest info (from the new file to be appended) for meta- and site data. In case of schema or content
    # evolution, this keeps each output file up to date with the standards at the time of saving.
    for frame in ['meta', 'site']:
        combined_frames[frame] = new_frames[frame]

    # Normally, we expect the most recently loaded file to be the older one, to which the data loaded earlier will be appended.
    older: pd.DataFrame = base_frames['data']
    newer: pd.DataFrame = new_frames['data']

    # In case the assumption above does not hold, simply reverse the designation. The only criterion for which file is older
    # is simple: whichever one has the older of the starting timestamps. Any other relationships between the two time series
    # (end timestamp vs end or starting timestamp) get handled by how we set the QA range.
    if older[timestamp_column].iloc[0] > newer[timestamp_column].iloc[0]:
        logger.warning('The new file has older timestamps than the base file. Reversing the order of concatenation.')
        older = newer
        newer = base_frames['data']

    # Updating the frame store triggers a new QA process. Limit this to at most the data from the existing file,
    # up to and including the first sample in the new data. If the existing has already been QA-ed, as indicated
    # by the presence of a Notes worksheet, only look at the transition from existing to new data, to detect
    # a possible gap or overlap.
    # Normally, newer_first = older_last + interval.
    #   newer_first > older_last  + interval ==> gap
    #   newer_first <= older_last            ==> overlap
    older_last:  pd.Timestamp = older[timestamp_column].iloc[-1]
    newer_first: pd.Timestamp = newer[timestamp_column].iloc[0]

    # qa_range is [start, end]
    qa_range: list[str] = ['', str(max(older_last, newer_first))]    # Fill in start later

    # If there's a Notes worksheet in the base file (the one to be appended to), copy it over and append
    # the new file's notes (generated in the current run).
    # If not, just copy over the new notes.
    if 'notes' in base_frames:
        combined_frames['notes'] = pd.concat([base_frames['notes'], new_frames['notes']])
        qa_range[0]              = str(min(older_last, newer_first))
    else:
        combined_frames['notes'] = new_frames['notes']
        qa_range[0]              = str(base_frames['data'][timestamp_column].min())

    if not base_frames['meta'].equals(new_frames['meta']):
        logger.warning('The metadata worksheets do not match. Keep the newer one.')
    
    if not base_frames['site'].equals(new_frames['site']):
        logger.warning('The site worksheets do not match. Keep the newer one.')
    
    logger.debug('Exit.')
    return combined_frames, qa_range