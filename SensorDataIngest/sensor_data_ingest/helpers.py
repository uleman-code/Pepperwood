"""Functions that implement logic independent of the environment: Dash (interactive) or command-line (batch)."""

import base64
import io
import logging

import decorator

from pathlib import Path
from functools import cache
from typing import Any, Callable

import pandas as pd
import plotly.graph_objects as go

from pandas.core.groupby.generic import SeriesGroupBy  # Just for type hinting

from . import config as cfg


class UnmatchedColumnsError(ValueError):
    pass

class UnsupportedFileType(ValueError):
    pass

class DuplicateTimestampError(ValueError):
    pass

class SiteIdNotFoundError(ValueError):
    pass

@decorator.decorator
def log_func(fn: Callable, *args, **kwargs) -> Any:
    """Function entry and exit logger, capturing exceptions as well.

    Very simplistic wrapper; no argument logging or execution timing.

    Parameters:
        fn      The function to be wrapped
        args    Positional arguments to be passed into the function
        kwargs  Keyword argments to be passed into the function

    Returns:
        The wrapped function's result
    """

    ee_logger: logging.Logger = logging.getLogger(f'{cfg.program_name}_ee.{__name__}')
    ee_logger.debug('>>> Enter.', extra={'fname': fn.__name__})

    try:
        out = fn(*args, **kwargs)
    except Exception as ex:
        ee_logger.debug(f'<<< Exception: {ex}', exc_info=True, extra={'fname': fn.__name__})
        raise

    ee_logger.debug('<<< Exit.', extra={'fname': fn.__name__})
    return out


# Configuration-derived variables for brevity
worksheet_names: dict[str, str] = cfg.config['output']['worksheet_names']
data_na_repr: str = cfg.config['output']['data_na_representation']
timestamp_column: str = cfg.config['metadata']['timestamp_column']
seqno_column: str = cfg.config['metadata']['sequence_number_column']
default_sampling_interval: str = cfg.config['metadata']['sampling_interval']        # To be overridden by the site metadata
qa_report_columns: list[str] = cfg.config['output']['notes_columns']
meta_columns: list[str] = cfg.config['metadata']['variable_description_columns']
station_columns: list[str] = cfg.config['metadata']['station_columns']
df_meta_sites: pd.DataFrame = cfg.metadata['sites']
df_meta_columns: pd.DataFrame = cfg.metadata['columns']
site_key_column: str = cfg.config['metadata']['site_key_column']

# Child logger inherits root logger settings
logger: logging.Logger = logging.getLogger(f'{cfg.program_name}.{__name__}')
pd.set_option('plotting.backend', 'plotly')


@log_func
def load_data(contents: str, filename: str) -> dict[str, pd.DataFrame]:
    """Load a data file (CSV, .dat or .csv suffix; or Excel) and return three DataFrames: data, metadata, and station data.

    Files from data loggers have additional information in the first few lines, including column names and descriptions and
    station information. Turn these into separate DataFrames for metadata and station data, respectively; skip them when loading
    the main data DataFrame.

    Excel files are expected to have been saved by this same app, in which case they have four worksheets, named
    Data, Columns, station, and Notes. Or, if saved by an earlier version, only three because there would be no Notes.

    Limitations:
        File type derived from filename suffix, not contents; this is fragile. In case the contents are not as expected,
            the error message is likely to be cryptic.
        No recovery in the case of an Excel file if meta- or station data are missing but valid data is there.
        No recovery if the case of a CSV file if the top rows are not as expected.
            (In both cases, the result is a file reading exception.)

    Parameters:
        contents    Base64-encoded string of the file contents
        filename    The name of the file; only needed to get the extension

    Returns:
        Four or three DataFrames: data, metadata, station data, and (unless from an early version) QA notes
    """

    logger.debug(f'Uploaded contents: {contents[:80]}')

    content_string: str
    _, content_string = contents.split(',')  # File contents are preceded by a file type string

    b64decoded: bytes = base64.b64decode(content_string)
    logger.debug('Got decoded file contents.')

    frames: dict[str, pd.DataFrame] = {}
    try:
        if Path(filename).suffix in ['.dat', '.csv']:
            # Assume that the user uploaded a raw-data CSV file
            logger.info('Reading CSV data file. Expect additional info in the first four rows.')
            decoded = io.StringIO(b64decoded.decode('utf-8'))

            # First pass to read the real data
            frames['data'] = pd.read_csv(
                decoded, skiprows=[0, 2, 3], parse_dates=[0], na_values='NAN'
            )

            # Second pass to read the column metadata
            decoded.seek(0)
            frames['meta'] = pd.read_csv(decoded, header=None, skiprows=[0], nrows=3).T
            frames['meta'].columns = meta_columns

            # Third pass to read the station data
            # NOTE: Limit the columns to avoid problems in case the raw-data file was edited in Excel
            decoded.seek(0)
            frames['station'] = pd.read_csv(
                decoded, header=None, nrows=1, usecols=list(range(len(station_columns)))
            )
            frames['station'].columns = station_columns

        elif Path(filename).suffix in ['.xlsx', '.xls']:
            # Assume that the user uploaded an Excel file
            logger.info('Reading Excel workbook. Expect three or four worksheets.')
            buffer = io.BytesIO(b64decoded)

            frames['data'] = pd.read_excel(buffer, sheet_name=worksheet_names['data'], na_values='NAN')
            frames['meta'] = pd.read_excel(buffer, sheet_name=worksheet_names['meta'])
            frames['station'] = pd.read_excel(buffer, sheet_name=worksheet_names['station'])

            try:
                frames['notes'] = pd.read_excel(buffer, sheet_name=worksheet_names['notes'])
                logger.info(
                    'Notes worksheet found. Unless in Append mode, will copy worksheet unaltered upon save.'
                )

                # Reconstitute any Links column upon write. This avoids having to keep track of whether the input file
                # is a .dat or .xlsx, whether notes were newly generated or appended, etc.
                if 'Link' in frames['notes'].columns:
                    frames['notes'].drop(columns='Link')
            except ValueError:
                logger.info(
                    'No Notes worksheet in this file. Will perform QA and write new worksheet upon save.'
                )
        else:
            # This should not happen: the Upload element limits the supported filename extensions.
            logger.error(f'Unsupported file type: {Path(filename).suffix}.')
            raise UnsupportedFileType(f'We do not support the **{Path(filename).suffix}** file type.')
    except (UnicodeDecodeError, ValueError) as e:
        logger.error(f'Error reading file {filename}. {e}')
        raise

    logger.debug('DataFrames for data, metadata, and station data populated.')
    return frames


@log_func
def merge_metadata(frames: dict[str, pd.DataFrame]) -> None:
    """Merge the limited metadata from the .DAT file with the static metadata read at startup; standardize field names.

    This also provides an opportunity for a few simple consistency checks. Any errors should not be
    treated as fatal: we can still ingest the data but metadata in the output will be limited.

    Args:
        frames      The four DataFrames (data, meta, station, notes) for one file

    Raises:
        SiteIdNotFoundError: The site ID in the station DataFrame was not found in the site or column metadata.
                             Output metadata will be limited to what the .DAT file provides.
    """

    site_id_dat = station_columns[1]                     # Just the way Campbell Scientific .DAT files are
    site_id_meta = df_meta_sites.columns[0]
    site_id = frames['station'].at[0, site_id_dat]                      # For logging purposes, the original site ID
    frames['station'][site_id_dat] = frames['station'][site_id_dat].str.replace(' ', '_').str.lower()    # Normalize the site ID
    # df_site = frames['station'].merge(df_meta_sites, left_on=site_id_dat, right_on=site_id_meta, how='inner')
    df_site = df_meta_sites.merge(frames['station'], left_on=site_id_meta, right_on=site_id_dat, how='inner')
    if df_site.empty:
        logger.info(f'Site ID {site_id} not found in static site metadata. Output will only have metadata from the file.')
        raise SiteIdNotFoundError(f'Site ID {site_id} not found in static site metadata.')
    
    # Show the original site ID from the .DAT file, not the normalized one. 
    frames['station'] = df_site.drop(columns=[site_key_column, site_id_dat]).assign(**{site_id_meta: site_id})
    logger.info(f'Site ID {site_id} found in static site metadata. Site metadata merged.')

    site_key = df_site.at[0, site_key_column]
    column_metadata = df_meta_columns[df_meta_columns[site_key_column] == site_key]
    if column_metadata.empty:
        logger.info(f'No static column metadata found for site ID {site_id}. Output will have limited column metadata.')
        raise SiteIdNotFoundError(f'No column metadata found for site key {site_key}.')
    
    # The "Units" column will come from the static metadata; get rid of the one from the .DAT file before merging,
    # to avoid name collision.
    df_columns = frames['meta'].drop(columns=meta_columns[1])
    df_columns = df_columns.merge(column_metadata, left_on=meta_columns[0], right_on='merge_key', how='left')

    # Replace the Name from the .DAT file with the standardized Field name from the static metadata, wherever available.
    df_columns[meta_columns[0]] = df_columns[meta_columns[0]].where(df_columns['Field'].isna(), df_columns['Field'])

    # Remove the Field name from the Aliases, because it is redundant. Turn the list back into a comma-separated string.
    # But first replace NaNs with empty lists.
    isna = df_columns['Aliases'].isna()
    df_columns.loc[isna, 'Aliases'] = pd.Series([[]] * isna.sum()).values
    df_columns['Aliases'] = df_columns.apply(lambda row: ','.join([x for x in row['Aliases'] if x != row['Field']]), axis='columns')
    df_columns.drop(columns=['merge_key', 'Field'], inplace=True)

    logger.info(f'Column metadata merged for site ID {site_id}.')
    num_missing_columns = isna.sum()
    if num_missing_columns:
        logger.info(f'Incomplete metadata: No static column metadata found for {num_missing_columns} column{"s" if num_missing_columns > 1 else ""}.')
    
    frames['meta'] = df_columns
    frames['data'].columns = df_columns[meta_columns[0]].to_list()  # Rename data columns to standard names
    return

@log_func
def get_sampling_interval(df_site: pd.DataFrame) -> str:
    """Determine the sampling interval for the site, either from the static metadata or from the data itself.

    Args:
        df_site     The combined site metadata (from .DAT file and static metadata)
    """

    # Find the sampling interval in the site metadata. If it's not there, use the default value from the
    # configuration settings. Be flexible about the column name, but it must contain the word "interval".
    interval_column = next((col for col in df_site.columns if 'interval' in col.lower()), None)
    sampling_minutes = df_site.at[0, interval_column] if interval_column else None
    return f'{sampling_minutes}min' if sampling_minutes else default_sampling_interval


@log_func
def multi_df_to_excel(frames: dict[str, pd.DataFrame]) -> bytes:
    """Save three DataFrames to a single Excel file buffer: data, (column) metadata, and station data.

    Each of the DataFrames becomes a separate worksheet in the file, named Data, Columns, and station.

    Parameters:
        frames      The four DataFrames (data, meta, station, notes) for one file

    Returns:
        The full Excel file contents (per specification of the dcc.send_bytes() convenience function)
    """

    # The worksheet names in the Excel file are not necessarily the same as the keys in frames.
    # So match each sheet name to the right frame. And keep them in the same order.
    sheets: dict[str, pd.DataFrame] = {worksheet_names[k]: frames[k] for k in frames}

    # Writing multiple worksheets is a little tricky and requires an ExcelWriter context manager.
    # Setting column widths is even trickier.
    buffer: io.BytesIO = io.BytesIO()
    with pd.ExcelWriter(buffer) as xl:
        for sheet, df in sheets.items():
            logger.debug(f'Writing {type(df).__name__} to sheet {sheet}.')

            # Worksheet "Notes": Add hyperlinks to the start timestamp in the "Data" worksheet
            if (sheet == worksheet_names['notes']):
                df.insert(
                    1,
                    'Link',
                    [
                        f'=HYPERLINK("#"&CELL("address",INDEX({worksheet_names["data"]}!$A:$A,MATCH($A{row},{worksheet_names["data"]}!$A:$A,0))),"   🔗")'
                        for row in range(2, len(df) + 2)
                    ],
                )

            df.to_excel(
                xl,
                index=False,
                sheet_name=sheet,
                na_rep=data_na_repr if sheet == worksheet_names['data'] else '',
            )

            # Automatically adjust column widths to fit all text, including the column header
            # NOTE: This may be an expensive operation. Beware of large files!
            # NOTE: This is only approximate; it's based on the number of characters in strings and numbers, not
            #       what it actually looks like in Excel on the user's system.
            for column_index, column in enumerate(df.columns):
                # The Link column contains nothing but single hyperlink characters, so only check the column header to set the width.
                header: pd.Series = pd.Series([column])
                to_check: pd.Series = (
                    header if column == 'Link' else pd.concat([header, df[column]])
                )
                column_width: int = to_check.astype(str).str.len().max()
                xl.sheets[sheet].set_column(column_index, column_index, column_width)

    logger.debug('Excel file buffer written.')
    return buffer.getvalue()  # Must return a byte string, not the IO buffer itself


@log_func
def render_graphs(
    df_data: pd.DataFrame, showcols: list[str], single_plot: bool = False
) -> go.Figure:
    """For each of the selected columns, generate a Plotly graph.

    The graphs are stacked vertically and rendered from the top down in the order in which the columns appear in the list.

    Parameters:
        df_data        The multicolumn time sequence of sensor data
        showcols       Names of the selected columns
        single_plot    If true, create a single multivariable plot; otherwise, multiple single-variable plots

    Returns:
        A Plotly Figure containing all graphs
    """

    df_show: pd.DataFrame = df_data.set_index(timestamp_column)[showcols]

    if single_plot:
        fig = (
            df_show.plot.line(height=720)
            .update_yaxes(title_text='')
            .update_layout(legend_title_text='Variable', title_text=', '.join(showcols))
        )
    else:
        # Using Plotly facet-plot graphing convenience: multiple graphs in one figure (facet_row='variable' makes it that way).
        # This is very convenient, but makes it somewhat inflexible with respect to individual plot titles/annotations. For now, good enough.
        fig = (
            # Use a simplistic attempt at calculating the height, depending on the number of graphs
            df_show.plot.line(facet_row='variable', height=120 + 200 * len(showcols))
            .update_yaxes(matches=None, title_text='')  # Don't show the axis title 'value'
            .update_xaxes(showticklabels=True)  # Repeat the time scale under each graph
            .for_each_annotation(lambda a: a.update(text=a.text.split('=')[-1]))  # Just print the variable name
            .update_layout(legend_title_text='Variable')
        )

    logger.debug('Plot generated.')
    return fig


@log_func
def report_duplicates(df: pd.DataFrame, sampling_interval: str) -> pd.DataFrame:
    """Construct a report listing each occurrence of duplicated rows or duplicate timestamps with otherwise distinct values.

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
        sampling_interval   The sampling interval for the current site/file (e.g., '15min')

    Returns:
        A DataFrame (possibly empty) with the rows representing the report

    Raises:
        DuplicateTimestampError, if an instance of case 3 is found.
    """

    # Get just the rows with repeated timestamps. Usually empty.
    # In what follows, ignore the sequence number column. It is not a sensor data variable and we don't care about it.
    df_repeat: pd.DataFrame = df[df[timestamp_column].duplicated(keep=False)].drop(
        columns=seqno_column
    )

    # Just the locations in the time series, for reporting purposes. Usually empty.
    ts_repeat: pd.Series = df_repeat[timestamp_column].drop_duplicates()

    # If they are just repeated whole samples, nunique for each occurrence should be one. But if there are differences in the
    # variable values, then it'll be greater than one. Empty if no duplicates.
    nunique: pd.Series = (
        df_repeat.groupby(timestamp_column, as_index=True).nunique().max(axis='columns')
    )

    if (len(nunique) and max(nunique) > 1):
        logger.info('Duplicate timestamps found.')
        raise DuplicateTimestampError(
            f'Repeated timestamp found at {", ".join(list((ts_repeat.astype(str))))}. Do not save to Excel.'
        )
    else:
        if len(ts_repeat):
            logger.info('Duplicate samples found.')
        grouper: pd.Series = (
            ts_repeat.diff()  # This compares each index label with its predecessor
            .bfill()  # The first one has no predecessor so becomes NaN; copy the first diff value (this actually works)
            .ne(sampling_interval)  # Only the ones after a gap become True/1
            .cumsum()  # This increments after each gap
        )
        grouped: SeriesGroupBy = ts_repeat.groupby(grouper)

        num_removed: int = len(df_repeat) - len(ts_repeat)
        return pd.DataFrame(
            dict(
                zip(
                    qa_report_columns,
                    [
                        grouped.first(),
                        grouped.last(),
                        'All',
                        'No',
                        f'Repeated samples; {num_removed} duplicate{"s" if num_removed > 1 else ""} removed.',
                    ],
                )
            ),
            columns=qa_report_columns,
        )


@log_func
def report_missing_column_values(
    df: pd.DataFrame, column: str, qa_range: pd.Series | slice
) -> pd.DataFrame:
    """Construct a missing-value report for each occurrence or range of missing values in a given column.

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

    Returns:
        A DataFrame (possibly empty) with the rows representing the report
    """

    nan_mask: pd.Series[bool]

    # In Append mode, limit the part of the time series analyzed.
    # But the mask that drives the analysis must have the same length as the DataFrame, initialized to False.
    if isinstance(qa_range, pd.Series):
        nan_mask = pd.Series(False, index=df.index)
        nan_mask[qa_range] = df.loc[qa_range, column].isna()
    else:
        nan_mask = df[column].isna()

    grouper: pd.Series = (
        nan_mask[nan_mask]  # Only keep the rows with NaNs
        .index.to_series()  # We only need the indices  # But we need them as a Series (otherwise diff() does not apply)
        .diff()  # This compares each index label with its predecessor
        .bfill()  # The first one has no predecessor so becomes NaN; copy the first diff value (this actually works)
        .ne(1)  # Only the ones after a gap become True/1
        .cumsum()  # This increments after each gap
    )
    grouped: SeriesGroupBy = df.loc[nan_mask, timestamp_column].groupby(grouper)
    report: pd.DataFrame = pd.DataFrame(
        dict(zip(qa_report_columns, [grouped.first(), grouped.last(), column, 'Yes', 'Unknown'])),
        columns=qa_report_columns,
    )

    if not report.empty:
        logger.info(f'Missing values found in column {column}.')

    return report


@log_func
def fill_missing_rows(df: pd.DataFrame, sampling_interval: str) -> pd.DataFrame:
    """Complete a regular time series by inserting NaN-valued records wherever there are gaps.

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
        sampling_interval   The sampling interval for the current site/file (e.g., '15min')

    Returns:
        The input DataFrame, with zero or more new rows inserted
    """

    df_fixed: pd.DataFrame = (
        df.set_index(timestamp_column, drop=True)
        .drop(columns=seqno_column)  # Will reconstruct later
        .asfreq(freq=sampling_interval)
        .reset_index()  # Bring the timestamp back as a column
        .reset_index(names=seqno_column)
        .loc[:, df.columns]  # Restore the original column order
    )

    return df_fixed


@log_func
def report_missing_samples(old_dt_index: pd.DatetimeIndex, new_dt_index: pd.DatetimeIndex,
                           sampling_interval:str) -> pd.DataFrame:
    """Given the datetime index before and after insertion of missing rows, report what was found and changed.

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
        old_dt_index        The datetime index of the time series before restoration (with gaps)
        new_dt_index        The datetime index of the time series after restoration (gaps filled in)
        sampling_interval   The sampling interval for the current site/file (e.g., '15min')

    Returns:
        A DataFrame (possibly empty) with the rows representing the report
    """

    inserted_timestamps: pd.Series = new_dt_index.difference(old_dt_index).to_series(name='')
    grouped: SeriesGroupBy = inserted_timestamps.groupby(
        inserted_timestamps.diff()  # Find each timestamp's increment from its predecessor
        .bfill()  # Fill in the first one (which does not have a predecessor)
        .ne(pd.Timedelta(sampling_interval))  # Mark if it's bigger than expected (so, a gap)
        .cumsum()  # This increments after each gap
    )

    first: pd.Series = grouped.first()
    last: pd.Series = grouped.last()

    def make_text(n: int) -> str:
        return f'Unknown; {n} NA-filled record{"s" if n > 1 else ""} inserted and {seqno_column} renumbered.'

    report: pd.DataFrame = pd.DataFrame(
        dict(
            zip(
                qa_report_columns,
                [
                    first,
                    last,
                    'All',
                    'Yes',
                    (((last - first) / pd.Timedelta(sampling_interval)).astype(int) + 1).apply(
                        make_text
                    ),
                ],
            )
        )
    )

    if not report.empty:
        logger.info('Missing samples found.')

    return report


@log_func
def run_qa(
    df_data: pd.DataFrame, df_site: pd.DataFrame, df_notes: pd.DataFrame | None, qa_range: list[str] | None
) -> tuple[bool, bool, bool, pd.DataFrame, pd.DataFrame]:
    """Run data integrity checks, make any corrections possible, and report results for both data notes in the output and interactive display.

    1. Test for duplicates, both repeated whole samples and samples with repeated timestamps but distinct variable values.

    2. Test for two kinds of data dropout:
        - Missing values in individual columns/variables
        - Missing samples/rows, constituding a gap in the regular time series

       In the case of missing samples, restore the regular time series by inserting rows with the expected timestamps and sequence numbers
       --the latter will be renumbered--, and with NaNs for all the other columns.

    3. In the test for duplicates, the sequence number column (seqno_column) is ignored. This column is fairly meaningless
       and gets reassigned at the end, erasing any effects of concatenation, duplicate removal, and gap filling.

    4. In Append mode, analyzing the combined dataset could potentially find the same issues found previously, leading to redundant report entries.
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
        In Append mode, it is theoretically possible (but probably rare) that a run of missing variable values, missing samples, or
        duplicates extends from the end of previously saved data through the beginning of the current set. This function would report on
        each part separately and make no attempt to consolidate the two parts into a single run.

    Parameters:
        df_data     Input/Output DataFrame: any restoration (inserted rows) is applied in place
        df_site     Site metadata, used to get the sampling interval
        df_notes    If not None, the notes previously generated from the new data set (in Append mode)
        qa_range    If not None, the range of timestamps [start, end] to be sanity-checked (in Append mode)

    Returns:
        Duplicate samples found?
        Missing values found?
        Missing samples found?
        A DataFrame (possibly empty) with the rows representing the report
        The original data DataFrame, possibly with duplicates dropped or gaps filled
    """

    logger.debug('Enter')

    sampling_interval: str = get_sampling_interval(df_site)

    try:
        duplicates_report: pd.DataFrame = report_duplicates(df_data, sampling_interval)
        duplicates_found: bool = bool(len(duplicates_report))
        df_fixed: pd.DataFrame = (
            df_data.drop_duplicates(subset=df_data.columns.drop(seqno_column), ignore_index=True)
            if duplicates_found
            else df_data
        )
        if duplicates_found:
            logger.info(
                f'Duplicates found. Original size: {len(df_data)}. Deduplicated size: {len(df_fixed)}.'
            )
    except DuplicateTimestampError as err:
        # Duplicate timestamps with distinct variable values. Just pass the exception on to the caller.
        logger.info(err)
        raise

    variable_columns: pd.Index[str] = df_fixed.columns.drop([timestamp_column, seqno_column])
    s_qa_range: pd.Series[bool] | slice = (
        df_fixed[timestamp_column].between(qa_range[0], qa_range[1]) if qa_range else slice(None)
    )
    missing_value_columns: pd.Series = df_fixed.loc[s_qa_range, variable_columns].isna().sum()
    missing_values_found: bool = bool(missing_value_columns.sum())

    missing_values_report: pd.DataFrame
    if missing_values_found:
        # Note that this is the one test that needs to be limited to data not previously analyzed, to avoid double reporting.
        missing_values_report = pd.concat(
            [
                report_missing_column_values(df_fixed, col, s_qa_range)
                for col in variable_columns[missing_value_columns.astype(bool)]
            ]
        )
    else:
        logger.info('No missing values found. Moving on to look for missing samples.')
        missing_values_report = pd.DataFrame([], columns=qa_report_columns)

    original_index: pd.DatetimeIndex = pd.DatetimeIndex(df_fixed[timestamp_column])
    df_fixed = fill_missing_rows(df_fixed, sampling_interval)
    new_index: pd.DatetimeIndex = pd.DatetimeIndex(df_fixed[timestamp_column])
    missing_samples_report: pd.DataFrame = report_missing_samples(original_index, new_index, sampling_interval)
    missing_samples_found: bool = bool(len(missing_samples_report))

    report = pd.concat(
        [df_notes, duplicates_report, missing_values_report, missing_samples_report]
    ).sort_values(['Start of issue', 'End of issue'])

    return duplicates_found, missing_values_found, missing_samples_found, report, df_fixed


@log_func
def append(
    base_frames: dict[str, pd.DataFrame], new_frames: dict[str, pd.DataFrame]
) -> tuple[dict[str, pd.DataFrame], list[str]]:
    """Append new sensor data to an existing set.

    The data already in memory is considered the new file, with the newly loaded data the base file to which the new file is
    to be appended.

    Concatenate the sensor data; use the metadata and station data from the new file, and keep the notes from the base file. New
    QA sanity checks will be performed on any part of the combined data that has not been QA-ed before. Determine the time
    range of the samples that still need to be QA-ed.

    Limitations:
        This assumes that the new file is newer than the base file, meaning that the timestamps in the base file are older
        and the new file's timestamps neatly follow after those of the base. This function handles exceptions to this
        assumption by sorting the time series after concatenation and by adjusting the way the QA range is determined.
        But for the meta- and station data, the new file's versions always replace the base file's. This could be fixed by
        comparing file creation/modification times, but this may not be reliable due, among other things, to the possibility
        of the base file (an Excel file) having been edited.

    Parameters:
        base_frames         The three or four DataFrames (data, meta, station, possibly notes) from the newly loaded base file
        new_frames          The three or four DataFrames (data, meta, station, possibly notes) from the previously loaded new file

    Returns:
        The four DataFrames for the combined files
        The time range, as a tuple (start, end), of the combined time series that still needs to be QA-ed

    Raises:
        UnmatchedColumnsError   if the lists of variables (columns other than timestamp and sequence number)
                                do not match between the two files
    """

    # Sanity check: yes, the schema may evolve, but the two files must have at least some columns in common.
    # This should catch the most egregious mistakes, trying to combine files from completely different sets of sensors,
    # while not causing too many false rejections.
    df_base: pd.DataFrame = base_frames['data']
    df_new: pd.DataFrame = new_frames['data']
    base_columns: pd.Index = df_base.columns
    new_columns: pd.Index = df_new.columns
    if (
        base_columns.drop([timestamp_column, seqno_column])
        .intersection(new_columns.drop([timestamp_column, seqno_column]))
        .empty
    ):
        raise UnmatchedColumnsError(
            'The two files are not compatible because their column lists are completely different. '
            + 'They are probably not from the same source.'
        )

    # Be sure to keep the ordering of the frames dict constant by assigning the frames in order: data, meta, station, notes.
    # This determines the order in which the corresponding worksheets appear in the Excel file.
    combined_frames: dict[str, pd.DataFrame] = {
        'data': pd.DataFrame([]),
        'meta': pd.DataFrame([]),
        'station': pd.DataFrame([]),
        'notes': pd.DataFrame([]),
    }

    # Concatenate the actual data; put the time series in order (making it independent of whether the "new" file is actually
    # newer or not); reset the index (leave renumbering the sequence number column till after sanity checks).
    # Pandas concat already deals with any changes to the column list mostly in the way you'd expect:
    #   - Columns of the same name are concatenated regardless of the order in which they appear in the list.
    #       - Their position in the list is the same as in the new file; this allows any reshuffling to take hold and not be undone.
    #   - In case of a change in column order, the first-mentioned frame prevails--in this case, the new file.
    #   - A new column (not present in the base file) appears in the right position in the list.
    #   - A dropped column (not present in the new file) ends up at the end of the list; this needs to be corrected separately.
    #   - A renamed column looks like a combination of a dropped column and a new column; this can be consolidated later, once
    #     we have a mechanism for loading metadata that includes column aliases.
    combined_frames['data'] = (
        pd.concat([df_new, df_base]).sort_values(timestamp_column).reset_index(drop=True)
    )

    # Fix up the case of dropped column(s). See the fourth item in the previous comment.
    dropped_columns: pd.Index = base_columns.difference(new_columns)
    added_columns: pd.Index = new_columns.difference(base_columns)
    combined_columns: list = combined_frames['data'].columns.to_list()
    for col in dropped_columns:
        idx: int = base_columns.get_loc(col) + 1

        # Skip to the next column that was kept in the new file
        while (idx < len(base_columns) and base_columns[idx] not in new_columns):
            idx += 1

        # If no kept columns found after this one, it and any dropped columns after it can just stay at the end of the list.
        if idx >= len(base_columns):
            continue

        # Move the dropped column to just before the next kept column
        combined_columns.insert(
            combined_columns.index(base_columns[idx]),
            combined_columns.pop(combined_columns.index(col)),
        )

    combined_frames['data'].columns = combined_columns

    # Use the newest info (from the new file to be appended) for meta- and station data. In case of schema or content
    # evolution, this keeps each output file up to date with the standards at the time of saving.
    # TODO: Fix up how this could mess up the Columns worksheet, losing info for dropped columns. But these worksheets will change anyway.
    for frame in ['meta', 'station']:
        combined_frames[frame] = new_frames[frame]

    if not base_frames['meta'].equals(new_frames['meta']):
        logger.warning('The metadata worksheets do not match. Keep the newer one.')

    if not base_frames['station'].equals(new_frames['station']):
        logger.warning('The station worksheets do not match. Keep the newer one.')

    # Normally, we expect the most recently loaded file (the base) to be the older one, to which the data loaded earlier (the new)
    # will be appended.
    # But we can handle the reverse situation. The only criterion for which file is older is simple: whichever one has the older
    # of the starting timestamps. Any other relationships between the two time series (end timestamp vs end or starting timestamp)
    # get handled by how we set the QA range.
    older: pd.DataFrame = (
        df_base if df_base[timestamp_column].iloc[0] <= df_new[timestamp_column].iloc[0] else df_new
    )
    newer: pd.DataFrame = (
        df_new if df_base[timestamp_column].iloc[0] <= df_new[timestamp_column].iloc[0] else df_base
    )

    if df_base[timestamp_column].iloc[0] > df_new[timestamp_column].iloc[0]:
        logger.warning(
            'Data in the "new" file is older than that in the base file. '
            + 'If the column order has changed, the result adopts that of the "new" (but older) file. '
            + 'If any columns have been added or dropped, the corresponding entries in Notes may point to the wrong timestamps.'
        )

    # Updating the frame store triggers a new QA process. Limit this to at most the data from the existing file,
    # up to and including the first sample in the new data. If the existing has already been QA-ed, as indicated
    # by the presence of a Notes worksheet, only look at the transition from existing to new data, to detect
    # a possible gap or overlap.
    # Normally, newer_first = older_last + sampling_interval.
    #   newer_first > older_last  + sampling_interval ==> gap
    #   newer_first <= older_last                     ==> overlap
    older_last: pd.Timestamp = older[timestamp_column].iloc[-1]
    newer_first: pd.Timestamp = newer[timestamp_column].iloc[0]

    # qa_range is [start, end]
    qa_range: list[str] = ['', str(max(older_last, newer_first))]  # Fill in start later

    # If there's a Notes worksheet in the base file (the one to be appended to), copy it over and append
    # the new file's notes (generated in the current run).
    # If not, just copy over the new notes.
    if 'notes' in base_frames:
        combined_frames['notes'] = pd.concat([base_frames['notes'], new_frames['notes']])
        qa_range[0] = str(min(older_last, newer_first))
    else:
        combined_frames['notes'] = new_frames['notes']
        qa_range[0] = str(df_base[timestamp_column].min())

    # If there are any schema changes, record the transitions.
    # For now, don't worry about newer and older, overlaps and gaps; assume that the "new" file is indeed new
    # and fits perfectly tip to tail.
    combined_frames['notes'] = pd.concat(
        [
            combined_frames['notes'],
            pd.DataFrame(
                [
                    [newer_first, newer_first, col, 'No', 'New variable name introduced.']
                    for col in added_columns
                ],
                index=qa_report_columns,
            ),
            pd.DataFrame(
                [
                    [newer_first, newer_first, col, 'No', 'Variable name dropped.']
                    for col in dropped_columns
                ],
                index=qa_report_columns,
            ),
        ]
    )

    return combined_frames, qa_range
