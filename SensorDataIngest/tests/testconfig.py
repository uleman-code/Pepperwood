miniconfig = {
    'output': {
        'worksheet_names': {'data': 'Data', 'meta': 'Columns', 'station': 'Meta', 'notes': 'Notes'},
        'data_na_representation': '#N/A',
    },
    'metadata': {
        'timestamp_column': 'TIMESTAMP',
        'sequence_number_column': 'RECORD',
        'sampling_interval': '15min',
        'variable_description_columns': ['Name', 'Units', 'DataProcess'],
        'station_columns': ['DataFileFormat', 'StationName', 'DataLoggerModel', 'DataLoggerSerialNumber',
                            'DataLoggerOsVersion', 'DataLoggerProgramName', 'DataLoggerProgramSignature', 'TableName'],
        'notes_columns': ['Start of issue', 'End of issue', 'Sensor or Data Field', 'Data Omitted', 'Nature of problem'],
    },
}
