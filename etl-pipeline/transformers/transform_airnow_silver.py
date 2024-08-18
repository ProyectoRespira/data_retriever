import pandas as pd
import numpy as np

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

# Helpers
def drop_id_column(df):
    return df.drop(columns = 'id', axis = 1)

def format_date_utc(df):
    df['date_utc'] = pd.to_datetime(df['date_utc'])
    df['date_utc'] = df['date_utc'].dt.strftime('%Y-%m-%d %H:%M')
    df['date_utc'] = pd.to_datetime(df['date_utc'])
    return df

def combine_existing_and_new_readings(df1, df2):
    df_combined = pd.concat([df1, df2])
    df_combined['date_utc'] = pd.to_datetime(df_combined['date_utc'])

    return df_combined

def interpolate_missing_pm2_5_values(df):

    df.set_index('date_utc', inplace = True)
    df['pm2_5'] = df['pm2_5'].replace(-999, np.nan) 
    df_resampled = df.resample('h').asfreq()
    df_resampled['pm2_5'] = df_resampled['pm2_5'].interpolate(method='linear')
    
    df_filled = df_resampled.reset_index(names = 'date_utc')
    df_filled['station_id'] = df_filled['station_id'].ffill()
    df_filled['station_id'] = df_filled['station_id'].bfill()

    return df_filled
    
def set_variable_dtypes(df):
    df = df.astype({
    'date_utc': 'datetime64[ns]',  # Convert to datetime
    'pm2_5': 'float',              # Convert to float
    'station_id': 'int'            # Convert to integer
    })
    return df

def process_group(group):
    group.drop_duplicates(subset = ['date_utc'], keep='first', inplace=True)
    group = interpolate_missing_pm2_5_values(group)
    group = set_variable_dtypes(group)
    group.sort_values(by=['date_utc'], ascending=True, inplace=True)
    
    order = ['station_id', 'date_utc', 'pm2_5']
    group = group[order]

    return group

# def add_id_column(df):
#     df['id'] = range(1, len(df) + 1)
#     df['id'] = df['id'].astype(int)
#     return df 


@transformer
def transform(data, data_2, *args, **kwargs):

    # adapt bronze readings data types to silver
    data = format_date_utc(data)
    data = drop_id_column(data) 

    # combine existing silver and new bronze readings
    # into a single df for interpolation
    if not data_2.empty:
        
        df = combine_existing_and_new_readings(data, data_2)
    
    else:
        df = data.copy()
        
    processed_data = df.groupby('station_id').apply(process_group).reset_index(drop=True)

    if not data_2.empty:
        data_2_filtered = data_2[['station_id', 'date_utc']]
        processed_data = processed_data.merge(data_2_filtered, on=['station_id', 'date_utc'], how='left', indicator=True)
        processed_data = processed_data[processed_data['_merge'] == 'left_only'].drop(columns=['_merge'])
    
    return processed_data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'