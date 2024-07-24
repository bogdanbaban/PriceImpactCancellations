import warnings  # ignore future warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import pandas as pd
import numpy as np
import os
import re

from utils import utils_load, utils_msg_expand, utils_cancel_expand, utils_returns, utils_cfi_agg, utils_counts

def extract_cfi_returns(csv_file_name, path_to_save):
    print(f"Extracting data from {csv_file_name}")
    
    freq_samples = 60
    cfi_params = {'AGE_MINS': True,
                  'WHOLE_LEVEL': False,
                  'LEVEL_DIFFERENCE_SINCE_ARRIVAL': True,
                  'QUEUE': False,
                  'age_mins': 40 / 60,
                  'level_ratio': 1,
                  'level_difference': 3,
                  'queue': 2,
                  'WEIGHT': 'Level',
                  'NORMALISE': False,
                  'c': 0.5}
    
    try:
        # Read the CSV file
        print(f"Loading data from {csv_file_name}")
        df_msg, df_ob, ob_dict = utils_load.load_msg_ob_from_csv(csv_file_name)
        date = re.findall(r"([0-9]{4}-[0-9]{2}-[0-9]{2})", csv_file_name)[0]
        
        if date == '2019-01-09':
            print(f"Skipping date {date}")
            return
        
        path_to_save_day = os.path.join(path_to_save, date + '.csv')
        if os.path.exists(path_to_save_day):
            print(f"File {path_to_save_day} already exists. Skipping.")
            return

        utils_msg_expand.expand_df_msg(df_msg, ob_dict)
        df_cancels = utils_cancel_expand.expand_df_cancel(df_msg, QUEUE=False)
        df_cancels_filtered = utils_cfi_agg.filter_df_cancels(df_cancels, cfi_params)

        returns = utils_returns.get_rets_all_h(df_msg, freq_samples)
        cfi_agg = utils_cfi_agg.extract_cfi_agg_day(df_msg, df_ob, df_cancels_filtered, freq_samples)
        cfi = pd.DataFrame(cfi_agg, columns=['CFI_AG'])
        counts_volume = utils_counts.get_counts_volume(df_msg)

        df_to_save = pd.concat([cfi, returns, counts_volume], axis=1)
        df_to_save.to_csv(path_to_save_day)
        print(f"Saved processed data to {path_to_save_day}")
    
    except FileNotFoundError as e:
        print(f"Order book file not found: {e}")
    except Exception as e:
        print(f"Error processing file {csv_file_name}: {e}")

if __name__ == '__main__':
    input_file = 'AAPL_2019-01-04_24900000_57900000_message_10.csv'
    output_path = '4_Main_Results_normaliser/AAPL_2019/'

    # Create output directory if it doesn't exist
    try:
        os.makedirs(output_path, exist_ok=True)
        print("Directory", output_path, "created")
    except FileExistsError:
        print("Directory", output_path, "already exists")

    # Process the specific input file
    extract_cfi_returns(input_file, output_path)
