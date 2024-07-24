# -*- coding: utf-8 -*-
"""
@author: pancho
"""

import pandas as pd


def load_message_contents_from_csv(csv_file_name):
    colnames = ["Time", "Type", "OrderID", "Size", "Price", "TradeDirection", "Message"]
    # Load CSV file
    df_msg = pd.read_csv(csv_file_name, names=colnames, header=0)
    # Convert appropriate columns to numeric, errors='coerce' will convert non-numeric values to NaN
    df_msg[["Time", "Type", "OrderID", "Size", "Price", "TradeDirection"]] = df_msg[["Time", "Type", "OrderID", "Size", "Price", "TradeDirection"]].apply(pd.to_numeric, errors='coerce')
    return df_msg

def load_ob_contents_from_csv(ob_file_name, nlevels=10):
    names = ['Ask Price ', 'Ask Size ', 'Bid Price ', 'Bid Size ']

    colnames = []
    for i in range(1, nlevels + 1):
        for j in names:
            colnames.append(str(j) + str(i))

    # Load CSV file
    ob = pd.read_csv(ob_file_name, names=colnames, header=0)

    # Convert appropriate columns to numeric, errors='coerce' will convert non-numeric values to NaN
    ob = ob.apply(pd.to_numeric, errors='coerce')

    ob_ask_price = ob[ob.columns[range(0, len(ob.columns), 4)]]
    ob_ask_size = ob[ob.columns[range(1, len(ob.columns), 4)]]
    ob_bid_price = ob[ob.columns[range(2, len(ob.columns), 4)]]
    ob_bid_size = ob[ob.columns[range(3, len(ob.columns), 4)]]

    ob_dict = {'ob_ask_price': ob_ask_price, 'ob_ask_size': ob_ask_size, 'ob_bid_price': ob_bid_price, 'ob_bid_size': ob_bid_size}

    return ob, ob_dict

def load_msg_ob_from_csv(csv_file_name):
    df_msg = load_message_contents_from_csv(csv_file_name)
    ob_file_name = csv_file_name.replace('message', 'orderbook')
    df_ob, ob_dict = load_ob_contents_from_csv(ob_file_name)
    return df_msg, df_ob, ob_dict
