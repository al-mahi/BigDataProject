#!/usr/bin python
from __future__ import print_function
import datetime as dt
import pandas as pd
from pandas import Series
from pprint import pprint
import matplotlib.pyplot as plt

if __name__ == '__main__':
    df = pd.DataFrame(pd.read_pickle("prediction_panda_df.pck"))
    print(list(df))
    pred = df[["time_stamp_ms", "prediction"]].sort_values(by="time_stamp_ms")
    pred['timestamp'] = pd.to_datetime(pred['time_stamp_ms'], unit='ms')
    pred = pd.DataFrame(pred[['timestamp', 'prediction']])
    pred['date'] = pd.to_datetime(pred['timestamp'].apply(lambda x: str(x)[:19]))
    pred = pd.DataFrame(pred[['date', 'prediction']])
    ser = Series(data=pred['prediction'].ravel(), index=pred['date'])
    # ser = Series(ser.groupby(lambda x: str(x)[:13]).aggregate(lambda x: sum(x)))
    ser = ser.resample('D').sum()
    ser.plot()
    # plt.annotate()
    print(ser)
    plt.show()
