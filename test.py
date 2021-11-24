import json
import sys
import time
import numpy as np
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
import seaborn as sns 
from matplotlib.ticker import FuncFormatter
import matplotlib.font_manager
from sklearn.ensemble import RandomForestRegressor

plt.rcParams['font.sans-serif']=['Arial Unicode MS']

import warnings
warnings.filterwarnings('ignore')


pd.set_option('display.max_columns', None)

PATH = "F:/Code/Python/Jupyter Project/House Prices/Data"
PATH_project = "D://Amazon metadata//meta_Gift_Cards.json"
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

df_gift_card = pd.DataFrame()

with open(PATH_project) as f:
    for i in f:
        dic = json.loads(i)
        df = pd.DataFrame(list(dic.items()))
        df = df.T
        df.columns = df.loc[0]
        df = df.drop(0)
        df_gift_card = df_gift_card.append(df, ignore_index = True)