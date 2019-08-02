#!/usr/bin/env python
# coding: utf-8

# In[18]:



import pandas as pd
import numpy as np


def weightedgini(act, pred, weight):
    df = pd.DataFrame({"act": act, "pred": pred, "weight": weight})
    df = df.sort_values(by='pred', ascending=False)
    df["random"] = (df.weight / df.weight.sum()).cumsum()
    total_pos = (df.act * df.weight).sum()
    df["cumposfound"] = (df.act * df.weight).cumsum()
    df["lorentz"] = df.cumposfound / total_pos
    n = df.shape[0]
    #df["gini"] = (df.lorentz - df.random) * df.weight
    #return df.gini.sum()
    gini = sum(df.lorentz[1:].values * (df.random[:-1])) - sum(
        df.lorentz[:-1].values * (df.random[1:]))
    return gini


def normalizedweightedgini(act, pred, weight):
    return weightedgini(act, pred, weight) / weightedgini(act, act, weight)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




