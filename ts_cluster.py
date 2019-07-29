import pandas as pd
import numpy as np
from statsmodels.tsa.stattools import coint
from itertools import combinations
import datetime as dt
import statsmodels.tsa.stattools as ts
import statsmodels.api as sm
from sklearn.manifold import TSNE
from sklearn.cluster import KMeans, DBSCAN
from sklearn.decomposition import PCA
from statsmodels.tsa.vector_ar.vecm import coint_johansen
from sklearn import preprocessing
import matplotlib.pyplot as plt
from dtw import dtw

market_data = pd.read_csv('./data/Book3.csv', parse_dates=True, header=3)

prices_data = market_data.iloc[:, [3].__add__(list(np.arange(4, 24, 2)))]

prices_data.columns = [
    'time',
    'AAPL',
    'INTEL',
    'ORACLE',
    'JNJ',
    'PFE',
    'LLY',
    'HD',
    'MCD',
    'NKE',
    'GM']

prices_data.loc[:, 'time'] = pd.to_datetime(prices_data.loc[:, 'time'])
prices_data.fillna(method='ffill', inplace=True)

prices_data.set_index(['time'], drop=True, inplace=True)

prices_data_monthly = prices_data.resample('M').last()

pairs_internet = list(combinations(['AAPL', 'INTEL', 'ORACLE'], r=2))

pairs_healthcare = list(combinations(['JNJ', 'PFE', 'LLY'], r=2))

pairs_consumer = list(combinations(['HD', 'MCD', 'NKE', 'GM'], r=2))


def is_stationary(x, p=10):
    x = np.array(x)
    result = ts.adfuller(x, regression='ctt')
    # 1% level
    if p == 1:
        # if DFStat <= critical value
        if result[0] >= result[4]['1%']:  # DFstat is less negative
            # is stationary
            return True
        else:
            # is nonstationary
            return False
    # 5% level
    if p == 5:
        # if DFStat <= critical value
        if result[0] >= result[4]['5%']:  # DFstat is less negative
            # is stationary
            return True
        else:
            # is nonstationary
            return False
    # 10% level
    if p == 10:
        # if DFStat <= critical value
        if result[0] >= result[4]['10%']:  # DFstat is less negative
            # is stationary
            return True
        else:
            # is nonstationary
            return False

# Engle-Granger test for cointegration for array x and array y


def are_cointegrated(x, y):
    # check x is I(1) via Augmented Dickey Fuller
    x_is_I1 = not (is_stationary(x))
    # check y is I(1) via Augmented Dickey Fuller
    y_is_I1 = not (is_stationary(y))

    # if x and y are no stationary
    if x_is_I1 and y_is_I1:
        X = sm.add_constant(x)
        # regress x on y
        model = sm.OLS(np.array(y), np.array(X))
        results = model.fit()
        const = results.params[1]
        beta_1 = results.params[0]
        # solve for ut_hat
        u_hat = []
        for i in range(0, len(y)):
            u_hat.append(y[i] - x[i] * beta_1 - const)
            # check ut_hat is I(0) via Augmented Dickey Fuller
        u_hat_is_I0 = is_stationary(u_hat)
        # if ut_hat is I(0)
        if u_hat_is_I0:
            # x and y are cointegrated
            return True
        else:
            # x and y are not cointegrated
            return False
    # if x or y are nonstationary they are not cointegrated
    else:
        return False


for sector in [pairs_internet, pairs_healthcare, pairs_consumer]:
    for pair_list in list(map(list, pairs_consumer)):
        print(are_cointegrated(
            prices_data_monthly[pair_list[0]], prices_data_monthly[pair_list[1]]))

#  augmented Engle-Granger two-step cointegration test
for sector in [pairs_internet, pairs_healthcare, pairs_consumer]:

    for pair_list in list(map(list, sector)):
        # null hypothesis: no integration
        if ts.coint(prices_data_monthly[pair_list[0]],
                    prices_data_monthly[pair_list[1]])[1] > 0.1:
            print('Not integrated')
        else:
            print('Integrated')
            print('the pair is ', pair_list)

candidates_pairs = [['AAPL', 'INTEL'], ['JNJ', 'LLY']]

# -------------------------------------------DBSCAN-----------------------------------------------
#prices_data_monthly_stand = preprocessing.StandardScaler().fit_transform(prices_data_monthly).T
prices_data_monthly_stand = prices_data_monthly.copy().T

clf = DBSCAN(min_samples=2)
labels = clf.fit_predict(prices_data_monthly_stand)
n_clusters_ = len(set(labels)) - (1 if -1 in labels else 0)
print("\nTotal clusters discovered: %d" % n_clusters_)

clustered = clf.labels_
clustered_series = pd.Series(index=prices_data_monthly.columns, data=clustered.flatten())
which_cluster = clustered_series.loc['INTEL']
clustered_series[clustered_series == which_cluster]

# -------------------------------------------KMeans-----------------------------------------------
clf = KMeans(n_clusters=3, random_state=0)

labels = clf.fit_predict(prices_data_monthly_stand)
n_clusters_ = len(set(labels)) - (1 if -1 in labels else 0)
print("\nTotal clusters discovered: %d" % n_clusters_)

clustered = clf.labels_
clustered_series = pd.Series(index=prices_data_monthly.columns, data=clustered.flatten())
which_cluster = clustered_series.loc['JNJ']
clustered_series[clustered_series == which_cluster]

# -------------------------------------------DTW---------------------------------------------------

def euclidean_norm(x, y): return np.abs(x - y)
def l2_norm(x, y): return (x - y)**2


pairs_dtw = {}
for sector in [pairs_internet, pairs_healthcare, pairs_consumer]:
    for pair_list in sector:
        pairs_dtw[pair_list], cost_matrix, acc_cost_matrix, path = dtw(prices_data_monthly[pair_list[0]].values.reshape(
            -1, 1), prices_data_monthly[pair_list[1]].values.reshape(-1, 1), dist=l2_norm)

pairs_dtw_df = pd.DataFrame(pairs_dtw, columns=pairs_internet.__add__(
    pairs_healthcare).__add__(pairs_consumer), index=[0])
