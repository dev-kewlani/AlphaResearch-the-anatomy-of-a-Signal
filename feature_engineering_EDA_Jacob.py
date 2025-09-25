#%% imports
import numpy as np
import pandas as pd
import math
import scipy


#%% loading the parquet file into a dataframe

#read the parq file into a dataframe
df = pd.read_parquet('feature.parq')
df.shape
#print the first 100 rows of the df to see what we're working with
df.head(100)

# %% modifiying the dataframe

#insert a new column "returns"
#insert "shifted_returns" as well (shift returns by 1 so returns aren't 
# shown/used with future values that shouldn't be known yet)
if 'returns' not in df.columns:
    df.insert(1, 'returns', 0)
    df.insert(2, 'shifted_returns', 0)
    

#fill in new column "returns" (return = (price[n] - price[n-1]) / price[n-1])
df['returns'] = (df['mid_prc'].shift(-1) - df['mid_prc']) / df['mid_prc']
df['shifted_returns'] = df['returns'].shift(1)

#new column "log returns"
#another column "shifted_log_returns" for the same issue
if 'log_returns' not in df.columns:
    df.insert(3, 'log_returns', 0)
    df.insert(4, 'shifted_log_returns', 0)

df['log_returns'] = np.log(df['mid_prc'].shift(-1) / df['mid_prc'])
df['shifted_log_returns'] = df['log_returns'].shift(1)

df.head(100)
# %% New dataframes, taking correlations

#list of feature names
feature_cols = [col for col in df.columns if col not in 
                ['timestamp', 'mid_prc', 'returns', 'shifted_returns', 'log_returns', 'shifted_log_returns']]

#for each of the 85 feature columns, calculate spearman correlation coefficient with returns, and log returns
#creates 2 1D series, with feature names as indexes, coefficients as values
corr_returns = df[feature_cols].corrwith(df['shifted_returns'], method='spearman')
corr_log_returns = df[feature_cols].corrwith(df['shifted_log_returns'], method='spearman')

#create new dataframe containing 3 columns: features, and spearman correlations between returns and log returns
correlation_df = pd.DataFrame({'feature': feature_cols, 'corr_returns': corr_returns.values, 'corr_log_returns': corr_log_returns.values})

#sort the dataframe using absolute value, so that strong positive and negative values are both at the top!
#strong positives good for momentum indicators, strong negatives good for reversion indicators, small values are weak predictors in general
correlation_df = correlation_df.reindex(
    correlation_df['corr_returns'].abs().sort_values(ascending=False).index
)
pd.set_option('display.max_rows', 10)
correlation_df

# %% display results

import matplotlib.pyplot as plt

plt.figure(figsize=(10, 8))
top_20 = correlation_df.head(20)
plt.barh(range(len(top_20)), top_20['corr_returns'], color='green')
plt.yticks(range(len(top_20)), top_20['feature'], fontsize=8)
plt.xlabel('Spearman Correlation')
plt.title('Top 20 Feature Correlations with Returns')
plt.gca().invert_yaxis()  # Best at top
plt.tight_layout()
plt.show()

pd.set_option('display.max_rows', None)
#correlation_df.head(20)

#%% more correlations

corr_returns_pearson = df[feature_cols].corrwith(df['shifted_returns'], method='pearson')
correlation_df_pearson = pd.DataFrame({'feature': feature_cols, 'corr_returns_pearson': corr_returns_pearson.values})

correlation_df_pearson = correlation_df_pearson.reindex(
    correlation_df_pearson['corr_returns_pearson'].abs().sort_values(ascending=False).index
)

pd.set_option('display.max_rows', 10)
print(correlation_df_pearson)

corr_returns_kendall = df[feature_cols].corrwith(df['shifted_returns'], method='kendall')
correlation_df_kendall = pd.DataFrame({'feature': feature_cols, 'corr_returns_kendall': corr_returns_kendall.values})

correlation_df_kendall = correlation_df_kendall.reindex(
    correlation_df_kendall['corr_returns_kendall'].abs().sort_values(ascending=False).index
)

pd.set_option('display.max_rows', 10)
print(correlation_df_kendall)

# %% More Graphs

plt.figure(figsize=(10, 8))
top_20 = correlation_df_pearson.head(20)
plt.barh(range(len(top_20)), top_20['corr_returns_pearson'], color='blue')
plt.yticks(range(len(top_20)), top_20['feature'], fontsize=8)
plt.xlabel('Pearson Correlation')
plt.title('Top 20 Feature Correlations with Returns')
plt.xlim(0, 0.7)
plt.gca().invert_yaxis()  # Best at top
plt.tight_layout()
plt.show()

plt.figure(figsize=(10, 8))
top_20 = correlation_df_kendall.head(20)
plt.barh(range(len(top_20)), top_20['corr_returns_kendall'], color='orange')
plt.yticks(range(len(top_20)), top_20['feature'], fontsize=8)
plt.xlabel('Kendall Tau Correlation')
plt.title('Top 20 Feature Correlations with Returns')
plt.xlim(0, 0.7)
plt.gca().invert_yaxis()  # Best at top
plt.tight_layout()
plt.show()

