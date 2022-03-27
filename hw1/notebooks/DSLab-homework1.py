# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.13.7
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # DSLab Homework 1 - Data Science with CO2
#
# ## Hand-in Instructions
#
# - __Due: 22.03.2022 23h59 CET__
# - `git push` your final verion to the master branch of your group's Renku repository before the due
# - check if `Dockerfile`, `environment.yml` and `requirements.txt` are properly written
# - add necessary comments and discussion to make your codes readable

# %% [markdown]
# ## Carbosense
#
# The project Carbosense establishes a uniquely dense CO2 sensor network across Switzerland to provide near-real time information on man-made emissions and CO2 uptake by the biosphere. The main goal of the project is to improve the understanding of the small-scale CO2 fluxes in Switzerland and concurrently to contribute to a better top-down quantification of the Swiss CO2 emissions. The Carbosense network has a spatial focus on the City of Zurich where more than 50 sensors are deployed. Network operations started in July 2017.
#
# <img src="http://carbosense.wdfiles.com/local--files/main:project/CarboSense_MAP_20191113_LowRes.jpg" width="500">
#
# <img src="http://carbosense.wdfiles.com/local--files/main:sensors/LP8_ZLMT_3.JPG" width="156">  <img src="http://carbosense.wdfiles.com/local--files/main:sensors/LP8_sensor_SMALL.jpg" width="300">

# %% [markdown]
# ## Description of the homework
#
# In this homework, we will curate a set of **CO2 measurements**, measured from cheap but inaccurate sensors, that have been deployed in the city of Zurich from the Carbosense project. The goal of the exercise is twofold: 
#
# 1. Learn how to deal with real world sensor timeseries data, and organize them efficiently using python dataframes.
#
# 2. Apply data science tools to model the measurements, and use the learned model to process them (e.g., detect drifts in the sensor measurements). 
#
# The sensor network consists of 46 sites, located in different parts of the city. Each site contains three different sensors measuring (a) **CO2 concentration**, (b) **temperature**, and (c) **humidity**. Beside these measurements, we have the following additional information that can be used to process the measurements: 
#
# 1. The **altitude** at which the CO2 sensor is located, and the GPS coordinates (latitude, longitude).
#
# 2. A clustering of the city of Zurich in 17 different city **zones** and the zone in which the sensor belongs to. Some characteristic zones are industrial area, residential area, forest, glacier, lake, etc.
#
# ## Prior knowledge
#
# The average value of the CO2 in a city is approximately 400 ppm. However, the exact measurement in each site depends on parameters such as the temperature, the humidity, the altitude, and the level of traffic around the site. For example, sensors positioned in high altitude (mountains, forests), are expected to have a much lower and uniform level of CO2 than sensors that are positioned in a business area with much higher traffic activity. Moreover, we know that there is a strong dependence of the CO2 measurements, on temperature and humidity.
#
# Given this knowledge, you are asked to define an algorithm that curates the data, by detecting and removing potential drifts. **The algorithm should be based on the fact that sensors in similar conditions are expected to have similar measurements.** 
#
# ## To start with
#
# The following csv files in the `../data/carbosense-raw/` folder will be needed: 
#
# 1. `CO2_sensor_measurements.csv`
#     
#    __Description__: It contains the CO2 measurements `CO2`, the name of the site `LocationName`, a unique sensor identifier `SensorUnit_ID`, and the time instance in which the measurement was taken `timestamp`.
#     
# 2. `temperature_humidity.csv`
#
#    __Description__: It contains the temperature and the humidity measurements for each sensor identifier, at each timestamp `Timestamp`. For each `SensorUnit_ID`, the temperature and the humidity can be found in the corresponding columns of the dataframe `{SensorUnit_ID}.temperature`, `{SensorUnit_ID}.humidity`.
#     
# 3. `sensor_metadata_updated.csv`
#
#    __Description__: It contains the name of the site `LocationName`, the zone index `zone`, the altitude in meters `altitude`, the longitude `LON`, and the latitude `LAT`. 
#
# Import the following python packages:

# %%
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go

# %%
pd.options.mode.chained_assignment = None

# %% [markdown]
# ## PART I: Handling time series with pandas (10 points)

# %% [markdown]
# ### a) **8/10**
#
# Merge the `CO2_sensor_measurements.csv`, `temperature_humidity.csv`, and `sensors_metadata_updated.csv`, into a single dataframe. 
#
# * The merged dataframe contains:
#     - index: the time instance `timestamp` of the measurements
#     - columns: the location of the site `LocationName`, the sensor ID `SensorUnit_ID`, the CO2 measurement `CO2`, the `temperature`, the `humidity`, the `zone`, the `altitude`, the longitude `lon` and the latitude `lat`.
#
# | timestamp | LocationName | SensorUnit_ID | CO2 | temperature | humidity | zone | altitude | lon | lat |
# |:---------:|:------------:|:-------------:|:---:|:-----------:|:--------:|:----:|:--------:|:---:|:---:|
# |    ...    |      ...     |      ...      | ... |     ...     |    ...   |  ... |    ...   | ... | ... |
#
#
#
# * For each measurement (CO2, humidity, temperature), __take the average over an interval of 30 min__. 
#
# * If there are missing measurements, __interpolate them linearly__ from measurements that are close by in time.
#
# __Hints__: The following methods could be useful
#
# 1. ```python 
# pandas.DataFrame.resample()
# ``` 
# https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.resample.html
#     
# 2. ```python
# pandas.DataFrame.interpolate()
# ```
# https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.interpolate.html
#     
# 3. ```python
# pandas.DataFrame.mean()
# ```
# https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.mean.html
#     
# 4. ```python
# pandas.DataFrame.append()
# ```
# https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.append.html

# %%
co = pd.read_csv('../data/carbosense-raw/CO2_sensor_measurements.csv', sep='\t')
temp = pd.read_csv('../data/carbosense-raw/temperature_humidity.csv', sep='\t', parse_dates=True)
sensor = pd.read_csv('../data/carbosense-raw/sensors_metadata_updated.csv', index_col=0)

# %%
temp = pd.melt(temp, id_vars='Timestamp')  #print this if you don't know the output
temp

# %%
temp['Timestamp'] = temp['Timestamp'].apply(
    lambda x: pd.to_datetime(x))  # turn the format to datetime
temp_t = temp[temp['variable'].str.contains("temperature")].rename(
    columns={
        'variable': 'SensorUnit_ID',
        'value': 'temperature',
        'Timestamp': 'timestamp'
    })
temp_t['SensorUnit_ID'] = temp_t['SensorUnit_ID'].apply(
    lambda x: str(x[:4]))  # turn the format to str to prevent be meaned
temp_h = temp[temp['variable'].str.contains("humidity")].rename(
    columns={
        'variable': 'SensorUnit_ID',
        'value': 'humidity',
        'Timestamp': 'timestamp'
    })
temp_h['SensorUnit_ID'] = temp_h['SensorUnit_ID'].apply(
    lambda x: str(x[:4])
)  #split the data  to temperature and humidity then merge after processed
t = temp_h.merge(temp_t, how='inner', on=['timestamp', 'SensorUnit_ID'])

# %% [markdown]
# Notice that for here, the default behavior for pandas  resample().mean() method, [skipna is True] so that it will ignore nan when calculating the mean value, so we resample first then we do the interpolate. This will reduce the number of times we interfere with the data and get nan free data. If you have other opinions, feel free to modify this part. If you have no column timestamp error or only work on timestamp error, delete or add the set_index before groupby or interpolate.|

# %%
t = t.set_index('timestamp', drop=True).groupby(['SensorUnit_ID']).resample('30min').mean().reset_index()
t = t.set_index('timestamp', drop=True).interpolate(method='time')
t

# %%
co['timestamp'] = co['timestamp'].apply(lambda x: pd.to_datetime(x))  # turn the format to datetime
co['SensorUnit_ID'] = co['SensorUnit_ID'].apply(lambda x: str(x))  # turn the format to str to prevent be meaned
co = co.set_index('timestamp', drop=True).groupby(['LocationName', 'SensorUnit_ID']).resample('30min').mean().reset_index()
co = co.set_index('timestamp', drop=True).interpolate(method='time')
co

# %%
df = pd.merge(t, co, how='inner', on=['timestamp', 'SensorUnit_ID'])  #merge co2,temperature and humidity
df

# %%
df = df.reset_index().merge(sensor, how='inner', on=['LocationName'])  #merge geo info
df.set_index('timestamp', drop=True, inplace=True)
df.drop(['X', 'Y'], axis=1, inplace=True)
df

# %%
df.isnull().any()  #check nan free or not

# %% [markdown]
# ### b) **2/10** 
#
# Export the curated and ready to use timeseries to a csv file, and properly push the merged csv to Git LFS.

# %%
from pathlib import Path
p = Path('../data/carbosense-raw/')
df.to_csv(p / 'merged.csv')

# %%
# # cd p
# git lfs track "*.csv"
# git add merged.csv
# git commit -m "add merged.csv"
# git push

# %% [markdown]
# ## PART II: Data visualization (15 points)

# %% [markdown]
# ### a) **5/15** 
# Group the sites based on their altitude, by performing K-means clustering. 
# - Find the optimal number of clusters using the [Elbow method](https://en.wikipedia.org/wiki/Elbow_method_(clustering)). 
# - Wite out the formula of metric you use for Elbow curve. 
# - Perform clustering with the optimal number of clusters and add an additional column `altitude_cluster` to the dataframe of the previous question indicating the altitude cluster index. 
# - Report your findings.
#
# __Note__: [Yellowbrick](http://www.scikit-yb.org/) is a very nice Machine Learning Visualization extension to scikit-learn, which might be useful to you. 

# %%
from sklearn.cluster import KMeans
from yellowbrick.cluster import KElbowVisualizer
from yellowbrick.cluster import InterclusterDistance

# %%
site_loc = df[['LocationName',
               'altitude']].drop_duplicates().reset_index(drop=True)
altitude = np.array(site_loc['altitude'].values.tolist()).reshape(-1,1) #reshape altitude format

# %%
model = KMeans() #build kmeans model
visualizer1 = KElbowVisualizer(model, k=(1, 12))
visualizer1.fit(altitude)
visualizer1.show()

# %% [markdown]
# The metric used for the Elbow method is Distortion,it is  the sum of squared distances from each point to its assigned center.
# For each cluster X, the distortion score is:
# <center>$score=\sum{Squared Euclidean Distance(datapoint_{X_i}-Centroid_{X})}$</center>
# <center>$Squared Euclidean Distance d(p,q)=(p_1-q_1)^2+...+(p_n-q_n)$</center>
#
# <center>$x_i \in X$</center>
#
# We choose k = 3 here.

# %%
model = KMeans(n_clusters=3, random_state=0)
visualizer2 = InterclusterDistance(model)
visualizer2.fit(altitude)
visualizer2.show()

# %%
model.fit(altitude)
label = model.labels_.flatten()
site_loc['altitude_cluster'] = label
site_loc

# %%
df = df.reset_index().merge(site_loc, on=['LocationName', 'altitude']).set_index('timestamp')
df

# %%
df.groupby('altitude_cluster')[['CO2', 'humidity', 'temperature']].agg('mean')

# %% [markdown]
# We observed that the mean CO2 emission of altitude cluster 2 is unusual which is  twice the amount of the other clusters.

# %% [markdown]
# ### b) **4/15** 
#
# Use `plotly` (or other similar graphing libraries) to create an interactive plot of the monthly median CO2 measurement for each site with respect to the altitude. 
#
# Add proper title and necessary hover information to each point, and give the same color to stations that belong to the same altitude cluster.

# %%
import plotly.graph_objects as go
import plotly.express as px

group_df = df.groupby('LocationName')
senser_df = group_df.agg('median').reset_index()
senser_df['altitude_cluster'] = senser_df['altitude_cluster'].astype("category")
fig = px.scatter(senser_df,
                 x="altitude",
                 y="CO2",
                 color="altitude_cluster",
                 hover_data=['LocationName', 'zone'],
                 title="Monthly median CO2 measurement for each site with respect to the altitude"
                 )
fig.show()

# %% [markdown]
# ### c) **6/15**
#
# Use `plotly` (or other similar graphing libraries) to plot an interactive time-varying density heatmap of the mean daily CO2 concentration for all the stations. Add proper title and necessary hover information.
#
# __Hints:__ Check following pages for more instructions:
# - [Animations](https://plotly.com/python/animations/)
# - [Density Heatmaps](https://plotly.com/python/mapbox-density-heatmaps/)

# %%
fig = px.density_mapbox(df, lat='LAT', lon='LON', z='CO2', radius=10, hover_data=['LocationName', 'zone'],
                        center=dict(lat=np.mean(df.LAT), lon=np.mean(df.LON)), zoom=10,
                        mapbox_style="stamen-terrain", animation_frame=df.index.day,
                        title='Time-varying density heatmap of the mean daily CO2 concentration for all the stations')
fig.show()

# %% [markdown]
# ## PART III: Model fitting for data curation (35 points)

# %% [markdown]
# ### a) **2/35**
#
# The domain experts in charge of these sensors report that one of the CO2 sensors `ZSBN` is exhibiting a drift on Oct. 24. Verify the drift by visualizing the CO2 concentration of the drifting sensor and compare it with some other sensors from the network. 

# %%
# Select sensors in the same zone as ZSBN to make comparisons
sub_df = df[df['zone'] == 3]
fig = px.line(sub_df, x=sub_df.index, y='CO2', color='LocationName')
fig.update_xaxes(rangeslider_visible=True)
fig

# %% [markdown]
# We selected sensors in the same zone as ZSBN for comparison. The plot illustrates that ZSBN showed a significant drift on Oct. 24. (The value of ZSBN decreased significantly during 0:00 to 6:00 while others didn't.)

# %% [markdown]
# ### b) **8/35**
#
# The domain experts ask you if you could reconstruct the CO2 concentration of the drifting sensor had the drift not happened. You decide to:
# - Fit a linear regression model to the CO2 measurements of the site, by considering as features the covariates not affected by the malfunction (such as temperature and humidity)
# - Create an interactive plot with `plotly` (or other similar graphing libraries):
#     - the actual CO2 measurements
#     - the values obtained by the prediction of the linear model for the entire month of October
#     - the __confidence interval__ obtained from cross validation
# - What do you observe? Report your findings.
#
# __Note:__ Cross validation on time series is different from that on other kinds of datasets. The following diagram illustrates the series of training sets (in orange) and validation sets (in blue). For more on time series cross validation, there are a lot of interesting articles available online. scikit-learn provides a nice method [`sklearn.model_selection.TimeSeriesSplit`](https://scikit-learn.org/stable/modules/generated/sklearn.model_selection.TimeSeriesSplit.html).
#
# ![ts_cv](https://player.slideplayer.com/86/14062041/slides/slide_28.jpg)

# %%
# Data preparation
zsbn_df = df[df['LocationName'] == 'ZSBN']
data = zsbn_df[['humidity', 'temperature']]
label = zsbn_df[['CO2']]
data['hour'] = zsbn_df.index.hour
data['minute'] = zsbn_df.index.minute

train_data, train_label = data[data.index < '2017-10-24'], label[label.index < '2017-10-24']

# %%
# Transform data: one hot encoding, min max scaling
from sklearn.preprocessing import OneHotEncoder
from sklearn.preprocessing import MinMaxScaler
from sklearn.compose import ColumnTransformer

one_hot_encoder = OneHotEncoder(handle_unknown="ignore", sparse=False)

ct = ColumnTransformer(
    transformers=[
        ("categorical", one_hot_encoder, ['hour', 'minute']),
    ],
    remainder=MinMaxScaler(),
)

transformed_train_data = ct.fit_transform(train_data)
transformed_train_data.shape

# %%
# Cross validation
from sklearn.model_selection import cross_validate
from sklearn.model_selection import TimeSeriesSplit
from sklearn.linear_model import Ridge
from scipy import stats

model = Ridge()

ts_cv = TimeSeriesSplit(n_splits=20, test_size=48)

cv_results = cross_validate(
    model,
    transformed_train_data,
    train_label,
    cv=ts_cv,
    scoring=["neg_mean_absolute_error"],
)

mae = -cv_results["test_neg_mean_absolute_error"]

# Calculate the 95% CI of mae
confidence_interval = stats.norm.interval(0.95, loc=mae.mean(), scale=mae.std())
confidence_interval

# %%
# Train model and test
model.fit(transformed_train_data, train_label)
transformed_data = ct.fit_transform(data)
label['pred'] = model.predict(transformed_data)
label


# %%
# Plot measurement and prediction
def plot(label, error_y):
    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=label.index,
            y=label['CO2'],
            name="Measurement"
        ))

    fig.add_trace(
        go.Scatter(
            x=label.index,
            y=label['pred'],
            name="Prediction",
        ))

    fig.add_trace(
        go.Scatter(
            name='Upper Bound',
            x=label.index,
            y=label['pred'] + error_y,
            mode='lines',
            marker=dict(color="#444"),
            line=dict(width=0),
            showlegend=False
        )),
    fig.add_trace(
        go.Scatter(
            name='Lower Bound',
            x=label.index,
            y=label['pred'] - error_y,
            marker=dict(color="#444"),
            line=dict(width=0),
            mode='lines',
            fillcolor='rgba(68, 68, 68, 0.3)',
            fill='tonexty',
            showlegend=False
        )),
    fig.update_xaxes(rangeslider_visible=True)
    fig.show()

plot(label, confidence_interval[1])

# %% [markdown]
# We can find that the model is able to generally follow the trend of the fluctuation of the CO2 concentration values before the 24th. After the 24th the predicted values deviate significantly from the measured values. Specifically, after the 24th, the predicted values maintain the trend before the 24th, while the measured values produce a drift. However, using only the features from ZSBN is not enough to fit the curve before 24th well, as reflected by the high errors [2.3995455011137032, 46.84830975333705] (95%CI). So we need more features, not limited to the information from ZSBN, to better reconstruct the unaffected CO2 concentration values.

# %% [markdown]
# ### c) **10/35**
#
# In your next attempt to solve the problem, you decide to exploit the fact that the CO2 concentrations, as measured by the sensors __experiencing similar conditions__, are expected to be similar.
#
# - Find the sensors sharing similar conditions with `ZSBN`. Explain your definition of "similar condition".
# - Fit a linear regression model to the CO2 measurements of the site, by considering as features:
#     - the information of provided by similar sensors
#     - the covariates associated with the faulty sensors that were not affected by the malfunction (such as temperature and humidity).
# - Create an interactive plot with `plotly` (or other similar graphing libraries):
#     - the actual CO2 measurements
#     - the values obtained by the prediction of the linear model for the entire month of October
#     - the __confidence interval__ obtained from cross validation
# - What do you observe? Report your findings.

# %% [markdown]
# Here we define similar sensors as sensors who share similar median humidity, median temperature, LAT, LON, altitude and identical zone, since these factors can have effects on the CO2 concentration. Therefore, we calculate the euclidian distance between sensors with the aforementioned information and select the closest 4 sensors to ZSBN as predictors. Specially, we give high weight on zone since we believe that sensors in identical zones i.e. similar environment have a higher probability to have similar CO2 concentration.

# %%
# Calculate the euclidian distance between sensors
# with features median humidity, median temperature, LAT, LON, altitude and zone

sensor_meta = sensor.copy()
sensor_meta.drop(['X', 'Y'], axis=1, inplace=True)

humidity_median = df.groupby('LocationName')['humidity'].median()
temperature_median = df.groupby('LocationName')['temperature'].median()
sensor_meta = sensor_meta.join(humidity_median, on='LocationName')
sensor_meta = sensor_meta.join(temperature_median, on='LocationName')

# Do normalization
sensor_meta.iloc[:,-5:] = sensor_meta.iloc[:,-5:].apply(lambda x: (x-x.mean())/ x.std(), axis=0)
meta_zsbn = sensor_meta[sensor_meta['LocationName'] == 'ZSBN'][['altitude', 'LAT', 'LON', 'humidity', 'temperature', 'zone']]
meta_others = sensor_meta[sensor_meta['LocationName'] != 'ZSBN']

meta_zsbn_np = meta_zsbn.to_numpy().ravel()

# Calculate euclidean distances
meta_others['dist'] = (meta_others['altitude'] - meta_zsbn_np[0]) ** 2 +\
                        (meta_others['LAT'] - meta_zsbn_np[1]) ** 2 +\
                        (meta_others['LON'] - meta_zsbn_np[2]) ** 2 +\
                        (meta_others['humidity'] - meta_zsbn_np[3]) ** 2 +\
                        (meta_others['temperature'] - meta_zsbn_np[4]) ** 2 +\
                        (2 * (meta_others['zone'] != meta_zsbn_np[5])) ** 2
meta_others = meta_others.sort_values(by=['dist'])

# Choose the 4 sensors with the least distance to ZSBN
similar_sensor_location = meta_others[:4]['LocationName'].to_numpy().ravel()
similar_sensor_location

# %%
# Prepare data
df_copy = df.copy()

df_copy['hour'] = df_copy.index.hour
df_copy = pd.get_dummies(df_copy, prefix=['zone'], columns=['zone'], drop_first=True)

zones = [c for c in df_copy.columns if c.startswith('zone')]

data = df_copy[df_copy['LocationName'] == 'ZSBN'][['humidity', 'temperature', 'altitude', 'LAT', 'LON', 'CO2', 'hour'] + zones]

for s in similar_sensor_location:
    df_s = df_copy[df_copy['LocationName'] == s][['humidity', 'temperature', 'altitude', 'LAT', 'LON', 'CO2'] + zones]
    data = data.join(df_s, rsuffix='_' + s)

label = data[['CO2']]
data = data.drop(['CO2'], axis=1)

train_data, train_label = data[data.index < '2017-10-24'], label[label.index < '2017-10-24']
train_data.head()

# %%
# Transform data: one hot encoding, min max scaling
from sklearn.preprocessing import OneHotEncoder
from sklearn.preprocessing import MinMaxScaler
from sklearn.compose import ColumnTransformer

one_hot_encoder = OneHotEncoder(handle_unknown="ignore", sparse=False)

ct = ColumnTransformer(
    transformers=[
        ("categorical", one_hot_encoder, ['hour']),
    ],
    remainder=MinMaxScaler(),
)

transformed_train_data = ct.fit_transform(train_data)
transformed_train_data.shape

# %%
# Cross validation
from sklearn.model_selection import cross_validate
from sklearn.model_selection import TimeSeriesSplit
from sklearn.linear_model import Ridge
from scipy import stats

model = Ridge()

ts_cv = TimeSeriesSplit(n_splits=20, test_size=48)

cv_results = cross_validate(
    model,
    transformed_train_data,
    train_label,
    cv=ts_cv,
    scoring=["neg_mean_absolute_error"],
)

mae = -cv_results["test_neg_mean_absolute_error"]
# Calculate the 95% CI of mae
confidence_interval = stats.norm.interval(0.95, loc=mae.mean(), scale=mae.std())
confidence_interval

# %%
# Train model and test
model.fit(transformed_train_data, train_label)
transformed_data = ct.fit_transform(data)
label['pred'] = model.predict(transformed_data)
label

# %%
# Plot the prediction and measurement
plot(label, confidence_interval[1])

# %% [markdown]
# By exploiting information from other similar sensors, we achieved significantly better results than only using the humidity and temperature in ZSBN, with error 95% CI [1.202799973219788, 15.619887873788203] compared to [2.3995455011137032, 46.84830975333705].
#
# For data before 2017-10-24, the prediction of the model fit the measurement well. Therefore, we can confidently say that the model can also reconstruct well the actual CO2 after 2017-10-24. For data after 2017-10-24, the measurement and prediction CO2 concentration follow similar fluctuation patterns, while the measurement has an obvious almost constant drift from the prediction.

# %% [markdown]
# ### d) **10/35**
#
# Now, instead of feeding the model with all features, you want to do something smarter by using linear regression with fewer features.
#
# - Start with the same sensors and features as in question c)
# - Leverage at least two different feature selection methods
# - Create similar interactive plot as in question c)
# - Describe the methods you choose and report your findings

# %%
# Use Pearson's correlation feature selection for numeric input and numeric output
from sklearn.feature_selection import SelectKBest
from sklearn.feature_selection import f_regression

fs = SelectKBest(score_func=f_regression, k=10)

X_selected = fs.fit_transform(transformed_train_data, train_label['CO2'])
X_test = fs.transform(transformed_data)

X_selected.shape

# %%
# Cross validation
from sklearn.model_selection import cross_validate
from scipy import stats

model = Ridge()

ts_cv = TimeSeriesSplit(n_splits=20, test_size=48)

cv_results = cross_validate(model, X_selected, train_label, cv=ts_cv, scoring=["neg_mean_absolute_error"])

mae = -cv_results["test_neg_mean_absolute_error"]
# Calculate the 95% CI of mae
confidence_interval = stats.norm.interval(0.95, loc=mae.mean(), scale=mae.std())
confidence_interval

# %%
# Train model and predict
model = Ridge()
model.fit(X_selected, train_label)
label['pred'] = model.predict(X_test)
label

# %%
# Plot the prediction and measurement
plot(label, confidence_interval[1])

# %%
# Use lasso model to select features
from sklearn import linear_model
from sklearn.feature_selection import SelectFromModel

clf = linear_model.Lasso(alpha=0.5).fit(transformed_train_data, train_label)
fs = SelectFromModel(clf, prefit=True)
X_selected = fs.transform(transformed_train_data)
X_test = fs.transform(transformed_data)

# %%
from sklearn.model_selection import cross_validate
from scipy import stats

model = Ridge()

ts_cv = TimeSeriesSplit(n_splits=20, test_size=48)

cv_results = cross_validate(model, X_selected, train_label, cv=ts_cv, scoring=["neg_mean_absolute_error"])

# Calculate the 95% CI of mae
mae = -cv_results["test_neg_mean_absolute_error"]
confidence_interval = stats.norm.interval(0.95, loc=mae.mean(), scale=mae.std())
confidence_interval

# %%
model = Ridge()
model.fit(X_selected, train_label)
label['pred']  = model.predict(X_test)
label

# %%
# Plot the prediction and measurement
plot(label, confidence_interval[1])

# %% [markdown]
# In this part, we select features using Pearson's correlation (select features by calculating the correlation of certain feature to the target) and lasso regression (lasso regression will obtain sparse parameters and can naturally select features) respectively. The cross validation results show that we get lower error (95% CI) ([1.0647748853499541, 15.059471331113222], [1.7774875151939264, 14.732102637794839] respectively) compared to model without feature selection ([1.202799973219788, 15.619887873788203]).
#
# The plot pattern is similar. For data before 2017-10-24, the prediction of the model fit the measurement well. For data after 2017-10-24, the measurement and prediction CO2 concentration follow similar fluctuation patterns, while the measurement has an obvious almost constant drift from the prediction.

# %% [markdown]
# ### e) **5/35**
#
# Eventually, you'd like to try something new - __Bayesian Structural Time Series Modelling__ - to reconstruct counterfactual values, that is, what the CO2 measurements of the faulty sensor should have been, had the malfunction not happened on October 24. You will use:
# - the information of provided by similar sensors - the ones you identified in question c)
# - the covariates associated with the faulty sensors that were not affected by the malfunction (such as temperature and humidity).
#
# To answer this question, you can choose between a Python port of the CausalImpact package (such as https://github.com/jamalsenouci/causalimpact) or the original R version (https://google.github.io/CausalImpact/CausalImpact.html) that you can run in your notebook via an R kernel (https://github.com/IRkernel/IRkernel).
#
# Before you start, watch first the [presentation](https://www.youtube.com/watch?v=GTgZfCltMm8) given by Kay Brodersen (one of the creators of the causal impact implementation in R), and this introductory [ipython notebook](https://github.com/jamalsenouci/causalimpact/blob/HEAD/GettingStarted.ipynb) with examples of how to use the python package.
#
# - Report your findings:
#     - Is the counterfactual reconstruction of CO2 measurements significantly different from the observed measurements?
#     - Can you try to explain the results?

# %%
from causalimpact import CausalImpact

# Select temperature, humidity features of ZSBN and similar sensors, and CO2 measurement of similar sensors as features
data2 = data[[col for col in data.columns if col.startswith('temperature')
                                            or col.startswith('humidity')
                                            or col.startswith('CO2')]].copy()
data2.insert(0, 'y', label['CO2'])

ts_index = data[data.index < '2017-10-24'].shape[0]
pre_period = [0, ts_index - 1]
post_period = [ts_index, data.shape[0] - 1]

data2.reset_index(inplace=True, drop=True)

ci = CausalImpact(data2, pre_period, post_period)
print(ci.summary())
print(ci.summary(output='report'))
ci.plot()


# %% [markdown]
# Observations:
# According to the report, subtracting this prediction from the observed response yields an estimate of the causal effect the intervention had on the response variable. This effect is -91.56 with a 95% interval of [-102.01, -81.29].  In relative terms, the response variable showed a decrease of -20.75%. The 95% interval of this percentage is [-23.12%, -18.42%], which means that the negative effect observed during the intervention period is statistically significant.
#
# Since the drift remains almost constant, we hypothesize that there may be something wrong with the measurement mechanism of the sensor in ZSBN so that the measured value of CO2 concentration is about 90 lower than the actual value.

# %% [markdown]
# # That's all, folks!
