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
# # Homework 2 - Data Wrangling with Hadoop
#
# The goal of this assignment is to put into action the data wrangling techniques from the exercises of week-3 and week-4. We highly suggest you to finish these two exercises first and then start the homework. In this homework, we are going to reuse the same __sbb__ and __twitter__ datasets as seen before during these two weeks. 
#
# ## Hand-in Instructions
# - __Due: 05.04.2022 23:59 CET__
# - Fork this project as a private group project
# - Verify that all your team members are listed as group members of the project
# - `git push` your final verion to your group's Renku repository before the due date
# - Verify that `Dockerfile`, `environment.yml` and `requirements.txt` are properly written and notebook is functional
# - Add necessary comments and discussion to make your queries readable
#
# ## Hive Documentation
#
# Hive queries: <https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Select>
#
# Hive functions: <https://cwiki.apache.org/confluence/display/Hive/LanguageManual+UDF>

# %% [markdown]
# <div style="font-size: 100%" class="alert alert-block alert-warning">
#     <b>Get yourself ready:</b> 
#     <br>
#     Before you jump into the questions, please first go through the notebook <a href='./prepare_env.ipynb'>prepare_env.ipynb</a> and make sure that your environment is properly set up.
#     <br><br>
#     <b>Cluster Usage:</b>
#     <br>
#     As there are many of you working with the cluster, we encourage you to prototype your queries on small data samples before running them on whole datasets.
#     <br><br>
#     <b>Try to use as much HiveQL as possible and avoid using pandas operations. Also, whenever possible, try to apply the methods you learned in class to optimize your queries to minimize the use of computing resources.</b>
# </div>

# %% [markdown]
# ## Part I: SBB/CFF/FFS Data (40 Points)
#
# Data source: <https://opentransportdata.swiss/en/dataset/istdaten>
#
# In this part, you will leverage Hive to perform exploratory analysis of data published by the [Open Data Platform Swiss Public Transport](https://opentransportdata.swiss).
#
# Format: the dataset is originally presented as a collection of textfiles with fields separated by ';' (semi-colon). For efficiency, the textfiles have been converted into Optimized Row Columnar ([_ORC_](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+ORC)) file format. 
#
# Location: you can find the data in ORC format on HDFS at the path `/data/sbb/part_orc/istdaten`.
#
# The full description from opentransportdata.swiss can be found in <https://opentransportdata.swiss/de/cookbook/ist-daten/> in four languages. There may be inconsistencies or missing information between the translations.. In that case we suggest you rely on the German version and use an automated translator when necessary. We will clarify if there is still anything unclear in class and Slack. Here we remind you the relevant column descriptions:
#
# - `BETRIEBSTAG`: date of the trip
# - `FAHRT_BEZEICHNER`: identifies the trip
# - `BETREIBER_ABK`, `BETREIBER_NAME`: operator (name will contain the full name, e.g. Schweizerische Bundesbahnen for SBB)
# - `PRODUKT_ID`: type of transport, e.g. train, bus
# - `LINIEN_ID`: for trains, this is the train number
# - `LINIEN_TEXT`,`VERKEHRSMITTEL_TEXT`: for trains, the service type (IC, IR, RE, etc.)
# - `ZUSATZFAHRT_TF`: boolean, true if this is an additional trip (not part of the regular schedule)
# - `FAELLT_AUS_TF`: boolean, true if this trip failed (cancelled or not completed)
# - `HALTESTELLEN_NAME`: name of the stop
# - `ANKUNFTSZEIT`: arrival time at the stop according to schedule
# - `AN_PROGNOSE`: actual arrival time
# - `AN_PROGNOSE_STATUS`: show how the actual arrival time is calcluated
# - `ABFAHRTSZEIT`: departure time at the stop according to schedule
# - `AB_PROGNOSE`: actual departure time
# - `AB_PROGNOSE_STATUS`: show how the actual departure time is calcluated
# - `DURCHFAHRT_TF`: boolean, true if the transport does not stop there
#
# Each line of the file represents a stop and contains arrival and departure times. When the stop is the start or end of a journey, the corresponding columns will be empty (`ANKUNFTSZEIT`/`ABFAHRTSZEIT`).
#
# In some cases, the actual times were not measured so the `AN_PROGNOSE_STATUS`/`AB_PROGNOSE_STATUS` will be empty or set to `PROGNOSE` and `AN_PROGNOSE`/`AB_PROGNOSE` will be empty.

# %% [markdown]
# __Initialization__

# %% tags=[]
import os
import pandas as pd
pd.set_option("display.max_columns", 50)
import matplotlib.pyplot as plt
# %matplotlib inline
import plotly.express as px
import plotly.graph_objects as go
import warnings
warnings.simplefilter(action='ignore', category=UserWarning)

username = os.environ['RENKU_USERNAME']
hiveaddr = os.environ['HIVE_SERVER2']
(hivehost,hiveport) = hiveaddr.split(':')
print("Operating as: {0}".format(username))

# %%
from pyhive import hive

# create connection
conn = hive.connect(host=hivehost, 
                    port=hiveport,
                    username=username) 
# create cursor
cur = conn.cursor()

# %% [markdown]
# ### a) Prepare the table - 5/40
#
# Complete the code in the cell below, replace all `TODO` in order to create a Hive Table of SBB Istadaten.
#
# The table has the following properties:
#
# * The table is in your database, which must have the same name as your gaspar ID
# * The table name is `sbb_orc`
# * The table must be external
# * The table content consist of ORC files in the HDFS folder `/data/sbb/part_orc/istdaten`
# * The table is _partitioned_, and the number of partitions should not exceed 50
#

# %%
### Create your database if it does not exist
query = """
CREATE DATABASE IF NOT EXISTS {0} LOCATION '/user/{0}/hive'
""".format(username)
cur.execute(query)

# %%
### Make your database the default
query = """
USE {0}
""".format(username)
cur.execute(query)

# %%
query = """
DROP TABLE IF EXISTS {0}.sbb_orc
""".format(username)
cur.execute(query)

# %%
query = """
    CREATE EXTERNAL TABLE {0}.sbb_orc(
        BETRIEBSTAG string,
        FAHRT_BEZEICHNER string,
        BETREIBER_ID string,
        BETREIBER_ABK string,
        BETREIBER_NAME string,
        PRODUKT_ID string,
        LINIEN_ID string,
        LINIEN_TEXT string,
        UMLAUF_ID string,
        VERKEHRSMITTEL_TEXT string,
        ZUSATZFAHRT_TF string,
        FAELLT_AUS_TF string,
        BPUIC string,
        HALTESTELLEN_NAME string,
        ANKUNFTSZEIT string,
        AN_PROGNOSE string,
        AN_PROGNOSE_STATUS string,
        ABFAHRTSZEIT string,
        AB_PROGNOSE string,
        AB_PROGNOSE_STATUS string,
        DURCHFAHRT_TF string
    )
    partitioned by (year string, month string)
    stored as orc
    location '/data/sbb/part_orc/istdaten'
""".format(username)
cur.execute(query)

# %%
query = """MSCK REPAIR TABLE {0}.sbb_orc""".format(username);
cur.execute(query)

# %% [markdown]
# **Checkpoint**
#
# Run the cells below and verify that your table satisfies all the required properties

# %%
query = """
DESCRIBE {0}.sbb_orc
""".format(username)
cur.execute(query)
cur.fetchall()

# %%
query = """
    select * from {0}.sbb_orc limit 5
""".format(username)
pd.read_sql(query, conn)

# %%
query = """
SHOW PARTITIONS {0}.sbb_orc
""".format(username)
cur.execute(query)
cur.fetchall()

# %% [markdown]
# ### b) Type of transport - 5/40
#
# In the exercise of week-3, you have already explored the stop distribution of different types of transport on a small data set. Now, let's do the same for a full two years worth of data.
#
# - Query `sbb_orc` to get the total number of stops for different types of transport in each month of 2019 and 2020, and order it by time and type of transport.
# |month_year|ttype|stops|
# |---|---|---|
# |...|...|...|
# - Use `plotly` to create a facet bar chart partitioned by the type of transportation. 
# - Document any patterns or abnormalities you can find.
#
# __Note__: 
# - In general, one entry in the `sbb_orc` table means one stop.
# - You might need to filter out the rows where:
#     - `BETRIEBSTAG` is not in the format of `__.__.____`
#     - `PRODUKT_ID` is NULL or empty
# - Facet plot with plotly: https://plotly.com/python/facet-plots/

# %%
# You may need more than one query, do not hesitate to create as many as you need.
# query = """
#
#     /* TODO */
#
# """
# cur.execute(query, conn)

query = """
    with temp as(
        select from_unixtime(unix_timestamp(betriebstag,'dd.MM.yyyy'), 'MM-yyyy') as month_year, lower(PRODUKT_ID) as ttype from {0}.sbb_orc
        where year between 2019 and 2020
    )
    select month_year, ttype, count(*) as stops from temp
    where ttype is not null and length(ttype) > 0
    group by month_year, ttype
    order by month_year
""".format(username)
df_ttype = pd.read_sql(query, conn)
df_ttype.head(10)

# %%
df_ttype.ttype.unique()

# %% tags=[]
fig = px.bar(
    df_ttype,
    x='month_year',
    y='stops',
    facet_col='ttype',
    facet_col_wrap=3,
    title= 'total number of stops by transportation type'
)
fig.show()

# %%
fig = px.bar(
    df_ttype,
    x='month_year',
    y='stops',
    log_y=True,
    facet_col='ttype',
    facet_col_wrap=3,
    title= 'total number of stops by transportation type'
)
fig.show()

# %% [markdown]
# We find that the numbers of metro,schiff and zahnradbahn are  far less than bus and tram. Most traffic are done by bus, tram and zug.

# %% [markdown]
# ### c) Schedule - 10/40
#
# - Select a any day on a typical week day (not Saturday, not Sunday, not a bank holiday) from `sbb_orc`. Query the table for that one-day and get the set of IC (`VERKEHRSMITTEL_TEXT`) trains you can take to go (without connections) from Genève to Lausanne on that day. 
# - Display the train number (`LINIEN_ID`) as well as the schedule (arrival and departure time) of the trains.
#
# |train_number|departure|arrival|
# |---|---|---|
# |...|...|...|
#
# __Note:__ 
# - The schedule of IC from Genève to Lausanne has not changed for the past few years. You can use the advanced search of SBB's website to check your answer.
# - Do not hesitate to create intermediary tables or views (see [_CTAS_](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-CreateTableAsSelect(CTAS)))
# - You might need to add filters on these flags: `ZUSATZFAHRT_TF`, `FAELLT_AUS_TF`, `DURCHFAHRT_TF` 
# - Functions that could be useful: `unix_timestamp`, `to_utc_timestamp`, `date_format`.

# %%
# You may need more than one query, do not hesitate to create more

# query = """
#
#     TODO
#
# """
# cur.execute(query, conn)

query = """
    with day_table as(
        select LINIEN_ID as train_number, ABFAHRTSZEIT as departure, ANKUNFTSZEIT as arrival, HALTESTELLEN_NAME as stops from {0}.sbb_orc
        where BETRIEBSTAG = '07.11.2019' and VERKEHRSMITTEL_TEXT='IC'
    )  select dept.train_number as train_number, dept.departure as departure, ariv.arrival as arrival from(
        select train_number, departure from day_table
        where instr(stops, "Genève") > 0
        )
    as dept
    inner join(
        select train_number, arrival from day_table
        where instr(stops, "Lausanne") > 0
    ) as ariv on (dept.train_number = ariv.train_number)
    where unix_timestamp(arrival,'dd.MM.yyyy HH:mm') > unix_timestamp(departure,'dd.MM.yyyy HH:mm')
    
""".format(username)
pd.read_sql(query, conn)

# %% [markdown]
# ### d) Delay percentiles - 10/40
#
# - Query `sbb_orc` to compute the 50th and 75th percentiles of __arrival__ delays for IC 702, 704, ..., 728, 730 (15 trains total) at Genève main station. 
# - Use `plotly` to plot your results in a proper way. 
# - Which trains are the most disrupted? Can you find the tendency and interpret?
#
# __Note:__
# - Do not hesitate to create intermediary tables. 
# - When the train is ahead of schedule, count this as a delay of 0.
# - Use only stops with `AN_PROGNOSE_STATUS` equal to __REAL__ or __GESCHAETZT__.
# - Functions that may be useful: `unix_timestamp`, `percentile_approx`, `if`

# %%
# You may need more than one query, do not hesitate to create more

# query = """
#
#     TODO
#
# """
# cur.execute(query, conn)

query = """
    with temp as (
        select LINIEN_ID as train_number, AN_PROGNOSE as actual_time, ANKUNFTSZEIT as schedule_time from {0}.sbb_orc
        where HALTESTELLEN_NAME = "Genève" 
        and (AN_PROGNOSE_STATUS = 'REAL' or AN_PROGNOSE_STATUS = 'GESCHAETZT') 
        and VERKEHRSMITTEL_TEXT='IC' 
        and cast(LINIEN_ID as int) between 702 and 730
        and cast(LINIEN_ID as int) % 2 = 0
    ), temp2 as (
     select train_number, percentile_approx(
            if(
                unix_timestamp(actual_time,'dd.MM.yyyy HH:mm:ss')-unix_timestamp(schedule_time,'dd.MM.yyyy HH:mm') > 0, 
                unix_timestamp(actual_time,'dd.MM.yyyy HH:mm:ss')-unix_timestamp(schedule_time,'dd.MM.yyyy HH:mm'), 0
            ), array(0.50, 0.75)
        ) as percentile from temp
        group by train_number
    ) select train_number, percentile[0] as percentile_50, percentile[1] as percentile_75 from temp2
""".format(username)
df_delays_ic_gen = pd.read_sql(query, conn)
df_delays_ic_gen

# %%
df_delays_ic_gen_melt = pd.melt(df_delays_ic_gen, id_vars =['train_number'], 
                                value_vars=['percentile_50', 'percentile_75'], 
                                var_name='percentile', value_name='value')
fig = px.bar(
    df_delays_ic_gen_melt, 
    x="train_number", y='value', color='percentile', barmode='group'
    # TODO
)
fig.show()

# %% [markdown]
# The IC708 is the most disrupted train. The top 3 disrupted train is IC708 IC730 IC706 . From IC702 to IC730, the schedule time of the train increases in sequence,The earliest time is 07:18 on IC702, when traffic is not heavy, and increases sequentially with time, peaking at 10:18 on IC708. It gradually decreases in the afternoon, while gradually increasing in the evening off-hours, reaching a second peak at IC730.Disrupted time generally corresponds to people's commuting time and trends, and disrupted time increases during periods of high commuting demand

# %% [markdown]
# ### e) Delay heatmap 10/40
#
# - For each week (1 to 52) of each year from 2019 to 2021, query `sbb_orc` to compute the median of delays of all trains __departing__ from any train stations in Zürich area during that week. 
# - Use `plotly` to draw a heatmap year x week (year columns x week rows) of the median delays. 
# - In which weeks were the trains delayed the most/least? Can you explain the results?
#
# __Note:__
# - Do not hesitate to create intermediary tables. 
# - When the train is ahead of schedule, count this as a delay of 0 (no negative delays).
# - Use only stops with `AB_PROGNOSE_STATUS` equal to __REAL__ or __GESCHAETZT__.
# - For simplicty, a train station in Zürich area <=> it's a train station & its `HALTESTELLEN_NAME` starts with __Zürich__.
# - Heatmap with `plotly`: https://plotly.com/python/heatmaps/
# - Functions that may be useful: `unix_timestamp`, `from_unixtime`, `weekofyear`, `percentile_approx`, `if`

# %%
import seaborn as sns

# %%
# You may need more than one query, do not hesitate to create more

# query = """
#
#     TODO
#
# """
# cur.execute(query, conn)
query = """
    with temp as (
        select 
            year,
            weekofyear(from_unixtime(unix_timestamp(BETRIEBSTAG, 'dd.MM.yyyy'), 'yyyy-MM-dd')) as week, 
            AB_PROGNOSE as actual_time, ABFAHRTSZEIT as schedule_time from {0}.sbb_orc
        where instr(HALTESTELLEN_NAME, "Zürich") > 0 
        and (AB_PROGNOSE_STATUS = 'REAL' or AB_PROGNOSE_STATUS = 'GESCHAETZT') 
        and year between 2019 and 2021
        and length(ANKUNFTSZEIT) = 0
        and length(ABFAHRTSZEIT) > 0
    ) select year, week, percentile_approx(
            if(
                unix_timestamp(actual_time,'dd.MM.yyyy HH:mm:ss')-unix_timestamp(schedule_time,'dd.MM.yyyy HH:mm') > 0, 
                unix_timestamp(actual_time,'dd.MM.yyyy HH:mm:ss')-unix_timestamp(schedule_time,'dd.MM.yyyy HH:mm'), 0
            ), 0.5
        ) as median from temp
        group by year, week
        order by year, week
""".format(username)
df_delays_zurich = pd.read_sql(query, conn)
df_delays_zurich

# %%
#ploty version
df1 = df_delays_zurich.pivot(index='week', columns='year', values='median')
fig = px.imshow(df1)
fig.show()

# %%
#sns version
ax = sns.heatmap(df1)
plt.show()

# %%
df_delays_zurich.max()

# %%
df_delays_zurich.min()

# %%
df_delays_zurich[df_delays_zurich['median']==96.0]

# %%
df_delays_zurich[df_delays_zurich['median']==38.0]

# %%
df_delays_zurich[(df_delays_zurich['week']<=46)&(df_delays_zurich['week']>=41)]

# %% [markdown]
# The delayed the most week is 2019 43 week. We can see from heatmap that every year around this time period, the delay is one of the highest, and the overall delay in 2019 is larger than the rest of the years, so the 43rd week of 2019 is the week with the highest delay.The week with the lowest delay was in week 52 of 2021, with lower delay due to less traffic around Christmas.

# %% [markdown]
# ## Part II: Twitter Data (20 Points)
#
# Data source: https://archive.org/details/twitterstream?sort=-publicdate 
#
# In this part, you will leverage Hive to extract the hashtags from the source data, and then perform light exploration of the prepared data. 
#
# ### Dataset Description 
#
# Format: the dataset is presented as a collection of textfiles containing one JSON document per line. The data is organized in a hierarchy of folders, with one file per minute. The textfiles have been compressed using bzip2. In this part, we will mainly focus on __2016 twitter data__.
#
# Location: you can find the data on HDFS at the path `/data/twitter/json/year={year}/month={month}/day={day}/{hour}/{minute}.json.bz2`. 
#
# Relevant fields: 
# - `created_at`, `timestamp_ms`: The first is a human-readable string representation of when the tweet was posted. The latter represents the same instant as a timestamp since UNIX epoch.
# - `lang`: the language of the tweet content 
# - `entities`: parsed entities from the tweet, e.g. hashtags, user mentions, URLs.
# - In this repository, you can find [a tweet example](../data/tweet-example.json).
#
# Note:  Pay attention to the time units! and check the Date-Time functions in the Hive [_UDF_](https://cwiki.apache.org/confluence/display/hive/Languagemanual+udf#LanguageManualUDF-DateFunctions) documentation.

# %% [markdown]
# <div style="font-size: 100%" class="alert alert-block alert-danger">
#     <b>Disclaimer</b>
#     <br>
#     This dataset contains unfiltered data from Twitter. As such, you may be exposed to tweets/hashtags containing vulgarities, references to sexual acts, drug usage, etc.
#     </div>

# %% [markdown]
# ### a) JsonSerDe - 4/20
#
# In the exercise of week 4, you have already seen how to use the [SerDe framework](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-RowFormats&SerDe) to extract JSON fields from raw text format. 
#
# In this question, please use SerDe to create an <font color="red" size="3px">EXTERNAL</font> table with __one day__ (e.g. 01.07.2016) of twitter data. You only need to extract three columns: `timestamp_ms`, `lang` and `entities`(with the field `hashtags` only) with following schema (you need to figure out what to fill in `TODO`):
# ```
# timestamp_ms string,
# lang         string,
# entities     struct<hashtags:array<...<text:..., indices:...>>>
# ```
#
# The table you create should be similar to:
#
# | timestamp_ms | lang | entities |
# |---|---|---|
# | 1234567890001 | en | {"hashtags":[]} |
# | 1234567890002 | fr | {"hashtags":[{"text":"hashtag1","indices":[10]}]} |
# | 1234567890002 | jp | {"hashtags":[{"text":"hashtag1","indices":[14,23]}, {"text":"hashtag2","indices":[45]}]} |
#
# __Note:__
#    - JsonSerDe: https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-RowFormats&SerDe
#    - Hive data types: https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types#LanguageManualTypes
#    - Hive complex types: https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types#LanguageManualTypes-ComplexTypes

# %%
query="""
    DROP TABLE IF EXISTS {0}.twitter_hashtags
""".format(username)
cur.execute(query)

query="""
    CREATE EXTERNAL TABLE {0}.twitter_hashtags(
        timestamp_ms STRING,
        lang STRING,
        entities STRUCT< hashtags: ARRAY < STRUCT < text: STRING, indices: ARRAY<INT> >>>
    )
    PARTITIONED BY (year STRING, month STRING, day STRING)
    -- ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
    WITH SERDEPROPERTIES(
        "ignore.malformed.json"="true"
    )
    STORED AS TEXTFILE
    LOCATION '/data/twitter/json/'
""".format(username)
cur.execute(query)

# %%
query="""
    MSCK REPAIR TABLE {0}.twitter_hashtags
""".format(username)
cur.execute(query)

# %%
query="""
    SELECT 
        timestamp_ms,
        lang,
        entities
    
    FROM {0}.twitter_hashtags 
    
    WHERE
        year=2016 AND month=07 AND day=01
        
    
    LIMIT 10
""".format(username)
pd.read_sql(query, conn)

# %% [markdown]
# ### b) Explosion - 4/20
#
# In a), you created a table where each row could contain a list of multiple hashtags. Create another table **containing one day of data only** by normalizing the table obtained from the previous step. This means that each row should contain exactly one hashtag. Include `timestamp_ms` and `lang` in the resulting table, as shown below.
#
# | timestamp_ms | lang | hashtag |
# |---|---|---|
# | 1234567890001 | es | hashtag1 |
# | 1234567890001 | es | hashtag2 |
# | 1234567890002 | en | hashtag2 |
# | 1234567890003 | zh | hashtag3 |
#
# __Note:__
#    - `LateralView`: https://cwiki.apache.org/confluence/display/Hive/LanguageManual+LateralView
#    - `explode` function: <https://cwiki.apache.org/confluence/display/Hive/LanguageManual+UDF#LanguageManualUDF-explode>

# %%
query="""
    DROP TABLE IF EXISTS {0}.twitter_hashtags_norm
""".format(username)
cur.execute(query)

query="""
    CREATE TABLE IF NOT EXISTS {0}.twitter_hashtags_norm
    STORED AS ORC
    AS 
    (
        SELECT 
            timestamp_ms,
            lang,
            hashtag.text AS hashtag
        FROM
            {0}.twitter_hashtags
            LATERAL VIEW explode(twitter_hashtags.entities.hashtags) exploded_table as hashtag
        WHERE 
            year=2016 AND month=07 AND day=01
    )
        
""".format(username)
cur.execute(query)

# %%
query="""
    SELECT * FROM {0}.twitter_hashtags_norm LIMIT 10
""".format(username)
pd.read_sql(query, conn)

# %% [markdown]
# ### c) Hashtags - 8/20
#
# Query the normailized table you obtained in b). Create a table of the top 20 most mentioned hashtags with the contribution of each language. And, for each hashtag, order languages by their contributions. You should have a table similar to:
#
# |hashtag|lang|lang_count|total_count|
# |---|---|---|---|
# |hashtag_1|en|2000|3500|
# |hashtag_1|fr|1000|3500|
# |hashtag_1|jp|500|3500|
# |hashtag_2|te|500|500|
#
# Use `plotly` to create a stacked bar chart to show the results.
#
# __Note:__ to properly order the bars, you may need:
# ```python
# fig.update_layout(xaxis_categoryorder = 'total descending')
# ```

# %%
# You may need more than one query, do not hesitate to create more
cur.execute("DROP VIEW IF EXISTS {0}.hashtag_count".format(username))
query="""
    CREATE VIEW {0}.hashtag_count
    AS 
    (
    SELECT 
        hashtag,
        count(1) AS total_count
    FROM {0}.twitter_hashtags_norm
    GROUP BY hashtag
    SORT BY total_count DESC
    LIMIT 20
    )
""".format(username)
cur.execute(query)

# %%
cur.execute("DROP VIEW IF EXISTS {0}.tag_lang_count".format(username))
query="""
    CREATE VIEW {0}.tag_lang_count
    AS 
    (
    SELECT 
        hashtag,
        lang,
        count(1) AS lang_count
    FROM {0}.twitter_hashtags_norm
    GROUP BY hashtag, lang
    )
""".format(username)
cur.execute(query)

# %%
query="""
    DROP TABLE IF EXISTS {0}.tag_count_join
""".format(username)
cur.execute(query)

query="""
    CREATE TABLE IF NOT EXISTS {0}.tag_count_join
    AS 
    (
    SELECT 
        a.hashtag AS hashtag,
        a.lang AS lang,
        a.lang_count AS lang_count,
        b.total_count AS total_count
    FROM {0}.tag_lang_count AS a JOIN {0}.hashtag_count AS b
    ON a.hashtag=b.hashtag 
    )
    SORT BY total_count DESC, lang_count DESC
""".format(username)
cur.execute(query)

# %%
query = """
    SELECT * FROM {0}.tag_count_join
""".format(username)
df_hashtag = pd.read_sql(query, conn)

# %%
df_hashtag.columns = df_hashtag.columns.str.replace('tag_count_join.', '')
df_hashtag.head()

# %%
fig = px.bar(
    df_hashtag,
    x="hashtag", 
    y="lang_count", 
    color="lang", 
    title="The top 20 most mentioned hashtags with the contribution of each language",
    width=800, height=600
)

fig.update_layout(xaxis_categoryorder = 'total descending')

fig.show()

# %% [markdown]
# ### d) HBase - 4/20
#
# In the lecture and exercise of week-4, you have learnt what's HBase, how to create an Hbase table and how to create an external Hive table on top of the HBase table. Now, let's try to save the results of question c) into HBase, such that each entry looks like:
# ```
# (b'PIE', {b'cf1:total_count': b'31415926', b'cf2:langs': b'ja,en,ko,fr'})
# ``` 
# where the key is the hashtag, `total_count` is the total count of the hashtag, and `langs` is a string of  unique language abbreviations concatenated with commas. 
#
# __Note:__
# - To accomplish the task, you need to follow these steps:
#     - Create an Hbase table called `twitter_hbase`, in **your hbase namespace**, with two column families and fields (cf1, cf2)
#     - Create an external Hive table called `twitter_hive_on_hbase` on top of the Hbase table. 
#     - Populate the HBase table with the results of question c).
# - You may find function `concat_ws` and `collect_list` useful.

# %%
import happybase
hbaseaddr = os.environ['HBASE_SERVER']
hbase_connection = happybase.Connection(hbaseaddr, transport='framed',protocol='compact')

# %%
try:
    hbase_connection.delete_table('{0}:twitter_hbase'.format(username),disable=True)
except Exception as e:
    print(e.message)
    pass

# %%
hbase_connection.create_table(
    '{0}:twitter_hbase'.format(username),
    {'cf1': dict(max_versions=10),
     'cf2': dict()
    }
)

# %%
query = """
DROP TABLE {0}.twitter_hbase
""".format(username)
cur.execute(query)

# %%
query = """
DROP TABLE {0}.twitter_hive_on_hbase
""".format(username)
cur.execute(query)
query = """
CREATE EXTERNAL TABLE {0}.twitter_hive_on_hbase(
    RowKey string,
    total_count STRING,
    langs STRING
) 
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
    "hbase.columns.mapping"=":key,cf1:total_count,cf2:langs"
)
TBLPROPERTIES(
    "hbase.table.name"="{0}:twitter_hbase",
    "hbase.mapred.output.outputtable"="{0}:twitter_hbase"
)
""".format(username)
cur.execute(query)

# %%
query="""
INSERT OVERWRITE TABLE {0}.twitter_hive_on_hbase
(
    select
         hashtag as RowKey,
         min(total_count),
         concat_ws(',', collect_list(lang)) as langs
    FROM {0}.tag_count_join
    GROUP BY hashtag
)
""".format(username)
cur.execute(query)

# %%
for r in hbase_connection.table('{0}:twitter_hbase'.format(username)).scan():
    print(r)

# %% [markdown]
# # That's all, folks!
