# Climate PySpark Demo

## Docker
   
  * Build container: `docker build -t pyspark .`
  * Start container: `run-container.bat`
  

## Create fake data and save to `data.parquet`.

```
root@333e83d87020:~# python create_data.py

Creating data.
        Years: 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019
        Crops: corn, soy_beans, wheat
        Num farms: 10
        Num fields per farm: 20
        Num rasters per field: 50

Data created!
Sample:
             crop  year    farm     field  raster  yield
97183        corn  2019  Farm-7   Field-3      33     26
80627        corn  2018  Farm-0  Field-12      27     25
212937      wheat  2011  Farm-2  Field-18      37     32
45132        corn  2014  Farm-5   Field-2      32     21
188780  soy_beans  2018  Farm-8  Field-15      30     26

Data saved to 'data.parquet'.
root@333e83d87020:~#
```


## Load `data.parquet` and do analysis.

```

root@05a651babe30:~# python main.py
19/09/30 19:23:32 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).

DataFrame schema:
root
 |-- crop: string (nullable = true)
 |-- year: long (nullable = true)
 |-- farm: string (nullable = true)
 |-- field: string (nullable = true)
 |-- raster: long (nullable = true)
 |-- yield: double (nullable = true)

Sample:
   crop  year    farm    field  raster  yield
0  corn  2010  Farm-0  Field-0       2  56.71
1  corn  2010  Farm-0  Field-0       4  52.54
2  corn  2010  Farm-0  Field-0       5  57.58
3  corn  2010  Farm-0  Field-0       6  56.93
4  corn  2010  Farm-0  Field-0       7  50.67

Analyzing yearly yields.

      avg(yield)
year
2010        47.6
2011        47.8
2012        46.9
2013        50.2
2014        46.3
2015        47.5
2016        47.3
2017        49.0
2018        43.4
2019        44.8

Outlier threshold (mean - 3.5 x std): 25.4
Mean yield: 47.1

Outliers (35 out of 300000):
Sample:
   crop  year    farm     field  raster  yield
0  corn  2018  Farm-1  Field-11      27  24.39
1  corn  2018  Farm-2   Field-1      45  25.37
2  corn  2018  Farm-2   Field-3      19  23.60
3  corn  2018  Farm-2   Field-7      27  25.01
4  corn  2018  Farm-3   Field-2      12  20.55

Outliers by crop:
           count
crop
corn          28
soy_beans      7

Outliers by year:
      count
year
2011      2
2014      1
2015      3
2017      1
2018     27
2019      1

Outliers by farm:
        count
farm
Farm-0      3
Farm-1      1
Farm-2      5
Farm-3     13
Farm-4      2
Farm-5      1
Farm-8      3
Farm-9      7

Duration: 10.3 seconds
```

## PySpark Code for Analysis

```

def show_yearly_averages(df):
    print("\nAnalyzing yearly yields.\n")
    # Used to do double for-loop here.
    df_filtered = df.groupBy("year").agg({'yield': 'avg'}).orderBy("year")
    pd_df = df_filtered.toPandas().round(1)
    pd_df.set_index("year", inplace=True)
    print(pd_df)


def find_outliers_by_field(outliers, field_name):
    df_counts = outliers.orderBy(field_name).groupBy(field_name).count()
    pd_df_counts = df_counts.toPandas()
    pd_df_counts.set_index(field_name, inplace=True)
    print(f"\nOutliers by {field_name}:")
    print(pd_df_counts)


def get_mean_and_std(df):
    df_stats = df.select(
        _mean(col('yield')).alias('mean'),
        _stddev(col('yield')).alias('std')
    ).collect()
    mean = df_stats[0]['mean']
    std = df_stats[0]['std']
    return mean, std


def find_outliers(df):
        mean, std = get_mean_and_std(df)
        min_yield = round(mean - OUTLIER_STDDEV_MULT * std, 1)
        print(f"\nOutlier threshold (mean - {OUTLIER_STDDEV_MULT} x std): {min_yield}")
        print(f"Mean yield: {round(mean, 1)}\n")
        outliers = df.filter(df['yield'] < min_yield)
        rows = outliers.collect()
        count = df.count()
        print(f"Outliers ({len(rows)} out of {count}):")
        print("Sample:")
        print(pd.DataFrame([row.asDict() for row in rows]).head(5))

        find_outliers_by_field(outliers, "crop")
        find_outliers_by_field(outliers, "year")
        find_outliers_by_field(outliers, "farm")
```
