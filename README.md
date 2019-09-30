# Climate PySpark Demo

## Description

This demo was created to learn more about and get hands on experience with PySpark.

In this demo, fake crop data is created and analyzed to find average yields among years/farms/crops as well as outliers.


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
19/09/30 20:59:07 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
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
0  corn  2010  Farm-0  Field-0       0  54.62
1  corn  2010  Farm-0  Field-0       2  55.39
2  corn  2010  Farm-0  Field-0       3  54.92
3  corn  2010  Farm-0  Field-0       8  59.57
4  corn  2010  Farm-0  Field-0       9  55.68

Analyzing yield by year.

      avg(yield)
year
2010        51.6
2011        55.2
2012        52.6
2013        53.9
2014        53.1
2015        50.1
2016        54.7
2017        48.2
2018        51.4
2019        49.3

Analyzing yield by farm.

        avg(yield)
farm
Farm-0        52.3
Farm-1        53.1
Farm-2        51.7
Farm-3        52.0
Farm-4        51.8
Farm-5        51.6
Farm-6        51.5
Farm-7        51.8
Farm-8        52.0
Farm-9        52.2

Analyzing yield by crop.

           avg(yield)
crop
corn             55.5
soy_beans        51.5
wheat            49.0

Outlier threshold (mean - 3.5 x std): 28.8
Mean yield: 52.0

Outliers (234 out of 300000):
Sample:
        crop  year    farm     field  raster  yield
0  soy_beans  2014  Farm-7   Field-7      12  27.78
1  soy_beans  2014  Farm-7   Field-7      21  27.57
2  soy_beans  2014  Farm-7  Field-14      14  28.60
3      wheat  2017  Farm-0   Field-9       8  27.99
4      wheat  2017  Farm-0   Field-9      15  28.18

Outliers by crop:
           count
crop
soy_beans      3
wheat        231

Outliers by year:
      count
year
2014      3
2017     55
2019    176

Outliers by farm:
        count
farm
Farm-0    165
Farm-1      1
Farm-2     29
Farm-3      7
Farm-4      9
Farm-5      3
Farm-7     11
Farm-8      3
Farm-9      6

Duration: 13.3 seconds

```

## PySpark Code for Analysis

```

def show_average_by_field(df, field_name):
    print(f"\nAnalyzing yield by {field_name}.\n")
    # Used to do double for-loop here.
    df_filtered = df.groupBy(field_name).agg({'yield': 'avg'}).orderBy(field_name)
    pd_df = df_filtered.toPandas().round(1)
    pd_df.set_index(field_name, inplace=True)
    print(pd_df)


def find_outliers_by_field(outliers, field_name):
    df_counts = outliers.orderBy(field_name).groupBy(field_name).count()
    pd_df_counts = df_counts.toPandas()
    pd_df_counts.set_index(field_name, inplace=True)
    print(f"\nOutliers by {field_name}:")
    print(pd_df_counts)


def get_mean_and_std(df):
    # https://stackoverflow.com/a/47995478
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
        print(f"Outliers ({outliers.count()} out of {df.count()}):")
        print("Sample:")
        print(outliers.toPandas().head(5))

        find_outliers_by_field(outliers, "crop")
        find_outliers_by_field(outliers, "year")
        find_outliers_by_field(outliers, "farm")

```
