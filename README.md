# Climate PySpark Demo

## Description

This demo was created to learn more about and get hands on experience with PySpark.

In this demo, fake crop data is created and analyzed to find average yields among years/farms/crops as well as outliers.


## Docker
   
  * Build container: `docker build -t pyspark .`
  * Start container: `run-container.bat`
  

## Create fake data and save to `data.parquet`.

```

root@05a651babe30:~# python create_data.py

Creating fake data.
        Years: 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019
        Crops: corn, soy_beans, wheat
        Num farms: 10
        Num fields per farm: 20
        Num rasters per field: 50

Data created!

Sample:
             crop  year    farm     field  raster  yield
193200  soy_beans  2019  Farm-3   Field-4       0  42.69
29270        corn  2012  Farm-9   Field-5      20  39.01
82172        corn  2018  Farm-2   Field-3      22  47.05
213106      wheat  2011  Farm-3   Field-2       6  48.53
33810        corn  2013  Farm-3  Field-16      10  45.37

Data saved to 'data.parquet'.

```


## Load `data.parquet` and do analysis.

```

root@05a651babe30:~# python main.py
19/10/01 19:39:18 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Loading data from 'data.parquet'.

DataFrame schema:
Sample:
   crop  year    farm    field  raster  yield
0  corn  2010  Farm-0  Field-0       0  45.90
1  corn  2010  Farm-0  Field-0       2  52.28
2  corn  2010  Farm-0  Field-0       4  49.58
3  corn  2010  Farm-0  Field-0       5  46.03
4  corn  2010  Farm-0  Field-0       6  45.56

Analyzing yield by year across all data.

      avg(yield)
year
2010        54.0
2011        47.1
2012        46.4
2013        48.9
2014        48.3
2015        48.3
2016        50.6
2017        57.1
2018        54.6
2019        44.1

Analyzing yield by farm across all data.

        avg(yield)
farm
Farm-0        47.2
Farm-1        48.4
Farm-2        51.1
Farm-3        47.5
Farm-4        51.8
Farm-5        52.8
Farm-6        48.9
Farm-7        55.0
Farm-8        49.9
Farm-9        46.9

Analyzing yield by crop across all data.

           avg(yield)
crop
corn             48.0
soy_beans        51.7
wheat            50.2

Outlier threshold (mean - 3 x std): 30.5
Mean yield: 50.0

Outliers (182 out of 300000):
Sample:
   crop  year    farm     field  raster  yield
0  corn  2011  Farm-1  Field-16       6  29.70
1  corn  2011  Farm-1  Field-16      15  29.54
2  corn  2011  Farm-3   Field-7       4  29.26
3  corn  2011  Farm-3  Field-13       6  29.77
4  corn  2011  Farm-3  Field-13      22  30.13

Outliers by crop:
           count
crop
corn         145
soy_beans      7
wheat         30

Outliers by year:
      count
year
2011     16
2012     22
2013      1
2019    143

Outliers by farm:
        count
farm
Farm-0     38
Farm-1     45
Farm-2      2
Farm-3     33
Farm-6      5
Farm-8      4
Farm-9     55

Duration: 15.1 seconds

```

## PySpark Code for Analysis

```
def get_all_harvest_df():
    # Use this for docker master/workers
    #sc = SparkContext(master="spark://my-spark-master:7077")
    sc = SparkContext()
    sqlContext = SQLContext(sc)
    print("Loading data from 'data.parquet'.")
    df = sqlContext.read.parquet('data.parquet')
    return df


def show_df_summary(df):
    print("\nDataFrame schema:")
    #df.printSchema()
    print("Sample:")
    samples = df.sample(fraction=0.5).limit(5).collect()
    print(pd.DataFrame([row.asDict() for row in samples]))


def show_average_by_field(all_harvest_df, field_name):
    print(f"\nAnalyzing yield by {field_name} across all data.\n")
    # Used to do double for-loop here.
    df_filtered = all_harvest_df.groupBy(field_name).agg({'yield': 'avg'}).orderBy(field_name)
    pd_df = df_filtered.toPandas().round(1)
    pd_df.set_index(field_name, inplace=True)
    print(pd_df)


def find_outliers_by_field(outliers, field_name):
    df_counts = outliers.orderBy(field_name).groupBy(field_name).count()
    pd_df_counts = df_counts.toPandas()
    pd_df_counts.set_index(field_name, inplace=True)
    print(f"\nOutliers by {field_name}:")
    print(pd_df_counts)


def get_mean_and_std(all_harvest_df):
    # https://stackoverflow.com/a/47995478
    df_stats = all_harvest_df.select(
        _mean(col('yield')).alias('mean'),
        _stddev(col('yield')).alias('std')
    ).collect()
    mean = df_stats[0]['mean']
    std = df_stats[0]['std']
    return mean, std


def find_outliers(all_harvest_df):
        mean, std = get_mean_and_std(all_harvest_df)
        min_yield = round(mean - OUTLIER_STDDEV_MULT * std, 1)
        print(f"\nOutlier threshold (mean - {OUTLIER_STDDEV_MULT} x std): {min_yield}")
        print(f"Mean yield: {round(mean, 1)}\n")
        outliers = all_harvest_df.filter(all_harvest_df['yield'] < min_yield)
        print(f"Outliers ({outliers.count()} out of {all_harvest_df.count()}):")
        print("Sample:")
        print(outliers.toPandas().head(5))

        find_outliers_by_field(outliers, "crop")
        find_outliers_by_field(outliers, "year")
        find_outliers_by_field(outliers, "farm")


def show_averages(df):
    show_average_by_field(df, "year")
    show_average_by_field(df, "farm")
    show_average_by_field(df, "crop")


def main():
    t = time()

    all_harvest_df = get_all_harvest_df()
    show_df_summary(all_harvest_df)
    show_averages(all_harvest_df)
    find_outliers(all_harvest_df)

    print(f"\nDuration: {round(time()-t, 1)} seconds\n")
```
