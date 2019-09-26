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
root@333e83d87020:~# python main.py
19/09/26 14:55:22 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
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
 |-- yield: long (nullable = true)

Sample:
Row(crop='corn', year=2010, farm='Farm-0', field='Field-0', raster=0, yield=32)
Row(crop='corn', year=2010, farm='Farm-0', field='Field-0', raster=1, yield=26)
Row(crop='corn', year=2010, farm='Farm-0', field='Field-0', raster=2, yield=34)
Row(crop='corn', year=2010, farm='Farm-0', field='Field-0', raster=3, yield=26)
Row(crop='corn', year=2010, farm='Farm-0', field='Field-0', raster=4, yield=29)
Row(crop='corn', year=2010, farm='Farm-0', field='Field-0', raster=5, yield=31)
Row(crop='corn', year=2010, farm='Farm-0', field='Field-0', raster=6, yield=35)
Row(crop='corn', year=2010, farm='Farm-0', field='Field-0', raster=7, yield=34)
Row(crop='corn', year=2010, farm='Farm-0', field='Field-0', raster=8, yield=34)
Row(crop='corn', year=2010, farm='Farm-0', field='Field-0', raster=9, yield=33)

Finding unique years/crops.

Crops: corn, soy_beans, wheat
Years: 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019

Analyzing yearly yields.

      corn  soy_beans  wheat
2010  26.0       21.9   31.1
2011  22.6       25.0   28.9
2012  22.0       22.7   27.8
2013  20.7       23.1   23.6
2014  23.4       17.5   25.4
2015  18.7       24.4   28.5
2016  22.2       18.8   30.6
2017  28.3       15.4   30.4
2018  26.2       20.6   24.4
2019  26.2       18.5   29.7
root@333e83d87020:~#
```

## PySpark Code for Analysis

```
def filter_and_get_avg(df, year, crop):
    # https://stackoverflow.com/a/32550907
    filtered_df = df.filter(df['year'] == year).filter(df['crop'] == crop)
    crop_avg = round(filtered_df.agg(avg(col("yield"))).rdd.map(lambda r: r[0]).collect()[0], 1)
    return crop_avg


def show_yearly_averages(df):
    print("\nFinding unique years/crops.\n")
    crops = sorted(df.select('crop').distinct().rdd.map(lambda r: r[0]).collect())
    years = sorted(df.select('year').distinct().rdd.map(lambda r: r[0]).collect())

    print("Crops: " + ", ".join([str(item) for item in crops]))
    print("Years: " + ", ".join([str(item) for item in years]))

    print("\nAnalyzing yearly yields.\n")

    data = []
    for year in years:
        year_data = {}
        for crop in crops:
            year_data[crop] = filter_and_get_avg(df, year, crop)
        data.append(year_data)

    print(pd.DataFrame(data, index=years))
```
