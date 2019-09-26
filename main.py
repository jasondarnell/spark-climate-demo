import pandas as pd
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, avg


def get_df():
    sc = SparkContext()
    sqlContext = SQLContext(sc)
    df = sqlContext.read.parquet('data.parquet')
    return df


def show_df_summary(df):
    print("\nDataFrame schema:")
    df.printSchema()
    print("Sample:")
    samples = df.sample(fraction=1.0).limit(10).collect()
    for sample in samples:
        print(str(sample))


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
            filtered_df = df.filter(df['year'] == year).filter(df['crop'] == crop)
            crop_avg = round(filtered_df.agg(avg(col("yield"))).rdd.map(lambda r: r[0]).collect()[0], 1)
            year_data[crop] = crop_avg

        data.append(year_data)

    print(pd.DataFrame(data, index=years))


def main():
    df = get_df()
    show_df_summary(df)
    show_yearly_averages(df)


if __name__ == "__main__":
    main()