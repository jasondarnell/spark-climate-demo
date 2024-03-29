
from time import time
import pandas as pd
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, avg, mean as _mean, stddev as _stddev


OUTLIER_STDDEV_MULT = 3


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


def find_unique(df, field_name):
    # Don't need this anymore. It is slow anyways.
    print(f"Finding unique {field_name}s.")
    #values = sorted(df.select(field_name).distinct().rdd.map(lambda r: r[0]).collect())
    values = sorted(df[(field_name,)].distinct().rdd.map(lambda r: r[0]).collect())
    print(f"{field_name.title()}s: " + ", ".join([str(item) for item in values]))
    return values


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


if __name__ == "__main__":
    main()