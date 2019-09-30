

from time import time
import pandas as pd
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, avg, mean as _mean, stddev as _stddev


OUTLIER_STDDEV_MULT = 3.5


def get_df():
    # Use this for docker master/workers
    #sc = SparkContext(master="spark://my-spark-master:7077")
    sc = SparkContext()
    sqlContext = SQLContext(sc)
    df = sqlContext.read.parquet('data.parquet')
    return df


def show_df_summary(df):
    print("\nDataFrame schema:")
    df.printSchema()
    print("Sample:")
    samples = df.sample(fraction=0.5).limit(5).collect()
    print(pd.DataFrame([row.asDict() for row in samples]))


def find_unique(df, field_name):
    print(f"Finding unique {field_name}s.")
    #values = sorted(df.select(field_name).distinct().rdd.map(lambda r: r[0]).collect())
    values = sorted(df[(field_name,)].distinct().rdd.map(lambda r: r[0]).collect())
    print(f"{field_name.title()}s: " + ", ".join([str(item) for item in values]))
    return values


def show_yearly_averages(df, crops, years):
    print("\nAnalyzing yearly yields.\n")
    # Used to do double for-loop here.
    df_filtered = df.groupBy("year").agg({'yield': 'avg'})
    pd_df = df_filtered.toPandas().round(1)
    pd_df.set_index("year")
    print(pd_df)


def find_outliers_by_field(outliers, field_name, field_values):
    field_counts = []
    for field in field_values:
        count = outliers.filter(outliers[field_name] == field).count()
        field_counts.append({"count": count})
    print(f"\nOutliers by {field_name}:")
    print(pd.DataFrame(field_counts, index=field_values))


def find_outliers(df, crops, years, farms):
        df_stats = df.select(
            _mean(col('yield')).alias('mean'),
            _stddev(col('yield')).alias('std')
        ).collect()
        mean = df_stats[0]['mean']
        std = df_stats[0]['std']
        min_yield = round(mean - OUTLIER_STDDEV_MULT * std, 1)
        print(f"\nOutlier threshold (min yield): {min_yield}")
        print(f"Mean yield: {round(mean, 1)}\n")
        outliers = df.filter(df['yield'] < min_yield)
        rows = outliers.collect()
        count = df.count()
        print(f"Outliers ({len(rows)} out of {count}):")
        print("Sample:")
        print(pd.DataFrame([row.asDict() for row in rows]).head(5))

        find_outliers_by_field(outliers, "crop", crops)
        find_outliers_by_field(outliers, "year", years)
        find_outliers_by_field(outliers, "farm", farms)


def main():
    t = time()

    df = get_df()
    crops = find_unique(df, "crop")
    years = find_unique(df, "year")
    farms = find_unique(df, "farm")

    show_df_summary(df)
    show_yearly_averages(df, crops, years)
    find_outliers(df, crops, years, farms)

    print(f"\nDuration: {round(time()-t, 1)} seconds\n")

if __name__ == "__main__":
    main()