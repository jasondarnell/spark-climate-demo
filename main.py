import pandas as pd
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, avg, mean as _mean, stddev as _stddev


def get_df():
    #sc = SparkContext(master="spark://my-spark-master:7077")
    sc = SparkContext()
    sqlContext = SQLContext(sc)
    df = sqlContext.read.parquet('data.parquet')
    return df


def show_df_summary(df):
    print("\nDataFrame schema:")
    df.printSchema()
    print("Sample:")
    samples = df.sample(fraction=0.5).limit(10).collect()
    print(pd.DataFrame([row.asDict() for row in samples]))
    #for sample in samples:
    #    print(str(sample))


def filter_and_get_avg(df, year, crop):
    # https://stackoverflow.com/a/32550907
    filtered_df = df.filter(df['year'] == year).filter(df['crop'] == crop)
    crop_avg = round(filtered_df.agg(avg(col("yield"))).rdd.map(lambda r: r[0]).collect()[0], 1)
    return crop_avg


def get_crops_and_years(df):
    print("\nFinding unique years/crops.\n")
    crops = sorted(df.select('crop').distinct().rdd.map(lambda r: r[0]).collect())
    years = sorted(df.select('year').distinct().rdd.map(lambda r: r[0]).collect())

    print("Crops: " + ", ".join([str(item) for item in crops]))
    print("Years: " + ", ".join([str(item) for item in years]))

    return crops, years

def get_farms(df):
    print("\nFinding unique farms.\n")
    farms = sorted(df.select('farm').distinct().rdd.map(lambda r: r[0]).collect())
    return farms

def find_unique(df, field_name):
    print(f"Finding unique {field_name}s.")
    values = sorted(df.select(field_name).distinct().rdd.map(lambda r: r[0]).collect())
    print(f"{field_name.title()}s: " + ", ".join([str(item) for item in values]))
    return values

def show_yearly_averages(df, crops, years):
    print("\nAnalyzing yearly yields.\n")

    data = []
    for year in years:
        year_data = {}
        for crop in crops:
            year_data[crop] = filter_and_get_avg(df, year, crop)
        data.append(year_data)

    print(pd.DataFrame(data, index=years))


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
        min_yield = round(mean - 3.5 * std, 1)
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
    df = get_df()
    #crops, years = get_crops_and_years(df)
    crops = find_unique(df, "crop")
    years = find_unique(df, "year")
    farms = find_unique(df, "farm")

    show_df_summary(df)
    show_yearly_averages(df, crops, years)
    find_outliers(df, crops, years, farms)


if __name__ == "__main__":
    main()