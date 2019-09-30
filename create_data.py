import pandas as pd
from numpy.random import normal
from random import uniform, random

YEARS = range(2010, 2020)
CROPS = ["corn", "soy_beans", "wheat"]
NUM_FARMS = 10
NUM_FIELDS = 20
NUM_RASTERS = 50

disaster = lambda: True if random() > 0.995 else False
get_random = lambda: max(normal(10, 3) * (random() if disaster() else 1), 0)


def main():
    print("\nCreating data.")
    print("\tYears: " + ', '.join([str(year) for year in YEARS]))
    print("\tCrops: " + ', '.join(CROPS))
    print(f"\tNum farms: {NUM_FARMS}")
    print(f"\tNum fields per farm: {NUM_FIELDS}")
    print(f"\tNum rasters per field: {NUM_RASTERS}\n")

    data = []
    for crop in CROPS:
        rand_crop = get_random()
        for year in YEARS:
            rand_year = get_random()
            for farm_num in range(NUM_FARMS):
                rand_farm = get_random()
                farm_name = f"Farm-{farm_num}"
                for field_num in range(NUM_FIELDS):
                    rand_field = get_random()
                    field_name = f"Field-{field_num}"
                    for raster in range(NUM_RASTERS):
                        raster_yield = round((rand_crop+rand_year+rand_farm+rand_field+get_random()), 2)
                        data.append({
                            "crop": crop,
                            "year": year,
                            "farm": farm_name,
                            "field": field_name,
                            "raster": raster,
                            "yield":  raster_yield
                        })

    df = pd.DataFrame(data)
    print("Data created!\nSample:")
    print(df.sample(5))

    df.to_parquet("data.parquet")
    print("\nData saved to 'data.parquet'.")


if __name__ == "__main__":
    main()