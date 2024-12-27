import sys
from operator import add
from random import random
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from utils import FS, SPARK_ENV, get_spark_context

def calc_pi(spark: SparkSession):
    """
        Usage: pi [partitions]
    """
    sc = spark.sparkContext

    partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 2
    n = 1000000 * partitions

    def f(_: int) -> float:
        x = random() * 2 - 1
        y = random() * 2 - 1
        return 1 if x ** 2 + y ** 2 <= 1 else 0

    count = sc.parallelize(range(1, n + 1), partitions).map(f).reduce(add)
    print("Pi is roughly %f" % (4.0 * count / n))

    spark.stop()
    
def calc_power(spark):
    def get_files(base_path: str = FS) -> str:
        return f"{base_path}/data/avro/topics/power_system_right_now/partition=0/*.avro"
    
    def get_ouput_path(base_path: str = FS) -> str:
        return f"{base_path}/data/results/topics/power_system_right_now/partition=0/summarized"

    try:
        # Read the data from HDFS
        df = spark.read.format("avro").load(get_files())
        
        # Select the columns to work with
        df = df.select(
            "Minutes1UTC",
            "CO2Emission",
            "ProductionGe100MW",
            "ProductionLt100MW",
            "SolarPower",
            "OffshoreWindPower",
            "OnshoreWindPower"
        )
        
        # Create new columns to store the result of the calculations
        df = df.withColumn("TotalGreenPower", F.col("SolarPower") + F.col("OffshoreWindPower") + F.col("OnshoreWindPower"))
        df = df.withColumn("TotalPower", F.col("ProductionGe100MW") + F.col("ProductionLt100MW") + F.col("TotalGreenPower"))
        
        # Calculate the percentage of green power
        df = df.withColumn("GreenPowerPercentage", F.col("TotalGreenPower") / F.col("TotalPower") * 100)
        
        # Total calculations
        
        # Calculate the CO2 emission
        total_co2_emission = df.agg(F.sum("CO2Emission")).collect()[0][0]
        average_co2_emission = df.agg(F.avg("CO2Emission")).collect()[0][0]
        max_co2_emission = df.agg(F.max("CO2Emission")).collect()[0][0]
        min_co2_emission = df.agg(F.min("CO2Emission")).collect()[0][0]
        print(f"Total CO2 emission: {total_co2_emission}, with an average of {average_co2_emission}, max of {max_co2_emission}, and min of {min_co2_emission}")
        
        # Calculate the green power
        total_green_power = df.agg(F.sum("TotalGreenPower")).collect()[0][0]
        average_green_power = df.agg(F.avg("TotalGreenPower")).collect()[0][0]
        max_green_power = df.agg(F.max("TotalGreenPower")).collect()[0][0]
        min_green_power = df.agg(F.min("TotalGreenPower")).collect()[0][0]
        print(f"Total green power: {total_green_power} with an average of {average_green_power}, max of {max_green_power}, and min of {min_green_power}")
        
        # Calculate the total power
        total_power = df.agg(F.sum("TotalPower")).collect()[0][0]
        average_power = df.agg(F.avg("TotalPower")).collect()[0][0]
        max_power = df.agg(F.max("TotalPower")).collect()[0][0]
        min_power = df.agg(F.min("TotalPower")).collect()[0][0]
        print(f"Total power: {total_power} with an average of {average_power}, max of {max_power}, and min of {min_power}")
        
        # Calculate the percentage of green power
        green_power_percentage = total_green_power / total_power * 100
        average_green_power_percentage = df.agg(F.avg("GreenPowerPercentage")).collect()[0][0]
        max_green_power_percentage = df.agg(F.max("GreenPowerPercentage")).collect()[0][0]
        min_green_power_percentage = df.agg(F.min("GreenPowerPercentage")).collect()[0][0]
        print(f"Green power percentage: {green_power_percentage} with an average of {average_green_power_percentage}, max of {max_green_power_percentage}, and min of {min_green_power_percentage}")
        
        timestamp = datetime.now().strftime("%Y-%m-%dT%H_%M_%S")
        
        # Persist the result to a new DataFrame
        result_df = spark.createDataFrame([
            ("Total CO2 emission", total_co2_emission, average_co2_emission, max_co2_emission, min_co2_emission, timestamp),
            ("Total green power", total_green_power, average_green_power, max_green_power, min_green_power, timestamp),
            ("Total power", total_power, average_power, max_power, min_power, timestamp),
            ("Green power percentage", green_power_percentage, average_green_power_percentage, max_green_power_percentage, min_green_power_percentage, timestamp)
        ], ["Metric", "Total", "Average", "Max", "Min", "timestamp"])
        
        
        # Get total number of rows
        total_rows = df.count()
        print(f"Total rows: {total_rows}")
                
        # Display the first 5 rows of the DataFrame
        print("Compute done")
        result_df.show(4)
        
        # Store the data in hdfs
        result_df.write.mode("append").format("avro").save(f"{get_ouput_path()}/summarized-{timestamp}")

    except Exception as e:
        print(f"Error computing: {e}")
        spark.stop()
        exit(0)

    # Stop the Spark session
    spark.stop()

def calculate(spark: SparkSession, system: str):
    if system == "pi":
        calc_pi(spark)
    elif system == "power":
        calc_power(spark)
    else:
        print(f"System {system} not found")
        spark.stop()
        exit(0)

if __name__ == "__main__":
    system = "power"
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    start_time = datetime.now()
    
    spark = get_spark_context(app_name=f"Calculate {system} - {timestamp}", config=SPARK_ENV.K8S)
    
    calculate(spark, system)
    
    end_time = datetime.now()
    
    print(f"Start time: {start_time}")
    print(f"End time: {end_time}")
    print(f"Total time: {end_time - start_time}")
    