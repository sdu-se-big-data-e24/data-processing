# Summarize power right now

Python Spark task, that summarize power right now to give an overview of the power system through all readings.

There are four metrics that are calculated:

- Total power
- Total green power
- Total CO2 emissions
- Green power percentage

Each with:

- Calculated timestamp
- Total
- Average
- Maximum
- Minimum

They are stored, such that each metric is a individual record in the output.  
This allows for easy addition of new metrics, without changing the output schema.

## Input

Uses all the data from the `power-right-now` data in HDFS.  
Located in `/data/avro/topics/power_system_right_now/partition=0/`

## Output

The output is stored in the `/data/results/topics/power_system_right_now/partition=0/summarized/` directory in HDFS.  
This mimics the structure of the input data, to allow for easy access to the data.
