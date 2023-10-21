from pyspark.sql import SparkSession
import pandas as pd
import os
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("ExcelReadExample").getOrCreate()

# Read the Excel files into dataframes
file1 = "/Users/suthan/Source/Agex/source/pacs/pacs1.xlsx"
file2 = "/Users/suthan/Source/Agex/source/dccb/dccb1.xlsx"
df1 = pd.read_excel(file1)
df2 = pd.read_excel(file2)

# Convert the pandas DataFrame to a PySpark DataFrame
df1 = spark.createDataFrame(df1)
df2 = spark.createDataFrame(df2)

# Make the DataFrames as temporary SQL tables
df1.createOrReplaceTempView("t1")
df2.createOrReplaceTempView("t2")

# Perform a sql with the specified conditions
result = spark.sql("""
SELECT t2.`Customer Name`, MAX(t1.`Date of Birth`) AS `Date of Birth`, MAX(t1.Age) AS Age, MAX(t1.Gender) AS Gender, MAX(t1.Village) AS Village, MAX(t2.`Mobile Number`) AS `Mobile Number`, MAX(t2.`Account No`) AS `Account No`
FROM t1, t2
WHERE (t1.`Adhaar Card No` = t2.UiDAI OR 
       t1.`Member Name` = t2.`Customer Name` OR 
       (t1.`Member Name` LIKE CONCAT('%', t2.`Customer Name`, '%'))
      )
      AND t1.`Member Name` IS NOT NULL AND t2.`Customer Name` IS NOT NULL
GROUP BY t2.`Customer Name`
HAVING COUNT(*) = 1;
""")

# Show the result
result.show()

output_directory = "/Users/suthan/Source/Agex/target"

# Define the file name with .csv extension
output_file_name = "farm.csv"

# Combine the directory and file name to create the full path
output_path = os.path.join(output_directory, output_file_name)

# Write the DataFrame to the specified path
result.write.format("csv").option("header", "true").mode("overwrite").option("delimiter", ",").option("path", output_path).save()

# Stop the Spark session
spark.stop()

