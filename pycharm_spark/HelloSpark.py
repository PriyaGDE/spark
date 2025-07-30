from pyspark.sql import *
if __name__ == "__main__":

    spark = SparkSession.builder \
            .appName("Hello spark") \
            .master("local[2]") \
            .getOrCreate()

    data_list = [
        ("Ravi",28),
        ("DAvid",45),
        ("Abdul",37)
    ]

    df = spark.createDataFrame(data_list).toDF("Name","Age")

    df.show()