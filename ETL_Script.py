import os
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, DataFrame, functions as F, Window
from sqlalchemy import create_engine

class Config:
    def __init__(self):
        self.aws_access_key = ''
        self.aws_secret_key = ''
        # Debug prints
        if not self.aws_access_key or not self.aws_secret_key:
            print("AWS Access Key or Secret Key not set in environment variables.")

        self.s3_bucket = ""
        self.mysql_url = ''
        self.driver = ""
        self.user = ''
        self.password = ''
        self.table_name = ""
        self.output_csv = ""

class ETL:
    def __init__(self, config: Config):
        self.config = config
        self.spark = self.create_spark_session()

    def create_spark_session(self):
        return SparkSession.builder \
            .appName("ETL Project") \
            .config("spark.executor.memory", "4g") \
            .config("spark.driver.memory", "4g") \
            .config("spark.jars", "/Users/dangthiphuongthao/Documents/Apps/spark-3.5.1-bin-hadoop3/jars/hadoop-aws-3.3.4.jar,/Users/dangthiphuongthao/Documents/Apps/spark-3.5.1-bin-hadoop3/jars/aws-java-sdk-bundle-1.12.262.jar,/Users/dangthiphuongthao/Documents/Apps/spark-3.5.1-bin-hadoop3/jars/mysql-connector-java-8.0.25.jar") \
            .config("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("fs.s3a.access.key", self.config.aws_access_key) \
            .config("fs.s3a.secret.key", self.config.aws_secret_key) \
            .getOrCreate()

    def process_data(self, path: str) -> DataFrame:
        print(f"Processing data for {path}...")
        df = self.spark.read.json(path)
        df = df.select('_source.*')
        date = F.regexp_extract(F.lit(path), r'(\d{8})', 1)
        df = df.withColumn("date", F.to_date(date, "yyyyMMdd"))
        return df

    def count_devices(self, df: DataFrame) -> DataFrame:
        print("Counting devices...")
        return df.groupBy('Contract').agg(F.countDistinct('Mac').alias('TotalDevices'))

    def categorize_app_name(self, df: DataFrame) -> DataFrame:
        return df.withColumn("Type", F.when(F.col("AppName") == "CHANNEL", "Truyen Hinh")
            .when(F.col("AppName") == "RELAX", "Giai Tri")
            .when(F.col("AppName") == "CHILD", "Thieu Nhi")
            .when((F.col("AppName") == "FIMS") | (F.col("AppName") == "VOD"), "Phim Truyen")
            .when((F.col("AppName") == "KPLUS") | (F.col("AppName") == "SPORT"), "The Thao")) \
            .select('Contract', 'Type', 'TotalDuration', 'date') \
            .filter((F.col("Contract") != '0') & (F.col("Type").isNotNull()))

    def calculate_most_watched(self, df: DataFrame) -> DataFrame:
        return df.withColumn("MostWatch", F.greatest(F.col("Giai Tri"), F.col("Phim Truyen"),
                                                      F.col("The Thao"), F.col("Thieu Nhi"), 
                                                      F.col("Truyen Hinh"))) \
            .withColumn("MostWatch", 
                        F.when(F.col("MostWatch") == F.col("Truyen Hinh"), "Truyen Hinh")
                        .when(F.col("MostWatch") == F.col("Phim Truyen"), "Phim Truyen")
                        .when(F.col("MostWatch") == F.col("The Thao"), "The Thao")
                        .when(F.col("MostWatch") == F.col("Thieu Nhi"), "Thieu Nhi")
                        .when(F.col("MostWatch") == F.col("Giai Tri"), "Giai Tri"))

    def find_activeness(self, df: DataFrame) -> DataFrame:
        windowspec = Window.partitionBy("Contract").orderBy("date")
        df = df.withColumn("Activeness", F.count("date").over(windowspec)) \
            .withColumn("Activeness", F.when(F.col("Activeness") > 4, "High")
                        .when((F.col("Activeness") > 2) & (F.col("Activeness") <= 4), "Medium")
                        .otherwise("Low"))
        df = df.groupBy("Contract").agg(
            F.sum("Giai Tri").alias("Total_Giai_Tri"),
            F.sum("Phim Truyen").alias("Total_Phim_Truyen"),
            F.sum("The Thao").alias("Total_The_Thao"),
            F.sum("Thieu Nhi").alias("Total_Thieu_Nhi"),
            F.sum("Truyen Hinh").alias("Total_Truyen_Hinh"),
            F.first("MostWatch").alias("MostWatch"),
            F.first("Activeness").alias("Active"))

        return df
    def transform_data(self, df: DataFrame) -> DataFrame:
        print("transforming data...")
        df = self.categorize_app_name(df)
        df = df.groupBy("Contract", "date").pivot("Type").sum("TotalDuration").fillna(0)
        df = self.calculate_most_watched(df)
        df = self.find_activeness(df)
        return df

    def run_etl_for_day(self, day: int) -> DataFrame:
        print("etl data for each day...")
        path_day = f"202204{day:02d}.json"
        path = f"{self.config.s3_bucket}{path_day}"
        df = self.process_data(path)
        df = df.join(self.count_devices(df), on="Contract", how="left")
        df = self.transform_data(df)
        # Cache the DataFrame if it's going to be reused
        df.cache()
        df.printSchema()
        return df

    def save_to_csv(self, df: DataFrame):
        print("Saving data to CSV...")
        pandas_df = df.toPandas()
        pandas_df.to_csv(self.config.output_csv, index=False)
        print(f"Data saved to {self.config.output_csv}")

    def run(self):
        daily_dfs = [self.run_etl_for_day(day) for day in range(1, 3)]
        combined_df = daily_dfs[0]
        for df in daily_dfs[1:]:
            combined_df = combined_df.unionByName(df)
        self.save_to_csv(combined_df)
        self.load_to_mysql(combined_df)
        

    def load_to_mysql(self, df: DataFrame):
        print("Loading data to MySQL...")
        
        df.write \
            .format('jdbc') \
            .option('url', self.config.mysql_url ) \
            .option('driver', self.config.driver) \
            .option('dbtable', self.config.table_name) \
            .option('user', self.config.user) \
            .option('password', self.config.password) \
            .mode('overwrite') \
            .save()
# Main execution
if __name__ == "__main__":
    config = Config()
    etl_process = ETL(config)
    etl_process.run()
