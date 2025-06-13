package com.spark.practice;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkBroadCastJoin {

    public static void main(String[] args) throws InterruptedException {

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder()
                .appName("DAG Reading for wide transformation")
                .master("local[*]")
                .getOrCreate();

        spark.conf().set("spark.sql.autoBroadcastJoinThreshold", 10485760);
        Dataset<Row> transactions = spark.read()
                .parquet("C:\\interview-workspace\\spark-experiments\\src\\main\\resources\\datasets\\transactions.parquet");

        Dataset<Row> customers = spark.read()
                .parquet("C:\\interview-workspace\\spark-experiments\\src\\main\\resources\\datasets\\customers.parquet");

        Dataset<Row> joinedDF = transactions.join(customers, transactions.col("cust_id").equalTo(customers.col("cust_id")));

        joinedDF.write().mode("overwrite").format("noop").save("C:\\interview-workspace\\spark-experiments\\src\\main\\resources\\output");

        Thread.sleep(1000000);
    }
}
