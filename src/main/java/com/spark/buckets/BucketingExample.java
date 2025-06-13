package com.spark.buckets;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class BucketingExample {

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder()
                .appName("Bucketing")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> orders = spark.read()
                .option("header", "True")
                .option("inferSchema", "True")
                .csv("C:\\interview-workspace\\spark-experiments\\src\\main\\resources\\bucketing\\orders.csv");

        Dataset<Row> products = spark.read()
                .option("header", "True")
                .option("inferSchema", "True")
                .csv("C:\\interview-workspace\\spark-experiments\\src\\main\\resources\\bucketing\\products.csv");

        orders.show(5);
        products.show(50);
    }
}
