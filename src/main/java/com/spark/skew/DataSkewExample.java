package com.spark.skew;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;
public class DataSkewExample {

    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder()
                .appName("Data SKEW")
                .master("local[*]")
                .getOrCreate();
        spark.conf().set("spark.sql.autoBroadcastJoinThreshold", -1);
        //40 M rows
        Dataset<Row> transactions = spark.read()
                .parquet("C:\\interview-workspace\\spark-experiments\\src\\main\\resources\\datasets\\transactions.parquet");

        //5K rows
        Dataset<Row> customers = spark.read()
                .parquet("C:\\interview-workspace\\spark-experiments\\src\\main\\resources\\datasets\\customers.parquet");

        transactions
                .groupBy(col("cust_id"))
                        .agg(countDistinct("txn_id").alias("txn_distinct_count"))
                                .orderBy(desc("txn_distinct_count"))
                                        .show(5);

        System.out.println(transactions.count());
        transactions.show(5);
        System.out.println(customers.count());
        customers.show(5);
    }
}
