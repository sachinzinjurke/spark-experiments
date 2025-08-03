package com.spark.query.join;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class ExplicitBroadCastJoinExample {

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder()
                .appName("Query Plans For Join")
                .master("local[*]")
                .getOrCreate();

        spark.conf().set("spark.sql.autoBroadcastJoinThreshold", 10485760);

        Dataset<Row> transactions = spark.read()
                .parquet("C:\\interview-workspace\\spark-experiments\\src\\main\\resources\\datasets\\transactions.parquet");

        Dataset<Row> customers = spark.read()
                .parquet("C:\\interview-workspace\\spark-experiments\\src\\main\\resources\\datasets\\customers.parquet");

        customers= customers.filter(customers.col("city").equalTo("boston"))
                .withColumn("split",split(col("name")," "))
                .withColumn("first_name",col("split").getItem(0))
                .withColumn("last_name",col("split").getItem(1))
                .drop(col("split"));

        Dataset<Row> broadCastJoin = transactions.
                join(broadcast(customers), transactions.col("cust_id").equalTo(customers.col("cust_id")),"inner");

        broadCastJoin.write().mode("overwrite").format("noop").save("C:\\interview-workspace\\spark-experiments\\src\\main\\resources\\output");

        try {
            Thread.sleep(5000000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
