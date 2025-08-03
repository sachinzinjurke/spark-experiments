package com.spark.query.join;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.split;

public class QueryPlanForJoinWithAQEOptimization {

    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder()
                .appName("Query Plans For Join")
                .config("spark.sql.adaptive.enabled", "true") // Enable AQE
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") // Enable partition coalescing
                .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB") // Set target partition size
                .config("spark.sql.adaptive.skewJoin.enabled", "true") // Enable skew join optimization
                .master("local[*]")
                .getOrCreate();

        //This property will prevent auto broadcast join happening and will force for Sort Merge join by shuffeling data
        spark.conf().set("spark.sql.autoBroadcastJoinThreshold", -1);

        Dataset<Row> transactions = spark.read()
                .parquet("C:\\interview-workspace\\spark-experiments\\src\\main\\resources\\datasets\\transactions.parquet");

        Dataset<Row> customers = spark.read()
                .parquet("C:\\interview-workspace\\spark-experiments\\src\\main\\resources\\datasets\\customers.parquet");

        customers= customers.filter(customers.col("city").equalTo("boston"))
                .withColumn("split",split(col("name")," "))
                .withColumn("first_name",col("split").getItem(0))
                .withColumn("last_name",col("split").getItem(1))
                .drop(col("split"));

       /* transactions.show(5);
        customers.show(5);*/

        Dataset<Row> join = transactions
                .join(customers, transactions.col("cust_id").equalTo(customers.col("cust_id")), "inner");
        join.show(2);

        try {
            Thread.sleep(5000000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
