package com.spark.query.groupby;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class GroupBySumExample {

    public static void main(String[] args) throws InterruptedException {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder()
                .appName("Group By Example")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> transactions = spark.read()
                .parquet("C:\\interview-workspace\\spark-experiments\\src\\main\\resources\\datasets\\transactions.parquet");

        transactions.show(5);

        Dataset<Row> groupedCustDF = transactions
                .groupBy(col("city"))
                .agg(sum("amt").alias("total_amount"));

        groupedCustDF.printSchema();
        groupedCustDF.show();
        Thread.sleep(1000000);
    }
}
