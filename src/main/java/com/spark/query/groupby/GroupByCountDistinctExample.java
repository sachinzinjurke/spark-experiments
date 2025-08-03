package com.spark.query.groupby;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class GroupByCountDistinctExample {

    public static void main(String[] args) throws InterruptedException {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder()
                .appName("Group By Example")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> transactions = spark.read()
                .parquet("C:\\interview-workspace\\spark-experiments\\src\\main\\resources\\datasets\\transactions.parquet");

        transactions.show(5);


        Dataset<Row> grpCustCountDistinctCity = transactions
                .groupBy(col("cust_id"))
                .agg(countDistinct("city"));

        grpCustCountDistinctCity.printSchema();
        grpCustCountDistinctCity.show();
        Thread.sleep(1000000);
    }
}
