package com.spark.query.groupby;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class GroupByExample {

    public static void main(String[] args) throws InterruptedException {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder()
                .appName("Group By Example")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> customers = spark.read()
                .parquet("C:\\interview-workspace\\spark-experiments\\src\\main\\resources\\datasets\\customers.parquet");

        Dataset<Row> groupedCustDF = customers
                .groupBy(col("cust_id"))
                .count();

        groupedCustDF.printSchema();
        groupedCustDF.show();
        Thread.sleep(1000000);
    }
}
