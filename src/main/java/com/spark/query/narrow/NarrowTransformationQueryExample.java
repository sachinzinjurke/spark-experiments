package com.spark.query.narrow;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class NarrowTransformationQueryExample {

    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder()
                .appName("Query Plans")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> customers = spark.read()
                .parquet("C:\\interview-workspace\\spark-experiments\\src\\main\\resources\\datasets\\customers.parquet");

        customers= customers.filter(customers.col("city").equalTo("boston"))
                .withColumn("split",split(col("name")," "))
                .withColumn("first_name",col("split").getItem(0))
                .withColumn("last_name",col("split").getItem(1))
                .withColumn("age",col("age").$plus(lit(100)))
                .drop(col("split"));

        customers.write().mode("overwrite").format("noop").save("C:\\interview-workspace\\spark-experiments\\src\\main\\resources\\output");

        try {
            Thread.sleep(5000000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
