package com.spark.query.plans;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;
public class QueryPlanExample {

    public static void main(String[] args) {


        //Syntax Check --> Unresolved Logical Plan --> Logical plan --> Optimized Logical Plan --> Multiple Physical Plan --> Cost Model --> RDD

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder()
                .appName("Query Plans")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> transactions = spark.read()
                .parquet("C:\\interview-workspace\\spark-experiments\\src\\main\\resources\\datasets\\transactions.parquet");

        Dataset<Row> customers = spark.read()
                .parquet("C:\\interview-workspace\\spark-experiments\\src\\main\\resources\\datasets\\customers.parquet");

        // Narrow transformation query plan
        customers= customers.filter(customers.col("city").equalTo("boston"))
                        .withColumn("split",split(col("name")," "))
                        .withColumn("first_name",col("split").getItem(0))
                        .withColumn("last_name",col("split").getItem(1))
                        .withColumn("age",col("age").$plus(lit(100)));
                       // .drop(col("split"));



        customers.show(5);
        customers.explain(true);

       System.out.println(transactions.rdd().getNumPartitions());

       transactions.repartition(24).explain(true);

        try {
            Thread.sleep(5000000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
