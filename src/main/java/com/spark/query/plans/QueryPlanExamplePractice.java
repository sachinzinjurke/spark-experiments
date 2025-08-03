package com.spark.query.plans;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class QueryPlanExamplePractice {

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

        customers= customers.filter(col("city").equalTo("boston")) ;
        Dataset<Row> finalDF =
                customers.filter(col("city").equalTo("boston"))
                .withColumn("split", split(col("name"), " "))
                .withColumn("first_name", col("split").getItem(0))
                .withColumn("last_name", col("split").getItem(1))
                .drop("split")
                .select("first_name","last_name","gender","city") ;

        System.out.println(" Total Partitions :: " + finalDF.toJavaRDD().getNumPartitions());
        Dataset<Row> repartition = finalDF.repartition(12);
        System.out.println(" Total Partitions After Repartition :: " + repartition.toJavaRDD().getNumPartitions());
        //repartition.show(5);
        repartition.explain(true);
        repartition.count();

        try {
            Thread.sleep(5000000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
