package com.spark.partition;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import static org.apache.spark.sql.functions.*;
public class PartitionPractice {

    public static void main(String[] args) throws InterruptedException {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder()
                .appName("Partitions")
                .master("local[*]")
                .getOrCreate();


       // Dataset<Row> df = spark.read().option("header", "True").option("inferSchema", "True").csv("C:\\interview-workspace\\spark-experiments\\src\\main\\resources\\partitioning\\raw\\Spotify_Artists.csv");

        Dataset<Row> df = spark.read()
                .option("header", "True")
                .option("inferSchema", "True")
                .csv("C:\\interview-workspace\\spark-experiments\\src\\main\\resources\\partitioning\\raw\\Spotify_Listening_Activity.csv");

        df=df.withColumnRenamed("listen_date","listen_time")
                        .withColumn("listen_date", to_date(col("listen_time"), "yyyy-MM-dd HH:mm:ss.SSSSSS"))
                        .withColumn("listen_hour", hour(col("listen_time")));

        /*
            format("noop") is added to check the DAG so that it should not shuffle the data

         */

       // df.write().mode("overwrite").format("noop").save("C:\\interview-workspace\\spark-experiments\\src\\main\\resources\\output");
        df.write()
                .partitionBy("listen_date")
                .format("parquet")
                .mode("overwrite")
                .save("C:\\interview-workspace\\spark-experiments\\src\\main\\resources\\output\\partition");


        /*
            Before writing data using partitionBy wanted to control the the number of files created under PartitionBy column
         */
       /* df.repartition(5).write()
                .partitionBy("listen_date")
                .mode("overwrite")
                .csv("C:\\interview-workspace\\spark-experiments\\src\\main\\resources\\output\\partition\\fixed");*/

        //df.show(5);
       // df.printSchema();

        Thread.sleep(100000);
    }
}
