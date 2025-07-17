package com.spark.buckets;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.WindowFunctionType;

public class BucketingExample {

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder()
                .appName("Bucketing")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> orders = spark.read()
                .option("header", "True")
                .option("inferSchema", "True")
                .csv("C:\\interview-workspace\\spark-experiments\\src\\main\\resources\\bucketing\\orders.csv");

        Dataset<Row> products = spark.read()
                .option("header", "True")
                .option("inferSchema", "True")
                .csv("C:\\interview-workspace\\spark-experiments\\src\\main\\resources\\bucketing\\products.csv");

        //orders.show(5);
        //products.show(5);

        orders
                .write()
                .bucketBy(4,"product_id")
                .mode(SaveMode.Overwrite)
                /*
                 *Below path will writes the data to specified path otherwise by default it will create managed tables under spark-warehouse folder
                 */
                .option("path","C:\\interview-workspace\\spark-experiments\\src\\main\\resources\\output\\buckets\\orders")
                .saveAsTable("orders_bucketed");

        products
                .write()
                .bucketBy(4,"product_id")
                .mode(SaveMode.Overwrite)
                /*
                 *Below path will writes the data to specified path otherwise by default it will create managed tables under spark-warehouse folder
                 */
                .option("path","C:\\interview-workspace\\spark-experiments\\src\\main\\resources\\output\\buckets\\products")
                .saveAsTable("products_bucketed");

        Dataset<Row> orderDF = spark.sql("SELECT * FROM orders_bucketed");
        Dataset<Row> productDF = spark.sql("SELECT * FROM products_bucketed");
        System.out.println("Order Count: " + orderDF.count());
        System.out.println("Product Count: " + productDF.count());
    }
}
