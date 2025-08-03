package com.spark.misc;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.*;

public class MalformedSample {

    public static void main(String[] args) throws InterruptedException {

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder()
                .appName("Malformed")
                .master("local[*]")
                .getOrCreate();

        StructType productSchema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("product_id", DataTypes.IntegerType, false),
                DataTypes.createStructField("product_name", DataTypes.StringType, false),
                DataTypes.createStructField("category", DataTypes.StringType, false),
                DataTypes.createStructField("brand", DataTypes.StringType,false),
                DataTypes.createStructField("price", DataTypes.IntegerType,false),
                DataTypes.createStructField("stock", DataTypes.IntegerType,false)
        });

        Dataset<Row> products = spark.read()
                .option("header", "true")
                //.option("sep","\t")
                .schema(productSchema)
                //.option("inferSchema","true")
                .option("mode","DROPMALFORMED")
                .csv("C:\\interview-workspace\\spark-experiments\\src\\main\\resources\\bucketing\\maformed.csv");

       // products.select(col("product_id"),col("product_name")).show();

        String[] columns = products.columns();
        for (String name:columns) {
            System.out.println(name);
            products = products
                    .withColumn(name + "_updated",col(name))
                    .withColumnRenamed(name + "_updated",name + "_Renamed")
                    .drop(col(name))
                    .drop(name + "_updated");
        }
         products.show();

        Thread.sleep(100000);


        }
}
