package com.spark.udf;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class UdfExample {

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder()
                .appName("UDF Example")
                .master("local[*]")
                .getOrCreate();

        //Create UDF function with custom logic
        UDF1<String, String> toUpperCase = (String input) -> input == null ? null : input.toUpperCase();

        //Register the UDF function with spark session
        spark.udf().register("toUpperCaseUDF",toUpperCase,DataTypes.StringType);


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

        products.createOrReplaceTempView("products_vw");
        Dataset<Row> udfDF = spark.sql("select product_id, product_name,toUpperCaseUDF(product_name) as UDF_UPDATED from products_vw ");
        udfDF.show(5,false);

    }
}
