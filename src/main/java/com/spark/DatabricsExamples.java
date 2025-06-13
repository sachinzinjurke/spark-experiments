package com.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class DatabricsExamples {
    public static void main(String[] args) throws InterruptedException {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder()
                .appName("Bucketing")
                .master("local[*]")
                .getOrCreate();

        StructType countrySchema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("COUNTRY_ID", DataTypes.IntegerType, false),
                DataTypes.createStructField("NAME", DataTypes.StringType, false),
                DataTypes.createStructField("NATIONALITY", DataTypes.StringType, false),
                DataTypes.createStructField("COUNTRY_CODE", DataTypes.StringType, false),
                DataTypes.createStructField("ISO_ALPHA2", DataTypes.StringType, false),
                DataTypes.createStructField("CAPITAL", DataTypes.StringType, false),
                DataTypes.createStructField("POPULATION", DataTypes.DoubleType, false),
                DataTypes.createStructField("AREA_KM2", DataTypes.IntegerType, false),
                DataTypes.createStructField("REGION_ID", DataTypes.IntegerType, true),
                DataTypes.createStructField("SUB_REGION_ID", DataTypes.IntegerType, true),
                DataTypes.createStructField("INTERMEDIATE_REGION_ID", DataTypes.IntegerType, true),
                DataTypes.createStructField("ORGANIZATION_REGION_ID", DataTypes.IntegerType, true)});

         Dataset<Row> countries = spark.read()
                .option("header", "True")
                .schema(countrySchema)
                .csv("C:\\interview-workspace\\spark-experiments\\src\\main\\resources\\databricks\\countries.csv");

        countries=countries.select("NAME","NATIONALITY","POPULATION");

        countries.show(5);

        Thread.sleep(1000000);
        }

}
