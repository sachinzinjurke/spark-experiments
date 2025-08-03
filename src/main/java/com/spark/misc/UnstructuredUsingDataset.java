package com.spark.misc;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.*;

import java.util.Arrays;
import java.util.List;
import static org.apache.spark.sql.functions.*;
public class UnstructuredUsingDataset {

    public static void main(String[] args) {


        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder()
                .appName("WordCountExample")
                .master("local[*]") // Use all available cores
                .getOrCreate();

        String inputFilePath = "C:\\interview-workspace\\spark-experiments\\src\\main\\resources\\datasets\\sample\\unstructured.txt";

        List<String> specificKeys = Arrays.asList("Java","Spark,","Databricks","Azure","TIBCO","ElasticSearch","VisualVM");

        Dataset<String> inputDF = spark.read().textFile(inputFilePath);

        String name = inputDF.first();

        Dataset<String> skillsDS = inputDF
                .withColumn("skills", functions.explode(functions.split(inputDF.col("value"), "\\s+")))
                .select("skills").as(Encoders.STRING());

        Dataset<String> filter = skillsDS
                .filter((FilterFunction<String>) skill -> specificKeys.contains(skill));

        Dataset<Row> filterSkillCount = filter.groupBy(col("skills")).count();

        Dataset<Row> finalResult = filterSkillCount
                .withColumn("resource_name", lit(name))
                .select("resource_name","skills","count");

        finalResult.show();

        finalResult.explain(true);



    }
}
