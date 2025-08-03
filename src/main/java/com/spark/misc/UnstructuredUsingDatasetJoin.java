package com.spark.misc;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class UnstructuredUsingDatasetJoin {

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

        Dataset<String> skillDS = spark.createDataset(specificKeys, Encoders.STRING());
        Dataset<Row> resourceNameSkillsDS = skillDS.withColumn("resource_name", lit(name))
                .withColumnRenamed("value","input_skill_name");

        resourceNameSkillsDS.show();

        Dataset<String> inputWithSkills = inputDF
                .withColumn("skill_name", functions.explode(functions.split(inputDF.col("value"), "\\s+")))
                .select("skill_name").as(Encoders.STRING());

        inputWithSkills.show();

        Dataset<Row> joinedDS = inputWithSkills
                .join(resourceNameSkillsDS, inputWithSkills.col("skill_name").equalTo(resourceNameSkillsDS.col("input_skill_name")))
                        .select("resource_name","skill_name");

        joinedDS.show();
        joinedDS.groupBy(col("resource_name"),col("skill_name")).count().orderBy(col("count")).show();



    }
}
