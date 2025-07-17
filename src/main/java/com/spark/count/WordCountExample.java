package com.spark.count;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class WordCountExample {
    public static void main(String[] args) {
        // Initialize SparkSession
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder()
                .appName("WordCountExample")
                .master("local[*]") // Use all available cores
                .getOrCreate();

        // Input data (can be replaced with a file path)
        String inputFilePath = "C:\\interview-workspace\\spark-experiments\\src\\main\\resources\\datasets\\sample\\sample.txt";

        // Read the input file into a Dataset
        Dataset<String> textFile = spark.read()
                .textFile(inputFilePath);

        Dataset<Row> words = textFile
                .withColumn("word", functions.explode(functions.split(textFile.col("value"), "\\s+")))
                .select("word");

        // Group by word and count occurrences
        Dataset<Row> wordCounts = words.groupBy("word").count();

        words.show(true);
        // Show the result
        wordCounts.show();
        //textFile.show();
        spark.stop();
    }
}

