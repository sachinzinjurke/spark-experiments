package com.spark.misc;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

public class LineSplitter {

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder()
                .appName("Line Splitter")
                .master("local[*]") // Use all available cores
                .getOrCreate();

        Dataset<String> lines = spark.createDataset(Arrays.asList("Hello I am sachin", "Arjun whats up buddy", "sachin Arjun up I"), Encoders.STRING());

        FlatMapFunction<String,String> flatMapper = input -> Arrays.stream(input.split(" ")).iterator();

        MapFunction<String,String> mapper = input -> input.toUpperCase();


        Dataset<String> words =
                lines.map(mapper, Encoders.STRING())
                        .flatMap(flatMapper, Encoders.STRING());

        Dataset<Row> value = words.groupBy("value").count().orderBy("value");

        value.show();
    }
}
