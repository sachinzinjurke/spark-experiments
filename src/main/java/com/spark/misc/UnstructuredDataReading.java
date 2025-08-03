package com.spark.misc;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;

import java.util.Arrays;
import java.util.List;

public class UnstructuredDataReading {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("UnstructuredDataExample").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        String filePath = "C:\\interview-workspace\\spark-experiments\\src\\main\\resources\\datasets\\sample\\unstructured.txt";

        List<String> specificKeys = Arrays.asList("Java","Spark,","Databricks","Azure","TIBCO","ElasticSearch","VisualVM");

        JavaRDD<String> rawData = sc.textFile(filePath);

        String first = rawData.first();

        JavaRDD<String> words = rawData.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        JavaPairRDD<String, Integer> wordPairs = words.mapToPair(word -> new Tuple2<>(word, 1));

        JavaPairRDD<String, Integer> filteredRDD = wordPairs.filter(tuple -> specificKeys.contains(tuple._1));

        JavaPairRDD<String, Integer> counts = filteredRDD.reduceByKey(Integer::sum);

        JavaPairRDD<Integer, String> swapped = counts.mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1));

        JavaPairRDD<Integer, String> sorted = swapped.sortByKey(false);

        sorted.collect().forEach(System.out::println);

        JavaRDD<Tuple3<String, Integer, String>> tuple3RDD = sorted.map(pair -> {
            Integer count = pair._1;
            String skill = pair._2;
            String additionalValue = first;
            return new Tuple3<>(additionalValue,count, skill);
        });

        System.out.println("Final Count ::");
        tuple3RDD.foreach(tuple -> System.out.println(tuple));



    }
}
