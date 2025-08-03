package com.spark.misc;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JavaFlatMapPractice {

    public static void main(String[] args) {

        List<String> lines = Arrays.asList("Hello I am sachin", "Arjun whats up buddy", "sachin Arjun up I");

        Stream<String> words = lines.stream().map(line->line.toUpperCase()).flatMap(line -> Arrays.asList(line.split(" ")).stream());

        TreeMap<String, Long> collect = words.collect(Collectors.groupingBy(Function.identity(), TreeMap::new, Collectors.counting()));

       // collect.entrySet().stream().sorted(Map.Entry.<String, Long>comparingByValue().reversed()).forEach(entry -> System.out.println("Key :: " + entry.getKey() + " Value :: " + entry.getValue() ));

        collect.entrySet().stream().sorted(Map.Entry.<String,Long>comparingByValue().reversed()).forEach(entry -> System.out.println("Key :: " + entry.getKey() + " Value :: " + entry.getValue() ));

        System.out.println(collect);

        System.out.println("---------------------------");

    }
}
