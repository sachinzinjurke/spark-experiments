package com.spark.malformed;

import com.spark.model.Person;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ReduceByExample {

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder()
                .appName("Reduce By Example")
                .master("local[*]")
                .getOrCreate();

        List<Person> personList = new ArrayList<>();
        personList.add(new Person("Alice", 25, "IT",100));
        personList.add(new Person("Bob", 30,"HR",200));
        personList.add(new Person("Tom", 35,"HR",200));
        personList.add(new Person("Brown", 32,"ADMIN",200));
        personList.add(new Person("Eva", 25,"IT",200));
        personList.add(new Person("Bob", 30,"HR",200));
        personList.add(new Person("Charlie", 25,"IT",200));

        Dataset<Row> dataFrame = spark.createDataFrame(personList, Person.class);

        Function2<Integer, Integer, Integer> reduction= (num1,num2)->num1+num2;

        Integer reduce = dataFrame.toJavaRDD().map(row -> row.getInt(0)).reduce(reduction);

        System.out.println("Reduction Result :: " + reduce);

    }
}
