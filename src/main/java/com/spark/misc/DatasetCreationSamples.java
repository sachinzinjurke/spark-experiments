package com.spark.misc;

import com.spark.model.Person;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;
import static org.apache.spark.sql.functions.*;
public class DatasetCreationSamples {

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder()
                .appName("Dataset Creation")
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

        Dataset<Row> age = dataFrame.filter("age > 20");


        Dataset<Row> salaryAggr = dataFrame.groupBy(col("dept"),col("age")).agg(sum(col("salary"))).alias("total_salary");

        Dataset<Person> dataset = spark.createDataset(personList, Encoders.bean(Person.class));

       // Dataset<Person> filter = dataset.filter(person -> {person.getAge()});
        //Dataset<Person> filteredDataset = dataset.filter(p -> p.getAge() > 28);
        //dataset.filter(dataset.col("age").$greater(30)).show();

        Dataset<Person> filter = dataset.filter((FilterFunction<Person>) person -> person.getAge() > 32);

       // filter.show();
        salaryAggr.show();
       // dataFrame.printSchema();
    }
}
