package com.spark.misc;

import com.spark.model.Employee;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Collections;
import java.util.List;

public class DataFrameAndDataSetDifeferenceExample {

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder()
                .appName("Dataset Vs Dataframe Creation")
                .master("local[*]")
                .getOrCreate();

        String jsonData = "[{\"id\":1,\"name\":\"Alice\"},{\"id\":2,\"name\":\"Bob\"}]";
        List<String> list = Collections.singletonList(jsonData);
        System.out.println(list);
         /* Dataset<Row> dataFrame = spark.read().json(spark.createDataset(
                Collections.singletonList(jsonData), Encoders.STRING()));*/

        Dataset<String> dataset = spark.createDataset(list, Encoders.STRING());

        Dataset<Row> df = spark.read().json(dataset);

        df.show();
       /* Dataset<Person> ds = spark.read().json(spark.createDataset(
                        java.util.Collections.singletonList(jsonData), Encoders.STRING()))
                .as(Encoders.bean(Person.class));*/

        Dataset<Employee> ds = spark.read().json(dataset).as(Encoders.bean(Employee.class));
        //Dataset<String> map = ds.map((Function1<Employee, String>)  person -> "Name : " + person.getName(),Encoders.STRING());
        Dataset<String> map = ds.map((MapFunction<Employee, String>) emp -> "Name :" + emp.getName(), Encoders.STRING());
        map.show();


    }
}
