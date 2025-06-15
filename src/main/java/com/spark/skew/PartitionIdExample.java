package com.spark.skew;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class PartitionIdExample {

    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder()
                .appName("Data SKEW")
                .master("local[*]")
                .getOrCreate();
        Dataset<Row> rangeDF = spark.range(0, 100000).toDF("number");
        Dataset<Row> dataWithPartitionId = rangeDF.withColumn("partition_id", spark_partition_id());
        dataWithPartitionId=dataWithPartitionId.groupBy(col("partition_id")).count().orderBy("partition_id");
        // dataWithPartitionId.show(100);

        Dataset<Row> df1 = spark.range(0, 100000).toDF().repartition(1);
        Dataset<Row> df2 = spark.range(0, 10).toDF().repartition(1);
        Dataset<Row> df3 = spark.range(0, 1000).toDF().repartition(1);
        Dataset<Row> skewDF = df1.union(df2).union(df3);
        skewDF= skewDF.withColumn("partition_id", spark_partition_id());

        skewDF=skewDF.groupBy(col("partition_id")).count().orderBy("partition_id");
        skewDF.show(5);



    }


}
