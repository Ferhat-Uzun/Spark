package com.ferhat.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class SparkSqlFirst {
    public static void main(String[] args) {

        StructType schema = new StructType()
                .add("first_name", DataTypes.StringType)
                .add("last_name", DataTypes.StringType)
                .add("email", DataTypes.StringType)
                .add("country", DataTypes.StringType)
                .add("price", DataTypes.DoubleType)
                .add("product", DataTypes.StringType);

        SparkSession sparkSession = SparkSession.builder().master("local").appName("FirstSparkSql").getOrCreate();
        Dataset<Row> rawData = sparkSession.read().schema(schema).option("multiline",true).json("C:\\Users\\Ferhat\\Desktop\\product.json");

        Dataset<Row> countryPrice = rawData.groupBy("country","product").sum();

        countryPrice.show();

        //rawData.show();
        //rawData.printSchema();

        //Dataset<Row> filteredData = rawData.filter(rawData.col("country").equalTo("Russia").and(rawData.col("email").contains("microsoft")));
        //filteredData.sort("first_name").show();

        //Dataset<Row> countryGroup = rawData.groupBy("first_name","country").count();
        //countryGroup.show();

    }
}
