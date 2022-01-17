package com.ferhat.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class FirstExam {
    public static void main(String[] args) {


        JavaSparkContext javaSparkContext = new JavaSparkContext("local","First Exam Spark");

        /*
        JavaRDD<String> firstData = javaSparkContext.textFile("C:\\Users\\Ferhat\\Desktop\\firstdata.txt");
        System.out.println(firstData.countByValue());
        */

        List<String> data = Arrays.asList("data", "turkey", "big_data", "istanbul", "bursa");
        JavaRDD<String> secondData = javaSparkContext.parallelize(data);

        System.out.println(secondData.count());


    }
}
