package com.ferhat.spark;

import com.ferhat.spark.model.Person;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

public class MapTrans {
    public static void main(String[] args) {

        JavaSparkContext javaSparkContext = new JavaSparkContext("local", "Map Transformation");
        JavaRDD<String> rawdata = javaSparkContext.textFile("C:\\Users\\Ferhat\\Desktop\\Data Engineering\\person.csv");

        JavaRDD<Person> loadedPersons = rawdata.map(new Function<String, Person>() {
            @Override
            public Person call(String line) throws Exception {
                String[] data = line.split(",");
                Person person = new Person();
                person.setFirst_name(data[0]);
                person.setLast_name(data[1]);
                person.setEmail(data[2]);
                person.setGender(data[3]);
                person.setCountry(data[4]);
                return person;
            }
        });

        loadedPersons.foreach(new VoidFunction<Person>() {
            @Override
            public void call(Person person) throws Exception {
                System.out.println(person.getFirst_name());
            }
        });

    }
}
