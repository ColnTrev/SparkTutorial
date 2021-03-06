package main.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

import static main.java.Paths.INPUT_PATH;
import static main.java.Paths.OUTPUT_PATH;

/**
 * Created by colntrev on 2/17/18.
 */
public class Main {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Word Count");
        JavaSparkContext javaConf = new JavaSparkContext(conf);
        JavaRDD<String> textFile = javaConf.textFile(INPUT_PATH);
        JavaPairRDD<String,Integer> counts = textFile
                .flatMap(s -> Arrays.asList(s.split("[ ,]")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a,b) -> a + b);
        counts.foreach(p -> System.out.println(p));
        System.out.println("Total Words: " + counts.count());
        counts.saveAsTextFile(OUTPUT_PATH);
    }
}
