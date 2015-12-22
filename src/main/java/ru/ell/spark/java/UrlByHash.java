package ru.ell.spark.java;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Created by Alexey Kutin on 12/22/2015.
 */
public class UrlByHash {

    private final static Pattern LOG_PATTERN = Pattern.compile("(\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.\\d+)Z\\s\\S+\\s(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}):.+\\s\"(?:[A-Z]+)\\s(\\S+)\\sHTTP/");

    public static void main(String[] args) {
        if( args.length != 2) {
            System.out.println("Usage: UrlByHash [input path] [output path]");
            System.exit(1);
        }

        final SparkConf sparkConf = new SparkConf().setAppName("Java8 URL hash");
        final JavaSparkContext sc = new JavaSparkContext(sparkConf);

        final JavaRDD<String> log = sc.textFile(args[0]);
        log
            .map(LOG_PATTERN::matcher)
            .filter(Matcher::find)
            .map(m -> m.group(3))
            .map(URL::new)
            .map(URL::getPath)
            /* check how unique hashCode is */
            // .mapToPair(s -> new Tuple2<>(s.hashCode(), ImmutableSet.of(s)))
            /* check uniqueness of s.hashCode + s.reverse.hashCode */
            .mapToPair(s -> new Tuple2<>(new Tuple2(s.hashCode(), new StringBuilder(s).reverse().hashCode()), ImmutableSet.of(s)))
            .reduceByKey((x, y) -> Sets.union(x, y).immutableCopy())
            .filter(p -> p._2().size() > 1)
            .flatMap(p -> p._2().stream().map(s -> new Tuple2<>(p._1(), s)).collect(Collectors.toList()))
            .map( s -> String.format("%1$s\t%2$s", s._1(), s._2()))
            .saveAsTextFile(args[1]);
    }
}
