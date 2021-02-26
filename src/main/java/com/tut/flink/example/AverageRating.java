package com.tut.flink.example;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.dns.DnsOpCode;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.Collector;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class AverageRating {
    public static void main(String... s) throws Exception {
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<Tuple3<Long, String, String>> movies = executionEnvironment
                .readCsvFile("/home/amit/Documents/myworkspace/Flink-KickStart/src/main/resources/ml-latest-small/movies.csv")
                .ignoreFirstLine()
                .parseQuotedStrings('"')
                .ignoreInvalidLines()
                .types(Long.class, String.class, String.class);

        DataSource<Tuple2<Long, Double>> ratings = executionEnvironment
                .readCsvFile("/home/amit/Documents/myworkspace/Flink-KickStart/src/main/resources/ml-latest-small/ratings.csv")
                .ignoreFirstLine()
                .parseQuotedStrings('"')
                .ignoreInvalidLines()
                .includeFields(false, true, true, false)
                .types(Long.class, Double.class);

        List<Tuple2<String, Double>> distributions = movies.join(ratings)
                //where clause which field from movie is being compared
                .where(0)
                //equal to what field is being matched to Rating class
                .equalTo(0)
                .with(new JoinFunction<Tuple3<Long, String, String>, Tuple2<Long, Double>, Tuple3<StringValue, StringValue, DoubleValue>>() {
                    StringValue name = new StringValue();
                    StringValue genre = new StringValue();
                    DoubleValue score = new DoubleValue();

                    @Override
                    public Tuple3<StringValue, StringValue, DoubleValue> join(Tuple3<Long, String, String> movie, Tuple2<Long, Double> rating) throws Exception {
                        name.setValue(movie.f1);
                        genre.setValue(movie.f2.split("\\|")[0]);
                        score.setValue(rating.f1);
                        return new Tuple3<>(name, genre, score);
                    }
                }).groupBy(1)
                .reduceGroup(new GroupReduceFunction<Tuple3<StringValue, StringValue, DoubleValue>, Tuple2<String, Double>>() {
                    @Override
                    public void reduce(Iterable<Tuple3<StringValue, StringValue, DoubleValue>> iterable, Collector<Tuple2<String, Double>> collector) throws Exception {
                        String genre = null;
                        int count = 0;
                        double totalScore = 0;
                        for (Tuple3<StringValue, StringValue, DoubleValue> movie : iterable) {
                            genre = movie.f1.getValue();
                            totalScore += movie.f2.getValue();
                            count++;
                        }
                        collector.collect(new Tuple2<>(genre, totalScore / count));
                    }
                }).collect();

        String res = distributions.stream()
                .sorted(Comparator.comparingDouble(r -> r.f1))
                .map(Object::toString)
                .collect(Collectors.joining("\n"));

        System.out.println(res);
    }
}
