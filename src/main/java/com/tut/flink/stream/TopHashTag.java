package com.tut.flink.stream;

import com.tut.flink.stream.dto.Tweet;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;

import java.util.Date;
import java.util.Properties;

public class TopHashTag {
    public static void main(String... s) throws Exception {
        final StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        //Properties for Twitter api to connect
        Properties twitterProps = new Properties();
        twitterProps.setProperty(TwitterSource.CONSUMER_KEY, "");
        twitterProps.setProperty(TwitterSource.CONSUMER_SECRET, "");
        twitterProps.setProperty(TwitterSource.TOKEN, "");
        twitterProps.setProperty(TwitterSource.TOKEN_SECRET, "");

        //Set the time strategy to be used
        streamExecutionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        //add twitter source
        streamExecutionEnvironment.addSource(new TwitterSource(twitterProps))
                //map the json twitter response to custom dto
                .map(new FilterTweets.MapToTweet())
                //convert list of tags in each tweet into single list of has tags
                .flatMap(new FlatMapFunction<Tweet, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(Tweet tweet, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        for (String tag : tweet.getTags()) {
                            collector.collect(new Tuple2<>(tag, 1));
                        }
                    }
                })
                //key by tag (It will split into multiple stream based on the hash tags)
                .keyBy(0)
                //time windows. It will give result periodically for all hashtags within a minute
                .timeWindow(Time.minutes(1))
                //count of each hashtags
                .sum(1)
                //applying non keyed windows ( it will merge all keyed streams into a single streams
                .timeWindowAll(Time.minutes(1))
                //apply method to find the has tag having the maximum count
                .apply(new AllWindowFunction<Tuple2<String, Integer>, Tuple3<Date, String, Integer>, TimeWindow>() {

                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<Tuple2<String, Integer>> iterable, Collector<Tuple3<Date, String, Integer>> collector) throws Exception {

                        String topHashtag = null;
                        int count = 0;
                        for (Tuple2<String, Integer> hasTag : iterable) {
                            if (hasTag.f1 > count) {
                                count = hasTag.f1;
                                topHashtag = hasTag.f0;
                            }
                        }
                        collector.collect(new Tuple3<>(new Date(timeWindow.getEnd()), topHashtag, count));
                    }

                })
                .print();

        streamExecutionEnvironment.execute();
    }
}
