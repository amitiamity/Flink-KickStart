package com.tut.flink.stream;

import com.tut.flink.stream.dto.Tweet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;
import scala.Tuple3;

import java.util.Date;
import java.util.Properties;

public class NumberOfTweetsPerLang {

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
                //filter only if language is available
                .filter(tweet -> tweet.getLang() != null)
                //key strategy : use it to create keyed windows
                .keyBy((KeySelector<Tweet, String>) tweet -> tweet.getLang())
                //time for each window
                .timeWindow(Time.minutes(1))
                //apply customer logic one each window
                .apply(new WindowFunction<Tweet, Tuple3<String, Long, Date>, String, TimeWindow>() {
                    @Override
                    public void apply(String langKey, TimeWindow timeWindow, Iterable<Tweet> inputTweets, Collector<Tuple3<String, Long, Date>> collector) throws Exception {
                        long count = 0;
                        for (Tweet tweet : inputTweets) {
                            count++;
                        }
                        collector.collect(new Tuple3<>(langKey, count, new Date(timeWindow.getEnd())));
                    }
                }).print();
        streamExecutionEnvironment.execute();
    }
}
