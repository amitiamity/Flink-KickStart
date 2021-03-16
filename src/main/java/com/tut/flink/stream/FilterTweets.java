package com.tut.flink.stream;

import com.tut.flink.stream.dto.Tweet;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;

import java.util.Properties;

public class FilterTweets {
    public static void main(String... s) throws Exception {
        final StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties twitterProps = new Properties();
        twitterProps.setProperty(TwitterSource.CONSUMER_KEY, "");
        twitterProps.setProperty(TwitterSource.CONSUMER_SECRET, "");
        twitterProps.setProperty(TwitterSource.TOKEN, "");
        twitterProps.setProperty(TwitterSource.TOKEN_SECRET, "");

        streamExecutionEnvironment.addSource(new TwitterSource(twitterProps))
                .map(new MapToTweet())
                .filter(new FilterFunction<Tweet>() {
                    @Override
                    public boolean filter(Tweet tweet) throws Exception {
                        return "en".equalsIgnoreCase(tweet.getLang());
                    }
                })
                .print();
        streamExecutionEnvironment.execute();
    }

    private static class MapToTweet implements MapFunction<String, Tweet> {

        static private final ObjectMapper mapper = new ObjectMapper();

        @Override
        public Tweet map(String s) throws Exception {
            JsonNode tweetJson = mapper.readTree(s);
            JsonNode textNode = tweetJson.get("text");
            JsonNode langNode = tweetJson.get("lang");
            String text = textNode != null ? textNode.textValue() : null;
            String lang = langNode != null ? langNode.textValue() : null;
            return new Tweet(text, lang);
        }
    }
}
