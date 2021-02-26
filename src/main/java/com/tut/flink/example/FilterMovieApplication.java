package com.tut.flink.example;

import com.tut.flink.dto.Movie;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.Arrays;
import java.util.HashSet;

public class FilterMovieApplication {

    public static void main(String...s) throws Exception {
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();

        //Read command line argument to accept file path names
        ParameterTool parameterTool = ParameterTool.fromArgs(s);
        String inputFile = parameterTool.get("input");
        String outputFile =  parameterTool.get("output");

        DataSource<Tuple3<Long, String, String>> lines = executionEnvironment
                .readCsvFile(inputFile)
                .ignoreFirstLine()
                .parseQuotedStrings('"')
                .ignoreInvalidLines()
                .types(Long.class, String.class, String.class);

        DataSet<Movie> movieDataSet = lines.map(csvLine -> {
           String movieName = csvLine.f1;
           String[] genres = csvLine.f2.split("\\|");
           return Movie.builder().name(movieName).genres(new HashSet<>(Arrays.asList(genres)))
                   .build();
        });

        DataSet<Movie> filteredMovies = movieDataSet.filter(movie -> movie.getGenres().contains("Drama"));

        filteredMovies.writeAsText(outputFile);
        executionEnvironment.execute();
    }
}
