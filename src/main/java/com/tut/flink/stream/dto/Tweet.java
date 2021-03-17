package com.tut.flink.stream.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
@AllArgsConstructor
@Builder
public class Tweet {
    private String text;
    private String lang;
    private List<String> tags;


    @Override
    public String toString() {
        return "Lang : " + this.lang + " and text : " + this.text;
    }
}
