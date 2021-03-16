package com.tut.flink.stream.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class Tweet {
    private String text;
    private String lang;

    @Override
    public String toString() {
        return "Lang : " + this.lang + " and text : " + this.text;
    }
}
