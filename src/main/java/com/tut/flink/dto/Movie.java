package com.tut.flink.dto;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.util.Set;

@Getter
@Setter
@Builder
public class Movie {
    private String name;
    private Set<String> genres;

    @Override
    public String toString() {
        return "Movie(Name : " + this.name + " and genres are : " + genres + ")";
    }
}
