package org.example.mapreduce.model;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.nio.file.Path;
import java.util.List;

@AllArgsConstructor
@Getter
public class ReduceTask {

    private int reduceId;
    private List<Path> intermediateFiles;

}