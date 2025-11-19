package org.example.mapreduce.config;

import java.nio.file.Files;

public class JobConfigValidator {

    public static void validate(JobConfig cfg) {
        if (cfg.getInputFiles().isEmpty()) {
            throw new IllegalArgumentException("No input files");
        }
        if (cfg.getReduceCount() <= 0) {
            throw new IllegalArgumentException("reduceTaskCount must be > 0");
        }
        if (!Files.isDirectory(cfg.getWorkingDir())) {
            throw new IllegalArgumentException("workingDir must exist");
        }
        if (cfg.getMapperClass() == null || cfg.getReducerClass() == null) {
            throw new IllegalArgumentException("Mapper/Reducer must not be null");
        }
    }
}

