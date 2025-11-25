package org.example.mapreduce.functions;

import java.nio.file.Path;

public interface Mapper {

    void map(Path inputFile, Path workingDir, int reduceBucketCount);
}