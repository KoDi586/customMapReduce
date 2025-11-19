package org.example.mapreduce.functions;

import org.example.mapreduce.model.KeyValue;

import java.nio.file.Path;
import java.util.List;

public interface Mapper {

    void /*List<KeyValue>? */map(Path inputFile, Path workingDir, int reduceBucketCount);
}