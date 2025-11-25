package org.example;

import org.example.mapreduce.MapReduceApp;
import org.example.mapreduce.config.JobConfig;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class Main {

    public static void main(String[] args) {

        Path rootPath = Paths.get("");
        JobConfig config = new JobConfig(
                List.of(                                      // inputFiles
                        Path.of("data/input/first.txt"),
                        Path.of("data/input/second.txt"),
                        Path.of("data/input/third.txt")
                ),
                rootPath.toAbsolutePath().resolve("data/workdir"),   // workingDir
                rootPath.toAbsolutePath().resolve("data/output"),    // outputDir
                2,  // workerCount
                3   // reduceCount

        );

        MapReduceApp app = new MapReduceApp(config);
        app.execute();

    }

}
