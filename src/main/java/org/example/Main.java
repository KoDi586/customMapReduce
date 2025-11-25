package org.example;

import org.example.mapreduce.config.JobConfig;
import org.example.mapreduce.coordinator.Coordinator;
import org.example.mapreduce.worker.WorkerManager;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class Main {

    public static void main(String[] args) {

        JobConfig config = new JobConfig(
                List.of(                                      // inputFiles
                        Path.of("data/input/first.txt"),
                        Path.of("data/input/second.txt"),
                        Path.of("data/input/third.txt")
                ),
                Paths.get("").toAbsolutePath().resolve("data/workdir"),               // workingDir
                Path.of("/data/output"),             // outputDir
                2,                                            // workerCount (mapper workers)
                3                              // reduceCount (buckets)
//                WordCountMapper.class,                        // mapperClass
//                WordCountReducer.class                        // reducerClass
        );

        Coordinator coordinator = new Coordinator(config);
        coordinator.start();
        WorkerManager workerManager = new WorkerManager(config.getWorkerCount(), coordinator, config);
        workerManager.start();

        
    }

}
