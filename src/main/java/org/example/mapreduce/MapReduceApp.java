package org.example.mapreduce;

import org.example.mapreduce.config.JobConfig;
import org.example.mapreduce.coordinator.Coordinator;
import org.example.mapreduce.worker.WorkerManager;

public class MapReduceApp {


    private final JobConfig config;

    public MapReduceApp(JobConfig config) {
        this.config = config;
    }

    public void execute() {

        Coordinator coordinator = new Coordinator(config);
        coordinator.start();

        WorkerManager workerManager = new WorkerManager(config.getWorkerCount(), coordinator, config);
        workerManager.start();

    }
}