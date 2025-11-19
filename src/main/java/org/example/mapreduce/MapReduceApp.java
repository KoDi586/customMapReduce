package org.example.mapreduce;

import org.example.mapreduce.config.JobConfig;
import org.example.mapreduce.coordinator.Coordinator;
import org.example.mapreduce.worker.WorkerManager;

public class MapReduceApp {

    private final int buckets;
    private final JobConfig jobConfig;

    public MapReduceApp(JobConfig config) {
        this.buckets = config.getReduceCount();
        this.jobConfig = config;
    }

    public void execute() {

        Coordinator coordinator = new Coordinator(jobConfig);

        WorkerManager workerManager = new WorkerManager(buckets, coordinator);

        coordinator.start(); // запускаем координатор(


        // ... launcher logic ...

        workerManager.stopAll();




    }
}