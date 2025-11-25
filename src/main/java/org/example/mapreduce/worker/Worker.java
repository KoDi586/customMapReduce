package org.example.mapreduce.worker;

import org.example.mapreduce.config.JobConfig;
import org.example.mapreduce.coordinator.Coordinator;
import org.example.mapreduce.functions.DefaultSumReducer;
import org.example.mapreduce.functions.DefaultWordCountMapper;
import org.example.mapreduce.model.MapTask;
import org.example.mapreduce.model.ReduceTask;
import org.example.mapreduce.model.Task;
import org.example.mapreduce.functions.Mapper;
import org.example.mapreduce.functions.Reducer;

import java.util.concurrent.atomic.AtomicBoolean;

public class Worker implements Runnable {

    private final int id;
    private final Coordinator coordinator;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final JobConfig config;

    public Worker(int id, Coordinator coordinator, JobConfig config) {
        this.id = id;
        this.coordinator = coordinator;
        this.config = config;
    }

    @Override
    public void run() {
        while (running.get()) {
            Task task;

            try {
                // 🔥 Worker ждёт задание у Coordinator
                task = coordinator.requestTask();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }

            if (task.getType() == Task.Type.STOP) {
                // Завершаем работу
                running.set(false);
                break;
            }

            try {
                switch (task.getType()) {
                    case MAP -> handleMap(task.getMapTask());
                    case REDUCE -> handleReduce(task.getReduceTask());
                }
            } catch (Exception ex) {
                // можно логировать ошибку работы
                ex.printStackTrace();
            } finally {
                // 🔥 Уведомляем Coordinator о завершении
                switch (task.getType()) {
                    case MAP ->
                            coordinator.reportMapCompletion(task.getMapTask().getTaskId());
                    case REDUCE ->
                            coordinator.reportReduceCompletion(task.getReduceTask().getReduceId());
                }
            }
        }
    }

    private void handleMap(MapTask task) {

        Mapper mapper = new DefaultWordCountMapper();
        mapper.map(task.getFilePath(), config.getWorkingDir(), config.getReduceCount());
//                task.getMapper();
//        mapper.map(task.getInputFile(), task.getTempDir(), task.getReduceBuckets());
    }

    private void handleReduce(ReduceTask task) {
        Reducer reducer = new DefaultSumReducer();
//        Reducer reducer = task.getReducer();
        reducer.reduce(task.getIntermediateFiles(), config.getWorkingDir(), task.getReduceId());
    }

    public void setRunning(boolean isRunning) {
        running.set(isRunning);
    }
}
