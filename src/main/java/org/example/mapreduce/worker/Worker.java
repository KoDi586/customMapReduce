package org.example.mapreduce.worker;

import org.example.mapreduce.coordinator.Coordinator;
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

    public Worker(int id, Coordinator coordinator) {
        this.id = id;
        this.coordinator = coordinator;
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
        Mapper mapper = task.getMapper();
        mapper.map(task.getInputFile(), task.getTempDir(), task.getReduceBuckets());
    }

    private void handleReduce(ReduceTask task) {
        Reducer reducer = task.getReducer();
        reducer.reduce(task.getIntermediateFiles(), task.getOutputFile());
    }

    public void setRunning(boolean isRunning) {
        running.set(isRunning);
    }
}
