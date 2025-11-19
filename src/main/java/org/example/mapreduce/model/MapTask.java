package org.example.mapreduce.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.example.mapreduce.config.JobConfig;

import java.nio.file.Path;

/**
 * Назначение: содержит данные map-задачи, которые координатор отдает воркеру.
 */
@Getter
@AllArgsConstructor
public class MapTask {
    private int taskId;
    private Path filePath;
//    private Path outputPath;
//    private int reduceCount;

//    public MapTask(Path filePath, int taskId) {
//        this.filePath = filePath;
//        this.taskId = taskId;
//    }

    // todo может быть здесь стоит добавить сеттеры?
}