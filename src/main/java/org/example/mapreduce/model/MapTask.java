package org.example.mapreduce.model;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.nio.file.Path;

/**
 * Назначение: содержит данные map-задачи, которые координатор отдает воркеру.
 */
@Getter
@AllArgsConstructor
public class MapTask {
    private int taskId;
    private Path filePath;

}