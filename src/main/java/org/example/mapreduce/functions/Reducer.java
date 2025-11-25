package org.example.mapreduce.functions;

import java.nio.file.Path;
import java.util.List;

public interface Reducer {

    /**
     * Возвращает итоговое значение (для WordCount — сумма как строка).
     * @param bucketFiles список файлов с результатами map-задач
     * @param workDir рабочая директория для промежуточных файлов
     * @param reduceId номер редьюсера
     */
    void reduce(List<Path> bucketFiles, Path workDir, int reduceId);
}