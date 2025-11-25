package org.example.mapreduce.io;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class FileManager {
    /**
     * Возвращает список расположений промежуточных файлов, соответствующих заданному reduceId.
     * Файлы имеют формат "mr-*-reduceId" в указанной временной директории.
     *
     * @param reduceId идентификатор редьюсера
     * @param workDir  временная директория с промежуточными файлами
     * @return список путей к файлам, подходящим под шаблон mr-*-reduceId
     */

    public static List<Path> listIntermediateFilesForReducer(int reduceId, Path workDir) {
        String suffix = "-" + reduceId + ".txt";
        try (Stream<Path> files = Files.list(workDir)) {
            return files
                    .filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(suffix))
                    .filter(path -> path.getFileName().toString().startsWith("mr-"))
                    .sorted() // упорядочиваем для детерминированности
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to list intermediate files for reducer " + reduceId, e);
        }
    }
}