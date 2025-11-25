package org.example.mapreduce.io;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class FileManager {
    /**
     * Возвращает список расположений промежуточных файлов, соответствующих заданному reduceId.
     * Файлы имеют формат "mr-*-reduceId" в указанной временной директории.
     *
     * @param reduceId идентификатор редьюсера
     * @param workDir временная директория с промежуточными файлами
     * @return список путей к файлам, подходящим под шаблон mr-*-reduceId
     */
    //todo проверить на соответствие описанию
    public static List<Path> listIntermediateFilesForReducer(int reduceId, Path workDir) {
        String suffix = "-" + reduceId + ".txt";
        try (Stream<Path> files = Files.list(workDir)) {
            return files
                    .peek(path -> {
                        String string = path.toString();
                        System.out.println("path = " + string);
                    })
                    .filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(suffix))
                    .filter(path -> path.getFileName().toString().startsWith("mr-"))
                    .sorted() // упорядочиваем для детерминированности
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to list intermediate files for reducer " + reduceId, e);
        }
    }

    /**
     * Записывает результат редукции в финальный файл result-reduceId.txt.
     * Ключи сортируются в лексикографическом порядке, каждый ключ и значение записываются как "key value\n".
     *
     * @param reduceId идентификатор редьюсера
     * @param reduced ассоциативная карта результатов после редукции
     */
    public void writeFinalResult(int reduceId, Map<String, String> reduced) {
        Path outputPath = Path.of("result-" + reduceId + ".txt");
        try (BufferedWriter writer = Files.newBufferedWriter(outputPath)) {
            reduced.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .forEach(entry -> {
                        try {
                            writer.write(entry.getKey() + " " + entry.getValue() + "\n");
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to write final result for reduceId " + reduceId, e);
        }
    }

    /**
     * Очищает временную директорию, удаляя все её содержимое.
     * Метод опционален — можно не вызывать, если очистка не требуется.
     *
     * @param tempDir временная директория для удаления
     */
    public void cleanupTemp(Path tempDir) {
        try {
            if (Files.exists(tempDir)) {
                Files.walk(tempDir)
                        .sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(File::delete);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to clean up temp directory: " + tempDir, e);
        }
    }
}