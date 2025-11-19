package org.example.mapreduce.io;

import org.example.mapreduce.model.KeyValue;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class IntermediateFileWriter {

    public void write(List<KeyValue> kvs, int mapTaskId, int reduceCount) {
        List<BufferedWriter> writers = new ArrayList<>();
        try {
            // Создаём BufferedWriter для каждого из reduceCount файлов mr-mapTaskId-i
            for (int i = 0; i < reduceCount; i++) {
                String filename = "mr-" + mapTaskId + "-" + i;
                BufferedWriter writer = Files.newBufferedWriter(Paths.get(filename));
                writers.add(writer);
            }

            // Для каждой пары определяем bucket и записываем в соответствующий файл
            for (KeyValue kv : kvs) {
                int bucket = Math.abs(kv.getKey().hashCode()) % reduceCount;
                BufferedWriter writer = writers.get(bucket);
                writer.write(kv.getKey() + "\t" + kv.getValue() + "\n");
            }
        } catch (IOException e) {
            throw new RuntimeException("Error writing intermediate files", e);
        } finally {
            // Закрываем все writer'ы с обработкой возможных исключений
            for (BufferedWriter writer : writers) {
                if (writer != null) {
                    try {
                        writer.close();
                    } catch (IOException e) {
                        // Логируем или игнорируем ошибку закрытия
                    }
                }
            }
        }
    }
}