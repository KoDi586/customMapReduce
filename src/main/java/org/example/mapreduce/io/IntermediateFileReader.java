package org.example.mapreduce.io;

import lombok.SneakyThrows;
import org.example.mapreduce.model.KeyValue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
public class IntermediateFileReader {
    
    @SneakyThrows
    public Stream<KeyValue> read(Path file){
        // Предполагается, что файл содержит строки в формате "key=value"
        return Files.lines(file)
                .map(line -> {
                    String[] parts = line.split("=", 2);
                    return new KeyValue(parts[0], parts[1]);
                });
    }

    @SneakyThrows
    public List<KeyValue> readAll(List<Path> files) {
        return files.stream()
                .flatMap(this::read)
                .toList();
    }
}