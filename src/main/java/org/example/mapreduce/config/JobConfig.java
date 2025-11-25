package org.example.mapreduce.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.nio.file.Path;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class JobConfig {
    private List<Path> inputFiles;   // список входных файлов
    private Path workingDir;         // директория, в которой будут создаваться временные файлы
    private Path outputDir;          // директория, куда класть выходные файлы
    private int workerCount;      // кол-во воркеров
    private int reduceCount;     // buckets (кол-во reduce-частей)

}
