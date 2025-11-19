package org.example.mapreduce.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.example.mapreduce.functions.Mapper;
import org.example.mapreduce.functions.Reducer;

import java.nio.file.Path;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class JobConfig {
    private List<Path> inputFiles;   // список входных файлов
    private Path workingDir;         // директория, куда класть m-* и r-* файлы
    private int workerCount;      // кол-во воркеров
//    private int reduceWorkerCount;   // кол-во редюс-воркеров
    private int reduceCount;     // buckets (кол-во reduce-частей)

    private Class<? extends Mapper> mapperClass;
    private Class<? extends Reducer> reducerClass;

    // конструкторы + геттеры
}
