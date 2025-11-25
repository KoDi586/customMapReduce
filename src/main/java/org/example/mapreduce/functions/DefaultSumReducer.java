package org.example.mapreduce.functions;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DefaultSumReducer implements Reducer {

    @Override
    public void reduce(List<Path> bucketFiles, Path workDir, int reduceId) {
        System.out.println("----------------- Reduce started -----------------");

        System.out.println("bucketFiles.size() = " + bucketFiles.size());
        bucketFiles.stream()
                .forEach(path -> System.out.println(path));

        Map<String, Integer> counter = new HashMap<>();

        try {
            for (Path f : bucketFiles) {
                try (BufferedReader br = Files.newBufferedReader(f)) {
                    String word;
                    while ((word = br.readLine()) != null) {
                        counter.merge(word, 1, Integer::sum);
                    }
                }
            }
            System.out.println("reduce step 0");
            System.out.println("counter.entrySet().size() = " + counter.entrySet().size());
            counter.forEach((key, value) -> System.out.println(key + " = " + value));

            System.out.println("reduce step 1");

            // Создаём папку если её нет
            Files.createDirectories(workDir);
            Path outputFile = workDir.resolve("result-" + reduceId + ".txt");

            // записываем результат
            try (BufferedWriter bw = Files.newBufferedWriter(outputFile)) {
                for (Map.Entry<String, Integer> e : counter.entrySet()) {
                    System.out.println("reduce step 2");
                    bw.write(e.getKey() + " " + e.getValue());
                    System.out.println("reduce step 3");
                    bw.newLine();
                }
            }

        } catch (Exception e) {
            throw new RuntimeException("Reduce failed", e);
        }
    }
}
