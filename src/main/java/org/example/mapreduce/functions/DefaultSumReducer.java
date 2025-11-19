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
    public void reduce(List<Path> bucketFiles, Path outputFile) {
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

            // записываем результат
            try (BufferedWriter bw = Files.newBufferedWriter(outputFile)) {
                for (var e : counter.entrySet()) {
                    bw.write(e.getKey() + " " + e.getValue());
                    bw.newLine();
                }
            }

        } catch (Exception e) {
            throw new RuntimeException("Reduce failed", e);
        }
    }
}
