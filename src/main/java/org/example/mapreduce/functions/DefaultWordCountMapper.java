package org.example.mapreduce.functions;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class DefaultWordCountMapper implements Mapper {

    @Override
    public void map(Path inputFile, Path workingDir, int reduceBucketCount) {
        try (BufferedReader br = Files.newBufferedReader(inputFile)) {
            Map<Integer, BufferedWriter> bucketStreams = new HashMap<>();

            String line;
            while ((line = br.readLine()) != null) {
                for (String word : line.split("\\s+")) {
                    if (word.isEmpty()) continue;

                    int bucket = Math.abs(word.hashCode() % reduceBucketCount);
                    BufferedWriter out = bucketStreams.computeIfAbsent(bucket, b -> {
                        try {
                            Path f = workingDir.resolve("m-" + inputFile.getFileName() + "-" + b + ".txt");
                            return Files.newBufferedWriter(f);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });

                    out.write(word + "\n");
                }
            }

            // закрыть все writers
            for (BufferedWriter w : bucketStreams.values()) w.close();

        } catch (Exception e) {
            throw new RuntimeException("Map failed for " + inputFile, e);
        }
    }
}
