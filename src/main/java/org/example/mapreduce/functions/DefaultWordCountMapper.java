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
            // Карта для хранения BufferedWriter'ов, по одному на каждый bucket (группа для редьюсера)
            Map<Integer, BufferedWriter> bucketStreams = new HashMap<>();

            String line;
            // Построчно читаем входной файл
            while ((line = br.readLine()) != null) {

                // Разбиваем строку на слова по пробельным символам
                for (String word : line.split("\\s+")) {

                    // Пропускаем пустые строки, которые могут появиться из-за лишних пробелов
                    if (word.isEmpty()) continue;

                    // Определяем номер bucket (группы), в которую попадёт слово, на основе хеша слова
                    int bucket = Math.abs(word.hashCode() % reduceBucketCount);
                    // Получаем или создаём BufferedWriter для нужного bucket'а

                    BufferedWriter out = bucketStreams.computeIfAbsent(bucket, b -> {
                        try {

                            // чтобы убрать лишние расширения файла
                            Path preFileName = inputFile.getFileName();
                            String fileName =
                                    preFileName.toString().replaceFirst("\\.[^.]+$", "");

                            // Формируем путь к временному файлу: mr-<имя_файла>-<номер_бакета>.txt
                            Path f = workingDir.resolve("mr-" + fileName + "-" + b + ".txt");

                            // Создаём директории, если отсутствуют
                            Files.createDirectories(f.getParent());

                            return Files.newBufferedWriter(f);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });

                    // Записываем слово в соответствующий временный файл
                    out.write(word + "\n");
                }
            }

            // Закрываем все открытые потоки записи, чтобы освободить ресурсы и корректно завершить запись
            for (BufferedWriter w : bucketStreams.values()) w.close();

        } catch (Exception e) {
            throw new RuntimeException("Map failed for " + inputFile, e);
        }
    }
}
