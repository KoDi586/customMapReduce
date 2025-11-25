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
        System.out.println("0 step");
        System.out.println("inputFile.toString() = " + inputFile.toString());
        try (BufferedReader br = Files.newBufferedReader(inputFile)) {
            // Карта для хранения BufferedWriter'ов, по одному на каждый bucket (группа для редьюсера)
            Map<Integer, BufferedWriter> bucketStreams = new HashMap<>();
            System.out.println("1 step");
            String line;
            // Построчно читаем входной файл
            while ((line = br.readLine()) != null) {
                System.out.println(line + " line");
                // Разбиваем строку на слова по пробельным символам
                for (String word : line.split("\\s+")) {
                    System.out.println(word+ " word");
                    // Пропускаем пустые строки, которые могут появиться из-за лишних пробелов
                    if (word.isEmpty()) continue;

                    // Определяем номер bucket (группы), в которую попадёт слово, на основе хеша слова
                    int bucket = Math.abs(word.hashCode() % reduceBucketCount);
                    // Получаем или создаём BufferedWriter для нужного bucket'а

                    //TODO: а не нужно ли проверить есть ли такой файл?
// Проверка на существование файла не нужна, так как Files.newBufferedWriter() перезапишет файл, если он существует.
                    // В контексте MapReduce это допустимо, потому что каждый mapper работает с уникальным именем файла (включая имя входного файла и номер bucket).
                    // Поэтому конфликты маловероятны, и перезапись безопасна.
                    BufferedWriter out = bucketStreams.computeIfAbsent(bucket, b -> {
                        try {
                            System.out.println("2 step");
                            // Формируем путь к временному файлу: m-<имя_файла>-<номер_бакета>.txt
                            //TODO: было m а не mr я заменил потом проверить
                            Path f = workingDir.resolve("mr-" + inputFile.getFileName() + "-" + b + ".txt");

                            // Создаём директории, если отсутствуют
                            Files.createDirectories(f.getParent());

                            return Files.newBufferedWriter(f);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
                    System.out.println("3 step");

                    // Записываем слово в соответствующий временный файл
                    out.write(word + "\n");
                }
            }

            // Закрываем все открытые потоки записи, чтобы освободить ресурсы и корректно завершить запись
            for (BufferedWriter w : bucketStreams.values()) w.close();

        } catch (Exception e) {
            // Оборачиваем любые исключения в RuntimeException с понятным сообщением
            throw new RuntimeException("Map failed for " + inputFile, e);
        }
    }
}
