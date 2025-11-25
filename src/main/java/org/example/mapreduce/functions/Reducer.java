package org.example.mapreduce.functions;

import java.nio.file.Path;
import java.util.List;

public interface Reducer {

    /**
     * Возвращает итоговое значение (для WordCount — сумма как строка).
     *
//     * @param key ключ, сгруппированный из маппера
//     * @param values список промежуточных значений, соответствующих ключу
     * @return итоговое значение в виде строки
     */
    void /*String?*/ reduce(List<Path> bucketFiles, Path workDir, int reduceId);
}