package org.example.mapreduce.model;

import lombok.Getter;
@Getter
public class KeyValue {
    private String key;
    private String value;

    public KeyValue(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public String toLine() {
        return key + "\t" + value;
    }

    // todo не факт что правильная реализация
    public static KeyValue fromLine(String line) {
        String[] parts = line.split("\t", 2);
        if (parts.length == 2) {
            return new KeyValue(parts[0], parts[1]);
        }
        throw new IllegalArgumentException("Invalid line format: " + line);
    }
}