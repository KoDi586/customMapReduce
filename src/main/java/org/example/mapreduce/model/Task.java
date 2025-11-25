package org.example.mapreduce.model;

public class Task {
    public enum Type { MAP, REDUCE, STOP }

    private final Type type;
    private final MapTask mapTask;
    private final ReduceTask reduceTask;

    private Task(Type type, MapTask mapTask, ReduceTask reduceTask) {
        this.type = type;
        this.mapTask = mapTask;
        this.reduceTask = reduceTask;
    }

    public static Task map(MapTask mt) {
        return new Task(Type.MAP, mt, null);
    }

    public static Task reduce(ReduceTask rt) {
        return new Task(Type.REDUCE, null, rt);
    }

    public static Task stop() {
        return new Task(Type.STOP, null, null);
    }

    public Type getType() { return type; }
    public MapTask getMapTask() { return mapTask; }
    public ReduceTask getReduceTask() { return reduceTask; }
}
