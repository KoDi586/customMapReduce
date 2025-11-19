package org.example.mapreduce.worker;

import org.example.mapreduce.coordinator.Coordinator;
import org.example.mapreduce.model.Task;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Управляет пулом рабочих процессов: при создании немедленно запускает указанное количество рабочих в отдельных потоках.
 * Каждый работник начинает обработку задач из очереди сразу после запуска.
 * Взаимодействует с координатором для получения и выполнения задач.
 * Ключевая особенность — централизованное управление жизненным циклом работников (запуск и остановка).
 * <p>
 * Примечание: конструктор {@link WorkerManager} автоматически запускает всех работников.
 * Нет необходимости вызывать дополнительный метод запуска — выполнение начинается сразу
 * при создании экземпляра.
 */
public class WorkerManager {
    private final List<Worker> workers = new ArrayList<>();
    private final List<Thread> threads = new ArrayList<>();

    public WorkerManager(int workerCount, Coordinator coordinator) {
        for (int i = 0; i < workerCount; i++) {
            Worker worker = new Worker(i, coordinator);
            workers.add(worker);

            Thread thread = new Thread(worker, "worker-" + i);
            thread.start();

            threads.add(thread);
        }
    }

    public void stopAll() {
        for (Worker worker : workers) {
            worker.setRunning(false);
//            worker.submitTask(Task.stop());
        }
    }
}
