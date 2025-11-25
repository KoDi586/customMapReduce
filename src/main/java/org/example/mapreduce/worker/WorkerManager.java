package org.example.mapreduce.worker;

import org.example.mapreduce.config.JobConfig;
import org.example.mapreduce.coordinator.Coordinator;
import java.util.ArrayList;
import java.util.List;

/**
 * Управляет пулом рабочих процессов.
 * При создании экземпляра класс только сохраняет параметры; запуск работников выполняется методом start().
 * Каждый работник начинает обработку задач из очереди сразу после вызова start().
 * Взаимодействует с координатором для получения и выполнения задач.
 * Ключевая особенность — централизованное управление жизненным циклом работников (запуск и остановка).
 */
public class WorkerManager {
    private final int workerCount;
    private final Coordinator coordinator;
    private final JobConfig config;

    private final List<Worker> workers = new ArrayList<>();
    private final List<Thread> threads = new ArrayList<>();

    private boolean started = false;

    /**
     * Конструктор теперь только сохраняет параметры.
     * Реальный запуск работников выполняется через start().
     */
    public WorkerManager(int workerCount, Coordinator coordinator, JobConfig config) {
        this.workerCount = workerCount;
        this.coordinator = coordinator;
        this.config = config;
    }

    /**
     * Запускает всех воркеров в отдельных потоках.
     * Повторные вызовы безопасны (не запустит второй раз).
     */
    public synchronized void start() {
        if (started) return;
        started = true;

        for (int i = 0; i < workerCount; i++) {
            Worker worker = new Worker(i, coordinator, config);
            workers.add(worker);

            Thread thread = new Thread(worker, "worker-" + i);
            thread.start();

            threads.add(thread);
        }
    }

    /**
     * Останавливает всех воркеров. Метод опционален, так как воркеры
     * останавливаются автоматически, по завершении всех reduce-задач.
     */
    public void stopAll() {
        for (Worker worker : workers) {
            worker.setRunning(false);
        }
    }
}
