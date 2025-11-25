package org.example.mapreduce.coordinator;

import org.example.mapreduce.config.JobConfig;
import org.example.mapreduce.io.FileManager;
import org.example.mapreduce.model.MapTask;
import org.example.mapreduce.model.ReduceTask;
import org.example.mapreduce.model.Task;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Coordinator — распределитель задач для воркеров.
 *
 * Внутри возвращает объект Coordinator.Task, который может быть:
 * - MAP (с MapTask)
 * - REDUCE (с ReduceTask)
 * - STOP (команда завершиться)
 *
 * Логика:
 * - Пока MAP_STAGE: выдаём map задачи из mapQueue.
 * - Когда все map задачи завершены (и нет in-progress map): формируем reduceQueue и переходим в REDUCE_STAGE.
 * - Пока REDUCE_STAGE: выдаём reduce задачи из reduceQueue.
 * - Когда все reduce завершены: переводим state в FINISHED и возвращаем STOP всем воркерам.
 */
public class Coordinator {

    private final JobConfig config;

    // Очереди задач (thread-safe)
    private final BlockingQueue<MapTask> mapQueue;
    private final BlockingQueue<ReduceTask> reduceQueue;

    // Статусы завершения по id
    private final ConcurrentMap<Integer, Boolean> mapCompleted;
    private final ConcurrentMap<Integer, Boolean> reduceCompleted;

    // Счётчики задач, которые выданы воркерам и ещё выполняются (in-progress).
    // например когда мы хотим перейти на следующий этап и нам нужно чтобы счетчики были = 0
    private final AtomicInteger inProgressMapCount;
    private final AtomicInteger inProgressReduceCount;

    private State state;

    public enum State { MAP_STAGE, REDUCE_STAGE, FINISHED }

    /**
     * Task — внутренний wrapper, который возвращается воркеру.
     */


    public Coordinator(JobConfig config) {
        this.config = config;
        this.mapQueue = new LinkedBlockingQueue<>();
        this.reduceQueue = new LinkedBlockingQueue<>();
        this.mapCompleted = new ConcurrentHashMap<>();
        this.reduceCompleted = new ConcurrentHashMap<>();
        this.inProgressMapCount = new AtomicInteger(0);
        this.inProgressReduceCount = new AtomicInteger(0);
        this.state = State.MAP_STAGE;
    }

    /**
     * Инициализация: заполняет mapQueue и инициализирует mapCompleted
     * Должен вызываться до старта воркеров.
     */
    public void start() {
        List<Path> inputs = config.getInputFiles();
        for (int i = 0; i < inputs.size(); i++) {
            mapQueue.add(new MapTask(i, inputs.get(i)));
            mapCompleted.put(i, false);
        }

        // Если нет map задач — сразу подготовим стадию reduce (в synchronized для безопасности)
        if (mapQueue.isEmpty()) {
            synchronized (this) {
                transitionToReduceStageIfReady();
            }
        }
    }

    /**
     * Основной метод — воркер вызывает его, чтобы получить задачу.
     * Блокируется (wait), пока не появится работа или пока не придёт STOP.
     */
    // TODO: добавить реализацию: воркер должен запрашивать задачи и потом фиксировать их завершение(по-моему решено)
    public Task requestTask() throws InterruptedException {
        synchronized (this) {
            while (true) {
                // --- MAP stage logic ---
                if (state == State.MAP_STAGE) {
                    MapTask mt = mapQueue.poll();
                    if (mt != null) {
                        // выдаём map задачу воркеру и помечаем как in-progress
                        inProgressMapCount.incrementAndGet();
                        return Task.map(mt);
                    }

                    // mapQueue пуст; если есть in-progress maps — ждём их окончания
                    if (inProgressMapCount.get() > 0) {
                        this.wait();
                        continue;
                    }

                    // нет in-progress и очередь пуста -> возможно все map завершены
                    if (allMapsCompleted()) {
                        // переход на REDUCE_STAGE (если ещё не сделан)
                        transitionToReduceStageIfReady();
                        // Если после перехода состояние REDUCE_STAGE, loop итерация даст возможность обработать reduce
                        continue;
                    } else {
                        // есть map, которые ещё не помечены завершёнными (возможно mapQueue заполнится позже) — ждём
                        this.wait();
                        continue;
                    }
                }

                // --- REDUCE stage logic ---
                if (state == State.REDUCE_STAGE) {
                    ReduceTask rt = reduceQueue.poll();
                    if (rt != null) {
                        inProgressReduceCount.incrementAndGet();
                        return Task.reduce(rt);
                    }

                    // нет задач в очереди reduce
                    if (inProgressReduceCount.get() > 0) {
                        // ждём пока выполняемые reduce не закончатся
                        this.wait();
                        continue;
                    }

                    // нет in-progress reduce и очередь пуста -> все reduce должны быть завершены
                    if (allReducesCompleted()) {
                        state = State.FINISHED;
                        // разбудим всех, чтобы они получили STOP
                        this.notifyAll();
                        return Task.stop();
                    } else {
                        // ждём появления reduce задач (неожиданно) или их завершения
                        this.wait();
                        continue;
                    }
                }

                // --- FINISHED ---
                if (state == State.FINISHED) {

                    return Task.stop();
                }
            }
        }
    }

    /**
     * Worker вызывает после завершения map задачи.
     * mapId — id задачи (task.getMapTask().getTaskId()).
     */
    public void reportMapCompletion(int mapId) {
        // пометить как завершённую и уменьшить in-progress
        mapCompleted.put(mapId, true);
        inProgressMapCount.decrementAndGet();

        synchronized (this) {
            // если это был последний map и очередь пуста, перейти на reduce
            transitionToReduceStageIfReady();
            // разбудить ожидающие воркеры
            this.notifyAll();
        }
    }

    /**
     * Worker вызывает после завершения reduce задачи.
     */
    public void reportReduceCompletion(int reduceId) {
        reduceCompleted.put(reduceId, true);
        inProgressReduceCount.decrementAndGet();

        synchronized (this) {
            // если все reduce завершены и нет in-progress -> FINISHED будет поставлен в requestTask()
            this.notifyAll();
        }
    }

    /**
     * Ожидание окончательного завершения Coordinator'а (внешний вызов).
     */
    // TODO: что это? и для чего?
    public void awaitCompletion() throws InterruptedException {
        synchronized (this) {
            while (state != State.FINISHED) {
                this.wait();
            }
        }
    }

    /* ----------------- Вспомогательные методы ----------------- */

    private boolean allMapsCompleted() {
        // если карт пуст (нет входных файлов) — считаем завершёнными
        if (mapCompleted.isEmpty()) return true;
        return mapCompleted.values().stream().allMatch(Boolean::booleanValue);
    }

    private boolean allReducesCompleted() {
        if (reduceCompleted.isEmpty()) return true;
        return reduceCompleted.values().stream().allMatch(Boolean::booleanValue);
    }

    /**
     * Переводит coordinator в REDUCE_STAGE только если:
     * - текущее состояние MAP_STAGE
     * - mapQueue пуст
     * - нет in-progress map
     * - все map помечены завершёнными
     *
     * Этот метод синхронизирован должен вызываться под monitor'ом (synchronized(this))
     */
    private void transitionToReduceStageIfReady() {
        if (state != State.MAP_STAGE) return;

        // Требования для перехода. и если хоть одно не выполнено — не переходим
        boolean queueEmpty = mapQueue.isEmpty();
        boolean noInProgress = inProgressMapCount.get() == 0;
        boolean allCompleted = allMapsCompleted();

        if (!(queueEmpty && noInProgress && allCompleted)) {
            return; // ещё не готово
        }

        // Выполняем переход
        state = State.REDUCE_STAGE;

        // Создаём reduceTask'и один раз

        for (int reduceId = 0; reduceId < config.getReduceCount(); reduceId++) {
            // предполагается сигнатура FileManager.listIntermediateFilesForReducer(Path tempDir, int reduceId)
            List<Path> intermediateFiles = FileManager.listIntermediateFilesForReducer(reduceId, config.getWorkingDir());
            System.out.println("---------------------------------------");
            System.out.println("intermediateFiles.size() = " + intermediateFiles.size());
            intermediateFiles
                    .forEach(System.out::println);
            System.out.println("---------------------------------------");
            ReduceTask reduceTask = new ReduceTask(reduceId, intermediateFiles);
            reduceQueue.add(reduceTask);
            reduceCompleted.put(reduceId, false);
        }

        // Уведомляем всех воркеров - чтобы те, кто ждут, проверили REDUCE очередь
        this.notifyAll();
    }
}
