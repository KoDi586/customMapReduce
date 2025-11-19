Отлично. Ниже я дам тебе **полное, профессиональное, продуманное архитектурное разложение**,
но **без кода**, только структура и объяснения.
Это будет основа уровня “архитектор распределённых систем”.

Готовься — будет детально, но понятно 👇

---

# 🔥 Полная архитектура MAPREDUCE-МИНИ-ФРЕЙМВОРКА (JAVA)

---

# 1. 🧱 **Общая структура проекта**

Этот проект **НЕ требует Spring**, потому что:

* нет сети,
* нет REST,
* нет внешних сервисов,
* только многопоточность и файловая система.

Поэтому:

* **Maven / Gradle — идеально**
* чистый Java Core + Collections + IO + Concurrency

**Spring — лишняя нагрузка**.

---

# 2. 🗂️ **Проектная структура (пакеты и классы)**

```
src/main/java/
└── mapreduce/
    ├── coordinator/
    │     └── Coordinator.java
    │
    ├── worker/
    │     ├── Worker.java
    │     ├── MapTask.java
    │     ├── ReduceTask.java
    │     └── TaskType.java (ENUM)
    │
    ├── io/
    │     ├── FileManager.java
    │     ├── IntermediateFileWriter.java
    │     └── IntermediateFileReader.java
    │
    ├── functions/
    │     ├── Mapper.java (interface)
    │     ├── Reducer.java (interface)
    │     ├── DefaultWordCountMapper.java
    │     └── DefaultSumReducer.java
    │
    ├── model/
    │     ├── KeyValue.java
    │     ├── MapResult.java
    │     └── ReduceResult.java
    │
    ├── config/
    │     └── JobConfig.java
    │
    └── MapReduceApp.java
```

---

# 3. 🧠 **Главные сущности и потоки**

### 🔹 **Coordinator — 1 поток**

Отвечает за:

* список map-задач
* список reduce-задач
* очередь свободных задач
* выдачу задач воркерам
* переход из map-стадии в reduce-стадию
* завершение воркеров

### 🔹 **Worker — N потоков**

Каждый worker:

* запрашивает задачу
* выполняет задачу
* отдаёт результат координатору

---

# 4. 🧩 **Классы в деталях**

---

## 4.1. 📦 `JobConfig`

**Назначение:** конфигурация джобы.

**Свойства:**

* `List<String> inputFiles`
* `int workerCount`
* `int reduceCount`
* `Path inputDir`
* `Path outputDir`
* `Path tempDir`

---

## 4.2. 📦 `KeyValue`

**Назначение:** элементарная пара для map.

```
class KeyValue {
   String key;
   String value;
}
```

---

## 4.3. 📦 `MapTask`, `ReduceTask`

Оба наследуются от абстрактного `Task`.

### `MapTask`

* `int taskId`
* `String fileName`
* `int reduceCount`

### `ReduceTask`

* `int reduceId`
* `List<Path> intermediateFiles`

---

## 4.4. 📦 `TaskType`

ENUM:

```
MAP
REDUCE
NO_TASK
STOP
```

---

## 4.5. 🧠 Coordinator (главный менеджер)

**Основные поля:**

* `Queue<MapTask> mapTasks`
* `Queue<ReduceTask> reduceTasks`
* `Map<Integer, Boolean> mapCompleted`
* `Map<Integer, Boolean> reduceCompleted`
* `JobConfig config`
* состояние системы (**MAP_STAGE** / **REDUCE_STAGE** / **FINISHED**)

**Методы:**

### `Task requestTask(workerId)`

Логика:

* если MAP_STAGE → отдаёт MapTask
* если MAP закончились → ждёт завершения всех и переключается на REDUCE_STAGE
* если REDUCE_STAGE → отдаёт ReduceTask
* если REDUCE закончились → отдаёт STOP

### `void reportMapCompletion(int mapId)`

Отмечает завершение map-задачи.

### `void reportReduceCompletion(int reduceId)`

Отмечает завершение reduce-задачи.

---

# 4.6. 🔧 Worker (поток)

Каждый воркер — **Thread**:

**Алгоритм:**

```
while (true):
    task = coordinator.requestTask()
    
    if (task.type == MAP):
         executeMapTask()
    elif (task.type == REDUCE):
         executeReduceTask()
    elif (task.type == STOP):
         break
```

### Методы воркера:

#### `executeMapTask(MapTask task)`

* читает файл
* вызывает mapper.map()
* распределяет KeyValue по промежуточным файлам (`mr-X-Y`)

#### `executeReduceTask(ReduceTask task)`

* читает все промежуточные файлы для reduceId
* сортирует key-value
* группирует по ключу
* вызывает reducer.reduce()
* записывает результат в `result-Y.txt`

---

# 4.7. 📂 FileManager

Отвечает за:

* чтение исходных файлов
* создание каталогов
* управление временными файлами
* запись результатов

---

# 4.8. ✏️ IntermediateFileWriter

Обеспечивает запись пар KeyValue в `mr-X-Y`.

Разделяет ключи по формуле:

```
bucket = key.hashCode() % reduceCount
```

---

# 4.9. 📖 IntermediateFileReader

Читает все промежуточные `mr-X-Y` файлы для reduce-задачи.

---

# 4.10. 🧩 Mapper (interface)

```
List<KeyValue> map(String fileName, String content);
```

### DefaultWordCountMapper

Разбивает текст на слова → создаёт `(word, "1")`.

---

# 4.11. 🧩 Reducer (interface)

```
String reduce(String key, List<String> values);
```

### DefaultSumReducer

Суммирует строки → число.

---

---

# 5. 🧵 **Потоки и синхронизация**

### Используем:

* `synchronized` или `ReentrantLock` для выдачи задач
* `wait()` / `notifyAll()` или `Condition`
  чтобы воркеры ждали, пока координатор сменит стадию
* `AtomicInteger` для подсчёта завершённых задач
* `ExecutorService` — опционально (можно вручную запустить N threads)

---

# 6. ⚙️ Maven зависимости

Минимальный pom.xml:

```xml
<dependencies>
    <!-- Ничего больше не надо. Чистый Java. -->
</dependencies>
```

Можно добавить **Lombok**, если хочешь удобства:

```xml
<dependency>
    <groupId>org.projectlombok</groupId>
    <artifactId>lombok</artifactId>
    <version>1.18.32</version>
</dependency>
```

Но это НЕ обязательно.

---

# 7. ▶️ Запуск джобы

Класс:

```
MapReduceApp
```

**Логика:**

```
1. Считать конфиги
2. Создать Coordinator
3. Создать N Workers
4. Запустить потоки Workers
5. Coordinator работает в главном потоке (или своём)
6. Ждать завершения всех
7. Вывести результат
```

---

# 8. 📌 Краткое итоговое понимание

Ты создаёшь:

* 1 поток координатора
* N потоков воркеров
* интерфейс map и reduce
* структуру файлов (mr-X-Y и result-Y.txt)
* планировщик задач (Coordinator)
* многопоточное распределение работ
* парсинг и сортировку данных

То есть фактически **мини Hadoop**, но внутри одного процесса.

---

