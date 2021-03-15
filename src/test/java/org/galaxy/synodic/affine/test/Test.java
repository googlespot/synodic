package org.galaxy.synodic.affine.test;

import lombok.SneakyThrows;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Test {
    static int maxline = 3000;

    public static void main(String[] args) {
        Dispatcher dispatcher = new Dispatcher(8192, Executors.newFixedThreadPool(6), 8);

        dispatcher.startWorker();
        ScheduledExecutorService sch = Executors.newScheduledThreadPool(1);

        final int workerNumber = 4;
        ExecutorService exec = Executors.newFixedThreadPool(workerNumber);
        String rootDir = "/Users/huanghaiping/logs/test";
        String readRootDir = rootDir + "/old";
        String writeRootDir = rootDir + "/new";
        String[] fileNames = {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n"};
        for (String fileName : fileNames) {
            String readPath = readRootDir + "/" + fileName;
            exec.execute(() -> {
                try {
                    gen(readPath, fileName, maxline);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
        LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(3));
        for (int i = 0; i < fileNames.length; ) {
            final int n = ThreadLocalRandom.current().nextInt(1, fileNames.length / workerNumber);
            String[] range = Arrays.copyOfRange(fileNames, i, i + n);
            i = i + n;
            range = Arrays.stream(range).filter(Objects::nonNull).toArray(String[]::new);
            Producer producer = new Producer(readRootDir, writeRootDir, range, dispatcher, maxline);
            exec.execute(producer);
        }

    }

    static class Producer implements Runnable {
        String                     readRootDir;
        String                     writeRootDir;
        String[]                   keys;
        Dispatcher                 dispatcher;
        Map<String, FileWriter>    fileWriters;
        Map<String, Queue<String>> queueMap;
        String                     end;
        private int counter;

        public Producer(String readRootDir, String writeRootDir, String[] keys, Dispatcher dispatcher, int maxline) {
            this.readRootDir = readRootDir;
            this.writeRootDir = writeRootDir;
            this.keys = keys;
            this.fileWriters = new ConcurrentHashMap<>();
            this.queueMap = new ConcurrentHashMap<>();
            this.dispatcher = dispatcher;
            this.end = String.valueOf(maxline);
            System.out.println("------");
            System.out.println("readRootDir=" + readRootDir);
            System.out.println("writeRootDir=" + writeRootDir);
            System.out.println("keys=" + Arrays.deepToString(keys));
        }

        @SneakyThrows
        @Override
        public void run() {
            for (String key : keys) {
                String readFile = readRootDir + "/" + key;
                Queue<String> q = new ArrayDeque<>(read(readFile));
                queueMap.put(key, q);
                fileWriters.put(key, new FileWriter(writeRootDir + "/" + key));
            }
            ConcurrentHashMap<String, Queue<String>> remainingMap = new ConcurrentHashMap<>(queueMap);
            int mask = keys.length - 1;
            while (remainingMap.size() > 0) {
                int startIndex = ThreadLocalRandom.current().nextInt(keys.length);
                for (int d = 0; d < keys.length; d++) {
                    int index = Math.min((d + startIndex), mask);
                    String key = keys[index];
                    Queue<String> lines = queueMap.get(key);
                    String line = lines.poll();
                    if (line == null) {
                        remainingMap.remove(key);
                        continue;
                    }
                    if ((counter & 31) == 31) {
                        Thread.yield();
                    }
                    dispatcher.dispatch(key, () -> {
                        try {
                            //  fileWriters.get(key).append(line).append("\n");
                            counter++;
                            if (counter % 100 == 0) {
                                System.out.println("out line =" + line);
                            }
                            LockSupport.parkNanos(3000);
                            if (line.endsWith(end)) {
                                System.out.println("process finish " + key);
                                fileWriters.get(key).close();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    });
                }
            }
            System.out.println("send finish " + Arrays.deepToString(keys));
        }
    }


    public static List<String> read(String path) throws IOException {
        return Files.lines(Paths.get(path)).collect(Collectors.toList());
    }

    public static void gen(String path, String key, int maxLine) throws IOException {
        AtomicInteger counter = new AtomicInteger();
        Iterable<String> strs = (Iterable) () -> {
            return Stream.generate(() -> key + counter.incrementAndGet()).limit(maxLine).iterator();
        };

        Files.write(Paths.get(path), strs, StandardCharsets.UTF_8, StandardOpenOption.CREATE);

    }
}
