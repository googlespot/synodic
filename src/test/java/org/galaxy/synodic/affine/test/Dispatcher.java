package org.galaxy.synodic.affine.test;

import com.conversantmedia.util.concurrent.DisruptorBlockingQueue;
import lombok.extern.slf4j.Slf4j;
import org.galaxy.synodic.affine.base.GvQueue;
import org.galaxy.synodic.affine.base.GvSpscChunkedArrayQueue;

import java.util.AbstractMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.stream.Stream;

@Slf4j
public class Dispatcher {
    private static final long IN     = 1;
    private static final long OUT    = 0;
    private static final long ENTER  = 3;
    private static final long UPDATE = 2;
    private static final long LEAVE  = 1;

    private final    ConcurrentHashMap<Comparable<?>, GvQueue<Runnable>> taskDataMap;
    private final    DisruptorBlockingQueue<GvQueue<Runnable>>           globalQueue;
    private final    Executor                                            executor;
    private volatile boolean                                             runnable = true;
    private final    int                                                 workerNumber;
    Worker[] workers;

    public Dispatcher(int globalQueueCapacity, Executor executor, int workerNumber) {
        this.taskDataMap = new ConcurrentHashMap<>();
        this.globalQueue = new DisruptorBlockingQueue<>(globalQueueCapacity);
        this.executor = executor;
        this.workerNumber = workerNumber;
    }

    public void startWorker() {
        workers = new Worker[workerNumber];
        for (int i = 0; i < workerNumber; i++) {
            workers[i] = this.new Worker();
            executor.execute(workers[i]);
        }
    }

    public void stopWorker() {
        this.runnable = false;
    }

    public Stream<Map.Entry<Comparable<?>, Integer>> getRemainingTaskCount() {
        return taskDataMap.entrySet().stream().map(e -> new AbstractMap.SimpleEntry<>(e.getKey(), e.getValue().size()));
    }

    public int getGlobalTaskCount() {
        return globalQueue.size();
    }

    //线程不安全的,每个key必须在相同的线程调用
    public void dispatch(Comparable<?> key, Runnable task) {
        GvQueue<Runnable> partition = getOrCreatePartition(key);
        long oldFlag = partition.getVersion();
        boolean added = partition.offer(task);
        if (!added) {
            // todo 拒绝策略
            log.warn("drop");
        }
        for (; ; oldFlag = partition.getVersion()) {
            //  如果原来是out，enter失败 其实说明了出现异常了
            if ((oldFlag & IN) == OUT && partition.casVersion(oldFlag, oldFlag + ENTER)) {
                globalQueue.offer(partition);
                break;
            } else if ((oldFlag & IN) == IN && partition.casVersion(oldFlag, oldFlag + UPDATE)) {
                break;
            }
        }
    }

    private GvQueue<Runnable> getOrCreatePartition(Comparable<?> key) {
        GvQueue<Runnable> partition = taskDataMap.get(key);
        if (partition == null) {
            //todo 容量参数化
            GvQueue<Runnable> q = new GvSpscChunkedArrayQueue<>(key, 256, 8192);
            GvQueue<Runnable> old = taskDataMap.putIfAbsent(key, q);
            partition = old == null ? q : old;
            log.info("create queue key={}", key);
        }
        return partition;
    }

    class Worker implements Runnable {
        //  考虑到复杂度后继再实现
        // private int maxTaskPerDispatch;

        @Override
        public void run() {
            try {
                while (runnable) {
                    GvQueue<Runnable> partition = globalQueue.take();
                    long takeVal = partition.getVersion();
                    while (runnable) {
                        Runnable task = partition.poll();
                        if (task == null) {
                            //
                            if (partition.casVersion(takeVal, takeVal + LEAVE)) {
                                System.out.println("thread =" + Thread.currentThread().getName() + " key=" + partition.getGroupKey());
                                break;
                            } else {
                                takeVal = partition.getVersion();
                            }
                            continue;
                        }
                        try {
                            task.run();
                        } catch (Exception e) {
                            log.error("task Exception key={} ", partition.getGroupKey(), e);
                        }
                    }
                }
            } catch (InterruptedException e) {
                runnable = false;
                log.warn("work interrupted ", e);
            } catch (Throwable throwable) {
                log.error("uncatched ", throwable);
            }
        }
    }
}
