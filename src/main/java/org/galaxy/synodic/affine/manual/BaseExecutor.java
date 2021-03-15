package org.galaxy.synodic.affine.manual;


import lombok.extern.slf4j.Slf4j;
import org.galaxy.synodic.affine.base.GvQueue;
import org.galaxy.synodic.affine.base.GvSpscChunkedArrayQueue;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

import static org.galaxy.synodic.affine.base.GvQueue.*;

@Slf4j
public abstract class BaseExecutor implements PartitionSequenceExecutor {
    protected        ConcurrentMap<Comparable<?>, GvQueue<Runnable>> taskDataMap;
    protected        BlockingQueue<GvQueue<Runnable>>                globalQueue;
    protected        Executor                                        executor;
    protected        int                                             keyQueueChunkSize;
    protected        int                                             keyQueueCapacity;
    protected        int                                             globalQueueCapacity;
    protected        int                                             workerNumber;
    protected        RejectHandler                                   rejectHandler;
    private volatile boolean                                         runnable;

    protected void init(Setting setting) {

    }

    public void execute(Comparable<?> key, Runnable task) {
        GvQueue<Runnable> partition = getOrCreatePartition(key);
        long oldFlag = partition.getVersion();
        boolean added = partition.offer(task);
        if (!added) {
            rejectHandler.handlePartition(key, task, this);
        }
        for (; ; oldFlag = partition.getVersion()) {
            //  如果原来是out，enter失败 其实说明了出现异常了
            if ((oldFlag & IN) == OUT && partition.casVersion(oldFlag, oldFlag + ENTER)) {
                boolean success = globalQueue.offer(partition);
                if (!success) {
                    rejectHandler.handleGlobal(key, task, this);
                }
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
            GvQueue<Runnable> q = new GvSpscChunkedArrayQueue<>(key, keyQueueChunkSize, keyQueueCapacity);
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
