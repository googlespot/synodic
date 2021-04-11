package org.galaxy.synodic.affine.executor;

import lombok.extern.slf4j.Slf4j;

import java.util.Queue;

@Slf4j
public class LimitedGroupingTask extends AbstractGroupingTask implements Runnable {

    private int              batchSize = 5;
    private GroupingExecutor executor;

    public LimitedGroupingTask(Comparable<?> key, Queue<Runnable> queue) {
        super(key, queue);
    }

    public void setExecutor(GroupingExecutor executor) {
        this.executor = executor;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    @Override
    public void run() {
        try {
            long expected = getVersion();
            int count = 0;
            Thread thread = Thread.currentThread();
            while (!thread.isInterrupted()) {
                Runnable task = queue.poll();
                if (task == null) {
                    if (casVersion(expected, expected + LEAVE)) {
                        break;
                    } else {
                        expected = getVersion();
                        continue;
                    }
                }
                try {
                    task.run();
                } catch (Throwable e) {
                    log.warn("task Exception key={} ", getGroupKey(), e);
                    if (handler != null) {
                        handler.handle(e);
                    }
                }
                if (++count >= batchSize) {
                    executor.execute(this.key, this);
                    break;
                }
            }
        } catch (Throwable throwable) {
            log.error("uncatched ", throwable);
        }
    }
}
