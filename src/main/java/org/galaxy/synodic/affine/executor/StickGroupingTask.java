package org.galaxy.synodic.affine.executor;

import lombok.extern.slf4j.Slf4j;
import org.jctools.queues.MpmcArrayQueue;

import java.util.Queue;


@Slf4j
public class StickGroupingTask extends AbstractGroupingTask implements Runnable {

    public StickGroupingTask(Comparable<?> key, Queue<Runnable> queue) {
        super(key, queue);
    }

    public StickGroupingTask(Comparable<?> key, int capacity) {
        super(key, new MpmcArrayQueue<>(capacity));
    }

    @Override
    public void run() {
        try {
            long expected = getVersion();
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
            }
        } catch (Throwable throwable) {
            log.error("uncatched ", throwable);
        }
    }

}
