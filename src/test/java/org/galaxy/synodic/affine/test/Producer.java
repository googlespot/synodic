package org.galaxy.synodic.affine.test;

import com.conversantmedia.util.concurrent.DisruptorBlockingQueue;
import com.google.common.util.concurrent.RateLimiter;
import lombok.extern.slf4j.Slf4j;
import org.galaxy.synodic.affine.jdk.SequentialExecutor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

@Slf4j
public class Producer implements Runnable {
    final RateLimiter                generatorSpeed;
    final RateLimiter                logSpeed;
    final List<String>               groupKeys;
    final Map<String, AtomicInteger> msgLastIndex;
    final int                        max = 200_000;

    SequentialExecutor executor;

    public Producer(List<String> groupKeys) {
        this.groupKeys = groupKeys;
        this.msgLastIndex = new HashMap<>();
        for (String key : groupKeys) {
            msgLastIndex.put(key, new AtomicInteger(0));
        }
        this.generatorSpeed = RateLimiter.create(10000);
        this.logSpeed = RateLimiter.create(20);
    }

    public void joinExecutor(SequentialExecutor executor) {
        this.executor = executor;
    }


    @Override
    public void run() {
        int count = groupKeys.size();
        log.info("##-- groupKeySize={}", groupKeys.size());
        Thread current = Thread.currentThread();
        while (!current.isInterrupted()) {
            int index = Math.abs(ThreadLocalRandom.current().nextInt(count));
            String key = groupKeys.get(index);
            AtomicInteger indicator = msgLastIndex.get(key);
            int value = indicator.incrementAndGet();
            if (value > max) {
                log.info("stop all");
                return;
            }
            LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(1));
            executor.execute(key, () -> {
                LockSupport.parkNanos(TimeUnit.NANOSECONDS.toNanos(1));
                if ((value & 511) == 511) {
                    log.info("key={} value={}", key, value);
                }
            });
        }
    }


}
