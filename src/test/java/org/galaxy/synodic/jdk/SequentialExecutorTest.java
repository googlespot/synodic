package org.galaxy.synodic.jdk;

import com.conversantmedia.util.concurrent.DisruptorBlockingQueue;
import lombok.extern.slf4j.Slf4j;
import org.galaxy.synodic.affine.executor.SequentialExecutor;
import org.galaxy.synodic.affine.executor.StickGroupingTask;
import org.openjdk.jmh.annotations.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@State(Scope.Benchmark)
public class SequentialExecutorTest {

    private SequentialExecutor sequentialExecutor;
    private List<String>       groups;
    int groupCount;
    private AtomicInteger gen = new AtomicInteger();

    public SequentialExecutorTest() {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(3, 5, 1, TimeUnit.MINUTES, new DisruptorBlockingQueue<>(1024), r -> new Thread(r, "exec-" + gen.incrementAndGet()));

        sequentialExecutor = new SequentialExecutor(executor, (key) -> new StickGroupingTask(key, 128));
        groupCount = 5;
        groups = new ArrayList<>(groupCount);


        for (int i = 0; i < groupCount; i++) {
            groups.add("group" + (i < 10 ? ("0" + i) : i));
        }

    }


    @Warmup(iterations = 3, time = 5, timeUnit = TimeUnit.MILLISECONDS)
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void test() throws Exception {
        int n = ThreadLocalRandom.current().nextInt(groupCount);
        String k = groups.get(n);
        //      CountDownLatch latch = new CountDownLatch(1);
        sequentialExecutor.execute(k, () -> {
            String s = n + k;
            //   log.info(s);
            //   latch.countDown();
        });
        //  latch.await();
    }

    public static void main(String[] args) throws Exception {
        SequentialExecutorTest test = new SequentialExecutorTest();
        for (int i = 0; i < 300; i++) {
            test.test();


        }

    }
}
