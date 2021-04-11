//package org.galaxy.synodic.affine.test;
//
//import com.conversantmedia.util.concurrent.DisruptorBlockingQueue;
//import lombok.extern.slf4j.Slf4j;
//import org.galaxy.synodic.affine.executor.SequentialExecutor;
//
//import java.util.ArrayList;
//import java.util.List;
//import java.util.concurrent.BlockingQueue;
//import java.util.concurrent.ThreadFactory;
//import java.util.concurrent.ThreadPoolExecutor;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.atomic.AtomicInteger;
//
//@Slf4j
//public class SequentialExecutorTest {
//
//    public static void main(String[] args) {
//        BlockingQueue workQueue;
//        AtomicInteger gen = new AtomicInteger();
//
//        ThreadPoolExecutor executor = new ThreadPoolExecutor(4, 5, 1, TimeUnit.MINUTES, new DisruptorBlockingQueue<>(1024), new ThreadFactory() {
//            @Override
//            public Thread newThread(Runnable r) {
//                return new Thread(r, "exec-" + gen.incrementAndGet());
//            }
//        });
//        SequentialExecutor sequentialExecutor = new SequentialExecutor(executor, 128);
//
//        ThreadPoolExecutor poolExecutor = new ThreadPoolExecutor(2, 5, 1, TimeUnit.MINUTES, new DisruptorBlockingQueue<>(1024));
//        int n = 3;
//
//        List<Producer> ps = new ArrayList();
//        for (int i = 0; i < 20; i = i + n) {
//            List<String> list = new ArrayList<>(n);
//            for (int k = 0; k < n; k++) {
//                String key = Character.toString((char) ('a' + i + k));
//                log.info("---key={}", key);
//                list.add(key);
//            }
//            Producer producer = new Producer(list);
//            producer.joinExecutor(sequentialExecutor);
//            ps.add(producer);
//            log.info("i={}", i);
//        }
//
//        for (Producer p : ps) {
//            executor.execute(p);
//        }
//    }
//}
