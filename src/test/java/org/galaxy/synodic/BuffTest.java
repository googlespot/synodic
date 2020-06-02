package org.galaxy.synodic;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

public class BuffTest {

  static final AtomicInteger counter = new AtomicInteger(1);
  public static final int MAX = 1_000_000;
  private static final Logger logger = LoggerFactory.getLogger(BuffTest.class);


  @Test
  public void test() throws Exception {
    logger.info("----");



    final RingQueue<String> ringQueue = new RingQueue<>(20);
    final ConcurrentHashMap<String,Integer> totalMap= new ConcurrentHashMap<>(MAX*3);
    ScheduledExecutorService executor = Executors.newScheduledThreadPool(4);
    Runnable p1 = new P(ringQueue);
    Runnable p2 = new P(ringQueue);
    Runnable c1 = new C(ringQueue,totalMap);
    Runnable c2 = new C(ringQueue,totalMap);

    executor.schedule(c1, 100, TimeUnit.MICROSECONDS);
    executor.schedule(c2, 100, TimeUnit.MICROSECONDS);

    executor.schedule(p1, 100, TimeUnit.MICROSECONDS);
    executor.schedule(p2, 100, TimeUnit.MICROSECONDS);



    Thread.sleep(100);
    executor.shutdown();
    executor.awaitTermination(2,TimeUnit.MINUTES);
    Assertions.assertEquals(totalMap.size(),2*MAX);
  }


  public static class P implements Runnable {
    int index = 0;
    RingQueue<String> ringQueue;

    public P(RingQueue<String> ringQueue) {
      this.ringQueue = ringQueue;
    }

    @Override
    public void run() {
      String pName = "msg-" + counter.getAndIncrement() + "#--";
      for (; index <= MAX; ) {
        String v = pName + index;
        if (ringQueue.offer(v)) {
          logger.info("ppp\t" + v);
          int n = ThreadLocalRandom.current().nextInt(10000);
          if (n < 1) {
            try {
              TimeUnit.MICROSECONDS.sleep(n);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }
          index++;
        } else {
          long ri = ringQueue.getReadIndex();
          long wi = ringQueue.getWriteIndex();
          logger.info("full ri=" + ri + "\t wi=" + wi);
          LockSupport.parkNanos(100);
        }
      }

      logger.warn("p finished");
    }
  }

  public static class C implements Runnable {

    int count;
    long miss;
    RingQueue<String> ringQueue;
    ConcurrentHashMap<String,Integer> totalMap;

    public C(RingQueue ringQueue, ConcurrentHashMap<String,Integer> map) {
      this.ringQueue = ringQueue;
      totalMap=map;
    }

    @Override
    public void run() {

      for (; count < MAX; ) {
        String v = ringQueue.poll();
        if (v != null) {
          logger.info("ccc\t" + v);
          if(totalMap.putIfAbsent(v,count) ==null){
            count++;
          }else{
            logger.error("key={}",v);
          }


          // ThreadLocalRandom.current().nextInt(100000);
        } else {
          long ri = ringQueue.getReadIndex();
          long wi = ringQueue.getWriteIndex();
          logger.info("empty ri=" + ri + "\t wi=" + wi+"\t count="+count);
          LockSupport.parkNanos(100);
          miss++;
        }
      }

      logger.warn("c finished count="+count);
    }
  }
}



