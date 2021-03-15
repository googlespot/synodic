package org.galaxy.synodic.affine.jdk;

import lombok.extern.slf4j.Slf4j;
import org.galaxy.synodic.affine.base.GroupingTask;
import org.galaxy.synodic.affine.base.GvMpmcQueue;
import org.galaxy.synodic.affine.base.GvQueue;
import org.galaxy.synodic.affine.base.GvQueueFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Function;

import static org.galaxy.synodic.affine.base.GvQueue.*;

@Slf4j
public class SequentialExecutor {

    private final    Map<Comparable<?>, GvQueue<Runnable>>     taskMapQueue;
    private final    ThreadPoolExecutor                        executor;
    private volatile OverFlowHandler                           overFlowHandler;
    private          ExceptionHandler                          exceptionHandler;
    private          Function<GvQueue<Runnable>, GroupingTask> generate;
    private          boolean                                   autoCreateGroup;
    private          GvQueueFactory                            queueFactory;
    private          int                                       batchSizePerExecute;


    public SequentialExecutor(Map<Comparable<?>, GvQueue<Runnable>> taskMapQueue, ThreadPoolExecutor executor, OverFlowHandler overFlowHandler, ExceptionHandler exceptionHandler, boolean autoCreateGroup, GvQueueFactory queueFactory, int batchSizePerExecute) {

        this.taskMapQueue = taskMapQueue;
        this.executor = executor;
        this.overFlowHandler = overFlowHandler;
        this.exceptionHandler = exceptionHandler;
        this.autoCreateGroup = autoCreateGroup;
        this.queueFactory = queueFactory;
        if (batchSizePerExecute == 0) {
            generate = StickTask::new;
        } else {
            generate = LimitedTask::new;
        }
        this.batchSizePerExecute = batchSizePerExecute;
    }

    public SequentialExecutor(ThreadPoolExecutor executor, int groupQueueCapacity) {
        this(new ConcurrentHashMap<>(), executor, OverFlowHandler.DiscardOldest, null, true, new GvQueueFactory() {
            @Override
            public <E> GvQueue<E> createGvQueue(Comparable<?> key) {
                return new GvMpmcQueue<>(key, groupQueueCapacity);
            }
        }, 0);
    }

    public void execute(Comparable<?> key, Runnable task) {
        // check shutdown 参考jdk
        GvQueue<Runnable> gvQueue = getOrCreateGroup(key);
        long oldFlag = gvQueue.getVersion();
        boolean added = gvQueue.offer(task);
        if (!added) {
            overFlowHandler.onOverFlow(key, task, this);
        }
        for (; ; oldFlag = gvQueue.getVersion()) {
            if ((oldFlag & IN) == OUT && gvQueue.casVersion(oldFlag, oldFlag + ENTER)) {
                // 相比自己管理线程,这里每个任务多了一次new操作,另外线程池的线程数控制,比自己管理时都要多一次的volatile读开销
                // 这里的拒绝策略也比较受限，因为前面已经进入分区队列了，如果这里不能提交成功就会产生死消息,所以可考虑参考jdk再拉回来
                // 另外这里可以考虑再加个旁路的补偿任务,用于处理这个分区队列被拒绝的情况
                executor.execute(generate.apply(gvQueue));
                break;
            } else if ((oldFlag & IN) == IN && gvQueue.casVersion(oldFlag, oldFlag + UPDATE)) {
                break;
            }
        }
    }

    private GvQueue<Runnable> getOrCreateGroup(Comparable<?> key) {
        GvQueue<Runnable> partition = taskMapQueue.get(key);
        if (partition == null) {
            if (autoCreateGroup) {
                GvQueue<Runnable> q = queueFactory.createGvQueue(key);
                GvQueue<Runnable> old = taskMapQueue.putIfAbsent(key, q);
                partition = old == null ? q : old;
                log.info("create queue key={}", key);
            } else {
                throw new IllegalArgumentException("group queue not found");
            }

        }
        return partition;
    }

    public void createGroupIfAbsent(Comparable<?> key) {

    }

    public void shutdownNow() {
        executor.shutdownNow();
    }

    private class StickTask implements Runnable, GroupingTask {

        protected final GvQueue<Runnable> gvQueue;

        public StickTask(GvQueue<Runnable> gvQueue) {
            this.gvQueue = gvQueue;
        }

        // 也可以采用把旧的任务都丢掉,只取最新一个执行的方式
        // 另外这里也可以增加一次最多处理多少个任务就重新调度的策略
        public void run() {
            try {
                long takeVal = gvQueue.getVersion();
                while (!executor.isShutdown()) {
                    Runnable task = gvQueue.poll();
                    if (task == null) {
                        if (gvQueue.casVersion(takeVal, takeVal + LEAVE)) {
                            break;
                        } else {
                            takeVal = gvQueue.getVersion();
                        }
                        continue;
                    }
                    try {
                        task.run();
                    } catch (Exception e) {
                        //exceptionHandler
                        log.error("task Exception key={} ", gvQueue.getGroupKey(), e);
                    }
                    if (breakLoop()) {
                        break;
                    }
                }
            } catch (Throwable throwable) {
                log.error("uncatched ", throwable);
            }
        }

        protected boolean breakLoop() {
            return false;
        }

        public GvQueue<Runnable> getGroup() {
            return this.gvQueue;
        }
    }

    private class LimitedTask extends StickTask {
        private int count;

        public LimitedTask(GvQueue<Runnable> partition) {
            super(partition);
        }

        //exceptionHandler
        protected boolean breakLoop() {
            if (++count > batchSizePerExecute) {
                //所有权依然有效，所以可以直接放入后继任务
                executor.execute(generate.apply(gvQueue));
                return true;
            }
            return false;
        }
    }

    private class LimitedTask2 {
        // 可以先放弃然后根据队列是否为空尝试加入调度队列
    }

    public interface OverFlowHandler {
        void onOverFlow(Comparable<?> key, Runnable task, SequentialExecutor executor);

        // 如果使用这种策略需要PvQueue支持多线程消费
        OverFlowHandler DiscardOldest = (key, task, executor) -> {
            GvQueue<Runnable> partition = executor.taskMapQueue.get(key);
            partition.poll();
            executor.execute(key, task);
        };
    }

    public interface ExceptionHandler {

    }

}
