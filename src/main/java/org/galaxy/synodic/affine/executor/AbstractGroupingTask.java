package org.galaxy.synodic.affine.executor;

import java.util.Queue;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

public abstract class AbstractGroupingTask implements Runnable, GroupingTask {

    private static final AtomicLongFieldUpdater Updater;

    static {
        Updater = AtomicLongFieldUpdater.newUpdater(AbstractGroupingTask.class, "version");
    }

    private long p1, p2, p3, p4, p5, p6, p7;
    private volatile long version;
    private          long s1, s2, s3, s4, s5, s6, s7;
    protected final Comparable<?>    key;
    protected final Queue<Runnable>  queue;
    protected       ExceptionHandler handler;

    protected AbstractGroupingTask(Comparable<?> key, Queue<Runnable> queue) {
        this.key = key;
        this.queue = queue;
    }

    @Override
    public Comparable<?> getGroupKey() {
        return key;
    }

    @Override
    public long getVersion() {
        return version;
    }

    @Override
    public boolean casVersion(long expect, long newValue) {
        return Updater.compareAndSet(this, expect, newValue);
    }

    @Override
    public Queue<Runnable> getSubTasks() {
        return queue;
    }

    @Override
    public void setExceptionHandler(ExceptionHandler handler) {
        this.handler = handler;
    }

    @Override
    public ExceptionHandler getExceptionHandler() {
        return handler;
    }

}
