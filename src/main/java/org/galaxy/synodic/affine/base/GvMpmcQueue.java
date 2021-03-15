package org.galaxy.synodic.affine.base;

import org.jctools.queues.MpmcArrayQueue;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

public class GvMpmcQueue<E> extends MpmcArrayQueue<E> implements GvQueue<E> {

    private static final AtomicLongFieldUpdater flagUpdater;

    static {
        flagUpdater = AtomicLongFieldUpdater.newUpdater(GvMpmcQueue.class, "version");
    }

    private long p1, p2, p3, p4, p5, p6, p7;
    private volatile long          version;
    private final    Comparable<?> key;

    public GvMpmcQueue(Object key, int capacity) {
        super(capacity);
        this.key = key;
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
        return flagUpdater.compareAndSet(this, expect, newValue);
    }

}
