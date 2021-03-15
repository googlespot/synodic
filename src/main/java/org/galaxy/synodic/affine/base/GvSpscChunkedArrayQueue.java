package org.galaxy.synodic.affine.base;

import org.jctools.queues.SpscChunkedArrayQueue;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

public class GvSpscChunkedArrayQueue<E> extends SpscChunkedArrayQueue<E> implements GvQueue<E> {
    private static final AtomicLongFieldUpdater flagUpdater;

    static {
        flagUpdater = AtomicLongFieldUpdater.newUpdater(GvSpscChunkedArrayQueue.class, "version");
    }

    private long p1, p2, p3, p4, p5, p6, p7;
    private volatile long          version;
    private final    Comparable<?> key;

    public GvSpscChunkedArrayQueue(Comparable<?> key, int chunkSize, int capacity) {
        super(chunkSize, capacity);
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
