package org.galaxy.synodic.disruptor;


import org.galaxy.synodic.affine.collection.JdkUnsafe;
import org.galaxy.synodic.affine.collection.RingQueue;
import sun.misc.Contended;
import sun.misc.Unsafe;


public class EventSession<T> {
    private final    RingQueue<ReqEvent> queue;
    @Contended
    private volatile ReqEventHandler     handler;
    @Contended
    private volatile long                tryInCounter;

    private T attachData;

    public EventSession(int queueLength) {
        int queueCapacity = queueLength < 1 ? 1 : Math.min(queueLength, 1024);
        queue = new RingQueue<>(queueCapacity);
    }

    public long getTryInCounter() {
        return tryInCounter;
    }

    public long tryIn() {
        return THE_UNSAFE.getAndAddLong(this, I_OFFSET, 1L);
    }

    public final boolean addEvent(ReqEvent event) {
        return queue.offer(event);
    }

    public final ReqEvent pollEvent() {
        return queue.poll();
    }

    public boolean assign(ReqEventHandler handler) {
        return THE_UNSAFE.compareAndSwapObject(this, H_OFFSET, null, handler);
    }

    public boolean retrieve(ReqEventHandler handler) {
        return THE_UNSAFE.compareAndSwapObject(this, H_OFFSET, handler, null);
    }

    public ReqEventHandler getHandler() {
        return handler;
    }

    public T getAttachData() {
        return attachData;
    }

    public void setAttachData(T attachData) {
        this.attachData = attachData;
    }

    private static final long   I_OFFSET;
    private static final long   H_OFFSET;
    private static final Unsafe THE_UNSAFE = JdkUnsafe.getJdkUnsafe();

    static {
        try {
            H_OFFSET = THE_UNSAFE.objectFieldOffset(EventSession.class.getDeclaredField("handler"));
            I_OFFSET = THE_UNSAFE.objectFieldOffset(EventSession.class.getDeclaredField("tryInCounter"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
