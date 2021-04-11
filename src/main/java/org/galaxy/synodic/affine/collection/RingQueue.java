package org.galaxy.synodic.affine.collection;

import sun.misc.Contended;
import sun.misc.Unsafe;

/**
 * 64个字节一个cache line 8*8=64 8个 元素一个组 每组第一个元素的第一int的各个bit的作用，按大端字节序
 * <p>
 * 63-------------------------------0
 * 用低端3个bit读写完成状态，虽然2个bit也够了3个看来更有意义
 * <p>
 * 000  读完成了，写分配可以申请
 * 001  写申请分配成功了，还没有提交
 * 011  ❌
 * 111  ❌
 * 100  写完成了，读分配可以申请
 * 110  读申请分配成功了，还没有提交
 */

public class RingQueue<E> {
    //左移6位扩大 64倍，64字节是一个cacheline,代价过大是否合理需要评估
    public static final int  SCALA_MOVE     = 6;
    public static final long READ_COMPLETE  = 0B000;
    public static final long WRITE_ALLOC    = 0B001;
    public static final long WRITE_COMPLETE = 0B100;
    public static final long READ_ALLOC     = 0B100;

    final    long[] header;
    @Contended
    volatile long   readIndex;//读完最后的位置，由于提交顺序的问题可以一次推进很长,读完状态
    @Contended
    volatile long   readAlloc;//读申请到的位置
    @Contended
    volatile long   writeIndex;//写完最后的位置,由于提交顺序的问题可以一次推进很长，读完状态
    @Contended
    volatile long   writeAlloc;//写申请到的最后位置

    // readIndex<=readAlloc读申请到的位置 < writeIndex写完最后的位置<=writeAlloc
    // writeAlloc-readIndex <  数组长度  确保不撞车

    /**
     * 不少少于cpu核心数的16倍，如果慢消费可以考虑调整到1k，不过也不宜过大8k就是个非常大的值了
     */
    final int      capacity;
    final int      mask;
    final Object[] datas;

    public RingQueue(int size) {
        if (size < 4) {
            capacity = 4;
        } else {
            capacity = Integer.highestOneBit(size - 1) << 1;
        }
        mask = capacity - 1;
        header = new long[capacity * 8];
        datas = new Object[capacity];
    }


    public boolean offer(E e) {
        long wi = tryAllocWriteSN();
        if (wi < 0) {
            return false;
        }
        datas[(int) wi & mask] = e;
        pushlishWrite(wi);
        return true;
    }

    public E poll() {
        long ri = tryAllocReadSN();
        if (ri < 0) {
            return null;
        }
        int index = (int) ri & mask;
        E   e     = (E) datas[index];
        datas[index] = null;
        pushlishRead(ri);
        return e;
    }

    public long getReadIndex() {
        return readIndex;
    }

    public long getWriteIndex() {
        return writeIndex;
    }


    private long tryAllocWriteSN() {
        for (long wa = writeAlloc; ; wa = writeAlloc) {
            long sub = wa - readIndex;
            if (sub < capacity) {
                if (casWA(wa, wa + 1)) {
                    if (!casHead(wa, READ_COMPLETE, WRITE_ALLOC)) {
                        throw new IllegalMonitorStateException();
                    }
                    return wa;
                }
            } else if (sub == capacity && getHead(wa) == READ_COMPLETE) {
                Thread.yield();//jdk 升级可以考虑 onSpinWait​()
            } else {
                return -1;
            }
        }
    }

    private long tryAllocReadSN() {
        for (long ra = readAlloc; ; ra = readAlloc) {
            if (ra < writeIndex) {
                if (casRA(ra, ra + 1)) {
                    if (!casHead(ra, WRITE_COMPLETE, READ_ALLOC)) {
                        throw new IllegalMonitorStateException();
                    }
                    return ra;
                }
            } else if (ra == writeIndex && getHead(ra) == WRITE_COMPLETE) {
                Thread.yield();//jdk 升级可以考虑 onSpinWait​()
            } else {
                return -1;
            }
        }
    }

    private long pushlishWrite(final long index) {
        if (!casHead(index, WRITE_ALLOC, WRITE_COMPLETE)) {
            throw new IllegalMonitorStateException();
        }
        for (long wi = writeIndex; wi < writeAlloc; wi++) {
            if (getHead(wi) == WRITE_COMPLETE) {//优化成一步到位，如果小于直接跳过去
                casWI(wi, wi + 1);
            } else {
                return wi;
            }
        }
        return writeIndex;
    }

    private long pushlishRead(final long index) {
        if (!casHead(index, READ_ALLOC, READ_COMPLETE)) {
            throw new IllegalMonitorStateException();
        }
        for (long ri = readIndex; ri < readAlloc; ri++) {
            if (getHead(ri) == READ_COMPLETE) {//优化成一步到位，如果小于直接跳过去
                casRI(ri, ri + 1);
            } else {
                return ri;
            }
        }
        return readIndex;
    }


    private boolean casRI(long expect, long newVal) {
        return THE_UNSAFE.compareAndSwapLong(this, RI_OFFSET, expect, newVal);
    }

    private boolean casRA(long expect, long newVal) {
        return THE_UNSAFE.compareAndSwapLong(this, RA_OFFSET, expect, newVal);
    }

    private boolean casWI(long expect, long newVal) {
        return THE_UNSAFE.compareAndSwapLong(this, WI_OFFSET, expect, newVal);
    }

    private boolean casWA(long expect, long newVal) {
        return THE_UNSAFE.compareAndSwapLong(this, WA_OFFSET, expect, newVal);
    }

    private boolean casHead(long index, long expect, long newVal) {
        return THE_UNSAFE.compareAndSwapLong(header, BASE + ((index & mask) << SCALA_MOVE), expect, newVal);
    }

    private long getHead(long index) {
        return THE_UNSAFE.getLongVolatile(header, BASE + ((index & mask) << SCALA_MOVE));
    }

    private static final long   RI_OFFSET;
    private static final long   RA_OFFSET;
    private static final long   WI_OFFSET;
    private static final long   WA_OFFSET;
    private static final long   BASE;
    private static final Unsafe THE_UNSAFE = JdkUnsafe.getJdkUnsafe();

    static {
        try {
            RI_OFFSET = THE_UNSAFE.objectFieldOffset(RingQueue.class.getDeclaredField("readIndex"));
            RA_OFFSET = THE_UNSAFE.objectFieldOffset(RingQueue.class.getDeclaredField("readAlloc"));
            WI_OFFSET = THE_UNSAFE.objectFieldOffset(RingQueue.class.getDeclaredField("writeIndex"));
            WA_OFFSET = THE_UNSAFE.objectFieldOffset(RingQueue.class.getDeclaredField("writeAlloc"));
            BASE = THE_UNSAFE.arrayBaseOffset(long[].class);
        } catch (NoSuchFieldException e) {
            throw new Error(e);
        }
    }

}
