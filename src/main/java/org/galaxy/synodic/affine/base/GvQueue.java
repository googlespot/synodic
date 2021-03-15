package org.galaxy.synodic.affine.base;

import java.util.Queue;

/**
 * 使用这种继承队列的方式比组合的方式减少一次随机内存访问
 * 默认采用最低bit作为标记位，其他位作为版本号
 */
public interface GvQueue<E> extends Queue<E>, GroupingVersion {
    long IN     = 1;
    long OUT    = 0;
    long ENTER  = 3;
    long UPDATE = 2;
    long LEAVE  = 1;

    long MAX_VERSION = Long.MAX_VALUE / 2;

    /**
     * 可以在需要考虑溢出的场景，就可以采用轮转
     */
    default boolean casVersionRotate(long expect, long newValue) {
        if (newValue <= MAX_VERSION) return casVersion(expect, newValue);
        return casVersion(expect, 0);
    }

}
