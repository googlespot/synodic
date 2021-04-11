package org.galaxy.synodic.affine.executor;

public interface GroupingExecutor {
    void execute(Comparable<?> groupKey, Runnable task);
}
