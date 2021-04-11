package org.galaxy.synodic.affine.executor;

import java.util.Queue;

public interface GroupingTask extends Runnable {
    long IN     = 1;
    long OUT    = 0;
    long ENTER  = 3;
    long UPDATE = 2;
    long LEAVE  = 1;

    Comparable<?> getGroupKey();

    long getVersion();

    boolean casVersion(long expect, long newValue);

    Queue<Runnable> getSubTasks();

    void setExceptionHandler(ExceptionHandler handler);

    ExceptionHandler getExceptionHandler();

    interface ExceptionHandler {

        void handle(Throwable e);
    }

}
