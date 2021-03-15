package org.galaxy.synodic.affine.manual;

import java.util.Queue;
import java.util.concurrent.Executor;

public interface PartitionSequenceExecutor {

    class Setting {
        int           keyQueueChunkSize;
        int           keyQueueCapacity;
        int           globalQueueCapacity;
        int           batchSizePerExecute;
        int           workerNumber;
        Executor      executor;
        RejectHandler rejectHandler;

    }

    void startDefaultSetting();

    void start(Setting setting);

    boolean isShutdown();

    void shutdown();

    void shutdownNow();

    void execute(PartitionTask task);

    Queue<Queue<Runnable>> getGlobalQueue();

    Queue<Runnable> getPartitionQueue(Comparable<?> key);

    void execute(Comparable<?> key, Runnable runnable);


}
