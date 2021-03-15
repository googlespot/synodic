package org.galaxy.synodic.affine.manual;

import java.util.Queue;

public interface RejectHandler {

    void handleGlobal(Comparable<?> key, Runnable task, PartitionSequenceExecutor executor);

    void handlePartition(Comparable<?> key, Runnable task, PartitionSequenceExecutor executor);

    class DiscardOldest implements RejectHandler {

        @Override
        public void handleGlobal(Comparable<?> key, Runnable task, PartitionSequenceExecutor executor) {

        }

        @Override
        public void handlePartition(Comparable<?> key, Runnable task, PartitionSequenceExecutor executor) {
            if (!executor.isShutdown()) {
                Queue<Runnable> queue = executor.getPartitionQueue(key);
                if (queue != null) {
                    queue.poll();
                }
                executor.execute(key, task);
            }
        }
    }
}
