package org.galaxy.synodic.affine.executor;

import java.util.Queue;

public class RandomGroupingTask extends AbstractGroupingTask {

    public RandomGroupingTask(Comparable<?> key, Queue<Runnable> queue) {
        super(key, queue);
    }

    @Override
    public void run() {

    }
}
