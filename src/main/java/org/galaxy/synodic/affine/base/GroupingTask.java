package org.galaxy.synodic.affine.base;

public interface GroupingTask extends Runnable {
    GvQueue<Runnable> getGroup();
}
