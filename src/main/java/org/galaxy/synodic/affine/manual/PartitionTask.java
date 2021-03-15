package org.galaxy.synodic.affine.manual;

@FunctionalInterface
public interface PartitionTask extends Runnable {
    default Comparable<?> getPartitionKey() {
        throw new UnsupportedOperationException();
    }
}
