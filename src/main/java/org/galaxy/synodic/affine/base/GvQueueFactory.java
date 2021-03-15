package org.galaxy.synodic.affine.base;

@FunctionalInterface
public interface GvQueueFactory {

    <E> GvQueue<E> createGvQueue(Comparable<?> key);
}
