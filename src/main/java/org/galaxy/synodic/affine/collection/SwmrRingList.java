package org.galaxy.synodic.affine.collection;

import java.util.ArrayDeque;
import java.util.Iterator;

//SingleWriteMultiReadRingList
public class SwmrRingList<E> extends ArrayDeque<E> {
    int capacity;


    public void append(E e) {
        if (size() < capacity) {
            addLast(e);
            return;
        }
        removeFirst();
        addLast(e);
    }

    private class LastIterator implements Iterator<E> {

        int lowIndex;

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public E next() {
            return null;
        }
    }


}
