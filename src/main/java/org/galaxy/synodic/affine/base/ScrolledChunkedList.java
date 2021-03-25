package org.galaxy.synodic.affine.base;

import java.util.Iterator;

/**
 * 单线程只有追加写，多线程读的场景非常适用
 */
public class ScrolledChunkedList<E> implements Iterable<E> {

    private final int capacityPerChunk;
    private final int maxChunkSize;
    private       E[] head;
    private       E[] tail;
    private       int chunkSize;
    private       int dataLength;

    public ScrolledChunkedList(int capacityPerChunk, int maxChunkSize) {
        this.capacityPerChunk = capacityPerChunk;
        this.maxChunkSize = maxChunkSize;
    }

    public void append(E e) {
        if (head == null) {
            head = newChunk(e);
            tail = head;
            dataLength++;
            chunkSize = 1;
            return;
        }
        FirstNode firstOfTail = (FirstNode) tail[0];
        if (firstOfTail.dataLength < capacityPerChunk) {
            tail[++firstOfTail.dataLength] = e;
            dataLength++;
        } else {
            E[] newTail = newChunk(e);
            firstOfTail.next = newTail;
            tail = newTail;
            if (chunkSize == maxChunkSize) {
                FirstNode firstOfHead = (FirstNode) head[0];
                head = firstOfHead.next;
                firstOfHead.next = null; //help gc
                dataLength = dataLength + 1 - capacityPerChunk;
            } else {
                chunkSize++;
                dataLength++;
            }
        }

    }

    public E getFirst() {
        if (head != null) {
            return head[1];
        }
        return null;
    }

    public E getLast() {
        if (tail != null) {
            int index = ((FirstNode) tail[0]).dataLength;
            return tail[index];
        }
        return null;
    }


    private E[] newChunk(E e) {
        Object[] chunk = new Object[capacityPerChunk + 1];
        FirstNode first = new FirstNode();
        first.dataLength = 1;
        chunk[0] = first;
        chunk[1] = e;
        return (E[]) chunk;
    }

    @Override
    public Iterator<E> iterator() {
        return this.new Iter(head);
    }

    public int getDataLength() {
        return dataLength;
    }

    private class Iter implements Iterator<E> {
        E[] currentChunk;
        int posOfChunk;
        int chunkDataLength;
        E[] next;

        public Iter(E[] head) {
            begin(head);
        }

        @Override
        public boolean hasNext() {
            return posOfChunk < chunkDataLength || next != null;
        }

        @Override
        public E next() {
            if (posOfChunk < chunkDataLength) {
                return currentChunk[++posOfChunk];
            } else if (next != null) {
                begin(next);
                return currentChunk[++posOfChunk];
            }
            throw new IllegalStateException();
        }

        private void begin(E[] begin) {
            currentChunk = begin;
            posOfChunk = 0;
            FirstNode firstNode = (FirstNode) currentChunk[0];
            chunkDataLength = firstNode.dataLength;
            next = firstNode.next;
        }
    }

    private class FirstNode {
        int dataLength;
        E[] next;
    }

    public static void main(String[] args) {
        ScrolledChunkedList<String> scl = new ScrolledChunkedList<>(8, 3);

        for (int i = 0; i < 100; i++) {
            scl.append(String.valueOf(i));
            for (String s : scl) {
                System.out.print(s + "\t");
            }
            System.out.println("\t last=" + scl.getLast() + "\tcount=" + scl.getDataLength());
        }
        for (String s : scl) {
            System.out.println(s);
        }

    }


}
