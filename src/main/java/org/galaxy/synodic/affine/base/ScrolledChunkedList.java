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
        if (capacityPerChunk < 2 || maxChunkSize < 2) {
            throw new IllegalArgumentException();
        }
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
        InfoNode infoOfTail = (InfoNode) tail[0];
        if (infoOfTail.dataLength < capacityPerChunk) {
            tail[++infoOfTail.dataLength] = e;
            dataLength++;
        } else {
            E[] newTail = newChunk(e);
            infoOfTail.next = newTail;
            tail = newTail;
            if (chunkSize == maxChunkSize) {
                InfoNode infoOfHead = (InfoNode) head[0];
                head = infoOfHead.next;

                //  infoOfHead.next = null; //help gc
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
            int index = ((InfoNode) tail[0]).dataLength;
            return tail[index];
        }
        return null;
    }

    public E replaceLast(E e) {
        if (tail != null) {
            int index = ((InfoNode) tail[0]).dataLength;
            E old = tail[index];
            tail[index] = e;
            return old;
        } else {
            append(e);
            return null;
        }
    }

    private E[] newChunk(E e) {
        Object[] chunk = new Object[capacityPerChunk + 1];
        InfoNode info = new InfoNode();
        info.dataLength = 1;
        chunk[0] = info;
        chunk[1] = e;
        return (E[]) chunk;
    }

    @Override
    public Iterator<E> iterator() {
        return this.new Iter(head);
    }

    /**
     *
     */
    public Iterator<E> iterator(int last) {
        int len;
        if (tail != null && last <= (len = ((InfoNode) tail[0]).dataLength)) {
            int offset = len - last;
            return this.new Iter(tail, offset);
        }
        Iter itr = this.new Iter(head);
        int needSkip = itr.totalCount - last;
        if (needSkip > 0) {
            itr.skip(needSkip);
        }
        return itr;
    }

    public int getDataLength() {
        return dataLength;
    }

    private class Iter implements Iterator<E> {
        final int totalCount;
        E[] currentChunk;
        int posOfChunk;
        int chunkDataLength;
        E[] next;

        public Iter(E[] head, int pos) {
            if (head == null) {
                posOfChunk = 1;
                chunkDataLength = 0;
                totalCount = 0;
            } else {
                totalCount = dataLength;
                begin(head, pos);

            }
        }

        public Iter(E[] head) {
            this(head, 0);
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
                begin(next, 0);
                return currentChunk[++posOfChunk];
            }
            throw new IllegalStateException();
        }

        public void skip(int count) {
            int remain = count;
            while (remain >= chunkDataLength) {
                if (next != null) {
                    remain = remain - chunkDataLength;
                    begin(next, 0);
                } else {
                    throw new IllegalArgumentException();
                }
            }
            posOfChunk = remain;
        }

        private void begin(E[] begin, int pos) {
            currentChunk = begin;
            posOfChunk = pos;
            InfoNode infoNode = (InfoNode) currentChunk[0];
            chunkDataLength = infoNode.dataLength;
            next = infoNode.next;
        }
    }

    private class InfoNode {
        int dataLength;
        E[] next;
    }

    public static void main(String[] args) {
        ScrolledChunkedList<String> scl = new ScrolledChunkedList<>(6, 4);

        for (int i = 0; i < 100; i++) {
            scl.append(String.valueOf(i));
            for (String s : scl) {
                System.out.print(s + "\t");
            }
            System.out.println("\t last=" + scl.getLast() + "\tcount=" + scl.getDataLength());
        }
//        for (String s : scl) {
//            System.out.println(s);
//        }

        for (Iterator itr = scl.iterator(3); itr.hasNext(); )
            System.out.println(itr.next());

    }


}
