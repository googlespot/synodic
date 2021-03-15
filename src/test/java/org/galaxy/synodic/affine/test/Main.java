package org.galaxy.synodic.affine.test;

import org.galaxy.synodic.affine.base.GvSpscChunkedArrayQueue;

public class Main {
    public static void main(String[] args) {
        GvSpscChunkedArrayQueue queue = new GvSpscChunkedArrayQueue("1", 256, 8192);
        System.out.println(queue);
    }
}
