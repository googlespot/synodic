package org.galaxy.synodic.affine.base;


public interface GroupingVersion {

    Comparable<?> getGroupKey();

    long getVersion();

    boolean casVersion(long expect, long newValue);

}
