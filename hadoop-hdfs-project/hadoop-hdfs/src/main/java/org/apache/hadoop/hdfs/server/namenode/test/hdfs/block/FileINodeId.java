package org.apache.hadoop.hdfs.server.namenode.test.hdfs.block;

import org.apache.hadoop.util.SequentialNumber;

public class FileINodeId  extends SequentialNumber {
    public static final long LAST_RESERVED_ID = 2 << 14 - 1;

    public static final long ROOT_INODE_ID = LAST_RESERVED_ID + 1;

    public FileINodeId(long initialValue) {
        super(initialValue);
    }

    @Override
    public long getCurrentValue() {
        return super.getCurrentValue();
    }

    @Override
    public void setCurrentValue(long value) {
        super.setCurrentValue(value);
    }

    @Override
    public long nextValue() {
        return super.nextValue();
    }

    @Override
    public void skipTo(long newValue) throws IllegalStateException {
        super.skipTo(newValue);
    }

    @Override
    public boolean equals(Object that) {
        return super.equals(that);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }
    public FileINodeId() {
        super(ROOT_INODE_ID);
    }
}
