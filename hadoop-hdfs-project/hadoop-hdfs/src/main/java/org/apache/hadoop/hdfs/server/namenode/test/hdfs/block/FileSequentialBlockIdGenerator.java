package org.apache.hadoop.hdfs.server.namenode.test.hdfs.block;

import org.apache.hadoop.hdfs.server.namenode.test.hdfs.manager.FileBlockManager;
import org.apache.hadoop.util.SequentialNumber;

public class FileSequentialBlockIdGenerator extends SequentialNumber {
    public static final long LAST_RESERVED_BLOCK_ID = 1024L * 1024 * 1024;

    FileBlockManager blockManager;

   public FileSequentialBlockIdGenerator(FileBlockManager blockManagerRef) {
        super(LAST_RESERVED_BLOCK_ID);
        this.blockManager = blockManagerRef;
    }

    protected FileSequentialBlockIdGenerator(long initialValue) {
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
}
