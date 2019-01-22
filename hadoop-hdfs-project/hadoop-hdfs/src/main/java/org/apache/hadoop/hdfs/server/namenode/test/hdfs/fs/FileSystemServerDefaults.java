package org.apache.hadoop.hdfs.server.namenode.test.hdfs.fs;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.DataChecksum;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FileSystemServerDefaults implements Writable {
    private long blockSize;
    private int bytesPerChecksum;
    private int writePacketSize;
    private short replication;
    private int fileBufferSize;
    private boolean encryptDataTransfer;
    private long trashInterval;
    private DataChecksum.Type checksumType;

    public FileSystemServerDefaults(long blockSize, int bytesPerChecksum,
                                    int writePacketSize, short replication, int fileBufferSize,
                                    boolean encryptDataTransfer, long trashInterval,
                                    DataChecksum.Type checksumType) {
        this.blockSize = blockSize;
        this.bytesPerChecksum = bytesPerChecksum;
        this.writePacketSize = writePacketSize;
        this.replication = replication;
        this.fileBufferSize = fileBufferSize;
        this.encryptDataTransfer = encryptDataTransfer;
        this.trashInterval = trashInterval;
        this.checksumType = checksumType;
    }


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(blockSize);
        out.writeInt(bytesPerChecksum);
        out.writeInt(writePacketSize);
        out.writeShort(replication);
        out.writeInt(fileBufferSize);
        WritableUtils.writeEnum(out, checksumType);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        blockSize = in.readLong();
        bytesPerChecksum = in.readInt();
        writePacketSize = in.readInt();
        replication = in.readShort();
        fileBufferSize = in.readInt();
        checksumType = WritableUtils.readEnum(in, DataChecksum.Type.class);
    }
}
