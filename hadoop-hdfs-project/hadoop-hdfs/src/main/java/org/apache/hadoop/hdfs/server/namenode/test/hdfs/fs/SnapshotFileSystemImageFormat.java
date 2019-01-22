package org.apache.hadoop.hdfs.server.namenode.test.hdfs.fs;

import org.apache.hadoop.hdfs.server.namenode.FSImageFormat;
import org.apache.hadoop.hdfs.server.namenode.snapshot.INodeDirectorySnapshottable;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.server.namenode.test.hdfs.block.FileINodeDirectorySnapshottable;

import java.io.DataInput;
import java.io.IOException;

public class SnapshotFileSystemImageFormat {

    public static void loadSnapshotList(
            FileINodeDirectorySnapshottable snapshottableParent, int numSnapshots,
            DataInput in, FSImageFormat.Loader loader) throws IOException {
        for (int i = 0; i < numSnapshots; i++) {
            // read snapshots
            final Snapshot s = loader.getSnapshot(in);
            s.getRoot().setFileParent(snapshottableParent);
            snapshottableParent.addSnapshot(s);
        }
        int snapshotQuota = in.readInt();
        snapshottableParent.setSnapshotQuota(snapshotQuota);
    }
}
