package org.apache.hadoop.hdfs.server.namenode.test.hdfs.block;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.server.namenode.*;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.util.GSet;
import org.apache.hadoop.util.LightWeightGSet;

import java.util.List;

public class FileINodeMap {
    private final GSet<INode, INodeWithAdditionalFields> map;

  public  static FileINodeMap newInstance(FileINodeDirectory rootDir) {
        // Compute the map capacity by allocating 1% of total memory
        int capacity = LightWeightGSet.computeCapacity(1, "INodeMap");
        GSet<INode, INodeWithAdditionalFields> map
                = new LightWeightGSet<INode, INodeWithAdditionalFields>(capacity);
        map.put(rootDir);
        return new FileINodeMap(map);
    }

    private  FileINodeMap(GSet<INode, INodeWithAdditionalFields> map) {
        Preconditions.checkArgument(map != null);
        this.map = map;
    }
    public void clear() {
        map.clear();
    }
    public final void put(INode inode) {
        if (inode instanceof INodeWithAdditionalFields) {
            map.put((INodeWithAdditionalFields)inode);
        }
    }

    public INode get(long id) {
        INode inode = new INodeWithAdditionalFields(id, null, new PermissionStatus(
                "", "", new FsPermission((short) 0)), 0, 0) {
            @Override
            public INode recordFileModification(Snapshot latest, FileINodeMap inodeMap) throws QuotaExceededException {
                return null;
            }

            @Override
            public INode updateModificationTime(long mtime, Snapshot latest, INodeMap inodeMap) throws QuotaExceededException {
                return null;
            }

            @Override
            public    INode recordModification(Snapshot latest, INodeMap inodeMap)
                    throws QuotaExceededException {
                return null;
            }

            @Override
            public void destroyAndCollectBlocks(BlocksMapUpdateInfo collectedBlocks,
                                                List<INode> removedINodes) {
                // Nothing to do
            }

            @Override
            public Quota.Counts computeQuotaUsage(Quota.Counts counts, boolean useCache,
                                                  int lastSnapshotId) {
                return null;
            }

            @Override
            public Content.Counts computeContentSummary(Content.Counts counts) {
                return null;
            }

            @Override
            public Quota.Counts cleanSubtree(Snapshot snapshot, Snapshot prior,
                                             BlocksMapUpdateInfo collectedBlocks, List<INode> removedINodes,
                                             boolean countDiffChange) throws QuotaExceededException {
                return null;
            }
        };

        return map.get(inode);
    }
    public final void remove(INode inode) {
        map.remove(inode);
    }

}
