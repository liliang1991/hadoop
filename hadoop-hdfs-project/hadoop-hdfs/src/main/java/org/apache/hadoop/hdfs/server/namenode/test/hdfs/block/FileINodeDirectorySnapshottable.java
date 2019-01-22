package org.apache.hadoop.hdfs.server.namenode.test.hdfs.block;


import com.google.common.base.Preconditions;
import com.google.common.primitives.SignedBytes;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshotException;
import org.apache.hadoop.hdfs.server.namenode.*;
import org.apache.hadoop.hdfs.server.namenode.snapshot.FileWithSnapshot;
import org.apache.hadoop.hdfs.server.namenode.snapshot.INodeDirectorySnapshottable;
import org.apache.hadoop.hdfs.server.namenode.snapshot.INodeDirectoryWithSnapshot;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.util.Diff;
import org.apache.hadoop.hdfs.util.ReadOnlyList;
import org.apache.hadoop.util.Time;

import java.io.IOException;
import java.util.*;

public class FileINodeDirectorySnapshottable extends FileINodeDirectoryWithSnapshot {
  public   static final int SNAPSHOT_LIMIT = 1 << 16;

    int snapshotQuota=SNAPSHOT_LIMIT;
    private final List<Snapshot> snapshotsByNames = new ArrayList<Snapshot>();

    public FileINodeDirectorySnapshottable(FileINodeDirectory dir) {
        super(dir, true, dir instanceof FileINodeDirectoryWithSnapshot ?
                ((FileINodeDirectoryWithSnapshot) dir).getDiffs(): null);
    }
    public int getNumSnapshots() {
        return snapshotsByNames.size();
    }

    public void setSnapshotQuota(int snapshotQuota) {
        if (snapshotQuota < 0) {
            throw new HadoopIllegalArgumentException(
                    "Cannot set snapshot quota to " + snapshotQuota + " < 0");
        }
        this.snapshotQuota = snapshotQuota;
    }
    static public FileINodeDirectorySnapshottable valueOf(
            INode inode, String src) throws IOException {
        final FileINodeDirectory dir = FileINodeDirectory.valueOf(inode, src);
        if (!dir.isSnapshottable()) {
            throw new SnapshotException(
                    "Directory is not a snapshottable directory: " + src);
        }
        return (FileINodeDirectorySnapshottable)dir;
    }
  public   void addSnapshot(Snapshot snapshot) {
        this.snapshotsByNames.add(snapshot);
    }
    public int getSnapshotQuota() {
        return snapshotQuota;
    }
  public   ReadOnlyList<Snapshot> getSnapshotsByNames() {
        return ReadOnlyList.Util.asReadOnlyList(this.snapshotsByNames);
    }
    @Override
    public boolean isSnapshottable() {
        return true;
    }

  public   Snapshot addSnapshot(int id, String name) throws SnapshotException,
            QuotaExceededException {
        //check snapshot quota
        final int n = getNumSnapshots();
        if (n + 1 > snapshotQuota) {
            throw new SnapshotException("Failed to add snapshot: there are already "
                    + n + " snapshot(s) and the snapshot quota is "
                    + snapshotQuota);
        }
        final Snapshot s = new Snapshot(id, name, this);
        final byte[] nameBytes = s.getRoot().getLocalNameBytes();
        final int i = searchSnapshot(nameBytes);
        if (i >= 0) {
            throw new SnapshotException("Failed to add snapshot: there is already a "
                    + "snapshot with the same name \"" + Snapshot.getSnapshotName(s) + "\".");
        }

        final FileINodeDirectoryWithSnapshot.DirectoryDiff d = getDiffs().addDiff(s, this);
        d.snapshotINode = s.getFileRoot();
        snapshotsByNames.add(-i - 1, s);

        //set modification time
        updateModificationTime(Time.now(), null, null);
        s.getRoot().setModificationTime(getModificationTime(), null, null);
        return s;
    }
    private int searchSnapshot(byte[] snapshotName) {
        return Collections.binarySearch(snapshotsByNames, snapshotName);
    }
 public    Snapshot removeSnapshot(String snapshotName,
                            BlocksMapUpdateInfo collectedBlocks, final List<INode> removedINodes)
            throws SnapshotException {
        final int i = searchSnapshot(DFSUtil.string2Bytes(snapshotName));
        if (i < 0) {
            throw new SnapshotException("Cannot delete snapshot " + snapshotName
                    + " from path " + this.getFullPathName()
                    + ": the snapshot does not exist.");
        } else {
            final Snapshot snapshot = snapshotsByNames.get(i);
            Snapshot prior = Snapshot.findLatestSnapshot(this, snapshot);
            try {
                Quota.Counts counts = cleanSubtree(snapshot, prior, collectedBlocks,
                        removedINodes, true);
                FileINodeDirectory parent = getFileParent();
                if (parent != null) {
                    // there will not be any WithName node corresponding to the deleted
                    // snapshot, thus only update the quota usage in the current tree
                    parent.addSpaceConsumed(-counts.get(Quota.NAMESPACE),
                            -counts.get(Quota.DISKSPACE), true);
                }
            } catch(QuotaExceededException e) {
                LOG.error("BUG: removeSnapshot increases namespace usage.", e);
            }
            // remove from snapshotsByNames after successfully cleaning the subtree
            snapshotsByNames.remove(i);
            return snapshot;
        }
    }

    public void renameSnapshot(String path, String oldName, String newName)
            throws SnapshotException {
        if (newName.equals(oldName)) {
            return;
        }
        final int indexOfOld = searchSnapshot(DFSUtil.string2Bytes(oldName));
        if (indexOfOld < 0) {
            throw new SnapshotException("The snapshot " + oldName
                    + " does not exist for directory " + path);
        } else {
            final byte[] newNameBytes = DFSUtil.string2Bytes(newName);
            int indexOfNew = searchSnapshot(newNameBytes);
            if (indexOfNew > 0) {
                throw new SnapshotException("The snapshot " + newName
                        + " already exists for directory " + path);
            }
            // remove the one with old name from snapshotsByNames
            Snapshot snapshot = snapshotsByNames.remove(indexOfOld);
            final INodeDirectory ssRoot = snapshot.getRoot();
            ssRoot.setLocalName(newNameBytes);
            indexOfNew = -indexOfNew - 1;
            if (indexOfNew <= indexOfOld) {
                snapshotsByNames.add(indexOfNew, snapshot);
            } else { // indexOfNew > indexOfOld
                snapshotsByNames.add(indexOfNew - 1, snapshot);
            }
        }
    }

  public   FileINodeDirectory replaceSelf(final Snapshot latest, final FileINodeMap inodeMap)
            throws QuotaExceededException {
        if (latest == null) {
            Preconditions.checkState(getLastSnapshot() == null,
                    "latest == null but getLastSnapshot() != null, this=%s", this);
            return replaceSelf4INodeDirectory(inodeMap);
        } else {
            return replaceSelf4INodeDirectoryWithSnapshot(inodeMap)
                    .recordModification(latest, null);
        }
    }
    public SnapshotDiffInfo computeDiff(final String from, final String to)
            throws SnapshotException {
        Snapshot fromSnapshot = getSnapshotByName(from);
        Snapshot toSnapshot = getSnapshotByName(to);
        // if the start point is equal to the end point, return null
        if (from.equals(to)) {
            return null;
        }
        SnapshotDiffInfo diffs = new SnapshotDiffInfo(this, fromSnapshot,
                toSnapshot);
        computeDiffRecursively(this, new ArrayList<byte[]>(), diffs);
        return diffs;
    }
    private Snapshot getSnapshotByName(String snapshotName)
            throws SnapshotException {
        Snapshot s = null;
        if (snapshotName != null && !snapshotName.isEmpty()) {
            final int index = searchSnapshot(DFSUtil.string2Bytes(snapshotName));
            if (index < 0) {
                throw new SnapshotException("Cannot find the snapshot of directory "
                        + this.getFullPathName() + " with name " + snapshotName);
            }
            s = snapshotsByNames.get(index);
        }
        return s;
    }
    private void computeDiffRecursively(INode node, List<byte[]> parentPath,
                                        SnapshotDiffInfo diffReport) {
        ChildrenDiff diff = new ChildrenDiff();
        byte[][] relativePath = parentPath.toArray(new byte[parentPath.size()][]);
        if (node.isDirectory()) {
            FileINodeDirectory dir = node.asFileDirectory();
            if (dir instanceof FileINodeDirectoryWithSnapshot) {
                FileINodeDirectoryWithSnapshot sdir = (FileINodeDirectoryWithSnapshot) dir;
                boolean change = sdir.computeDiffBetweenSnapshots(
                        diffReport.from, diffReport.to, diff);
                if (change) {
                    diffReport.addDirDiff(sdir, relativePath, diff);
                }
            }
            ReadOnlyList<INode> children = dir.getChildrenList(diffReport
                    .isFromEarlier() ? diffReport.to : diffReport.from);
            for (INode child : children) {
                final byte[] name = child.getLocalNameBytes();
                if (diff.searchIndex(Diff.ListType.CREATED, name) < 0
                        && diff.searchIndex(Diff.ListType.DELETED, name) < 0) {
                    parentPath.add(name);
                    computeDiffRecursively(child, parentPath, diffReport);
                    parentPath.remove(parentPath.size() - 1);
                }
            }
        } else if (node.isFile() && node.asFile() instanceof FileWithSnapshot) {
            FileWithSnapshot file = (FileWithSnapshot) node.asFile();
            Snapshot earlierSnapshot = diffReport.isFromEarlier() ? diffReport.from
                    : diffReport.to;
            Snapshot laterSnapshot = diffReport.isFromEarlier() ? diffReport.to
                    : diffReport.from;
            boolean change = file.getDiffs().changedBetweenSnapshots(earlierSnapshot,
                    laterSnapshot);
            if (change) {
                diffReport.addFileDiff(file.asINodeFile(), relativePath);
            }
        }
    }
    public static class SnapshotDiffInfo {
        /** Compare two inodes based on their full names */
        public static final Comparator<INode> INODE_COMPARATOR =
                new Comparator<INode>() {
                    @Override
                    public int compare(INode left, INode right) {
                        if (left == null) {
                            return right == null ? 0 : -1;
                        } else {
                            if (right == null) {
                                return 1;
                            } else {
                                int cmp = compare(left.getParent(), right.getParent());
                                return cmp == 0 ? SignedBytes.lexicographicalComparator().compare(
                                        left.getLocalNameBytes(), right.getLocalNameBytes()) : cmp;
                            }
                        }
                    }
                };

        /** The root directory of the snapshots */
        private final FileINodeDirectorySnapshottable snapshotRoot;
        /** The starting point of the difference */
        private final Snapshot from;
        /** The end point of the difference */
        private final Snapshot to;
        /**
         * A map recording modified INodeFile and INodeDirectory and their relative
         * path corresponding to the snapshot root. Sorted based on their names.
         */
        private final SortedMap<INode, byte[][]> diffMap =
                new TreeMap<INode, byte[][]>(INODE_COMPARATOR);
        /**
         * A map capturing the detailed difference about file creation/deletion.
         * Each key indicates a directory whose children have been changed between
         * the two snapshots, while its associated value is a {@link INodeDirectoryWithSnapshot.ChildrenDiff}
         * storing the changes (creation/deletion) happened to the children (files).
         */
        private final Map<FileINodeDirectoryWithSnapshot, ChildrenDiff> dirDiffMap =
                new HashMap<FileINodeDirectoryWithSnapshot, ChildrenDiff>();

        SnapshotDiffInfo(FileINodeDirectorySnapshottable snapshotRoot, Snapshot start,
                         Snapshot end) {
            this.snapshotRoot = snapshotRoot;
            this.from = start;
            this.to = end;
        }

        /** Add a dir-diff pair */
        private void addDirDiff(FileINodeDirectoryWithSnapshot dir,
                                byte[][] relativePath, ChildrenDiff diff) {
            dirDiffMap.put(dir, diff);
            diffMap.put(dir, relativePath);
        }

        /** Add a modified file */
        private void addFileDiff(INodeFile file, byte[][] relativePath) {
            diffMap.put(file, relativePath);
        }

        /** @return True if {@link #from} is earlier than {@link #to} */
        private boolean isFromEarlier() {
            return Snapshot.ID_COMPARATOR.compare(from, to) < 0;
        }

        /**
         * Generate a {@link SnapshotDiffReport} based on detailed diff information.
         * @return A {@link SnapshotDiffReport} describing the difference
         */
        public SnapshotDiffReport generateReport() {
            List<SnapshotDiffReport.DiffReportEntry> diffReportList = new ArrayList<SnapshotDiffReport.DiffReportEntry>();
            for (INode node : diffMap.keySet()) {
                diffReportList.add(new SnapshotDiffReport.DiffReportEntry(SnapshotDiffReport.DiffType.MODIFY, diffMap
                        .get(node)));
                if (node.isDirectory()) {
                    ChildrenDiff dirDiff = dirDiffMap.get(node);
                    List<SnapshotDiffReport.DiffReportEntry> subList = dirDiff.generateReport(
                            diffMap.get(node), (INodeDirectoryWithSnapshot) node,
                            isFromEarlier());
                    diffReportList.addAll(subList);
                }
            }
            return new SnapshotDiffReport(snapshotRoot.getFullPathName(),
                    Snapshot.getSnapshotName(from), Snapshot.getSnapshotName(to),
                    diffReportList);
        }
    }
}
