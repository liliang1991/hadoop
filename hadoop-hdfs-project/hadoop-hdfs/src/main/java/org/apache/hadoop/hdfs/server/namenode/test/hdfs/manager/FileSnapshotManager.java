package org.apache.hadoop.hdfs.server.namenode.test.hdfs.manager;

import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.SnapshotException;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormat;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodesInPath;
import org.apache.hadoop.hdfs.server.namenode.snapshot.INodeDirectorySnapshottable;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotStats;
import org.apache.hadoop.hdfs.server.namenode.test.hdfs.block.FileINodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.test.hdfs.block.FileINodeDirectorySnapshottable;
import org.apache.hadoop.hdfs.server.namenode.test.hdfs.block.FileINodesInPath;
import org.apache.hadoop.hdfs.server.namenode.test.hdfs.fs.FileImageFormat;
import org.apache.hadoop.hdfs.server.namenode.test.hdfs.fs.FileSystemDirectory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class FileSnapshotManager implements SnapshotStats {
    FileSystemDirectory fsdir;
    private final AtomicInteger numSnapshots = new AtomicInteger();
    private final Map<Long, FileINodeDirectorySnapshottable> snapshottables
            = new HashMap<Long, FileINodeDirectorySnapshottable>();
    private int snapshotCounter = 0;
    private static final int SNAPSHOT_ID_BIT_WIDTH = 24;
    private boolean allowNestedSnapshots = false;

    public Map<Integer, Snapshot> read(DataInput in, FSImageFormat.Loader loader
    ) throws IOException {
        snapshotCounter = in.readInt();
        numSnapshots.set(in.readInt());

        // read snapshots
        final Map<Integer, Snapshot> snapshotMap = new HashMap<Integer, Snapshot>();
        for(int i = 0; i < numSnapshots.get(); i++) {
            final Snapshot s = Snapshot.read(in, loader);
            snapshotMap.put(s.getId(), s);
        }
        return snapshotMap;
    }

//    public Map<Integer, Snapshot> readfile(DataInput in, FileFSImageFormat.Loader loader
//    ) throws IOException {
//        snapshotCounter = in.readInt();
//        numSnapshots.set(in.readInt());
//
//        // read snapshots
//        final Map<Integer, Snapshot> snapshotMap = new HashMap<Integer, Snapshot>();
//        for(int i = 0; i < numSnapshots.get(); i++) {
//            final Snapshot s = Snapshot.read(in, loader);
//            snapshotMap.put(s.getId(), s);
//        }
//        return snapshotMap;
//    }
    @Override
    public int getNumSnapshottableDirs() {
        return 0;
    }

    @Override
    public int getNumSnapshots() {
        return 0;
    }
    public FileSnapshotManager(final FileSystemDirectory fsdir) {
        this.fsdir = fsdir;
    }
    public void addSnapshottable(FileINodeDirectorySnapshottable dir) {
        snapshottables.put(dir.getId(), dir);
    }
    /**
     * Write {@link #snapshotCounter}, {@link #numSnapshots},
     * and all snapshots to the DataOutput.
     */
    public void write(DataOutput out) throws IOException {
        out.writeInt(snapshotCounter);
        out.writeInt(numSnapshots.get());

        // write all snapshots.
        for(FileINodeDirectorySnapshottable snapshottableDir : snapshottables.values()) {
            for(Snapshot s : snapshottableDir.getSnapshotsByNames()) {
                s.write(out);
            }
        }
    }
    public Map<Integer, Snapshot> readFile(DataInput in, FileImageFormat.Loader loader
    ) throws IOException {
        snapshotCounter = in.readInt();
        numSnapshots.set(in.readInt());

        // read snapshots
        final Map<Integer, Snapshot> snapshotMap = new HashMap<Integer, Snapshot>();
        for(int i = 0; i < numSnapshots.get(); i++) {
            final Snapshot s = Snapshot.readFile(in, loader);
            snapshotMap.put(s.getId(), s);
        }
        return snapshotMap;
    }
    public void removeSnapshottable(FileINodeDirectorySnapshottable s) {
        snapshottables.remove(s.getId());
    }
    /** Remove snapshottable directories from {@link #snapshottables} */
    public void removeSnapshottable(List<FileINodeDirectorySnapshottable> toRemove) {
        if (toRemove != null) {
            for (FileINodeDirectorySnapshottable s : toRemove) {
                removeSnapshottable(s);
            }
        }
    }
    public SnapshottableDirectoryStatus[] getSnapshottableDirListing(
            String userName) {
        if (snapshottables.isEmpty()) {
            return null;
        }

        List<SnapshottableDirectoryStatus> statusList =
                new ArrayList<SnapshottableDirectoryStatus>();
        for (FileINodeDirectorySnapshottable dir : snapshottables.values()) {
            if (userName == null || userName.equals(dir.getUserName())) {
                SnapshottableDirectoryStatus status = new SnapshottableDirectoryStatus(
                        dir.getModificationTime(), dir.getAccessTime(),
                        dir.getFsPermission(), dir.getUserName(), dir.getGroupName(),
                        dir.getLocalNameBytes(), dir.getId(), dir.getChildrenNum(null),
                        dir.getNumSnapshots(),
                        dir.getSnapshotQuota(), dir.getParent() == null ?
                        DFSUtil.EMPTY_BYTES :
                        DFSUtil.string2Bytes(dir.getParent().getFullPathName()));
                statusList.add(status);
            }
        }
        Collections.sort(statusList, SnapshottableDirectoryStatus.COMPARATOR);
        return statusList.toArray(
                new SnapshottableDirectoryStatus[statusList.size()]);
    }

    public String createSnapshot(final String path, String snapshotName
    ) throws IOException {
        FileINodeDirectorySnapshottable srcRoot = getSnapshottableRoot(path);

        if (snapshotCounter == getMaxSnapshotID()) {
            // We have reached the maximum allowable snapshot ID and since we don't
            // handle rollover we will fail all subsequent snapshot creation
            // requests.
            //
            throw new SnapshotException(
                    "Failed to create the snapshot. The FileSystem has run out of " +
                            "snapshot IDs and ID rollover is not supported.");
        }

        srcRoot.addSnapshot(snapshotCounter, snapshotName);

        //create success, update id
        snapshotCounter++;
        numSnapshots.getAndIncrement();
        return Snapshot.getSnapshotPath(path, snapshotName);
    }
    public FileINodeDirectorySnapshottable getSnapshottableRoot(final String path
    ) throws IOException {
        final FileINodesInPath i = fsdir.getINodesInPath4Write(path);
        return FileINodeDirectorySnapshottable.valueOf(i.getLastINode(), path);
    }
    public int getMaxSnapshotID() {
        return ((1 << SNAPSHOT_ID_BIT_WIDTH) - 1);
    }

    public void deleteSnapshot(final String path, final String snapshotName,
                               INode.BlocksMapUpdateInfo collectedBlocks, final List<INode> removedINodes)
            throws IOException {
        // parse the path, and check if the path is a snapshot path
        // the INodeDirectorySnapshottable#valueOf method will throw Exception
        // if the path is not for a snapshottable directory
        FileINodeDirectorySnapshottable srcRoot = getSnapshottableRoot(path);
        srcRoot.removeSnapshot(snapshotName, collectedBlocks, removedINodes);
        numSnapshots.getAndDecrement();
    }
    public void renameSnapshot(final String path, final String oldSnapshotName,
                               final String newSnapshotName) throws IOException {
        // Find the source root directory path where the snapshot was taken.
        // All the check for path has been included in the valueOf method.
        final FileINodeDirectorySnapshottable srcRoot
                = FileINodeDirectorySnapshottable.valueOf(fsdir.getINode(path), path);
        // Note that renameSnapshot and createSnapshot are synchronized externally
        // through FSNamesystem's write lock
        srcRoot.renameSnapshot(path, oldSnapshotName, newSnapshotName);
    }

    public void setSnapshottable(final String path, boolean checkNestedSnapshottable)
            throws IOException {
        final FileINodesInPath iip = fsdir.getINodesInPath4Write(path);
        final FileINodeDirectory d = FileINodeDirectory.valueOf(iip.getLastINode(), path);
        if (checkNestedSnapshottable) {
            checkNestedSnapshottable(d, path);
        }


        final FileINodeDirectorySnapshottable s;
        if (d.isSnapshottable()) {
            //The directory is already a snapshottable directory.
            s = (FileINodeDirectorySnapshottable)d;
            s.setSnapshotQuota(FileINodeDirectorySnapshottable.SNAPSHOT_LIMIT);
        } else {
            s = d.replaceSelf4INodeDirectorySnapshottable(iip.getLatestSnapshot(),
                    fsdir.getINodeMap());
        }
        addSnapshottable(s);
    }

    private void checkNestedSnapshottable(FileINodeDirectory dir, String path)
            throws SnapshotException {
        if (allowNestedSnapshots) {
            return;
        }

        for(FileINodeDirectorySnapshottable s : snapshottables.values()) {
            if (s.isFileAncestorDirectory(dir)) {
                throw new SnapshotException(
                        "Nested snapshottable directories not allowed: path=" + path
                                + ", the subdirectory " + s.getFullPathName()
                                + " is already a snapshottable directory.");
            }
            if (dir.isFileAncestorDirectory(s)) {
                throw new SnapshotException(
                        "Nested snapshottable directories not allowed: path=" + path
                                + ", the ancestor " + s.getFullPathName()
                                + " is already a snapshottable directory.");
            }
        }
    }
    public void resetSnapshottable(final String path) throws IOException {
        final FileINodesInPath iip = fsdir.getINodesInPath4Write(path);
        final FileINodeDirectory d = FileINodeDirectory.valueOf(iip.getLastINode(), path);
        if (!d.isSnapshottable()) {
            // the directory is already non-snapshottable
            return;
        }
        final FileINodeDirectorySnapshottable s = (FileINodeDirectorySnapshottable) d;
        if (s.getNumSnapshots() > 0) {
            throw new SnapshotException("The directory " + path + " has snapshot(s). "
                    + "Please redo the operation after removing all the snapshots.");
        }

        if (s == fsdir.getRoot()) {
            s.setSnapshotQuota(0);
        } else {
            s.replaceSelf(iip.getLatestSnapshot(), fsdir.getINodeMap());
        }
        removeSnapshottable(s);
    }
    public FileINodeDirectorySnapshottable.SnapshotDiffInfo diff(final String path, final String from,
                                                                 final String to) throws IOException {
        if ((from == null || from.isEmpty())
                && (to == null || to.isEmpty())) {
            // both fromSnapshot and toSnapshot indicate the current tree
            return null;
        }

        // Find the source root directory path where the snapshots were taken.
        // All the check for path has been included in the valueOf method.
        FileINodesInPath inodesInPath = fsdir.getINodesInPath4Write(path.toString());
        final FileINodeDirectorySnapshottable snapshotRoot = FileINodeDirectorySnapshottable
                .valueOf(inodesInPath.getLastINode(), path);

        return snapshotRoot.computeDiff(from, to);
    }
}
