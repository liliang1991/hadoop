package org.apache.hadoop.hdfs.server.namenode.test.hdfs.block;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;

import java.util.Arrays;

public class FileINodesInPath {
    public static final Log LOG = LogFactory.getLog(FileINodesInPath.class);

    byte[][] path;

    private INode[] inodes;

    private int numNonNull;

    private int capacity;

    boolean isSnapshot;
    private Snapshot snapshot = null;
    private int snapshotRootIndex;
    int getSnapshotRootIndex() {
        return this.snapshotRootIndex;
    }

    public   boolean isSnapshot() {
        return this.isSnapshot;
    }
  public   byte[] getLastLocalName() {
        return path[path.length - 1];
    }
    private FileINodesInPath(byte[][] path, int number) {
        this.path = path;
        assert (number >= 0);
        inodes = new INode[number];
        capacity = number;
        numNonNull = 0;
        isSnapshot = false;
        snapshotRootIndex = -1;
    }
    private void addNode(INode node) {
        inodes[numNonNull++] = node;
    }
    private void updateLatestSnapshot(Snapshot s) {
        if (snapshot == null
                || (s != null && Snapshot.ID_COMPARATOR.compare(snapshot, s) < 0)) {
            snapshot = s;
        }
    }
    public Snapshot getLatestSnapshot() {
        return isSnapshot? null: snapshot;
    }
    private void setSnapshot(Snapshot s) {
        snapshot = s;
    }
    public INode getINode(int i) {
        return inodes[i >= 0? i: inodes.length + i];
    }
    private static boolean isDotSnapshotDir(byte[] pathComponent) {
        return pathComponent == null ? false
                : Arrays.equals(HdfsConstants.DOT_SNAPSHOT_DIR_BYTES, pathComponent);
    }
   public INode[] getINodes() {
        if (capacity < inodes.length) {
            INode[] newNodes = new INode[capacity];
            System.arraycopy(inodes, 0, newNodes, 0, capacity);
            inodes = newNodes;
        }
        return inodes;
    }
   public void setLastINode(INode last) {
        inodes[inodes.length - 1] = last;
    }

    public static FileINodesInPath resolve(final FileINodeDirectory startingDir,
                                final byte[][] components) throws UnresolvedLinkException {
        return resolve(startingDir, components, components.length, false);
    }
    public Snapshot getPathSnapshot() {
        return isSnapshot? snapshot: null;
    }

    public static FileINodesInPath resolve(final FileINodeDirectory startingDir,
                                       final byte[][] components, final int numOfINodes,
                                       final boolean resolveLink) throws UnresolvedLinkException {
        Preconditions.checkArgument(startingDir.compareTo(components[0]) == 0);

        INode curNode = startingDir;
        final FileINodesInPath existing = new FileINodesInPath(components, numOfINodes);
        int count = 0;
        int index = numOfINodes - components.length;
        if (index > 0) {
            index = 0;
        }
        while (count < components.length && curNode != null) {
            final boolean lastComp = (count == components.length - 1);
            if (index >= 0) {
                existing.addNode(curNode);
            }
            final boolean isRef = curNode.isReference();
            final boolean isDir = curNode.isDirectory();
            final FileINodeDirectory dir = isDir? curNode.asFileDirectory(): null;
            if (!isRef && isDir && dir instanceof FileINodeDirectoryWithSnapshot) {
                //if the path is a non-snapshot path, update the latest snapshot.
                if (!existing.isSnapshot()) {
                    existing.updateLatestSnapshot(
                            ((FileINodeDirectoryWithSnapshot)dir).getLastSnapshot());
                }
            } else if (isRef && isDir && !lastComp) {
                // If the curNode is a reference node, need to check its dstSnapshot:
                // 1. if the existing snapshot is no later than the dstSnapshot (which
                // is the latest snapshot in dst before the rename), the changes
                // should be recorded in previous snapshots (belonging to src).
                // 2. however, if the ref node is already the last component, we still
                // need to know the latest snapshot among the ref node's ancestors,
                // in case of processing a deletion operation. Thus we do not overwrite
                // the latest snapshot if lastComp is true. In case of the operation is
                // a modification operation, we do a similar check in corresponding
                // recordModification method.
                if (!existing.isSnapshot()) {
                    int dstSnapshotId = curNode.asReference().getDstSnapshotId();
                    Snapshot latest = existing.getLatestSnapshot();
                    if (latest == null ||  // no snapshot in dst tree of rename
                            dstSnapshotId >= latest.getId()) { // the above scenario
                        Snapshot lastSnapshot = null;
                        if (curNode.isDirectory()
                                && curNode.asFileDirectory() instanceof FileINodeDirectoryWithSnapshot) {
                            lastSnapshot = ((FileINodeDirectoryWithSnapshot) curNode
                                    .asFileDirectory()).getLastSnapshot();
                        }
                        existing.setSnapshot(lastSnapshot);
                    }
                }
            }
            if (curNode.isSymlink() && (!lastComp || (lastComp && resolveLink))) {
                final String path = constructPath(components, 0, components.length);
                final String preceding = constructPath(components, 0, count);
                final String remainder =
                        constructPath(components, count + 1, components.length);
                final String link = DFSUtil.bytes2String(components[count]);
                final String target = curNode.asSymlink().getSymlinkString();
                if (LOG.isDebugEnabled()) {
                    LOG.debug("UnresolvedPathException " +
                            " path: " + path + " preceding: " + preceding +
                            " count: " + count + " link: " + link + " target: " + target +
                            " remainder: " + remainder);
                }
                throw new UnresolvedPathException(path, preceding, remainder, target);
            }
            if (lastComp || !isDir) {
                break;
            }
            final byte[] childName = components[count + 1];

            // check if the next byte[] in components is for ".snapshot"
            if (isDotSnapshotDir(childName)
                    && isDir && dir instanceof FileINodeDirectorySnapshottable) {
                // skip the ".snapshot" in components
                count++;
                index++;
                existing.isSnapshot = true;
                if (index >= 0) { // decrease the capacity by 1 to account for .snapshot
                    existing.capacity--;
                }
                // check if ".snapshot" is the last element of components
                if (count == components.length - 1) {
                    break;
                }
                // Resolve snapshot root
                final Snapshot s = ((FileINodeDirectorySnapshottable)dir).getSnapshot(
                        components[count + 1]);
                if (s == null) {
                    //snapshot not found
                    curNode = null;
                } else {
                    curNode = s.getRoot();
                    existing.setSnapshot(s);
                }
                if (index >= -1) {
                    existing.snapshotRootIndex = existing.numNonNull;
                }
            } else {
                // normal case, and also for resolving file/dir under snapshot root
                curNode = dir.getChild(childName, existing.getPathSnapshot());
            }
            count++;
            index++;
        }
        return existing;
    }

    private static String constructPath(byte[][] components, int start, int end) {
        StringBuilder buf = new StringBuilder();
        for (int i = start; i < end; i++) {
            buf.append(DFSUtil.bytes2String(components[i]));
            if (i < end - 1) {
                buf.append(Path.SEPARATOR);
            }
        }
        return buf.toString();
    }
   public void setINode(int i, INode inode) {
        inodes[i >= 0? i: inodes.length + i] = inode;
    }
    public INode getLastINode() {
        return inodes[inodes.length - 1];
    }

}
