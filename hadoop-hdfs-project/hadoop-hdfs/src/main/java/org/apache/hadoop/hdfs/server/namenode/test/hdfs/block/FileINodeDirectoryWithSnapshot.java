package org.apache.hadoop.hdfs.server.namenode.test.hdfs.block;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.server.namenode.*;
import org.apache.hadoop.hdfs.server.namenode.snapshot.*;
import org.apache.hadoop.hdfs.util.Diff;
import org.apache.hadoop.hdfs.util.ReadOnlyList;

import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

import static java.util.Arrays.asList;
/*
表示带有快照的目录,不能创建新的快照，只能将目录的变化记录到现有的快照里面
 */
public class FileINodeDirectoryWithSnapshot extends FileINodeDirectoryWithQuota {
    DirectoryDiffList diffs;

    public Snapshot getLastSnapshot() {
        return diffs.getLastSnapshot();
    }

    public FileINodeDirectoryWithSnapshot(FileINodeDirectory that) {
        this(that, true, that instanceof FileINodeDirectoryWithSnapshot ?
                ((FileINodeDirectoryWithSnapshot) that).getDiffs() : null);
    }

    FileINodeDirectoryWithSnapshot(FileINodeDirectory that, boolean adopt,
                                   DirectoryDiffList diffs) {
        super(that, adopt, that.getNsQuota(), that.getDsQuota());
        this.diffs = diffs != null ? diffs : new DirectoryDiffList();
    }

    public DirectoryDiffList getDiffs() {
        return diffs;
    }

    public static class DirectoryDiffList
            extends AbstractINodeDiffList<FileINodeDirectory, FileINodeDirectoryAttributes, DirectoryDiff> {

        @Override
        public DirectoryDiff createDiff(Snapshot snapshot, FileINodeDirectory currentDir) {
            return new DirectoryDiff(snapshot, currentDir);
        }

        @Override
        public FileINodeDirectoryAttributes createSnapshotCopy(FileINodeDirectory currentDir) {
            return currentDir.isQuotaSet() ?
                    new CopyWithQuota(currentDir)
                    : new FileINodeDirectoryAttributes.SnapshotCopy(currentDir);
        }


        private boolean replaceChild(final Diff.ListType type, final INode oldChild,
                                     final INode newChild) {
            final List<DirectoryDiff> diffList = asList();
            for (int i = diffList.size() - 1; i >= 0; i--) {
                final ChildrenDiff diff = diffList.get(i).diff;
                if (diff.replace(type, oldChild, newChild)) {
                    return true;
                }
            }
            return false;
        }


        private boolean removeChild(final Diff.ListType type, final INode child) {
            final List<DirectoryDiff> diffList = asList();
            for (int i = diffList.size() - 1; i >= 0; i--) {
                final ChildrenDiff diff = diffList.get(i).diff;
                if (diff.removeChild(type, child)) {
                    return true;
                }
            }
            return false;
        }
    }


    /*    public boolean addChild(INode inode, boolean setModTime, Snapshot latest,
                                final INodeMap inodeMap) throws QuotaExceededException {
            ChildrenDiff diff = null;
            Integer undoInfo = null;
            if (isInLatestSnapshot(latest)) {
                diff = diffs.checkAndAddLatestSnapshotDiff(latest, this).diff;
                undoInfo = diff.create(inode);
            }
            final boolean added = super.addChild(inode, setModTime, null, inodeMap);
            if (!added && undoInfo != null) {
                diff.undoCreate(inode, undoInfo);
            }
            return added;
        }*/
    public void undoRename4ScrParent(final INodeReference oldChild,
                                     final INode newChild, Snapshot latestSnapshot)
            throws QuotaExceededException {
        diffs. removeChild(Diff.ListType.DELETED, oldChild);
        diffs.replaceChild(Diff.ListType.CREATED, oldChild, newChild);
        // pass null for inodeMap since the parent node will not get replaced when
        // undoing rename
        addChild(newChild, true, null, null);
    }

    public static class DirectoryDiff extends
            AbstractINodeDiff<FileINodeDirectory, FileINodeDirectoryAttributes, DirectoryDiff> {
        private final int childrenSize;
        ChildrenDiff diff;


        private boolean removeChild(final Diff.ListType type, final INode child) {
            final List<DirectoryDiff> diffList = asList();
            for (int i = diffList.size() - 1; i >= 0; i--) {
                final ChildrenDiff diff = diffList.get(i).diff;
                if (diff.removeChild(type, child)) {
                    return true;
                }
            }
            return false;
        }

        public boolean replaceChild(final Diff.ListType type, final INode oldChild,
                                    final INode newChild) {
            final List<DirectoryDiff> diffList = asList();
            for (int i = diffList.size() - 1; i >= 0; i--) {
                final ChildrenDiff diff = diffList.get(i).diff;
                if (diff.replace(type, oldChild, newChild)) {
                    return true;
                }
            }
            return false;
        }

        public DirectoryDiff(Snapshot snapshot, FileINodeDirectory dir) {
            super(snapshot, null, null);

            this.childrenSize = dir.getChildrenList(null).size();
            this.diff = new ChildrenDiff();
        }

        /**
         * @return the child with the given name.
         */
        public INode getChild(byte[] name, boolean checkPosterior,
                              FileINodeDirectory currentDir) {
            for (DirectoryDiff d = this; ; d = d.getPosterior()) {
                final Diff.Container<INode> returned = d.diff.accessPrevious(name);
                if (returned != null) {
                    // the diff is able to determine the inode
                    return returned.getElement();
                } else if (!checkPosterior) {
                    // Since checkPosterior is false, return null, i.e. not found.
                    return null;
                } else if (d.getPosterior() == null) {
                    // no more posterior diff, get from current inode.
                    return currentDir.getChild(name, null);
                }
            }
        }

        public DirectoryDiff(Snapshot snapshot, FileINodeDirectoryAttributes snapshotINode,
                             DirectoryDiff posteriorDiff, int childrenSize,
                             List<INode> createdList, List<INode> deletedList) {
            super(snapshot, snapshotINode, posteriorDiff);
            this.childrenSize = childrenSize;
            this.diff = new ChildrenDiff(createdList, deletedList);
        }


        public ChildrenDiff getChildrenDiff() {
            return diff;
        }


        boolean isSnapshotRoot() {
            return snapshotINode == snapshot.getRoot();
        }


        @Override
        public Quota.Counts combinePosteriorAndCollectBlocks(
                final FileINodeDirectory currentDir, final DirectoryDiff posterior,
                final BlocksMapUpdateInfo collectedBlocks,
                final List<INode> removedINodes) {
            final Quota.Counts counts = Quota.Counts.newInstance();
            diff.combinePosterior(posterior.diff, new Diff.Processor<INode>() {
                /** Collect blocks for deleted files. */
                @Override
                public void process(INode inode) {
                    if (inode != null) {
                        inode.computeQuotaUsage(counts, false);
                        inode.destroyAndCollectBlocks(collectedBlocks, removedINodes);
                    }
                }
            });
            return counts;
        }


        ReadOnlyList<INode> getChildrenList(final FileINodeDirectory currentDir) {
            return new ReadOnlyList<INode>() {
                private List<INode> children = null;

                private List<INode> initChildren() {
                    if (children == null) {
                        final ChildrenDiff combined = new ChildrenDiff();
                        for (DirectoryDiff d = DirectoryDiff.this; d != null; d = d.getPosterior()) {
                            combined.combinePosterior(d.diff, null);
                        }
                        children = combined.apply2Current(Util.asList(
                                currentDir.getChildrenList(null)));
                    }
                    return children;
                }

                @Override
                public Iterator<INode> iterator() {
                    return initChildren().iterator();
                }

                @Override
                public boolean isEmpty() {
                    return childrenSize == 0;
                }

                @Override
                public int size() {
                    return childrenSize;
                }

                @Override
                public INode get(int i) {
                    return initChildren().get(i);
                }
            };
        }




        @Override
        public String toString() {
            return super.toString() + " childrenSize=" + childrenSize + ", " + diff;
        }

        @Override
        public void write(DataOutput out, SnapshotFSImageFormat.ReferenceMap referenceMap) throws IOException {
            writeSnapshot(out);
            out.writeInt(childrenSize);

            // write snapshotINode
            if (isSnapshotRoot()) {
                out.writeBoolean(true);
            } else {
                out.writeBoolean(false);
                if (snapshotINode != null) {
                    out.writeBoolean(true);
                    FSImageSerialization.writeFileINodeDirectoryAttributes(snapshotINode, out);
                } else {
                    out.writeBoolean(false);
                }
            }
            // Write diff. Node need to write poseriorDiff, since diffs is a list.
            diff.write(out, referenceMap);
        }

        @Override
        public Quota.Counts destroyDiffAndCollectBlocks(FileINodeDirectory currentINode,
                                                        BlocksMapUpdateInfo collectedBlocks, final List<INode> removedINodes) {
            // this diff has been deleted
            Quota.Counts counts = Quota.Counts.newInstance();
            counts.add(diff.destroyDeletedList(collectedBlocks, removedINodes));
            return counts;
        }
    }

    public boolean computeDiffBetweenSnapshots(Snapshot fromSnapshot,
                                               Snapshot toSnapshot, ChildrenDiff diff) {
        Snapshot earlier = fromSnapshot;
        Snapshot later = toSnapshot;
        if (Snapshot.ID_COMPARATOR.compare(fromSnapshot, toSnapshot) > 0) {
            earlier = toSnapshot;
            later = fromSnapshot;
        }

        boolean modified = diffs.changedBetweenSnapshots(earlier,
                later);
        if (!modified) {
            return false;
        }

        final List<DirectoryDiff> difflist = diffs.asList();
        final int size = difflist.size();
        int earlierDiffIndex = Collections.binarySearch(difflist, earlier.getId());
        int laterDiffIndex = later == null ? size : Collections
                .binarySearch(difflist, later.getId());
        earlierDiffIndex = earlierDiffIndex < 0 ? (-earlierDiffIndex - 1)
                : earlierDiffIndex;
        laterDiffIndex = laterDiffIndex < 0 ? (-laterDiffIndex - 1)
                : laterDiffIndex;

        boolean dirMetadataChanged = false;
        FileINodeDirectoryAttributes dirCopy = null;
        for (int i = earlierDiffIndex; i < laterDiffIndex; i++) {
            DirectoryDiff sdiff = difflist.get(i);
            diff.combinePosterior(sdiff.diff, null);
            if (dirMetadataChanged == false && sdiff.snapshotINode != null) {
                if (dirCopy == null) {
                    dirCopy = sdiff.snapshotINode;
                } else if (!dirCopy.metadataEquals(sdiff.snapshotINode)) {
                    dirMetadataChanged = true;
                }
            }
        }

        if (!diff.isEmpty() || dirMetadataChanged) {
            return true;
        } else if (dirCopy != null) {
            for (int i = laterDiffIndex; i < size; i++) {
                if (!dirCopy.metadataEquals(difflist.get(i).snapshotINode)) {
                    return true;
                }
            }
            return !dirCopy.metadataEquals(this);
        } else {
            return false;
        }
    }

    public static class ChildrenDiff extends Diff<byte[], INode> {
        ChildrenDiff() {
        }

        public ChildrenDiff(final List<INode> created, final List<INode> deleted) {
            super(created, deleted);
        }

        /**
         * Replace the given child from the created/deleted list.
         *
         * @return true if the child is replaced; false if the child is not found.
         */
        public final boolean replace(final ListType type,
                                     final INode oldChild, final INode newChild) {
            final List<INode> list = getList(type);
            final int i = search(list, oldChild.getLocalNameBytes());
            if (i < 0) {
                return false;
            }

            final INode removed = list.set(i, newChild);
            Preconditions.checkState(removed == oldChild);
            return true;
        }

        private final boolean removeChild(ListType type, final INode child) {
            final List<INode> list = getList(type);
            final int i = searchIndex(type, child.getLocalNameBytes());
            if (i >= 0 && list.get(i) == child) {
                list.remove(i);
                return true;
            }
            return false;
        }

        /**
         * clear the created list
         */
        private Quota.Counts destroyCreatedList(
                final FileINodeDirectoryWithSnapshot currentINode,
                final INode.BlocksMapUpdateInfo collectedBlocks,
                final List<INode> removedINodes) {
            Quota.Counts counts = Quota.Counts.newInstance();
            final List<INode> createdList = getList(ListType.CREATED);
            for (INode c : createdList) {
                c.computeQuotaUsage(counts, true);
                c.destroyAndCollectBlocks(collectedBlocks, removedINodes);
                // c should be contained in the children list, remove it
                currentINode.removeChild(c);
            }
            createdList.clear();
            return counts;
        }

        /**
         * clear the deleted list
         */
        private Quota.Counts destroyDeletedList(
                final INode.BlocksMapUpdateInfo collectedBlocks,
                final List<INode> removedINodes) {
            Quota.Counts counts = Quota.Counts.newInstance();
            final List<INode> deletedList = getList(ListType.DELETED);
            for (INode d : deletedList) {
                d.computeQuotaUsage(counts, false);
                d.destroyAndCollectBlocks(collectedBlocks, removedINodes);
            }
            deletedList.clear();
            return counts;
        }


        private void writeCreated(DataOutput out) throws IOException {
            final List<INode> created = getList(ListType.CREATED);
            out.writeInt(created.size());
            for (INode node : created) {
                // For INode in created list, we only need to record its local name
                byte[] name = node.getLocalNameBytes();
                out.writeShort(name.length);
                out.write(name);
            }
        }

        private void writeDeleted(DataOutput out,
                                  SnapshotFSImageFormat.ReferenceMap referenceMap) throws IOException {
            final List<INode> deleted = getList(ListType.DELETED);
            out.writeInt(deleted.size());
            for (INode node : deleted) {
                FSImageSerialization.saveINode2Image(node, out, true, referenceMap);
            }
        }

        /**
         * Serialize to out
         */
        private void write(DataOutput out, SnapshotFSImageFormat.ReferenceMap referenceMap
        ) throws IOException {
            writeCreated(out);
            writeDeleted(out, referenceMap);
        }

        /**
         * Get the list of INodeDirectory contained in the deleted list
         */
        private void getDirsInDeleted(List<FileINodeDirectory> dirList) {
            for (INode node : getList(ListType.DELETED)) {
                if (node.isDirectory()) {
                    dirList.add(node.asFileDirectory());
                }
            }
        }

        /**
         * Interpret the diff and generate a list of {@link SnapshotDiffReport.DiffReportEntry}.
         *
         * @param parentPath  The relative path of the parent.
         * @param parent      The directory that the diff belongs to.
         * @param fromEarlier True indicates {@code diff=later-earlier},
         *                    False indicates {@code diff=earlier-later}
         * @return A list of {@link SnapshotDiffReport.DiffReportEntry} as the diff report.
         */
        public List<SnapshotDiffReport.DiffReportEntry> generateReport(byte[][] parentPath,
                                                                       FileINodeDirectoryWithSnapshot parent, boolean fromEarlier) {
            List<SnapshotDiffReport.DiffReportEntry> cList = new ArrayList<SnapshotDiffReport.DiffReportEntry>();
            List<SnapshotDiffReport.DiffReportEntry> dList = new ArrayList<SnapshotDiffReport.DiffReportEntry>();
            int c = 0, d = 0;
            List<INode> created = getList(ListType.CREATED);
            List<INode> deleted = getList(ListType.DELETED);
            byte[][] fullPath = new byte[parentPath.length + 1][];
            System.arraycopy(parentPath, 0, fullPath, 0, parentPath.length);
            for (; c < created.size() && d < deleted.size(); ) {
                INode cnode = created.get(c);
                INode dnode = deleted.get(d);
                if (cnode.compareTo(dnode.getLocalNameBytes()) == 0) {
                    fullPath[fullPath.length - 1] = cnode.getLocalNameBytes();
                    if (cnode.isSymlink() && dnode.isSymlink()) {
                        dList.add(new SnapshotDiffReport.DiffReportEntry(SnapshotDiffReport.DiffType.MODIFY, fullPath));
                    } else {
                        // must be the case: delete first and then create an inode with the
                        // same name
                        cList.add(new SnapshotDiffReport.DiffReportEntry(SnapshotDiffReport.DiffType.CREATE, fullPath));
                        dList.add(new SnapshotDiffReport.DiffReportEntry(SnapshotDiffReport.DiffType.DELETE, fullPath));
                    }
                    c++;
                    d++;
                } else if (cnode.compareTo(dnode.getLocalNameBytes()) < 0) {
                    fullPath[fullPath.length - 1] = cnode.getLocalNameBytes();
                    cList.add(new SnapshotDiffReport.DiffReportEntry(fromEarlier ? SnapshotDiffReport.DiffType.CREATE
                            : SnapshotDiffReport.DiffType.DELETE, fullPath));
                    c++;
                } else {
                    fullPath[fullPath.length - 1] = dnode.getLocalNameBytes();
                    dList.add(new SnapshotDiffReport.DiffReportEntry(fromEarlier ? SnapshotDiffReport.DiffType.DELETE
                            : SnapshotDiffReport.DiffType.CREATE, fullPath));
                    d++;
                }
            }
            for (; d < deleted.size(); d++) {
                fullPath[fullPath.length - 1] = deleted.get(d).getLocalNameBytes();
                dList.add(new SnapshotDiffReport.DiffReportEntry(fromEarlier ? SnapshotDiffReport.DiffType.DELETE
                        : SnapshotDiffReport.DiffType.CREATE, fullPath));
            }
            for (; c < created.size(); c++) {
                fullPath[fullPath.length - 1] = created.get(c).getLocalNameBytes();
                cList.add(new SnapshotDiffReport.DiffReportEntry(fromEarlier ? SnapshotDiffReport.DiffType.CREATE
                        : SnapshotDiffReport.DiffType.DELETE, fullPath));
            }
            dList.addAll(cList);
            return dList;
        }
    }

    @Override
    public Quota.Counts computeQuotaUsage4CurrentDirectory(Quota.Counts counts) {
        super.computeQuotaUsage4CurrentDirectory(counts);
        for (DirectoryDiff d : diffs) {
            for (INode deleted : d.getChildrenDiff().getList(Diff.ListType.DELETED)) {
                deleted.computeQuotaUsage(counts, false, Snapshot.INVALID_ID);
            }
        }
        counts.add(Quota.NAMESPACE, diffs.asList().size());
        return counts;
    }

    public void getSnapshotDirectory(List<FileINodeDirectory> snapshotDir) {
        for (DirectoryDiff sdiff : diffs) {
            sdiff.getChildrenDiff().getDirsInDeleted(snapshotDir);
        }
    }
    public void undoRename4DstParent(final INode deletedChild,
                                     Snapshot latestSnapshot) throws QuotaExceededException {
        boolean removeDeletedChild = diffs.removeChild(Diff.ListType.DELETED,
                deletedChild);
        // pass null for inodeMap since the parent node will not get replaced when
        // undoing rename
        final boolean added = addChild(deletedChild, true, removeDeletedChild ? null
                : latestSnapshot, null);
        // update quota usage if adding is successfully and the old child has not
        // been stored in deleted list before
        if (added && !removeDeletedChild) {
            final Quota.Counts counts = deletedChild.computeQuotaUsage();
            addSpaceConsumed(counts.get(Quota.NAMESPACE),
                    counts.get(Quota.DISKSPACE), false);
        }
    }
    public FileINodeDirectoryWithSnapshot saveSelf2Snapshot(
            final Snapshot latest, final FileINodeDirectory snapshotCopy)
            throws QuotaExceededException {
        diffs.saveSelf2Snapshot(latest, this, snapshotCopy);
        return this;
    }
    /**
     * Destroy a subtree under a DstReference node.
     */
    public static void destroyDstSubtree(INode inode, final Snapshot snapshot,
                                         final Snapshot prior, final BlocksMapUpdateInfo collectedBlocks,
                                         final List<INode> removedINodes) throws QuotaExceededException {
        Preconditions.checkArgument(prior != null);
        if (inode.isReference()) {
            if (inode instanceof INodeReference.WithName && snapshot != null) {
                // this inode has been renamed before the deletion of the DstReference
                // subtree
                inode.cleanSubtree(snapshot, prior, collectedBlocks, removedINodes,
                        true);
            } else {
                // for DstReference node, continue this process to its subtree
                destroyDstSubtree(inode.asReference().getReferredINode(), snapshot,
                        prior, collectedBlocks, removedINodes);
            }
        } else if (inode.isFile() && snapshot != null) {
            inode.cleanSubtree(snapshot, prior, collectedBlocks, removedINodes, true);
        } else if (inode.isDirectory()) {
            Map<INode, INode> excludedNodes = null;
            if (inode instanceof FileINodeDirectoryWithSnapshot) {
                FileINodeDirectoryWithSnapshot sdir = (FileINodeDirectoryWithSnapshot) inode;
                DirectoryDiffList diffList = sdir.getDiffs();
                if (snapshot != null) {
                    diffList.deleteSnapshotDiff(snapshot, prior, sdir, collectedBlocks,
                            removedINodes, true);
                }
                DirectoryDiff priorDiff = diffList.getDiff(prior);
                if (priorDiff != null && priorDiff.getSnapshot().equals(prior)) {
                    priorDiff.diff.destroyCreatedList(sdir, collectedBlocks,
                            removedINodes);
                    List<INode> dList = priorDiff.diff.getList(Diff.ListType.DELETED);
                    excludedNodes = new HashMap<INode, INode>(dList.size());
                    for (INode dNode : dList) {
                        excludedNodes.put(dNode, dNode);
                    }
                }
            }
            for (INode child : inode.asFileDirectory().getChildrenList(prior)) {
                if (excludedNodes != null && excludedNodes.containsKey(child)) {
                    continue;
                }
                destroyDstSubtree(child, snapshot, prior, collectedBlocks,
                        removedINodes);
            }
        }
    }
}
