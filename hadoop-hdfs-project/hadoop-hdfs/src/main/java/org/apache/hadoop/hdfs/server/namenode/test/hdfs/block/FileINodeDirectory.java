package org.apache.hadoop.hdfs.server.namenode.test.hdfs.block;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.PathIsNotDirectoryException;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.protocol.SnapshotAccessControlException;
import org.apache.hadoop.hdfs.server.namenode.*;
import org.apache.hadoop.hdfs.server.namenode.snapshot.*;
import org.apache.hadoop.hdfs.util.ReadOnlyList;
import org.apache.hadoop.util.LightWeightGSet;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public  class FileINodeDirectory extends INodeWithAdditionalFields
        implements FileINodeDirectoryAttributes {
   protected static final int DEFAULT_FILES_PER_DIRECTORY = 5;

   private List<INode> children = null;
   public FileINodeDirectory(FileINodeDirectory other, boolean adopt) {
      super(other);
      this.children = other.children;
      if (adopt && this.children != null) {
         for (INode child : children) {
            child.setFileParent(this);
         }
      }
   }
  public   FileINodesInPath getINodesInPath(String path, boolean resolveLink
    ) throws UnresolvedLinkException {
        final byte[][] components = getPathComponents(path);
        return FileINodesInPath.resolve(this, components, components.length, resolveLink);
    }
 public   INodeFileWithSnapshot replaceChild4INodeFileWithSnapshot(
           final INodeFile child, final FileINodeMap inodeMap) {
      Preconditions.checkArgument(!(child instanceof INodeFileWithSnapshot),
              "Child file is already an INodeFileWithSnapshot, child=" + child);
      final INodeFileWithSnapshot newChild = new INodeFileWithSnapshot(child);
      replaceChildFile(child, newChild, inodeMap);
      return newChild;
   }

   private void replaceChildFile(final INodeFile oldChild,
                                 final INodeFile newChild, final FileINodeMap inodeMap) {
      replaceChild(oldChild, newChild, inodeMap);
      oldChild.clear();
      newChild.updateBlockCollection();
   }
   public FileINodeDirectory(long id, byte[] name, PermissionStatus permissions,
                         long mtime) {
      super(id, name, permissions, mtime, 0L);
   }
   public ReadOnlyList<INode> getChildrenList(final Snapshot snapshot) {
      return children == null ? ReadOnlyList.Util.<INode>emptyList()
              : ReadOnlyList.Util.asReadOnlyList(children);
   }
    public INode getChild(byte[] name, Snapshot snapshot) {
        final ReadOnlyList<INode> c = getChildrenList(snapshot);
        final int i = ReadOnlyList.Util.binarySearch(c, name);
        return i < 0? null: c.get(i);
    }

   @Override
   public Quota.Counts cleanSubtree(Snapshot snapshot, Snapshot prior, BlocksMapUpdateInfo collectedBlocks, List<INode> removedFileINodes, boolean countDiffChange) throws QuotaExceededException {
      return null;
   }

   @Override
   public void destroyAndCollectBlocks(BlocksMapUpdateInfo collectedBlocks, List<INode> removedINodes) {

   }

   @Override
   public Content.Counts computeContentSummary(Content.Counts counts) {
      return null;
   }

   @Override
   public Quota.Counts computeQuotaUsage(Quota.Counts counts, boolean useCache, int lastSnapshotId) {
       if (children != null) {
           for (INode child : children) {
               child.computeQuotaUsage(counts, useCache, lastSnapshotId);
           }
       }
       return computeQuotaUsage4CurrentDirectory(counts);
   }

   @Override
   public boolean metadataEquals(FileINodeDirectoryAttributes other) {
      return false;
   }

   public static byte[] ROOT_NAME = DFSUtil.string2Bytes("");

   public FileINodeDirectory(INode parent, long id, byte[] name, long permission, long modificationTime, long accessTime) {
      super(parent, id, name, permission, modificationTime, accessTime);
   }

   public FileINodeDirectory(long id, byte[] name, PermissionStatus permissions, long modificationTime, long accessTime) {
      super(id, name, permissions, modificationTime, accessTime);
   }

   public FileINodeDirectory(INodeWithAdditionalFields other) {
      super(other);
   }

   @Override
   public void setNext(LightWeightGSet.LinkedElement next) {
      super.setNext(next);
   }

   @Override
   public LightWeightGSet.LinkedElement getNext() {
      return super.getNext();
   }

   @Override
   public long getPermissionLong() {
      return super.getPermissionLong();
   }

   @Override
   public INodeAttributes getSnapshotINode(Snapshot snapshot) {
      return super.getSnapshotINode(snapshot);
   }

   @Override
   public boolean isReference() {
      return super.isReference();
   }

   @Override
   public INodeReference asReference() {
      return super.asReference();
   }

   @Override
   public boolean isFile() {
      return super.isFile();
   }

   @Override
   public INodeFile asFile() {
      return super.asFile();
   }

   @Override
   public boolean isDirectory() {
      return true;
   }

   @Override
   public FileINodeDirectory asFileDirectory() {
      return this;
   }

   @Override
   public boolean isSymlink() {
      return super.isSymlink();
   }

   @Override
   public INodeSymlink asSymlink() {
      return super.asSymlink();
   }

   @Override
   public void addSpaceConsumed(long nsDelta, long dsDelta, boolean verify) throws QuotaExceededException {
      super.addSpaceConsumed(nsDelta, dsDelta, verify);
   }



   @Override
   public long getDsQuota() {
      return super.getDsQuota();
   }

   @Override
   public String getFullPathName() {
      return super.getFullPathName();
   }

   @Override
   public String toString() {
      return super.toString();
   }

   @Override
   public String toDetailString() {
      return super.toDetailString();
   }

   @Override
   public INodeReference getParentReference() {
      return super.getParentReference();
   }

   @Override
   public void clear() {
      super.clear();
   }

   @Override
   public void dumpTreeRecursively(PrintWriter out, StringBuilder prefix, Snapshot snapshot) {
      super.dumpTreeRecursively(out, prefix, snapshot);
   }

   @Override
   protected Object clone() throws CloneNotSupportedException {
      return super.clone();
   }

   @Override
   protected void finalize() throws Throwable {
      super.finalize();
   }
   @Override
   public FileINodeDirectory recordModification(Snapshot latest,
                                            final INodeMap inodeMap) throws QuotaExceededException {
      return null;
   }
 public   FileINodesInPath getLastINodeInPath(String path, boolean resolveLink
   ) throws UnresolvedLinkException {
      return FileINodesInPath.resolve(this, getPathComponents(path), 1, resolveLink);
   }

   public static FileINodeDirectory valueOf(INode inode, Object path
   ) throws FileNotFoundException, PathIsNotDirectoryException {
      if (inode == null) {
         throw new FileNotFoundException("Directory does not exist: "
                 + DFSUtil.path2String(path));
      }
      if (!inode.isDirectory()) {
         throw new PathIsNotDirectoryException(DFSUtil.path2String(path));
      }
      return inode.asFileDirectory();
   }
   public boolean isSnapshottable() {
      return false;
   }
   public INode getNode(String path, boolean resolveLink)
           throws UnresolvedLinkException {
      return getLastINodeInPath(path, resolveLink).getINode(0);
   }
   public boolean addChild(INode node, final boolean setModTime,
                           final Snapshot latest, FileINodeMap inodeMap)
           throws QuotaExceededException {
      final int low = searchChildren(node.getLocalNameBytes());
      if (low >= 0) {
         return false;
      }

      if (isInLatestSnapshot(latest)) {
         FileINodeDirectoryWithSnapshot sdir =
                 replaceSelf4INodeDirectoryWithSnapshot(inodeMap);
         boolean added = sdir.addChild(node, setModTime, latest, inodeMap);
         return added;
      }
      addChild(node, low);
      if (setModTime) {
         // update modification time of the parent directory
         updateFileModificationTime(node.getModificationTime(), latest, inodeMap);
      }
      return true;
   }
   private int searchChildren(byte[] name) {
      return children == null? -1: Collections.binarySearch(children, name);
   }
   public FileINodeDirectoryWithSnapshot replaceSelf4INodeDirectoryWithSnapshot(
           final FileINodeMap inodeMap) {
      return replaceSelf(new FileINodeDirectoryWithSnapshot(this), inodeMap);
   }

   private final <N extends FileINodeDirectory> N replaceSelf(final N newDir,
                                                          final FileINodeMap inodeMap) {
      final INodeReference ref = getParentReference();
      if (ref != null) {
         ref.setReferredINode(newDir);
         if (inodeMap != null) {
            inodeMap.put(newDir);
         }
      } else {
         final FileINodeDirectory parent = getFileParent();
         Preconditions.checkArgument(parent != null, "parent is null, this=%s", this);
         parent.replaceChild(this, newDir, inodeMap);
      }
      clear();
      return newDir;
   }

   private void addChild(final INode node, final int insertionPoint) {
      if (children == null) {
         children = new ArrayList<INode>(DEFAULT_FILES_PER_DIRECTORY);
      }
      node.setFileParent(this);
      children.add(-insertionPoint - 1, node);

      if (node.getGroupName() == null) {
         node.setGroup(getGroupName());
      }
   }
   public void replaceChild(INode oldChild, final INode newChild,
                            final FileINodeMap inodeMap) {
      Preconditions.checkNotNull(children);
      final int i = searchChildren(newChild.getLocalNameBytes());
      Preconditions.checkState(i >= 0);
      Preconditions.checkState(oldChild == children.get(i)
              || oldChild == children.get(i).asReference().getReferredINode()
              .asReference().getReferredINode());
      oldChild = children.get(i);

      if (oldChild.isReference() && !newChild.isReference()) {
         // replace the referred inode, e.g.,
         // INodeFileWithSnapshot -> INodeFileUnderConstructionWithSnapshot
         final INode withCount = oldChild.asReference().getReferredINode();
         withCount.asReference().setReferredINode(newChild);
      } else {
         if (oldChild.isReference()) {
            // both are reference nodes, e.g., DstReference -> WithName
            final INodeReference.WithCount withCount =
                    (INodeReference.WithCount) oldChild.asReference().getReferredINode();
            withCount.removeReference(oldChild.asReference());
         }
         children.set(i, newChild);
      }
      // update the inodeMap
      if (inodeMap != null) {
         inodeMap.put(newChild);
      }
   }
  public   FileINodesInPath getINodesInPath4Write(String src, boolean resolveLink)
            throws UnresolvedLinkException, SnapshotAccessControlException {
        final byte[][] components = INode.getPathComponents(src);
        FileINodesInPath inodesInPath = FileINodesInPath.resolve(this, components,
                components.length, resolveLink);
        if (inodesInPath.isSnapshot()) {
            throw new SnapshotAccessControlException(
                    "Modification on a read-only snapshot is disallowed");
        }
        return inodesInPath;
    }
    public Quota.Counts computeQuotaUsage4CurrentDirectory(Quota.Counts counts) {
        counts.add(Quota.NAMESPACE, 1);
        return counts;
    }

    public boolean addChild(INode node) {
        final int low = searchChildren(node.getLocalNameBytes());
        if (low >= 0) {
            return false;
        }
        addChild(node, low);
        return true;
    }
    /**
     * Remove the specified child from this directory.
     *
     * @param child the child inode to be removed
     * @param latest See {@link INode#recordModification(Snapshot, INodeMap)}.
     */
    public boolean removeChild(INode child, Snapshot latest,
                               final FileINodeMap inodeMap) throws QuotaExceededException {
        if (isInLatestSnapshot(latest)) {
            return replaceSelf4INodeDirectoryWithSnapshot(inodeMap)
                    .removeChild(child, latest, inodeMap);
        }

        return removeChild(child);
    }
    public final boolean removeChild(final INode child) {
        final int i = searchChildren(child.getLocalNameBytes());
        if (i < 0) {
            return false;
        }

        final INode removed = children.remove(i);
        Preconditions.checkState(removed == child);
        return true;
    }

   public INodeFileUnderConstructionWithSnapshot replaceChild4INodeFileUcWithSnapshot(
            final INodeFileUnderConstruction child, final FileINodeMap inodeMap) {
        Preconditions.checkArgument(!(child instanceof INodeFileUnderConstructionWithSnapshot),
                "Child file is already an INodeFileUnderConstructionWithSnapshot, child=" + child);
        final INodeFileUnderConstructionWithSnapshot newChild
                = new INodeFileUnderConstructionWithSnapshot(child, null);
        replaceChildFile(child, newChild, inodeMap);
        return newChild;
    }
  public   INodeReference.WithName replaceChild4ReferenceWithName(INode oldChild,
                                                           Snapshot latest) {
        Preconditions.checkArgument(latest != null);
        if (oldChild instanceof INodeReference.WithName) {
            return (INodeReference.WithName)oldChild;
        }

        final INodeReference.WithCount withCount;
        if (oldChild.isReference()) {
            Preconditions.checkState(oldChild instanceof INodeReference.DstReference);
            withCount = (INodeReference.WithCount) oldChild.asReference()
                    .getReferredINode();
        } else {
            withCount = new INodeReference.WithCount(null, oldChild);
        }
        final INodeReference.WithName ref = new INodeReference.WithName(this,
                withCount, oldChild.getLocalNameBytes(), latest.getId());
        replaceChild(oldChild, ref, null);
        return ref;
    }
  public   INode getINode4Write(String src, boolean resolveLink)
            throws UnresolvedLinkException, SnapshotAccessControlException {
        return getINodesInPath4Write(src, resolveLink).getLastINode();
    }

   public static int nextChild(ReadOnlyList<INode> children, byte[] name) {
        if (name.length == 0) { // empty name
            return 0;
        }
        int nextPos = ReadOnlyList.Util.binarySearch(children, name) + 1;
        if (nextPos >= 0) {
            return nextPos;
        }
        return -nextPos;
    }
    public final int getChildrenNum(final Snapshot snapshot) {
        return getChildrenList(snapshot).size();
    }

    public FileINodeDirectory replaceSelf4INodeDirectory(final FileINodeMap inodeMap) {
        Preconditions.checkState(getClass() != FileINodeDirectory.class,
                "the class is already INodeDirectory, this=%s", this);
        return replaceSelf(new FileINodeDirectory(this, true), inodeMap);
    }

  public   FileINodeDirectoryWithQuota replaceSelf4Quota(final Snapshot latest,
                                              final long nsQuota, final long dsQuota, final FileINodeMap inodeMap)
            throws QuotaExceededException {
        Preconditions.checkState(!(this instanceof FileINodeDirectoryWithQuota),
                "this is already an INodeDirectoryWithQuota, this=%s", this);

        if (!this.isInLatestSnapshot(latest)) {
            final FileINodeDirectoryWithQuota q = new FileINodeDirectoryWithQuota(
                    this, true, nsQuota, dsQuota);
            replaceSelf(q, inodeMap);
            return q;
        } else {
            final FileINodeDirectoryWithSnapshot s = new FileINodeDirectoryWithSnapshot(this);
            s.setQuota(nsQuota, dsQuota);
            return replaceSelf(s, inodeMap).saveSelf2Snapshot(latest, this);
        }
    }

    public FileINodeDirectorySnapshottable replaceSelf4INodeDirectorySnapshottable(
            Snapshot latest, final FileINodeMap inodeMap) throws QuotaExceededException {
        Preconditions.checkState(!(this instanceof FileINodeDirectorySnapshottable),
                "this is already an INodeDirectorySnapshottable, this=%s", this);
        final FileINodeDirectorySnapshottable s = new FileINodeDirectorySnapshottable(this);
        replaceSelf(s, inodeMap).saveSelf2Snapshot(latest, this);
        return s;
    }
}
