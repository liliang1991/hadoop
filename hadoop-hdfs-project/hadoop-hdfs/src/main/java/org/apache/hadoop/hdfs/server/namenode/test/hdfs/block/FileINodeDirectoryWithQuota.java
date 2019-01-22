package org.apache.hadoop.hdfs.server.namenode.test.hdfs.block;

import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.server.namenode.*;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.util.ReadOnlyList;
import org.apache.hadoop.util.LightWeightGSet;

import java.io.PrintWriter;
import java.util.List;

public class FileINodeDirectoryWithQuota extends FileINodeDirectory {
    /** Name space quota */
    private long nsQuota = Long.MAX_VALUE;
    /** Name space count */
    private long namespace = 1L;
    /** Disk space quota */
    private long dsQuota = HdfsConstants.QUOTA_RESET;
    /** Disk space count */
    private long diskspace = 0L;



    public FileINodeDirectoryWithQuota(FileINodeDirectory other, boolean adopt,
                                       long nsQuota, long dsQuota) {
        super(other, adopt);
        final Quota.Counts counts = other.computeQuotaUsage();
        this.namespace = counts.get(Quota.NAMESPACE);
        this.diskspace = counts.get(Quota.DISKSPACE);
        this.nsQuota = nsQuota;
        this.dsQuota = dsQuota;
    }
    public FileINodeDirectoryWithQuota(long id, byte[] name, PermissionStatus permissions,
                            long modificationTime, long nsQuota, long dsQuota) {
        super(id, name, permissions, modificationTime);
        this.nsQuota = nsQuota;
        this.dsQuota = dsQuota;
    }
    public FileINodeDirectoryWithQuota(long id, byte[] name, PermissionStatus permissions) {
        super(id, name, permissions, 0L);
    }

    @Override
    public ReadOnlyList<INode> getChildrenList(Snapshot snapshot) {
        return super.getChildrenList(snapshot);
    }

    @Override
    public Quota.Counts cleanSubtree(Snapshot snapshot, Snapshot prior, BlocksMapUpdateInfo collectedBlocks, List<INode> removedINodes, boolean countDiffChange) throws QuotaExceededException {
        return super.cleanSubtree(snapshot, prior, collectedBlocks, removedINodes, countDiffChange);
    }

    @Override
    public void destroyAndCollectBlocks(BlocksMapUpdateInfo collectedBlocks, List<INode> removedINodes) {
        super.destroyAndCollectBlocks(collectedBlocks, removedINodes);
    }

    @Override
    public Content.Counts computeContentSummary(Content.Counts counts) {
        return super.computeContentSummary(counts);
    }

    @Override
    public Quota.Counts computeQuotaUsage(Quota.Counts counts, boolean useCache, int lastSnapshotId) {
        return super.computeQuotaUsage(counts, useCache, lastSnapshotId);
    }

    @Override
    public boolean metadataEquals(FileINodeDirectoryAttributes other) {
        return super.metadataEquals(other);
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
    public long getNsQuota() {
        return nsQuota;
    }

    @Override
    public long getDsQuota() {
        return dsQuota;
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
    public void setQuota(long nsQuota, long dsQuota) {
        this.nsQuota = nsQuota;
        this.dsQuota = dsQuota;
    }

    @Override
    public INode recordFileModification(Snapshot latest, FileINodeMap inodeMap) throws QuotaExceededException {
        return null;
    }

    @Override
    public INode updateModificationTime(long mtime, Snapshot latest, INodeMap inodeMap) throws QuotaExceededException {
        return null;
    }
   public void verifyQuota(long nsDelta, long dsDelta) throws QuotaExceededException {
        verifyNamespaceQuota(nsDelta);

        if (Quota.isViolated(dsQuota, diskspace, dsDelta)) {
            throw new DSQuotaExceededException(dsQuota, diskspace + dsDelta);
        }
    }
    void verifyNamespaceQuota(long delta) throws NSQuotaExceededException {
        if (Quota.isViolated(nsQuota, namespace, delta)) {
            throw new NSQuotaExceededException(nsQuota, namespace + delta);
        }
    }
    public void addSpaceConsumed2Cache(long nsDelta, long dsDelta) {
        namespace += nsDelta;
        diskspace += dsDelta;
    }
 public    void setSpaceConsumed(long namespace, long diskspace) {
        this.namespace = namespace;
        this.diskspace = diskspace;
    }
   public long numItemsInTree() {
        return namespace;
    }

}
