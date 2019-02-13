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
/*
  INodeDirectoryWithQuota类继承于INodeDirectory类，
  管理员可以配置目录的配额。既可以配置目录下的名字数量，也可以配置子目录下的所有文件的空间配额
 */
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
    public Content.Counts computeContentSummary(Content.Counts counts) {
        final long original = counts.get(Content.DISKSPACE);
        super.computeContentSummary(counts);
        checkDiskspace(counts.get(Content.DISKSPACE) - original);
        return counts;
    }
    private void checkDiskspace(final long computed) {
        if (-1 != getDsQuota() && diskspace != computed) {
            NameNode.LOG.error("BUG: Inconsistent diskspace for directory "
                    + getFullPathName() + ". Cached = " + diskspace
                    + " != Computed = " + computed);
        }
    }
    @Override
    public Quota.Counts computeQuotaUsage(Quota.Counts counts, boolean useCache, int lastSnapshotId) {
        if (useCache && isQuotaSet()) {
            // use cache value
            counts.add(Quota.NAMESPACE, namespace);
            counts.add(Quota.DISKSPACE, diskspace);
        } else {
            super.computeQuotaUsage(counts, false, lastSnapshotId);
        }
        return counts;
    }






    @Override
    public void addSpaceConsumed(long nsDelta, long dsDelta, boolean verify) throws QuotaExceededException {
        if (isQuotaSet()) {
            // The following steps are important:
            // check quotas in this inode and all ancestors before changing counts
            // so that no change is made if there is any quota violation.

            // (1) verify quota in this inode
            if (verify) {
                verifyQuota(nsDelta, dsDelta);
            }
            // (2) verify quota and then add count in ancestors
            super.addSpaceConsumed(nsDelta, dsDelta, verify);
            // (3) add count in this inode
            addSpaceConsumed2Cache(nsDelta, dsDelta);
        } else {
            super.addSpaceConsumed(nsDelta, dsDelta, verify);
        }
    }

    @Override
    public long getNsQuota() {
        return nsQuota;
    }

    @Override
    public long getDsQuota() {
        return dsQuota;
    }






    public void setQuota(long nsQuota, long dsQuota) {
        this.nsQuota = nsQuota;
        this.dsQuota = dsQuota;
    }

  /*  @Override
    public INode recordFileModification(Snapshot latest, FileINodeMap inodeMap) throws QuotaExceededException {
        return null;
    }*/


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
    String namespaceString() {
        return "namespace: " + (nsQuota < 0? "-": namespace + "/" + nsQuota);
    }
    String diskspaceString() {
        return "diskspace: " + (dsQuota < 0? "-": diskspace + "/" + dsQuota);
    }
    String quotaString() {
        return ", Quota[" + namespaceString() + ", " + diskspaceString() + "]";
    }


}
