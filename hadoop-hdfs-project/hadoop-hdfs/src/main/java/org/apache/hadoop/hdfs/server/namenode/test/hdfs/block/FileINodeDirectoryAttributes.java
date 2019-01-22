package org.apache.hadoop.hdfs.server.namenode.test.hdfs.block;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.server.namenode.INodeAttributes;

public interface FileINodeDirectoryAttributes extends INodeAttributes {
    public long getNsQuota();

    public long getDsQuota();

    public boolean metadataEquals(FileINodeDirectoryAttributes other);

    public static class SnapshotCopy extends INodeAttributes.SnapshotCopy
            implements FileINodeDirectoryAttributes {
        public SnapshotCopy(byte[] name, PermissionStatus permissions,
                            long modificationTime) {
            super(name, permissions, modificationTime, 0L);
        }

        public SnapshotCopy(FileINodeDirectory dir) {
            super(dir);
        }

        @Override
        public long getNsQuota() {
            return -1;
        }

        @Override
        public long getDsQuota() {
            return -1;
        }

        @Override
        public boolean metadataEquals(FileINodeDirectoryAttributes other) {
            return other != null
                    && getNsQuota() == other.getNsQuota()
                    && getDsQuota() == other.getDsQuota()
                    && getPermissionLong() == other.getPermissionLong();
        }
    }

    public static class CopyWithQuota extends SnapshotCopy {
        private final long nsQuota;
        private final long dsQuota;

        public CopyWithQuota(byte[] name, PermissionStatus permissions,
                             long modificationTime, long nsQuota, long dsQuota) {
            super(name, permissions, modificationTime);
            this.nsQuota = nsQuota;
            this.dsQuota = dsQuota;
        }

        public CopyWithQuota(FileINodeDirectory dir) {
            super(dir);
            Preconditions.checkArgument(dir.isQuotaSet());
            this.nsQuota = dir.getNsQuota();
            this.dsQuota = dir.getDsQuota();
        }

        @Override
        public final long getNsQuota() {
            return nsQuota;
        }

        @Override
        public final long getDsQuota() {
            return dsQuota;
        }
    }

}
