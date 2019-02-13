package org.apache.hadoop.hdfs.server.namenode.test.hdfs.manager;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeFileUnderConstruction;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager;
import org.apache.hadoop.hdfs.server.namenode.test.hdfs.fs.FileNamesystem;

import java.io.IOException;
import java.util.*;

import static org.apache.hadoop.util.Time.now;

public class FileLeaseManager {
    public static final Log LOG = LogFactory.getLog(FileLeaseManager.class);
    // 按照时间进行排序的租约队列
    private SortedSet<Lease> sortedLeases = new TreeSet<Lease>();
    // 租约持有者对租约的映射图
    private SortedMap<String, Lease> leases = new TreeMap<String, Lease>();
    private long hardLimit = HdfsConstants.LEASE_HARDLIMIT_PERIOD;
    private long softLimit = HdfsConstants.LEASE_SOFTLIMIT_PERIOD;

    FileNamesystem fsnamesystem;
    private SortedMap<String, Lease> sortedLeasesByPath = new TreeMap<String, Lease>();

    public FileLeaseManager(FileNamesystem fsnamesystem) {this.fsnamesystem = fsnamesystem;}
   public Lease getLease(String holder) {
        return leases.get(holder);
    }
    public Lease getLeaseByPath(String src) {return sortedLeasesByPath.get(src);}

    public   synchronized Lease addLease(String holder, String src) {
       Lease lease = getLease(holder);
        if (lease == null) {
            lease = new Lease(holder);
            leases.put(holder, lease);
            sortedLeases.add(lease);
        } else {
            renewLease(lease);
        }
        sortedLeasesByPath.put(src, lease);
        lease.paths.add(src);
        return lease;
    }
   public synchronized void renewLease(Lease lease) {
        if (lease != null) {
            sortedLeases.remove(lease);
            lease.renew();
            sortedLeases.add(lease);
        }
    }
   public synchronized void renewLease(String holder) {
        renewLease(getLease(holder));
    }
  public   synchronized void removeAllLeases() {
        sortedLeases.clear();
        sortedLeasesByPath.clear();
        leases.clear();
    }
   public SortedSet<Lease> getSortedLeases() {return sortedLeases;}


   public class Lease implements Comparable<Lease> {
        // // 租约持有者
        private final String holder;\
       // 最近更新时间
        private long lastUpdate;
        //// 当前租约持有者打开的文件
        private final Collection<String> paths = new TreeSet<String>();

        /** Only LeaseManager object can create a lease */
        private Lease(String holder) {
            this.holder = holder;
            renew();
        }
        /** Only LeaseManager object can renew a lease */
        private void renew() {
            this.lastUpdate = now();
        }

        /** @return true if the Hard Limit Timer has expired */
        public boolean expiredHardLimit() {
            return now() - lastUpdate > hardLimit;
        }

        /** @return true if the Soft Limit Timer has expired */
        public boolean expiredSoftLimit() {
            return now() - lastUpdate > softLimit;
        }

        /**
         * @return the path associated with the pendingFile and null if not found.
         */
        private String findPath(INodeFileUnderConstruction pendingFile) {
            try {
                for (String src : paths) {
                    INode node = fsnamesystem.dir.getINode(src);
                    if (node == pendingFile
                            || (node.isFile() && node.asFile() == pendingFile)) {
                        return src;
                    }
                }
            } catch (UnresolvedLinkException e) {
                throw new AssertionError("Lease files should reside on this FS");
            }
            return null;
        }

        /** Does this lease contain any path? */
        boolean hasPath() {return !paths.isEmpty();}

        boolean removePath(String src) {
            return paths.remove(src);
        }

        @Override
        public String toString() {
            return "[Lease.  Holder: " + holder
                    + ", pendingcreates: " + paths.size() + "]";
        }

        @Override
        public int compareTo(Lease o) {
            Lease l1 = this;
           Lease l2 = o;
            long lu1 = l1.lastUpdate;
            long lu2 = l2.lastUpdate;
            if (lu1 < lu2) {
                return -1;
            } else if (lu1 > lu2) {
                return 1;
            } else {
                return l1.holder.compareTo(l2.holder);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Lease)) {
                return false;
            }
           Lease obj = (Lease) o;
            if (lastUpdate == obj.lastUpdate &&
                    holder.equals(obj.holder)) {
                return true;
            }
            return false;
        }

        @Override
        public int hashCode() {
            return holder.hashCode();
        }

       public Collection<String> getPaths() {
            return paths;
        }

     public    String getHolder() {
            return holder;
        }

        void replacePath(String oldpath, String newpath) {
            paths.remove(oldpath);
            paths.add(newpath);
        }

        @VisibleForTesting
        long getLastUpdate() {
            return lastUpdate;
        }
    }
   public synchronized void removeLeaseWithPrefixPath(String prefix) {
        for(Map.Entry<String, Lease> entry
                : findLeaseWithPrefixPath(prefix, sortedLeasesByPath).entrySet()) {

            removeLease(entry.getValue(), entry.getKey());
        }
    }
    static private Map<String, Lease> findLeaseWithPrefixPath(
            String prefix, SortedMap<String,Lease> path2lease) {


        final Map<String, Lease> entries = new HashMap<String, Lease>();
        final int srclen = prefix.length();

        for(Map.Entry<String, Lease> entry : path2lease.tailMap(prefix).entrySet()) {
            final String p = entry.getKey();
            if (!p.startsWith(prefix)) {
                return entries;
            }
            if (p.length() == srclen || p.charAt(srclen) == Path.SEPARATOR_CHAR) {
                entries.put(entry.getKey(), entry.getValue());
            }
        }
        return entries;
    }
   public synchronized void removeLease(Lease lease, String src) {
        sortedLeasesByPath.remove(src);

        if (!lease.hasPath()) {
            leases.remove(lease.holder);
            if (!sortedLeases.remove(lease)) {
            }
        }
    }

  public   synchronized void removeLease(String holder, String src) {
        Lease lease = getLease(holder);
        if (lease != null) {
            removeLease(lease, src);
        } else {
           // LOG.warn("Removing non-existent lease! holder=" + holder +
             //       " src=" + src);
        }
    }
  public   Map<String, INodeFileUnderConstruction> getINodesUnderConstruction() {
        Map<String, INodeFileUnderConstruction> inodes =
                new TreeMap<String, INodeFileUnderConstruction>();
        for (String p : sortedLeasesByPath.keySet()) {
            // verify that path exists in namespace
            try {
                INode node = fsnamesystem.dir.getINode(p);
                inodes.put(p, INodeFileUnderConstruction.valueOf(node, p));
            } catch (IOException ioe) {
            }
        }
        return inodes;
    }
    /**
     * Reassign lease for file src to the new holder.
     */
   public synchronized Lease reassignLease(Lease lease, String src, String newHolder) {
        assert newHolder != null : "new lease holder is null";
        if (lease != null) {
            removeLease(lease, src);
        }
        return addLease(newHolder, src);
    }
    /**
     * Finds the pathname for the specified pendingFile
     */
    public synchronized String findPath(INodeFileUnderConstruction pendingFile)
            throws IOException {
        Lease lease = getLease(pendingFile.getClientName());
        if (lease != null) {
            String src = lease.findPath(pendingFile);
            if (src != null) {
                return src;
            }
        }
        throw new IOException("pendingFile (=" + pendingFile + ") not found."
                + "(lease=" + lease + ")");
    }

 public    synchronized void changeLease(String src, String dst) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(getClass().getSimpleName() + ".changelease: " +
                    " src=" + src + ", dest=" + dst);
        }

        final int len = src.length();
        for(Map.Entry<String, Lease> entry
                : findLeaseWithPrefixPath(src, sortedLeasesByPath).entrySet()) {
            final String oldpath = entry.getKey();
            final Lease lease = entry.getValue();
            // replace stem of src with new destination
            final String newpath = dst + oldpath.substring(len);
            if (LOG.isDebugEnabled()) {
                LOG.debug("changeLease: replacing " + oldpath + " with " + newpath);
            }
            lease.replacePath(oldpath, newpath);
            sortedLeasesByPath.remove(oldpath);
            sortedLeasesByPath.put(newpath, lease);
        }
    }

}
