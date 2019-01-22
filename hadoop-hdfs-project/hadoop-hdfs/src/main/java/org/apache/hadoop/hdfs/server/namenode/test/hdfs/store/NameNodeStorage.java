package org.apache.hadoop.hdfs.server.namenode.test.hdfs.store;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.server.common.*;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.hdfs.server.namenode.FSImageStorageInspector;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.apache.hadoop.hdfs.server.namenode.test.hdfs.fs.FileSystemImagePreTransactionalStorageInspector;
import org.apache.hadoop.hdfs.server.namenode.test.hdfs.fs.FileSystemImageTransactionalStorageInspector;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.util.PersistentLongFile;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.util.Time;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

public class NameNodeStorage extends  Storage implements Closeable,
        StorageErrorReporter {
    public boolean restoreFailedStorage = false;
    final protected List<StorageDirectory> removedStorageDirs
            = new CopyOnWriteArrayList<StorageDirectory>();
  public   static final String LOCAL_URI_SCHEME = "file";
    protected String blockpoolID = ""; // id of the block pool
    protected long mostRecentCheckpointTxId = HdfsConstants.INVALID_TXID;
    private Object restorationLock = new Object();
    private HashMap<String, String> deprecatedProperties;
    static final String DEPRECATED_MESSAGE_DIGEST_PROPERTY = "imageMD5Digest";

    /**
     * Time of the last checkpoint, in milliseconds since the epoch.
     */
    private long mostRecentCheckpointTime = 0;
    public NameNodeStorage(Configuration conf,
                           Collection<URI> imageDirs, Collection<URI> editsDirs)throws IOException {
        super(HdfsServerConstants.NodeType.NAME_NODE);

        storageDirs = new CopyOnWriteArrayList<StorageDirectory>();

        // this may modify the editsDirs, so copy before passing in
        setStorageDirectories(imageDirs,
                Lists.newArrayList(editsDirs),
                FSNamesystem.getSharedEditsDirs(conf));
    }
  public   StorageDirectory getStorageDirectory(URI uri) {
        try {
            uri = Util.fileAsURI(new File(uri));
            Iterator<StorageDirectory> it = dirIterator();
            for (; it.hasNext(); ) {
                StorageDirectory sd = it.next();
                if (Util.fileAsURI(sd.getRoot()).equals(uri)) {
                    return sd;
                }
            }
        } catch (IOException ioe) {
            LOG.warn("Error converting file to URI", ioe);
        }
        return null;
    }
    public long getMostRecentCheckpointTxId() {
        return mostRecentCheckpointTxId;
    }

    synchronized void setStorageDirectories(Collection<URI> fsNameDirs,
                                            Collection<URI> fsEditsDirs,
                                            Collection<URI> sharedEditsDirs)
            throws IOException {
        this.storageDirs.clear();
        this.removedStorageDirs.clear();

        // Add all name dirs with appropriate NameNodeDirType
        for (URI dirName : fsNameDirs) {
            checkSchemeConsistency(dirName);
            boolean isAlsoEdits = false;
            for (URI editsDirName : fsEditsDirs) {
                if (editsDirName.compareTo(dirName) == 0) {
                    isAlsoEdits = true;
                    fsEditsDirs.remove(editsDirName);
                    break;
                }
            }
            NameNodeDirType dirType = (isAlsoEdits) ?
                   NameNodeDirType.IMAGE_AND_EDITS :
                    NameNodeDirType.IMAGE;
            // Add to the list of storage directories, only if the
            // URI is of type file://
            if(dirName.getScheme().compareTo("file") == 0) {
                this.addStorageDir(new StorageDirectory(new File(dirName.getPath()),
                        dirType,
                        !sharedEditsDirs.contains(dirName))); // Don't lock the dir if it's shared.
            }
        }

        // Add edits dirs if they are different from name dirs
        for (URI dirName : fsEditsDirs) {
            checkSchemeConsistency(dirName);
            // Add to the list of storage directories, only if the
            // URI is of type file://
            if(dirName.getScheme().compareTo("file") == 0)
                this.addStorageDir(new StorageDirectory(new File(dirName.getPath()),
                        NameNodeDirType.EDITS, !sharedEditsDirs.contains(dirName)));
        }
    }
    private static void checkSchemeConsistency(URI u) throws IOException {
        String scheme = u.getScheme();
        // the URI should have a proper scheme
        if(scheme == null) {
            throw new IOException("Undefined scheme for " + u);
        }
    }
   public static enum NameNodeDirType implements StorageDirType {
        UNDEFINED,
        IMAGE,
        EDITS,
        IMAGE_AND_EDITS;

        @Override
        public StorageDirType getStorageDirType() {
            return this;
        }

        @Override
        public boolean isOfType(StorageDirType type) {
            if ((this == IMAGE_AND_EDITS) && (type == IMAGE || type == EDITS))
                return true;
            return this == type;
        }
    }
  public   FSImageStorageInspector readAndInspectDirs()
            throws IOException {
        Integer layoutVersion = null;
        boolean multipleLV = false;
        StringBuilder layoutVersions = new StringBuilder();

        // First determine what range of layout versions we're going to inspect
        for (Iterator<StorageDirectory> it = dirIterator();
             it.hasNext();) {
            StorageDirectory sd = it.next();
            if (!sd.getVersionFile().exists()) {
                FSImage.LOG.warn("Storage directory " + sd + " contains no VERSION file. Skipping...");
                continue;
            }
            readProperties(sd); // sets layoutVersion
            int lv = getLayoutVersion();
            if (layoutVersion == null) {
                layoutVersion = Integer.valueOf(lv);
            } else if (!layoutVersion.equals(lv)) {
                multipleLV = true;
            }
            layoutVersions.append("(").append(sd.getRoot()).append(", ").append(lv).append(") ");
        }

        if (layoutVersion == null) {
            throw new IOException("No storage directories contained VERSION information");
        }
        if (multipleLV) {
            throw new IOException(
                    "Storage directories contain multiple layout versions: "
                            + layoutVersions);
        }
        // If the storage directories are with the new layout version
        // (ie edits_<txnid>) then use the new inspector, which will ignore
        // the old format dirs.
        FSImageStorageInspector inspector;
        if (LayoutVersion.supports(LayoutVersion.Feature.TXID_BASED_LAYOUT, getLayoutVersion())) {
            inspector = new FileSystemImageTransactionalStorageInspector();
        } else {
            inspector = new FileSystemImagePreTransactionalStorageInspector();
        }

        inspectStorageDirs(inspector);
        return inspector;
    }
 public    void inspectStorageDirs(FSImageStorageInspector inspector)
            throws IOException {

        // Process each of the storage directories to find the pair of
        // newest image file and edit file
        for (Iterator<StorageDirectory> it = dirIterator(); it.hasNext();) {
            StorageDirectory sd = it.next();
            inspector.inspectDirectory(sd);
        }
    }
    public NamespaceInfo getNamespaceInfo() {
        return new NamespaceInfo(
                getNamespaceID(),
                getClusterID(),
                getBlockPoolID(),
                getCTime());
    }
 public    static long readTransactionIdFile(StorageDirectory sd) throws IOException {
        File txidFile = getStorageFile(sd, NameNodeFile.SEEN_TXID);
        return PersistentLongFile.readFile(txidFile, 0);
    }
  public   static File getStorageFile(StorageDirectory sd, NameNodeFile type) {
        return new File(sd.getCurrentDir(), type.getName());
    }
  public   boolean getRestoreFailedStorage() {
        return restoreFailedStorage;
    }
   public void setRestoreFailedStorage(boolean val) {
        LOG.warn("set restore failed storage to " + val);
        restoreFailedStorage=val;
    }
    public   enum NameNodeFile {
        IMAGE     ("fsimage"),
        TIME      ("fstime"), // from "old" pre-HDFS-1073 format
        SEEN_TXID ("seen_txid"),
        EDITS     ("edits"),
        IMAGE_NEW ("fsimage.ckpt"),
        EDITS_NEW ("edits.new"), // from "old" pre-HDFS-1073 format
        EDITS_INPROGRESS ("edits_inprogress"),
        EDITS_TMP ("edits_tmp");

        private String fileName = null;
        private NameNodeFile(String name) { this.fileName = name; }
      public   String getName() { return fileName; }
    }

    public String getBlockPoolID() {
        return blockpoolID;
    }

    @Override
    public List<File> getFiles(StorageDirType dirType, String fileName) {
        return super.getFiles(dirType, fileName);
    }

    @Override
    public Iterator<StorageDirectory> dirIterator() {
        return super.dirIterator();
    }

    @Override
    public Iterator<StorageDirectory> dirIterator(StorageDirType dirType) {
        return super.dirIterator(dirType);
    }

    @Override
    public Iterable<StorageDirectory> dirIterable(StorageDirType dirType) {
        return super.dirIterable(dirType);
    }

    @Override
    public String listStorageDirectories() {
        return super.listStorageDirectories();
    }

    protected NameNodeStorage(HdfsServerConstants.NodeType type) {
        super(type);
    }

    protected NameNodeStorage(HdfsServerConstants.NodeType type, StorageInfo storageInfo) {
        super(type, storageInfo);
    }

    @Override
    public int getNumStorageDirs() {
        return super.getNumStorageDirs();
    }

    @Override
    public StorageDirectory getStorageDir(int idx) {
        return super.getStorageDir(idx);
    }

    @Override
    public StorageDirectory getSingularStorageDir() {
        return super.getSingularStorageDir();
    }

    @Override
    protected void addStorageDir(StorageDirectory sd) {
        super.addStorageDir(sd);
    }

    @Override
    public boolean isPreUpgradableLayout(StorageDirectory sd) throws IOException {
        return false;
    }

    @Override
    protected void setFieldsFromProperties(Properties props, StorageDirectory sd) throws IOException {
        super.setFieldsFromProperties(props, sd);

        if (layoutVersion == 0) {
            throw new IOException("NameNode directory "
                    + sd.getRoot() + " is not formatted.");
        }

        // Set Block pool ID in version with federation support
        if (versionSupportsFederation()) {
            String sbpid = props.getProperty("blockpoolID");
            setBlockPoolID(sd.getRoot(), sbpid);
        }
        setDeprecatedPropertiesForUpgrade(props);
    }
    private void setDeprecatedPropertiesForUpgrade(Properties props) {
        deprecatedProperties = new HashMap<String, String>();
        String md5 = props.getProperty(DEPRECATED_MESSAGE_DIGEST_PROPERTY);
        if (md5 != null) {
            deprecatedProperties.put(DEPRECATED_MESSAGE_DIGEST_PROPERTY, md5);
        }
    }
    private void setBlockPoolID(File storage, String bpid)
            throws InconsistentFSStateException {
        if (bpid == null || bpid.equals("")) {
            throw new InconsistentFSStateException(storage, "file "
                    + Storage.STORAGE_FILE_VERSION + " has no block pool Id.");
        }

        if (!blockpoolID.equals("") && !blockpoolID.equals(bpid)) {
            throw new InconsistentFSStateException(storage,
                    "Unexepcted blockpoolID " + bpid + " . Expected " + blockpoolID);
        }
        setBlockPoolID(bpid);
    }
 public    List<StorageDirectory> getRemovedStorageDirs() {
        return this.removedStorageDirs;
    }

    @Override
    protected void setPropertiesFromFields(Properties props, StorageDirectory sd) throws IOException {
        super.setPropertiesFromFields(props, sd);
    }

    @Override
    public void readProperties(StorageDirectory sd) throws IOException {
        Properties props = readPropertiesFile(sd.getVersionFile());
        setFieldsFromProperties(props, sd);
    }

    @Override
    public void readPreviousVersionProperties(StorageDirectory sd) throws IOException {
        super.readPreviousVersionProperties(sd);
    }

    @Override
    public void writeProperties(StorageDirectory sd) throws IOException {
        super.writeProperties(sd);
    }

    @Override
    public void writeProperties(File to, StorageDirectory sd) throws IOException {
        super.writeProperties(to, sd);
    }

    @Override
    public void writeAll() throws IOException {
        super.writeAll();
    }

    @Override
    public void unlockAll() throws IOException {
        super.unlockAll();
    }

    @Override
    protected void setStorageType(Properties props, StorageDirectory sd) throws InconsistentFSStateException {
        super.setStorageType(props, sd);
    }

    @Override
    protected void setcTime(Properties props, StorageDirectory sd) throws InconsistentFSStateException {
        super.setcTime(props, sd);
    }

    @Override
    protected void setClusterId(Properties props, int layoutVersion, StorageDirectory sd) throws InconsistentFSStateException {
        super.setClusterId(props, layoutVersion, sd);
    }

    @Override
    protected void setLayoutVersion(Properties props, StorageDirectory sd) throws IncorrectVersionException, InconsistentFSStateException {
        super.setLayoutVersion(props, sd);
    }

    @Override
    protected void setNamespaceID(Properties props, StorageDirectory sd) throws InconsistentFSStateException {
        super.setNamespaceID(props, sd);
    }

    @Override
    public int getLayoutVersion() {
        return super.getLayoutVersion();
    }

    @Override
    public int getNamespaceID() {
        return super.getNamespaceID();
    }

    @Override
    public String getClusterID() {
        return super.getClusterID();
    }

    @Override
    public long getCTime() {
        return super.getCTime();
    }

    @Override
    public void setStorageInfo(StorageInfo from) {
        super.setStorageInfo(from);
    }

    @Override
    public boolean versionSupportsFederation() {
        return super.versionSupportsFederation();
    }

    @Override
    public String toString() {
        return super.toString();
    }

    @Override
    public String toColonSeparatedString() {
        return super.toColonSeparatedString();
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
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
    public void reportErrorOnFile(File f) {

    }

    @Override
    public void close() throws IOException {

    }
   public void setMostRecentCheckpointInfo(long txid, long time) {
        this.mostRecentCheckpointTxId = txid;
        this.mostRecentCheckpointTime = time;
    }
   public void attemptRestoreRemovedStorage() {
        // if directory is "alive" - copy the images there...
        if (!restoreFailedStorage || removedStorageDirs.size() == 0)
            return;
       /* We don't want more than one thread trying to restore at a time */
       synchronized (this.restorationLock) {
           LOG.info("NNStorage.attemptRestoreRemovedStorage: check removed(failed) "+
                   "storarge. removedStorages size = " + removedStorageDirs.size());
           for(Iterator<StorageDirectory> it
               = this.removedStorageDirs.iterator(); it.hasNext();) {
               StorageDirectory sd = it.next();
               File root = sd.getRoot();
               LOG.info("currently disabled dir " + root.getAbsolutePath() +
                       "; type="+sd.getStorageDirType()
                       + ";canwrite="+ FileUtil.canWrite(root));
               if(root.exists() && FileUtil.canWrite(root)) {
                   LOG.info("restoring dir " + sd.getRoot().getAbsolutePath());
                   this.addStorageDir(sd); // restore
                   this.removedStorageDirs.remove(sd);
               }
           }
       }
    }
  public   int getNumStorageDirs(NameNodeDirType dirType) {
        if(dirType == null)
            return getNumStorageDirs();
        Iterator<StorageDirectory> it = dirIterator(dirType);
        int numDirs = 0;
        for(; it.hasNext(); it.next())
            numDirs++;
        return numDirs;
    }

  public   void reportErrorsOnDirectories(List<StorageDirectory> sds) {
        for (StorageDirectory sd : sds) {
        }
    }
   public static File getStorageFile(StorageDirectory sd, NameNodeFile type, long imageTxId) {
        return new File(sd.getCurrentDir(),
                String.format("%s_%019d", type.getName(), imageTxId));
    }
    public void writeTransactionIdFileToStorage(long txid) {
        // Write txid marker in all storage directories
        for (StorageDirectory sd : storageDirs) {
            try {
                writeTransactionIdFile(sd, txid);
            } catch(IOException e) {
                // Close any edits stream associated with this dir and remove directory
                LOG.warn("writeTransactionIdToStorage failed on " + sd,
                        e);
                reportErrorsOnDirectory(sd);
            }
        }
    }
    void writeTransactionIdFile(StorageDirectory sd, long txid) throws IOException {
        Preconditions.checkArgument(txid >= 0, "bad txid: " + txid);

        File txIdFile = getStorageFile(sd, NameNodeFile.SEEN_TXID);
        PersistentLongFile.writeFile(txIdFile, txid);
    }

    private void reportErrorsOnDirectory(StorageDirectory sd) {
        LOG.error("Error reported on storage directory " + sd);

        String lsd = listStorageDirectories();
        LOG.debug("current list of storage dirs:" + lsd);

        LOG.warn("About to remove corresponding storage: "
                + sd.getRoot().getAbsolutePath());
        try {
            sd.unlock();
        } catch (Exception e) {
            LOG.warn("Unable to unlock bad storage directory: "
                    +  sd.getRoot().getPath(), e);
        }

        if (this.storageDirs.remove(sd)) {
            this.removedStorageDirs.add(sd);
        }

        lsd = listStorageDirectories();
        LOG.debug("at the end current list of storage dirs:" + lsd);
    }
   public void processStartupOptionsForUpgrade(HdfsServerConstants.StartupOption startOpt, int layoutVersion)
            throws IOException {
        if (startOpt == HdfsServerConstants.StartupOption.UPGRADE) {
            // If upgrade from a release that does not support federation,
            // if clusterId is provided in the startupOptions use it.
            // Else generate a new cluster ID
            if (!LayoutVersion.supports(LayoutVersion.Feature.FEDERATION, layoutVersion)) {
                if (startOpt.getClusterId() == null) {
                    startOpt.setClusterId(newClusterID());
                }
                setClusterID(startOpt.getClusterId());
                setBlockPoolID(newBlockPoolID());
            } else {
                // Upgrade from one version of federation to another supported
                // version of federation doesn't require clusterID.
                // Warn the user if the current clusterid didn't match with the input
                // clusterid.
                if (startOpt.getClusterId() != null
                        && !startOpt.getClusterId().equals(getClusterID())) {
                    LOG.warn("Clusterid mismatch - current clusterid: " + getClusterID()
                            + ", Ignoring given clusterid: " + startOpt.getClusterId());
                }
            }
            LOG.info("Using clusterid: " + getClusterID());
        }
    }
    public static String newClusterID() {
        return "CID-" + UUID.randomUUID().toString();
    }
    void setClusterID(String cid) {
        clusterID = cid;
    }
    static String newBlockPoolID() throws UnknownHostException {
        String ip = "unknownIP";
        try {
            ip = DNS.getDefaultIP("default");
        } catch (UnknownHostException e) {
            LOG.warn("Could not find ip address of \"default\" inteface.");
            throw e;
        }

        int rand = DFSUtil.getSecureRandom().nextInt(Integer.MAX_VALUE);
        String bpid = "BP-" + rand + "-"+ ip + "-" + Time.now();
        return bpid;
    }
    void setBlockPoolID(String bpid) {
        blockpoolID = bpid;
    }

}
