package org.apache.hadoop.hdfs.server.namenode.test.hdfs.fs;

import com.google.common.base.Preconditions;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.*;
import org.apache.hadoop.hdfs.server.namenode.snapshot.INodeDirectorySnapshottable;
import org.apache.hadoop.hdfs.server.namenode.snapshot.INodeDirectoryWithSnapshot;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.server.namenode.test.hdfs.NameNodeTest;
import org.apache.hadoop.hdfs.server.namenode.test.hdfs.block.*;
import org.apache.hadoop.hdfs.server.namenode.test.hdfs.manager.FileBlockManager;
import org.apache.hadoop.hdfs.util.ByteArray;
import org.apache.hadoop.hdfs.util.ReadOnlyList;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.hadoop.util.Time.now;

public class FileSystemDirectory {
    private static FileINodeDirectoryWithQuota createRoot(FileNamesystem namesystem) {
        final FileINodeDirectoryWithQuota r = new FileINodeDirectoryWithQuota(
                INodeId.ROOT_INODE_ID,
                INodeDirectory.ROOT_NAME,
                namesystem.createFsOwnerPermissions(new FsPermission((short) 0755)));
        final FileINodeDirectorySnapshottable s = new FileINodeDirectorySnapshottable(r);
        s.setSnapshotQuota(0);
        return s;
    }
    public final static String DOT_INODES_STRING = ".inodes";

    public final static byte[] DOT_INODES =
            DFSUtil.string2Bytes(DOT_INODES_STRING);
    ReentrantReadWriteLock dirLock;
    private Condition cond;
 public    FileINodeDirectoryWithQuota rootDir;
    FileINodeMap inodeMap;
   public FileSystenImage fsImage ;
    int lsLimit;
    int maxComponentLength;
    int maxDirItems;
    FileNameCache<ByteArray> nameCache;
   FileNamesystem namesystem;
    public final static String DOT_RESERVED_STRING = ".reserved";
    static boolean CHECK_RESERVED_FILE_NAMES = true;
    public final static String DOT_RESERVED_PATH_PREFIX = Path.SEPARATOR
            + DOT_RESERVED_STRING;
    public final static byte[] DOT_RESERVED =
            DFSUtil.string2Bytes(DOT_RESERVED_STRING);
    volatile boolean ready = false;
    FileSystemDirectory(FileSystenImage fsImage, FileNamesystem ns, Configuration conf)  {
        this.dirLock = new ReentrantReadWriteLock(true); // fair
        this.cond = dirLock.writeLock().newCondition();
        rootDir = createRoot(ns);
        inodeMap = FileINodeMap.newInstance(rootDir);
        this.fsImage = fsImage;
        int configuredLimit = conf.getInt(
                DFSConfigKeys.DFS_LIST_LIMIT, DFSConfigKeys.DFS_LIST_LIMIT_DEFAULT);
        this.lsLimit = configuredLimit>0 ?
                configuredLimit : DFSConfigKeys.DFS_LIST_LIMIT_DEFAULT;

        // filesystem limits
        this.maxComponentLength = conf.getInt(
                DFSConfigKeys.DFS_NAMENODE_MAX_COMPONENT_LENGTH_KEY,
                DFSConfigKeys.DFS_NAMENODE_MAX_COMPONENT_LENGTH_DEFAULT);
        this.maxDirItems = conf.getInt(
                DFSConfigKeys.DFS_NAMENODE_MAX_DIRECTORY_ITEMS_KEY,
                DFSConfigKeys.DFS_NAMENODE_MAX_DIRECTORY_ITEMS_DEFAULT);

        int threshold = conf.getInt(
                DFSConfigKeys.DFS_NAMENODE_NAME_CACHE_THRESHOLD_KEY,
                DFSConfigKeys.DFS_NAMENODE_NAME_CACHE_THRESHOLD_DEFAULT);
        NameNode.LOG.info("Caching file names occuring more than " + threshold
                + " times");
        nameCache = new FileNameCache<ByteArray>(threshold);
        namesystem = ns;
    }
    static byte[][] getPathComponentsForReservedPath(String src) {
        return !isReservedName(src) ? null : INode.getPathComponents(src);
    }

    protected void setReady(boolean flag) {
        ready = flag;
    }

    void writeLock() {
        this.dirLock.writeLock().lock();
    }
    void writeUnlock() {
        this.dirLock.writeLock().unlock();
    }
    void readLock() {
        this.dirLock.readLock().lock();
    }

    void readUnlock() {
        this.dirLock.readLock().unlock();
    }
    private FileNamesystem getFSNamesystem() {
        return namesystem;
    }
    void reset() {
        writeLock();
        try {
            setReady(false);
            rootDir = createRoot(getFSNamesystem());
            inodeMap.clear();
            addToInodeMap(rootDir);
            nameCache.reset();
        } finally {
            writeUnlock();
        }
    }
    public final void addToInodeMap(INode inode) {
        if (inode instanceof INodeWithAdditionalFields) {
            inodeMap.put((INodeWithAdditionalFields)inode);
        }
    }
    public INode getINode(String src) throws UnresolvedLinkException {
        return getLastINodeInPath(src).getINode(0);
    }
    public FileINodesInPath getLastINodeInPath(String src)
            throws UnresolvedLinkException {
        readLock();
        try {
            return rootDir.getLastINodeInPath(src, true);
        } finally {
            readUnlock();
        }
    }
    public INode getInode(long id) {
        readLock();
        try {
            return inodeMap.get(id);
        } finally {
            readUnlock();
        }
    }

    INodeFile unprotectedAddFile( long id,
                                  String path,
                                  PermissionStatus permissions,
                                  short replication,
                                  long modificationTime,
                                  long atime,
                                  long preferredBlockSize,
                                  boolean underConstruction,
                                  String clientName,
                                  String clientMachine) {
        final INodeFile newNode;
        assert hasWriteLock();
        if (underConstruction) {
            newNode = new INodeFileUnderConstruction(id, permissions, replication,
                    preferredBlockSize, modificationTime, clientName, clientMachine, null);
        } else {
            newNode = new INodeFile(id, null, permissions, modificationTime, atime,
                    BlockInfo.EMPTY_ARRAY, replication, preferredBlockSize);
        }

        try {
            if (addINode(path, newNode)) {
                return newNode;
            }
        } catch (IOException e) {
            if(NameNode.stateChangeLog.isDebugEnabled()) {
                NameNode.stateChangeLog.debug(
                        "DIR* FSDirectory.unprotectedAddFile: exception when add " + path
                                + " to the file system", e);
            }
        }
        return null;
    }
    void cacheName(INode inode) {
        // Name is cached only for files
        if (!inode.isFile()) {
            return;
        }
        ByteArray name = new ByteArray(inode.getLocalNameBytes());
        name = nameCache.put(name);
        if (name != null) {
            inode.setLocalName(name.getBytes());
        }
    }
    private boolean addINode(String src, INode child
    ) throws QuotaExceededException, UnresolvedLinkException {
        byte[][] components = INode.getPathComponents(src);
        child.setLocalName(components[components.length-1]);
        cacheName(child);
        writeLock();
        try {
            return addLastINode(getExistingPathINodes(components), child, true);
        } finally {
            writeUnlock();
        }
    }
    private boolean addLastINode(FileINodesInPath inodesInPath,
                                 INode inode, boolean checkQuota) throws QuotaExceededException {
        final int pos = inodesInPath.getINodes().length - 1;
        return addChild(inodesInPath, pos, inode, checkQuota);
    }

    private boolean addChild(FileINodesInPath iip, int pos,
                             INode child, boolean checkQuota) throws QuotaExceededException {
        final INode[] inodes = iip.getINodes();
        // Disallow creation of /.reserved. This may be created when loading
        // editlog/fsimage during upgrade since /.reserved was a valid name in older
        // release. This may also be called when a user tries to create a file
        // or directory /.reserved.
        if (pos == 1 && inodes[0] == rootDir && isReservedName(child)) {
            throw new HadoopIllegalArgumentException(
                    "File name \"" + child.getLocalName() + "\" is reserved and cannot "
                            + "be created. If this is during upgrade change the name of the "
                            + "existing file or directory to another name before upgrading "
                            + "to the new release.");
        }
        // The filesystem limits are not really quotas, so this check may appear
        // odd. It's because a rename operation deletes the src, tries to add
        // to the dest, if that fails, re-adds the src from whence it came.
        // The rename code disables the quota when it's restoring to the
        // original location becase a quota violation would cause the the item
        // to go "poof".  The fs limits must be bypassed for the same reason.
        if (checkQuota) {
            verifyMaxComponentLength(child.getLocalNameBytes(), inodes, pos);
            verifyMaxDirItems(inodes, pos);
        }
        // always verify inode name
        verifyINodeName(child.getLocalNameBytes());

        final Quota.Counts counts = child.computeQuotaUsage();
        updateCount(iip, pos,
                counts.get(Quota.NAMESPACE), counts.get(Quota.DISKSPACE), checkQuota);
        final FileINodeDirectory parent = inodes[pos-1].asFileDirectory();
        boolean added = false;
        try {
            added = parent.addChild(child, true, iip.getLatestSnapshot(),
                    inodeMap);
        } catch (QuotaExceededException e) {
            updateCountNoQuotaCheck(iip, pos,
                    -counts.get(Quota.NAMESPACE), -counts.get(Quota.DISKSPACE));
            throw e;
        }
        if (!added) {
            updateCountNoQuotaCheck(iip, pos,
                    -counts.get(Quota.NAMESPACE), -counts.get(Quota.DISKSPACE));
        } else {
            iip.setINode(pos - 1, child.getParent());
            addToInodeMap(child);
        }
        return added;
    }
    FileINodesInPath getExistingPathINodes(byte[][] components)
            throws UnresolvedLinkException {
        return FileINodesInPath.resolve(rootDir, components);
    }
    public static boolean isReservedName(INode inode) {
        return CHECK_RESERVED_FILE_NAMES
                && Arrays.equals(inode.getLocalNameBytes(), DOT_RESERVED);
    }

    /** Check if a given path is reserved */
    public static boolean isReservedName(String src) {
        return src.startsWith(DOT_RESERVED_PATH_PREFIX);
    }
    void verifyMaxComponentLength(byte[] childName, Object parentPath, int pos)
            throws FSLimitException.PathComponentTooLongException {
        if (maxComponentLength == 0) {
            return;
        }

        final int length = childName.length;
        if (length > maxComponentLength) {
            final String p = parentPath instanceof INode[]?
                    getFullPathName((INode[])parentPath, pos - 1): (String)parentPath;
            final FSLimitException.PathComponentTooLongException e = new FSLimitException.PathComponentTooLongException(
                    maxComponentLength, length, p, DFSUtil.bytes2String(childName));
            if (ready) {
                throw e;
            } else {
                // Do not throw if edits log is still being processed
                NameNode.LOG.error("ERROR in FSDirectory.verifyINodeName", e);
            }
        }
    }
    void verifyMaxDirItems(INode[] pathComponents, int pos)
            throws FSLimitException.MaxDirectoryItemsExceededException {
        if (maxDirItems == 0) {
            return;
        }

        final INodeDirectory parent = pathComponents[pos-1].asDirectory();
        final int count = parent.getChildrenList(null).size();
        if (count >= maxDirItems) {
            final FSLimitException.MaxDirectoryItemsExceededException e
                    = new FSLimitException.MaxDirectoryItemsExceededException(maxDirItems, count);
            if (ready) {
                e.setPathName(getFullPathName(pathComponents, pos - 1));
                throw e;
            } else {
                // Do not throw if edits log is still being processed
                NameNode.LOG.error("FSDirectory.verifyMaxDirItems: "
                        + e.getLocalizedMessage());
            }
        }
    }
    void verifyINodeName(byte[] childName) throws HadoopIllegalArgumentException {
        if (Arrays.equals(HdfsConstants.DOT_SNAPSHOT_DIR_BYTES, childName)) {
            String s = "\"" + HdfsConstants.DOT_SNAPSHOT_DIR + "\" is a reserved name.";
            if (!ready) {
                s += "  Please rename it before upgrade.";
            }
            throw new HadoopIllegalArgumentException(s);
        }
    }

    static String getFullPathName(INode[] inodes, int pos) {
        StringBuilder fullPathName = new StringBuilder();
        if (inodes[0].isRoot()) {
            if (pos == 0) return Path.SEPARATOR;
        } else {
            fullPathName.append(inodes[0].getLocalName());
        }

        for (int i=1; i<=pos; i++) {
            fullPathName.append(Path.SEPARATOR_CHAR).append(inodes[i].getLocalName());
        }
        return fullPathName.toString();
    }
    public void updateCount(FileINodesInPath iip, int numOfINodes,
                             long nsDelta, long dsDelta, boolean checkQuota)
            throws QuotaExceededException {
        assert hasWriteLock();
        if (!ready) {
            //still initializing. do not check or update quotas.
            return;
        }
        final INode[] inodes = iip.getINodes();
        if (numOfINodes > inodes.length) {
            numOfINodes = inodes.length;
        }
        if (checkQuota) {
            verifyQuota(inodes, numOfINodes, nsDelta, dsDelta, null);
        }
        unprotectedUpdateCount(iip, numOfINodes, nsDelta, dsDelta);
    }
    private void updateCountNoQuotaCheck(FileINodesInPath inodesInPath,
                                         int numOfINodes, long nsDelta, long dsDelta) {
        assert hasWriteLock();
        try {
            updateCount(inodesInPath, numOfINodes, nsDelta, dsDelta, false);
        } catch (QuotaExceededException e) {
            NameNode.LOG.error("BUG: unexpected exception ", e);
        }
    }
    private static void verifyQuota(INode[] inodes, int pos, long nsDelta,
                                    long dsDelta, INode commonAncestor) throws QuotaExceededException {
        if (nsDelta <= 0 && dsDelta <= 0) {
            // if quota is being freed or not being consumed
            return;
        }

        // check existing components in the path
        for(int i = (pos > inodes.length? inodes.length: pos) - 1; i >= 0; i--) {
            if (commonAncestor == inodes[i]) {
                // Stop checking for quota when common ancestor is reached
                return;
            }
            if (inodes[i].isQuotaSet()) { // a directory with quota
                try {
                    ((FileINodeDirectoryWithQuota) inodes[i].asFileDirectory()).verifyQuota(
                            nsDelta, dsDelta);
                } catch (QuotaExceededException e) {
                    e.setPathName(getFullPathName(inodes, i));
                    throw e;
                }
            }
        }
    }
    private static void unprotectedUpdateCount(FileINodesInPath inodesInPath,
                                               int numOfINodes, long nsDelta, long dsDelta) {
        final INode[] inodes = inodesInPath.getINodes();
        for(int i=0; i < numOfINodes; i++) {
            if (inodes[i].isQuotaSet()) { // a directory with quota
                FileINodeDirectoryWithQuota node = (FileINodeDirectoryWithQuota) inodes[i]
                        .asFileDirectory();
                node.addSpaceConsumed2Cache(nsDelta, dsDelta);
            }
        }
    }
    private HdfsFileStatus createFileStatus(byte[] path, INode node,
                                            boolean needLocation, Snapshot snapshot) throws IOException {
        if (needLocation) {
            return createLocatedFileStatus(path, node, snapshot);
        } else {
            return createFileStatus(path, node, snapshot);
        }
    }
    boolean hasWriteLock() {
        return this.dirLock.isWriteLockedByCurrentThread();
    }

    boolean hasReadLock() {
        return this.dirLock.getReadHoldCount() > 0;
    }
    HdfsFileStatus createFileStatus(byte[] path, INode node,
                                    Snapshot snapshot) {
        long size = 0;     // length is zero for directories
        short replication = 0;
        long blocksize = 0;
        if (node.isFile()) {
            final INodeFile fileNode = node.asFile();
            size = fileNode.computeFileSize(snapshot);
            replication = fileNode.getFileReplication(snapshot);
            blocksize = fileNode.getPreferredBlockSize();
        }
        int childrenNum = node.isDirectory() ?
                node.asDirectory().getChildrenNum(snapshot) : 0;

        return new HdfsFileStatus(
                size,
                node.isDirectory(),
                replication,
                blocksize,
                node.getModificationTime(snapshot),
                node.getAccessTime(snapshot),
                node.getFsPermission(snapshot),
                node.getUserName(snapshot),
                node.getGroupName(snapshot),
                node.isSymlink() ? node.asSymlink().getSymlink() : null,
                path,
                node.getId(),
                childrenNum);
    }
    private HdfsLocatedFileStatus createLocatedFileStatus(byte[] path,
                                                          INode node, Snapshot snapshot) throws IOException {
        assert hasReadLock();
        long size = 0; // length is zero for directories
        short replication = 0;
        long blocksize = 0;
        LocatedBlocks loc = null;
        if (node.isFile()) {
            final INodeFile fileNode = node.asFile();
            size = fileNode.computeFileSize(snapshot);
            replication = fileNode.getFileReplication(snapshot);
            blocksize = fileNode.getPreferredBlockSize();

            final boolean inSnapshot = snapshot != null;
            final boolean isUc = inSnapshot ? false : fileNode.isUnderConstruction();
            final long fileSize = !inSnapshot && isUc ?
                    fileNode.computeFileSizeNotIncludingLastUcBlock() : size;
            loc = getFSNamesystem().getBlockManager().createLocatedBlocks(
                    fileNode.getBlocks(), fileSize, isUc, 0L, size, false,
                    inSnapshot);
            if (loc == null) {
                loc = new LocatedBlocks();
            }
        }
        int childrenNum = node.isDirectory() ?
                node.asDirectory().getChildrenNum(snapshot) : 0;

        return new HdfsLocatedFileStatus(size, node.isDirectory(), replication,
                blocksize, node.getModificationTime(snapshot),
                node.getAccessTime(snapshot), node.getFsPermission(snapshot),
                node.getUserName(snapshot), node.getGroupName(snapshot),
                node.isSymlink() ? node.asSymlink().getSymlink() : null, path,
                node.getId(), loc, childrenNum);
    }
    public FileINodeMap getINodeMap() {
        return inodeMap;
    }
    void replaceINodeFile(String path, INodeFile oldnode,
                          INodeFile newnode) throws IOException {
        writeLock();
        try {
            unprotectedReplaceINodeFile(path, oldnode, newnode);
        } finally {
            writeUnlock();
        }
    }
    void unprotectedReplaceINodeFile(final String path, final INodeFile oldnode,
                                     final INodeFile newnode) {
        Preconditions.checkState(hasWriteLock());

        oldnode.getFileParent().replaceChild(oldnode, newnode, inodeMap);
        oldnode.clear();

        /* Currently oldnode and newnode are assumed to contain the same
         * blocks. Otherwise, blocks need to be removed from the blocksMap.
         */
        int index = 0;
        for (BlockInfo b : newnode.getBlocks()) {
            BlockInfo info = getBlockManager().addBlockCollection(b, newnode);
            newnode.setBlock(index, info); // inode refers to the block in BlocksMap
            index++;
        }
    }
    private FileBlockManager getBlockManager() {
        return getFSNamesystem().getBlockManager();
    }

  public   Block[] unprotectedSetReplication(String src, short replication,
                                      short[] blockRepls) throws QuotaExceededException,
            UnresolvedLinkException, SnapshotAccessControlException {
        assert hasWriteLock();

        final FileINodesInPath iip = rootDir.getINodesInPath4Write(src, true);
        final INode inode = iip.getLastINode();
        if (inode == null || !inode.isFile()) {
            return null;
        }
        INodeFile file = inode.asFile();
        final short oldBR = file.getBlockReplication();

        // before setFileReplication, check for increasing block replication.
        // if replication > oldBR, then newBR == replication.
        // if replication < oldBR, we don't know newBR yet.
        if (replication > oldBR) {
            long dsDelta = (replication - oldBR)*(file.diskspaceConsumed()/oldBR);
            updateCount(iip, 0, dsDelta, true);
        }

        file = file.setReplication(replication, iip.getLatestSnapshot(),
                inodeMap);

        final short newBR = file.getBlockReplication();
        // check newBR < oldBR case.
        if (newBR < oldBR) {
            long dsDelta = (newBR - oldBR)*(file.diskspaceConsumed()/newBR);
            updateCount(iip, 0, dsDelta, true);
        }

        if (blockRepls != null) {
            blockRepls[0] = oldBR;
            blockRepls[1] = newBR;
        }
        return file.getBlocks();
    }
    void unprotectedConcat(String target, String [] srcs, long timestamp)
            throws UnresolvedLinkException, QuotaExceededException,
            SnapshotAccessControlException, SnapshotException {
        assert hasWriteLock();
        if (NameNode.stateChangeLog.isDebugEnabled()) {
            NameNode.stateChangeLog.debug("DIR* FSNamesystem.concat to "+target);
        }
        // do the move

        final FileINodesInPath trgIIP = rootDir.getINodesInPath4Write(target, true);
        final INode[] trgINodes = trgIIP.getINodes();
        final INodeFile trgInode = trgIIP.getLastINode().asFile();
        INodeDirectory trgParent = trgINodes[trgINodes.length-2].asDirectory();
        final Snapshot trgLatestSnapshot = trgIIP.getLatestSnapshot();

        final INodeFile [] allSrcInodes = new INodeFile[srcs.length];
        for(int i = 0; i < srcs.length; i++) {
            final FileINodesInPath iip = getINodesInPath4Write(srcs[i]);
            final Snapshot latest = iip.getLatestSnapshot();
            final INode inode = iip.getLastINode();

            // check if the file in the latest snapshot
            if (inode.isInLatestSnapshot(latest)) {
                throw new SnapshotException("Concat: the source file " + srcs[i]
                        + " is in snapshot " + latest);
            }

            // check if the file has other references.
            if (inode.isReference() && ((INodeReference.WithCount)
                    inode.asReference().getReferredINode()).getReferenceCount() > 1) {
                throw new SnapshotException("Concat: the source file " + srcs[i]
                        + " is referred by some other reference in some snapshot.");
            }

            allSrcInodes[i] = inode.asFile();
        }
        trgInode.concatBlocks(allSrcInodes);

        // since we are in the same dir - we can use same parent to remove files
        int count = 0;
        for(INodeFile nodeToRemove: allSrcInodes) {
            if(nodeToRemove == null) continue;

            nodeToRemove.setBlocks(null);
            trgParent.removeChild(nodeToRemove, trgLatestSnapshot, null);
            inodeMap.remove(nodeToRemove);
            count++;
        }

        // update inodeMap
        removeFromInodeMap(Arrays.asList(allSrcInodes));

        trgInode.setFileModificationTime(timestamp, trgLatestSnapshot, inodeMap);
        trgParent.updateFileModificationTime(timestamp, trgLatestSnapshot, inodeMap);
        // update quota on the parent directory ('count' files removed, 0 space)
        unprotectedUpdateCount(trgIIP, trgINodes.length-1, -count, 0);
    }
    private void updateCount(FileINodesInPath iip, long nsDelta, long dsDelta,
                             boolean checkQuota) throws QuotaExceededException {
        updateCount(iip, iip.getINodes().length - 1, nsDelta, dsDelta, checkQuota);
    }
    public FileINodesInPath getINodesInPath4Write(String src
    ) throws UnresolvedLinkException, SnapshotAccessControlException {
        readLock();
        try {
            return rootDir.getINodesInPath4Write(src, true);
        } finally {
            readUnlock();
        }
    }
    public final void removeFromInodeMap(List<? extends INode> inodes) {
        if (inodes != null) {
            for (INode inode : inodes) {
                if (inode != null && inode instanceof INodeWithAdditionalFields) {
                    inodeMap.remove(inode);
                }
            }
        }
    }
    void imageLoadComplete() {
        Preconditions.checkState(!ready, "FSDirectory already loaded");
        setReady();
    }
    void setReady() {
        if(ready) return;
        writeLock();
        try {
            setReady(true);
            this.nameCache.initialized();
            cond.signalAll();
        } finally {
            writeUnlock();
        }
    }
    /**
     * Block until the object is ready to be used.
     */
    void waitForReady() {
        if (!ready) {
            writeLock();
            try {
                while (!ready) {
                    try {
                        cond.await(5000, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException ie) {
                    }
                }
            } finally {
                writeUnlock();
            }
        }
    }

    /**
     * Sets the access time on the file/directory. Logs it in the transaction log.
     */
    void setTimes(String src, INode inode, long mtime, long atime, boolean force,
                  Snapshot latest) throws QuotaExceededException {
        boolean status = false;
        writeLock();
        try {
            status = unprotectedSetTimes(inode, mtime, atime, force, latest);
        } finally {
            writeUnlock();
        }
        if (status) {
            fsImage.getEditLog().logTimes(src, mtime, atime);
        }
    }
    private boolean unprotectedSetTimes(INode inode, long mtime,
                                        long atime, boolean force, Snapshot latest) throws QuotaExceededException {
        assert hasWriteLock();
        boolean status = false;
        if (mtime != -1) {
            inode = inode.setFileModificationTime(mtime, latest, inodeMap);
            status = true;
        }
        if (atime != -1) {
            long inodeTime = inode.getAccessTime(null);

            // if the last access time update was within the last precision interval, then
            // no need to store access time
            if (atime <= inodeTime + getFSNamesystem().getAccessTimePrecision() && !force) {
                status =  false;
            } else {
                inode.setFileAccessTime(atime, latest, inodeMap);
                status = true;
            }
        }
        return status;
    }

    /**
     * Resolve the path of /.reserved/.inodes/<inodeid>/... to a regular path
     *
     * @param src path that is being processed
     * @param pathComponents path components corresponding to the path
     * @param fsd FSDirectory
     * @return if the path indicates an inode, return path after replacing upto
     *         <inodeid> with the corresponding path of the inode, else the path
     *         in {@code src} as is.
     * @throws FileNotFoundException if inodeid is invalid
     */
    static String resolvePath(String src, byte[][] pathComponents, FileSystemDirectory fsd)
            throws FileNotFoundException {
        if (pathComponents == null || pathComponents.length <= 3) {
            return src;
        }
        // Not /.reserved/.inodes
        if (!Arrays.equals(DOT_RESERVED, pathComponents[1])
                || !Arrays.equals(DOT_INODES, pathComponents[2])) { // Not .inodes path
            return src;
        }
        final String inodeId = DFSUtil.bytes2String(pathComponents[3]);
        long id = 0;
        try {
            id = Long.valueOf(inodeId);
        } catch (NumberFormatException e) {
            throw new FileNotFoundException("Invalid inode path: " + src);
        }
        if (id == INodeId.ROOT_INODE_ID && pathComponents.length == 4) {
            return Path.SEPARATOR;
        }
        INode inode = fsd.getInode(id);
        if (inode == null) {
            throw new FileNotFoundException(
                    "File for given inode path does not exist: " + src);
        }

        // Handle single ".." for NFS lookup support.
        if ((pathComponents.length > 4)
                && DFSUtil.bytes2String(pathComponents[4]).equals("..")) {
            INode parent = inode.getParent();
            if (parent == null || parent.getId() == INodeId.ROOT_INODE_ID) {
                // inode is root, or its parent is root.
                return Path.SEPARATOR;
            } else {
                return parent.getFullPathName();
            }
        }

        StringBuilder path = id == INodeId.ROOT_INODE_ID ? new StringBuilder()
                : new StringBuilder(inode.getFullPathName());
        for (int i = 4; i < pathComponents.length; i++) {
            path.append(Path.SEPARATOR).append(DFSUtil.bytes2String(pathComponents[i]));
        }
        if (NameNode.LOG.isDebugEnabled()) {
            NameNode.LOG.debug("Resolved path is " + path);
        }
        return path.toString();
    }



    /** Get the file info for a specific file.
     * @param src The string representation of the path to the file
     * @param resolveLink whether to throw UnresolvedLinkException
     * @return object containing information regarding the file
     *         or null if file not found
     */
    HdfsFileStatus getFileInfo(String src, boolean resolveLink)
            throws UnresolvedLinkException {
        String srcs = normalizePath(src);
        readLock();
        try {
            if (srcs.endsWith(HdfsConstants.SEPARATOR_DOT_SNAPSHOT_DIR)) {
                return getFileInfo4DotSnapshot(srcs);
            }
            final FileINodesInPath inodesInPath = rootDir.getLastINodeInPath(srcs, resolveLink);
            final INode i = inodesInPath.getINode(0);
            return i == null? null: createFileStatus(HdfsFileStatus.EMPTY_NAME, i,
                    inodesInPath.getPathSnapshot());
        } finally {
            readUnlock();
        }
    }
    String normalizePath(String src) {
        if (src.length() > 1 && src.endsWith("/")) {
            src = src.substring(0, src.length() - 1);
        }
        return src;
    }
    /**
     * Currently we only support "ls /xxx/.snapshot" which will return all the
     * snapshots of a directory. The FSCommand Ls will first call getFileInfo to
     * make sure the file/directory exists (before the real getListing call).
     * Since we do not have a real INode for ".snapshot", we return an empty
     * non-null HdfsFileStatus here.
     */
    private HdfsFileStatus getFileInfo4DotSnapshot(String src)
            throws UnresolvedLinkException {
        Preconditions.checkArgument(
                src.endsWith(HdfsConstants.SEPARATOR_DOT_SNAPSHOT_DIR),
                "%s does not end with %s", src, HdfsConstants.SEPARATOR_DOT_SNAPSHOT_DIR);

        final String dirPath = normalizePath(src.substring(0,
                src.length() - HdfsConstants.DOT_SNAPSHOT_DIR.length()));

        final INode node = this.getINode(dirPath);
        if (node != null
                && node.isDirectory()
                && node.asDirectory() instanceof INodeDirectorySnapshottable) {
            return new HdfsFileStatus(0, true, 0, 0, 0, 0, null, null, null, null,
                    HdfsFileStatus.EMPTY_NAME, -1L, 0);
        }
        return null;
    }


    /**
     * Add the given filename to the fs.
     * @throws FileAlreadyExistsException
     * @throws QuotaExceededException
     * @throws UnresolvedLinkException
     * @throws SnapshotAccessControlException
     */
    INodeFileUnderConstruction addFile(String path,
                                       PermissionStatus permissions,
                                       short replication,
                                       long preferredBlockSize,
                                       String clientName,
                                       String clientMachine,
                                       DatanodeDescriptor clientNode)
            throws FileAlreadyExistsException, QuotaExceededException,
            UnresolvedLinkException, SnapshotAccessControlException {
        waitForReady();

        // Always do an implicit mkdirs for parent directory tree.
        long modTime = now();

        Path parent = new Path(path).getParent();
        if (parent == null) {
            // Trying to add "/" as a file - this path has no
            // parent -- avoids an NPE below.
            return null;
        }

        if (!mkdirs(parent.toString(), permissions, true, modTime)) {
            return null;
        }
        INodeFileUnderConstruction newNode = new INodeFileUnderConstruction(
                namesystem.allocateNewInodeId(),
                permissions,replication,
                preferredBlockSize, modTime, clientName,
                clientMachine, clientNode);
        boolean added = false;
        writeLock();
        try {
            added = addINode(path, newNode);
        } finally {
            writeUnlock();
        }
        if (!added) {
            NameNodeTest.stateChangeLog.info("DIR* addFile: failed to add " + path);
            return null;
        }

        if(NameNodeTest.stateChangeLog.isDebugEnabled()) {
            NameNodeTest.stateChangeLog.debug("DIR* addFile: " + path + " is added");
        }
        return newNode;
    }

    /**
     * Create a directory
     * If ancestor directories do not exist, automatically create them.

     * @param src string representation of the path to the directory
     * @param permissions the permission of the directory
     * @param now creation time
     * @return true if the operation succeeds false otherwise
     * @throws FileNotFoundException if an ancestor or itself is a file
     * @throws QuotaExceededException if directory creation violates
     *                                any quota limit
     * @throws UnresolvedLinkException if a symlink is encountered in src.
     * @throws SnapshotAccessControlException if path is in RO snapshot
     */
    boolean mkdirs(String src, PermissionStatus permissions,
                   boolean inheritPermission, long now)
            throws FileAlreadyExistsException, QuotaExceededException,
            UnresolvedLinkException, SnapshotAccessControlException {
        src = normalizePath(src);
        String[] names = INode.getPathNames(src);
        byte[][] components = INode.getPathComponents(names);
        final int lastInodeIndex = components.length - 1;

        writeLock();
        try {
            FileINodesInPath iip = getExistingPathINodes(components);
            if (iip.isSnapshot()) {
                throw new SnapshotAccessControlException(
                        "Modification on RO snapshot is disallowed");
            }
            INode[] inodes = iip.getINodes();

            // find the index of the first null in inodes[]
            StringBuilder pathbuilder = new StringBuilder();
            int i = 1;
            for(; i < inodes.length && inodes[i] != null; i++) {
                pathbuilder.append(Path.SEPARATOR).append(names[i]);
                if (!inodes[i].isDirectory()) {
                    throw new FileAlreadyExistsException("Parent path is not a directory: "
                            + pathbuilder+ " "+inodes[i].getLocalName());
                }
            }

            // default to creating parent dirs with the given perms
            PermissionStatus parentPermissions = permissions;

            // if not inheriting and it's the last inode, there's no use in
            // computing perms that won't be used
            if (inheritPermission || (i < lastInodeIndex)) {
                // if inheriting (ie. creating a file or symlink), use the parent dir,
                // else the supplied permissions
                // NOTE: the permissions of the auto-created directories violate posix
                FsPermission parentFsPerm = inheritPermission
                        ? inodes[i-1].getFsPermission() : permissions.getPermission();

                // ensure that the permissions allow user write+execute
                if (!parentFsPerm.getUserAction().implies(FsAction.WRITE_EXECUTE)) {
                    parentFsPerm = new FsPermission(
                            parentFsPerm.getUserAction().or(FsAction.WRITE_EXECUTE),
                            parentFsPerm.getGroupAction(),
                            parentFsPerm.getOtherAction()
                    );
                }

                if (!parentPermissions.getPermission().equals(parentFsPerm)) {
                    parentPermissions = new PermissionStatus(
                            parentPermissions.getUserName(),
                            parentPermissions.getGroupName(),
                            parentFsPerm
                    );
                    // when inheriting, use same perms for entire path
                    if (inheritPermission) permissions = parentPermissions;
                }
            }

            // create directories beginning from the first null index
            for(; i < inodes.length; i++) {
                pathbuilder.append(Path.SEPARATOR + names[i]);
                unprotectedMkdir(namesystem.allocateNewInodeId(), iip, i,
                        components[i], (i < lastInodeIndex) ? parentPermissions
                                : permissions, now);
                if (inodes[i] == null) {
                    return false;
                }
                // Directory creation also count towards FilesCreated
                // to match count of FilesDeleted metric.
                if (getFSNamesystem() != null)
                    NameNode.getNameNodeMetrics().incrFilesCreated();

                final String cur = pathbuilder.toString();
                fsImage.getEditLog().logMkDir(cur, inodes[i]);
                if(NameNode.stateChangeLog.isDebugEnabled()) {
                    NameNode.stateChangeLog.debug(
                            "DIR* FSDirectory.mkdirs: created directory " + cur);
                }
            }
        } finally {
            writeUnlock();
        }
        return true;
    }
    /** create a directory at index pos.
     * The parent path to the directory is at [0, pos-1].
     * All ancestors exist. Newly created one stored at index pos.
     */
    private void unprotectedMkdir(long inodeId, FileINodesInPath inodesInPath,
                                  int pos, byte[] name, PermissionStatus permission, long timestamp)
            throws QuotaExceededException {
        assert hasWriteLock();
        final INodeDirectory dir = new INodeDirectory(inodeId, name, permission,
                timestamp);
        if (addChild(inodesInPath, pos, dir, true)) {
            inodesInPath.setINode(pos, dir);
        }
    }

    long totalInodes() {
        readLock();
        try {
            return rootDir.numItemsInTree();
        } finally {
            readUnlock();
        }
    }

    /**
     * @return true if the path is a non-empty directory; otherwise, return false.
     */
    boolean isNonEmptyDirectory(String path) throws UnresolvedLinkException {
        readLock();
        try {
            final FileINodesInPath inodesInPath = rootDir.getLastINodeInPath(path, false);
            final INode inode = inodesInPath.getINode(0);
            if (inode == null || !inode.isDirectory()) {
                //not found or not a directory
                return false;
            }
            final Snapshot s = inodesInPath.getPathSnapshot();
            return !inode.asDirectory().getChildrenList(s).isEmpty();
        } finally {
            readUnlock();
        }
    }

    boolean delete(String src, INode.BlocksMapUpdateInfo collectedBlocks,
                   List<INode> removedINodes, boolean logRetryCache) throws IOException {
        if (NameNode.stateChangeLog.isDebugEnabled()) {
            NameNode.stateChangeLog.debug("DIR* FSDirectory.delete: " + src);
        }
        waitForReady();
        long now = now();
        final long filesRemoved;
        writeLock();
        try {
            final FileINodesInPath inodesInPath = rootDir.getINodesInPath4Write(
                    normalizePath(src), false);
            if (!deleteAllowed(inodesInPath, src) ) {
                filesRemoved = -1;
            } else {
                // Before removing the node, first check if the targetNode is for a
                // snapshottable dir with snapshots, or its descendants have
                // snapshottable dir with snapshots
                final INode targetNode = inodesInPath.getLastINode();
                List<FileINodeDirectorySnapshottable> snapshottableDirs =
                        new ArrayList<FileINodeDirectorySnapshottable>();
                checkSnapshot(targetNode, snapshottableDirs);
                filesRemoved = unprotectedDelete(inodesInPath, collectedBlocks,
                        removedINodes, now);
                if (snapshottableDirs.size() > 0) {
                    // There are some snapshottable directories without snapshots to be
                    // deleted. Need to update the SnapshotManager.
                    namesystem.removeSnapshottableDirs(snapshottableDirs);
                }
            }
        } finally {
            writeUnlock();
        }
        if (filesRemoved < 0) {
            return false;
        }
        fsImage.getEditLog().logDelete(src, now, logRetryCache);
        incrDeletedFileCount(filesRemoved);
        // Blocks/INodes will be handled later by the caller of this method
        getFSNamesystem().removePathAndBlocks(src, null, null);
        return true;
    }
    private static boolean deleteAllowed(final FileINodesInPath iip,
                                         final String src) {
        final INode[] inodes = iip.getINodes();
        if (inodes == null || inodes.length == 0
                || inodes[inodes.length - 1] == null) {
            if(NameNodeTest.stateChangeLog.isDebugEnabled()) {
                NameNodeTest.stateChangeLog.debug("DIR* FSDirectory.unprotectedDelete: "
                        + "failed to remove " + src + " because it does not exist");
            }
            return false;
        } else if (inodes.length == 1) { // src is the root
            NameNodeTest.stateChangeLog.warn("DIR* FSDirectory.unprotectedDelete: "
                    + "failed to remove " + src
                    + " because the root is not allowed to be deleted");
            return false;
        }
        return true;
    }


    /**
     * Check if the given INode (or one of its descendants) is snapshottable and
     * already has snapshots.
     *
     * @param target The given INode
     * @param snapshottableDirs The list of directories that are snapshottable
     *                          but do not have snapshots yet
     */
    private static void checkSnapshot(INode target,
                                      List<FileINodeDirectorySnapshottable> snapshottableDirs) throws IOException {
        if (target.isDirectory()) {
            FileINodeDirectory targetDir = target.asFileDirectory();
            if (targetDir.isSnapshottable()) {
                FileINodeDirectorySnapshottable ssTargetDir =
                        (FileINodeDirectorySnapshottable) targetDir;
                if (ssTargetDir.getNumSnapshots() > 0) {
                    throw new IOException("The directory " + ssTargetDir.getFullPathName()
                            + " cannot be deleted since " + ssTargetDir.getFullPathName()
                            + " is snapshottable and already has snapshots");
                } else {
                    if (snapshottableDirs != null) {
                        snapshottableDirs.add(ssTargetDir);
                    }
                }
            }
            for (INode child : targetDir.getChildrenList(null)) {
                checkSnapshot(child, snapshottableDirs);
            }
        }
    }
    /**
     * Delete a path from the name space
     * Update the count at each ancestor directory with quota
     * @param iip the inodes resolved from the path
     * @param collectedBlocks blocks collected from the deleted path
     * @param removedINodes inodes that should be removed from {@link #inodeMap}
     * @param mtime the time the inode is removed
     * @return the number of inodes deleted; 0 if no inodes are deleted.
     */
    long unprotectedDelete(FileINodesInPath iip, INode.BlocksMapUpdateInfo collectedBlocks,
                           List<INode> removedINodes, long mtime) throws QuotaExceededException {
        assert hasWriteLock();

        // check if target node exists
        INode targetNode = iip.getLastINode();
        if (targetNode == null) {
            return -1;
        }

        // record modification
        final Snapshot latestSnapshot = iip.getLatestSnapshot();
        targetNode = targetNode.recordFileModification(latestSnapshot, inodeMap);
        iip.setLastINode(targetNode);

        // Remove the node from the namespace
        long removed = removeLastINode(iip);
        if (removed == -1) {
            return -1;
        }

        // set the parent's modification time
        final INodeDirectory parent = targetNode.getParent();
        parent.updateFileModificationTime(mtime, latestSnapshot, inodeMap);
        if (removed == 0) {
            return 0;
        }

        // collect block
        if (!targetNode.isInLatestSnapshot(latestSnapshot)) {
            targetNode.destroyAndCollectBlocks(collectedBlocks, removedINodes);
        } else {
            Quota.Counts counts = targetNode.cleanSubtree(null, latestSnapshot,
                    collectedBlocks, removedINodes, true);
            parent.addSpaceConsumed(-counts.get(Quota.NAMESPACE),
                    -counts.get(Quota.DISKSPACE), true);
            removed = counts.get(Quota.NAMESPACE);
        }
        if (NameNode.stateChangeLog.isDebugEnabled()) {
            NameNode.stateChangeLog.debug("DIR* FSDirectory.unprotectedDelete: "
                    + targetNode.getFullPathName() + " is removed");
        }
        return removed;
    }
    private void incrDeletedFileCount(long count) {
        if (getFSNamesystem() != null)
            NameNodeTest.getNameNodeMetrics().incrFilesDeleted(count);
    }

    private long removeLastINode(final FileINodesInPath iip)
            throws QuotaExceededException {
        final Snapshot latestSnapshot = iip.getLatestSnapshot();
        final INode last = iip.getLastINode();
        final FileINodeDirectory parent = iip.getINode(-2).asFileDirectory();
        if (!parent.removeChild(last, latestSnapshot, inodeMap)) {
            return -1;
        }
        FileINodeDirectory newParent = last.getFileParent();
        if (parent != newParent) {
            iip.setINode(-2, newParent);
        }

        if (!last.isInLatestSnapshot(latestSnapshot)) {
            final Quota.Counts counts = last.computeQuotaUsage();
            updateCountNoQuotaCheck(iip, iip.getINodes().length - 1,
                    -counts.get(Quota.NAMESPACE), -counts.get(Quota.DISKSPACE));

            if (INodeReference.tryRemoveReference(last) > 0) {
                return 0;
            } else {
                return counts.get(Quota.NAMESPACE);
            }
        }
        return 1;
    }

    /**
     * Close file.
     */
    void closeFile(String path, INodeFile file) {
        waitForReady();
        writeLock();
        try {
            // file is closed
            fsImage.getEditLog().logCloseFile(path, file);
            if (NameNode.stateChangeLog.isDebugEnabled()) {
                NameNode.stateChangeLog.debug("DIR* FSDirectory.closeFile: "
                        +path+" with "+ file.getBlocks().length
                        +" blocks is persisted to the file system");
            }
        } finally {
            writeUnlock();
        }
    }


    /**
     * Set file replication
     *
     * @param src file name
     * @param replication new replication
     * @param blockRepls block replications - output parameter
     * @return array of file blocks
     * @throws QuotaExceededException
     * @throws SnapshotAccessControlException
     */
    Block[] setReplication(String src, short replication, short[] blockRepls)
            throws QuotaExceededException, UnresolvedLinkException,
            SnapshotAccessControlException {
        waitForReady();
        writeLock();
        try {
            final Block[] fileBlocks = unprotectedSetReplication(
                    src, replication, blockRepls);
            if (fileBlocks != null)  // log replication change
                fsImage.getEditLog().logSetReplication(src, replication);
            return fileBlocks;
        } finally {
            writeUnlock();
        }
    }

    void setPermission(String src, FsPermission permission)
            throws FileNotFoundException, UnresolvedLinkException,
            QuotaExceededException, SnapshotAccessControlException {
        writeLock();
        try {
            unprotectedSetPermission(src, permission);
        } finally {
            writeUnlock();
        }
        fsImage.getEditLog().logSetPermissions(src, permission);
    }

    void unprotectedSetPermission(String src, FsPermission permissions)
            throws FileNotFoundException, UnresolvedLinkException,
            QuotaExceededException, SnapshotAccessControlException {
        assert hasWriteLock();
        final FileINodesInPath inodesInPath = rootDir.getINodesInPath4Write(src, true);
        final INode inode = inodesInPath.getLastINode();
        if (inode == null) {
            throw new FileNotFoundException("File does not exist: " + src);
        }
        inode.setFilePermission(permissions, inodesInPath.getLatestSnapshot(),
                inodeMap);
    }


    void setOwner(String src, String username, String groupname)
            throws FileNotFoundException, UnresolvedLinkException,
            QuotaExceededException, SnapshotAccessControlException {
        writeLock();
        try {
            unprotectedSetOwner(src, username, groupname);
        } finally {
            writeUnlock();
        }
        fsImage.getEditLog().logSetOwner(src, username, groupname);
    }
    void unprotectedSetOwner(String src, String username, String groupname)
            throws FileNotFoundException, UnresolvedLinkException,
            QuotaExceededException, SnapshotAccessControlException {
        assert hasWriteLock();
        final FileINodesInPath inodesInPath = rootDir.getINodesInPath4Write(src, true);
        INode inode = inodesInPath.getLastINode();
        if (inode == null) {
            throw new FileNotFoundException("File does not exist: " + src);
        }
        if (username != null) {
            inode = inode.setFileUser(username, inodesInPath.getLatestSnapshot(),
                    inodeMap);
        }
        if (groupname != null) {
            inode.setFileGroup(groupname, inodesInPath.getLatestSnapshot(), inodeMap);
        }
    }
    /**
     * Remove a block from the file.
     * @return Whether the block exists in the corresponding file
     */
    boolean removeBlock(String path, INodeFileUnderConstruction fileNode,
                        Block block) throws IOException {
        waitForReady();

        writeLock();
        try {
            return unprotectedRemoveBlock(path, fileNode, block);
        } finally {
            writeUnlock();
        }
    }

    boolean unprotectedRemoveBlock(String path,
                                   INodeFileUnderConstruction fileNode, Block block) throws IOException {
        // modify file-> block and blocksMap
        boolean removed = fileNode.removeLastBlock(block);
        if (!removed) {
            return false;
        }
        getBlockManager().removeBlockFromMap(block);

        if(NameNode.stateChangeLog.isDebugEnabled()) {
            NameNode.stateChangeLog.debug("DIR* FSDirectory.removeBlock: "
                    +path+" with "+block
                    +" block is removed from the file system");
        }

        // update space consumed
        final FileINodesInPath iip = rootDir.getINodesInPath4Write(path, true);
        updateCount(iip, 0, -fileNode.getBlockDiskspace(), true);
        return true;
    }
    /**
     * Persist the block list for the inode.
     */
    void persistBlocks(String path, INodeFileUnderConstruction file,
                       boolean logRetryCache) {
        waitForReady();

        writeLock();
        try {
            fsImage.getEditLog().logUpdateBlocks(path, file, logRetryCache);
            if(NameNode.stateChangeLog.isDebugEnabled()) {
                NameNode.stateChangeLog.debug("DIR* FSDirectory.persistBlocks: "
                        +path+" with "+ file.getBlocks().length
                        +" blocks is persisted to the file system");
            }
        } finally {
            writeUnlock();
        }
    }

    /**
     * Add a block to the file. Returns a reference to the added block.
     */
    BlockInfo addBlock(String path, FileINodesInPath inodesInPath, Block block,
                       DatanodeDescriptor targets[]) throws IOException {
        waitForReady();

        writeLock();
        try {
            final INodeFileUnderConstruction fileINode =
                    INodeFileUnderConstruction.valueOf(inodesInPath.getLastINode(), path);

            // check quota limits and updated space consumed
            updateCount(inodesInPath, 0, fileINode.getBlockDiskspace(), true);

            // associate new last block for the file
            BlockInfoUnderConstruction blockInfo =
                    new BlockInfoUnderConstruction(
                            block,
                            fileINode.getFileReplication(),
                            HdfsServerConstants.BlockUCState.UNDER_CONSTRUCTION,
                            targets);
            getBlockManager().addBlockCollection(blockInfo, fileINode);
            fileINode.addBlock(blockInfo);

            if(NameNode.stateChangeLog.isDebugEnabled()) {
                NameNode.stateChangeLog.debug("DIR* FSDirectory.addBlock: "
                        + path + " with " + block
                        + " block is added to the in-memory "
                        + "file system");
            }
            return blockInfo;
        } finally {
            writeUnlock();
        }
    }

    /** Updates namespace and diskspace consumed for all
     * directories until the parent directory of file represented by path.
     *
     * @param path path for the file.
     * @param nsDelta the delta change of namespace
     * @param dsDelta the delta change of diskspace
     * @throws QuotaExceededException if the new count violates any quota limit
     * @throws FileNotFoundException if path does not exist.
     */
    void updateSpaceConsumed(String path, long nsDelta, long dsDelta)
            throws QuotaExceededException, FileNotFoundException,
            UnresolvedLinkException, SnapshotAccessControlException {
        writeLock();
        try {
            final FileINodesInPath iip = rootDir.getINodesInPath4Write(path, false);
            if (iip.getLastINode() == null) {
                throw new FileNotFoundException("Path not found: " + path);
            }
            updateCount(iip, nsDelta, dsDelta, true);
        } finally {
            writeUnlock();
        }
    }
    boolean isDir(String src) throws UnresolvedLinkException {
        src = normalizePath(src);
        readLock();
        try {
            INode node = rootDir.getNode(src, false);
            return node != null && node.isDirectory();
        } finally {
            readUnlock();
        }
    }

    boolean renameTo(String src, String dst, boolean logRetryCache)
            throws QuotaExceededException, UnresolvedLinkException,
            FileAlreadyExistsException, SnapshotAccessControlException, IOException {
        if (NameNode.stateChangeLog.isDebugEnabled()) {
            NameNode.stateChangeLog.debug("DIR* FSDirectory.renameTo: "
                    +src+" to "+dst);
        }
        waitForReady();
        long now = now();
        writeLock();
        try {
            if (!unprotectedRenameTo(src, dst, now))
                return false;
        } finally {
            writeUnlock();
        }
        fsImage.getEditLog().logRename(src, dst, now, logRetryCache);
        return true;
    }
    boolean unprotectedRenameTo(String src, String dst, long timestamp)
            throws QuotaExceededException, UnresolvedLinkException,
            FileAlreadyExistsException, SnapshotAccessControlException, IOException {
        assert hasWriteLock();
        FileINodesInPath srcIIP = rootDir.getINodesInPath4Write(src, false);
        final INode srcInode = srcIIP.getLastINode();

        // check the validation of the source
        if (srcInode == null) {
            NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
                    + "failed to rename " + src + " to " + dst
                    + " because source does not exist");
            return false;
        }
        if (srcIIP.getINodes().length == 1) {
            NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
                    +"failed to rename "+src+" to "+dst+ " because source is the root");
            return false;
        }

        // srcInode and its subtree cannot contain snapshottable directories with
        // snapshots
        List<FileINodeDirectorySnapshottable> snapshottableDirs =
                new ArrayList<FileINodeDirectorySnapshottable>();
        checkSnapshot(srcInode, snapshottableDirs);

        if (isDir(dst)) {
            dst += Path.SEPARATOR + new Path(src).getName();
        }

        // check the validity of the destination
        if (dst.equals(src)) {
            return true;
        }
        if (srcInode.isSymlink() &&
                dst.equals(srcInode.asSymlink().getSymlinkString())) {
            throw new FileAlreadyExistsException(
                    "Cannot rename symlink "+src+" to its target "+dst);
        }

        // dst cannot be directory or a file under src
        if (dst.startsWith(src) &&
                dst.charAt(src.length()) == Path.SEPARATOR_CHAR) {
            NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
                    + "failed to rename " + src + " to " + dst
                    + " because destination starts with src");
            return false;
        }

        byte[][] dstComponents = INode.getPathComponents(dst);
        FileINodesInPath dstIIP = getExistingPathINodes(dstComponents);
        if (dstIIP.isSnapshot()) {
            throw new SnapshotAccessControlException(
                    "Modification on RO snapshot is disallowed");
        }
        if (dstIIP.getLastINode() != null) {
            NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
                    +"failed to rename "+src+" to "+dst+
                    " because destination exists");
            return false;
        }
        INode dstParent = dstIIP.getINode(-2);
        if (dstParent == null) {
            NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
                    +"failed to rename "+src+" to "+dst+
                    " because destination's parent does not exist");
            return false;
        }

        // Ensure dst has quota to accommodate rename
        verifyQuotaForRename(srcIIP.getINodes(), dstIIP.getINodes());

        boolean added = false;
        INode srcChild = srcIIP.getLastINode();
        final byte[] srcChildName = srcChild.getLocalNameBytes();
        final boolean isSrcInSnapshot = srcChild.isInLatestSnapshot(
                srcIIP.getLatestSnapshot());
        final boolean srcChildIsReference = srcChild.isReference();

        // Record the snapshot on srcChild. After the rename, before any new
        // snapshot is taken on the dst tree, changes will be recorded in the latest
        // snapshot of the src tree.
        if (isSrcInSnapshot) {
            srcChild = srcChild.recordFileModification(srcIIP.getLatestSnapshot(),
                    inodeMap);
            srcIIP.setLastINode(srcChild);
        }

        // check srcChild for reference
        final INodeReference.WithCount withCount;
        Quota.Counts oldSrcCounts = Quota.Counts.newInstance();
        int srcRefDstSnapshot = srcChildIsReference ? srcChild.asReference()
                .getDstSnapshotId() : Snapshot.INVALID_ID;
        if (isSrcInSnapshot) {
            final INodeReference.WithName withName =
                    srcIIP.getINode(-2).asFileDirectory().replaceChild4ReferenceWithName(
                            srcChild, srcIIP.getLatestSnapshot());
            withCount = (INodeReference.WithCount) withName.getReferredINode();
            srcChild = withName;
            srcIIP.setLastINode(srcChild);
            // get the counts before rename
            withCount.getReferredINode().computeQuotaUsage(oldSrcCounts, true,
                    Snapshot.INVALID_ID);
        } else if (srcChildIsReference) {
            // srcChild is reference but srcChild is not in latest snapshot
            withCount = (INodeReference.WithCount) srcChild.asReference().getReferredINode();
        } else {
            withCount = null;
        }

        try {
            // remove src
            final long removedSrc = removeLastINode(srcIIP);
            if (removedSrc == -1) {
                NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
                        + "failed to rename " + src + " to " + dst
                        + " because the source can not be removed");
                return false;
            }

            if (dstParent.getParent() == null) {
                // src and dst file/dir are in the same directory, and the dstParent has
                // been replaced when we removed the src. Refresh the dstIIP and
                // dstParent.
                dstIIP = getExistingPathINodes(dstComponents);
                dstParent = dstIIP.getINode(-2);
            }

            // add src to the destination

            srcChild = srcIIP.getLastINode();
            final byte[] dstChildName = dstIIP.getLastLocalName();
            final INode toDst;
            if (withCount == null) {
                srcChild.setLocalName(dstChildName);
                toDst = srcChild;
            } else {
                withCount.getReferredINode().setLocalName(dstChildName);
                Snapshot dstSnapshot = dstIIP.getLatestSnapshot();
                final INodeReference.DstReference ref = new INodeReference.DstReference(
                        dstParent.asDirectory(), withCount,
                        dstSnapshot == null ? Snapshot.INVALID_ID : dstSnapshot.getId());
                toDst = ref;
            }

            added = addLastINodeNoQuotaCheck(dstIIP, toDst);
            if (added) {
                if (NameNodeTest.stateChangeLog.isDebugEnabled()) {
                    NameNodeTest.stateChangeLog.debug("DIR* FSDirectory.unprotectedRenameTo: "
                            + src + " is renamed to " + dst);
                }
                // update modification time of dst and the parent of src
                final INode srcParent = srcIIP.getINode(-2);
                srcParent.updateFileModificationTime(timestamp, srcIIP.getLatestSnapshot(),
                        inodeMap);
                dstParent = dstIIP.getINode(-2); // refresh dstParent
                dstParent.updateFileModificationTime(timestamp, dstIIP.getLatestSnapshot(),
                        inodeMap);
                // update moved leases with new filename
                getFSNamesystem().unprotectedChangeLease(src, dst);

                // update the quota usage in src tree
                if (isSrcInSnapshot) {
                    // get the counts after rename
                    Quota.Counts newSrcCounts = srcChild.computeQuotaUsage(
                            Quota.Counts.newInstance(), false, Snapshot.INVALID_ID);
                    newSrcCounts.subtract(oldSrcCounts);
                    srcParent.addSpaceConsumed(newSrcCounts.get(Quota.NAMESPACE),
                            newSrcCounts.get(Quota.DISKSPACE), false);
                }

                return true;
            }
        } finally {
            if (!added) {
                final INodeDirectory srcParent = srcIIP.getINode(-2).asDirectory();
                final INode oldSrcChild = srcChild;
                // put it back
                if (withCount == null) {
                    srcChild.setLocalName(srcChildName);
                } else if (!srcChildIsReference) { // src must be in snapshot
                    // the withCount node will no longer be used thus no need to update
                    // its reference number here
                    final INode originalChild = withCount.getReferredINode();
                    srcChild = originalChild;
                    srcChild.setLocalName(srcChildName);
                } else {
                    withCount.removeReference(oldSrcChild.asReference());
                    final INodeReference originalRef = new INodeReference.DstReference(
                            srcParent, withCount, srcRefDstSnapshot);
                    srcChild = originalRef;
                    withCount.getReferredINode().setLocalName(srcChildName);
                }

                if (isSrcInSnapshot) {
                    // srcParent must be an INodeDirectoryWithSnapshot instance since
                    // isSrcInSnapshot is true and src node has been removed from
                    // srcParent
                    ((INodeDirectoryWithSnapshot) srcParent).undoRename4ScrParent(
                            oldSrcChild.asReference(), srcChild, srcIIP.getLatestSnapshot());
                } else {
                    // original srcChild is not in latest snapshot, we only need to add
                    // the srcChild back
                    addLastINodeNoQuotaCheck(srcIIP, srcChild);
                }
            }
        }
        NameNodeTest.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
                +"failed to rename "+src+" to "+dst);
        return false;
    }
    private void verifyQuotaForRename(INode[] src, INode[] dst)
            throws QuotaExceededException {
        if (!ready) {
            // Do not check quota if edits log is still being processed
            return;
        }
        int i = 0;
        for(; src[i] == dst[i]; i++);
        // src[i - 1] is the last common ancestor.

        final Quota.Counts delta = src[src.length - 1].computeQuotaUsage();

        // Reduce the required quota by dst that is being removed
        final int dstIndex = dst.length - 1;
        if (dst[dstIndex] != null) {
            delta.subtract(dst[dstIndex].computeQuotaUsage());
        }
        verifyQuota(dst, dstIndex, delta.get(Quota.NAMESPACE),
                delta.get(Quota.DISKSPACE), src[i - 1]);
    }
    private boolean addLastINodeNoQuotaCheck(FileINodesInPath inodesInPath, INode i) {
        try {
            return addLastINode(inodesInPath, i, false);
        } catch (QuotaExceededException e) {
            NameNode.LOG.warn("FSDirectory.addChildNoQuotaCheck - unexpected", e);
        }
        return false;
    }

    void concat(String target, String [] srcs, boolean supportRetryCache)
            throws UnresolvedLinkException, QuotaExceededException,
            SnapshotAccessControlException, SnapshotException {
        writeLock();
        try {
            // actual move
            waitForReady();
            long timestamp = now();
            unprotectedConcat(target, srcs, timestamp);
            // do the commit
            fsImage.getEditLog().logConcat(target, srcs, timestamp,
                    supportRetryCache);
        } finally {
            writeUnlock();
        }
    }
    public INode getINode4Write(String src) throws UnresolvedLinkException,
            SnapshotAccessControlException {
        readLock();
        try {
            return rootDir.getINode4Write(src, true);
        } finally {
            readUnlock();
        }
    }
    void renameTo(String src, String dst, boolean logRetryCache,
                  Options.Rename... options)
            throws FileAlreadyExistsException, FileNotFoundException,
            ParentNotDirectoryException, QuotaExceededException,
            UnresolvedLinkException, IOException {
        if (NameNode.stateChangeLog.isDebugEnabled()) {
            NameNode.stateChangeLog.debug("DIR* FSDirectory.renameTo: " + src
                    + " to " + dst);
        }
        waitForReady();
        long now = now();
        writeLock();
        try {
            if (unprotectedRenameTo(src, dst, now, options)) {
                incrDeletedFileCount(1);
            }
        } finally {
            writeUnlock();
        }
        fsImage.getEditLog().logRename(src, dst, now, logRetryCache, options);
    }
    boolean unprotectedRenameTo(String src, String dst, long timestamp,
                                Options.Rename... options) throws FileAlreadyExistsException,
            FileNotFoundException, ParentNotDirectoryException,
            QuotaExceededException, UnresolvedLinkException, IOException {
        assert hasWriteLock();
        boolean overwrite = false;
        if (null != options) {
            for (Options.Rename option : options) {
                if (option == Options.Rename.OVERWRITE) {
                    overwrite = true;
                }
            }
        }
        String error = null;
        final FileINodesInPath srcIIP = rootDir.getINodesInPath4Write(src, false);
        final INode srcInode = srcIIP.getLastINode();
        // validate source
        if (srcInode == null) {
            error = "rename source " + src + " is not found.";
            NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
                    + error);
            throw new FileNotFoundException(error);
        }
        if (srcIIP.getINodes().length == 1) {
            error = "rename source cannot be the root";
            NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
                    + error);
            throw new IOException(error);
        }
        // srcInode and its subtree cannot contain snapshottable directories with
        // snapshots
        checkSnapshot(srcInode, null);

        // validate the destination
        if (dst.equals(src)) {
            throw new FileAlreadyExistsException(
                    "The source "+src+" and destination "+dst+" are the same");
        }
        if (srcInode.isSymlink() &&
                dst.equals(srcInode.asSymlink().getSymlinkString())) {
            throw new FileAlreadyExistsException(
                    "Cannot rename symlink "+src+" to its target "+dst);
        }
        // dst cannot be a directory or a file under src
        if (dst.startsWith(src) &&
                dst.charAt(src.length()) == Path.SEPARATOR_CHAR) {
            error = "Rename destination " + dst
                    + " is a directory or file under source " + src;
            NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
                    + error);
            throw new IOException(error);
        }
        FileINodesInPath dstIIP = rootDir.getINodesInPath4Write(dst, false);
        if (dstIIP.getINodes().length == 1) {
            error = "rename destination cannot be the root";
            NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
                    + error);
            throw new IOException(error);
        }

        final INode dstInode = dstIIP.getLastINode();
        List<FileINodeDirectorySnapshottable> snapshottableDirs =
                new ArrayList<FileINodeDirectorySnapshottable>();
        if (dstInode != null) { // Destination exists
            // It's OK to rename a file to a symlink and vice versa
            if (dstInode.isDirectory() != srcInode.isDirectory()) {
                error = "Source " + src + " and destination " + dst
                        + " must both be directories";
                NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
                        + error);
                throw new IOException(error);
            }
            if (!overwrite) { // If destination exists, overwrite flag must be true
                error = "rename destination " + dst + " already exists";
                NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
                        + error);
                throw new FileAlreadyExistsException(error);
            }
            if (dstInode.isDirectory()) {
                final ReadOnlyList<INode> children = dstInode.asDirectory()
                        .getChildrenList(null);
                if (!children.isEmpty()) {
                    error = "rename destination directory is not empty: " + dst;
                    NameNode.stateChangeLog.warn(
                            "DIR* FSDirectory.unprotectedRenameTo: " + error);
                    throw new IOException(error);
                }
            }
            checkSnapshot(dstInode, snapshottableDirs);
        }

        INode dstParent = dstIIP.getINode(-2);
        if (dstParent == null) {
            error = "rename destination parent " + dst + " not found.";
            NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
                    + error);
            throw new FileNotFoundException(error);
        }
        if (!dstParent.isDirectory()) {
            error = "rename destination parent " + dst + " is a file.";
            NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
                    + error);
            throw new ParentNotDirectoryException(error);
        }

        // Ensure dst has quota to accommodate rename
        verifyQuotaForRename(srcIIP.getINodes(), dstIIP.getINodes());

        INode srcChild = srcIIP.getLastINode();
        final byte[] srcChildName = srcChild.getLocalNameBytes();
        final boolean isSrcInSnapshot = srcChild.isInLatestSnapshot(
                srcIIP.getLatestSnapshot());
        final boolean srcChildIsReference = srcChild.isReference();

        // Record the snapshot on srcChild. After the rename, before any new
        // snapshot is taken on the dst tree, changes will be recorded in the latest
        // snapshot of the src tree.
        if (isSrcInSnapshot) {
            srcChild = srcChild.recordFileModification(srcIIP.getLatestSnapshot(),
                    inodeMap);
            srcIIP.setLastINode(srcChild);
        }

        // check srcChild for reference
        final INodeReference.WithCount withCount;
        int srcRefDstSnapshot = srcChildIsReference ? srcChild.asReference()
                .getDstSnapshotId() : Snapshot.INVALID_ID;
        Quota.Counts oldSrcCounts = Quota.Counts.newInstance();
        if (isSrcInSnapshot) {
            final INodeReference.WithName withName = srcIIP.getINode(-2).asFileDirectory()
                    .replaceChild4ReferenceWithName(srcChild, srcIIP.getLatestSnapshot());
            withCount = (INodeReference.WithCount) withName.getReferredINode();
            srcChild = withName;
            srcIIP.setLastINode(srcChild);
            // get the counts before rename
            withCount.getReferredINode().computeQuotaUsage(oldSrcCounts, true,
                    Snapshot.INVALID_ID);
        } else if (srcChildIsReference) {
            // srcChild is reference but srcChild is not in latest snapshot
            withCount = (INodeReference.WithCount) srcChild.asReference().getReferredINode();
        } else {
            withCount = null;
        }

        boolean undoRemoveSrc = true;
        final long removedSrc = removeLastINode(srcIIP);
        if (removedSrc == -1) {
            error = "Failed to rename " + src + " to " + dst
                    + " because the source can not be removed";
            NameNode.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
                    + error);
            throw new IOException(error);
        }

        if (dstParent.getParent() == null) {
            // src and dst file/dir are in the same directory, and the dstParent has
            // been replaced when we removed the src. Refresh the dstIIP and
            // dstParent.
            dstIIP = rootDir.getINodesInPath4Write(dst, false);
        }

        boolean undoRemoveDst = false;
        INode removedDst = null;
        try {
            if (dstInode != null) { // dst exists remove it
                if (removeLastINode(dstIIP) != -1) {
                    removedDst = dstIIP.getLastINode();
                    undoRemoveDst = true;
                }
            }

            srcChild = srcIIP.getLastINode();

            final byte[] dstChildName = dstIIP.getLastLocalName();
            final INode toDst;
            if (withCount == null) {
                srcChild.setLocalName(dstChildName);
                toDst = srcChild;
            } else {
                withCount.getReferredINode().setLocalName(dstChildName);
                Snapshot dstSnapshot = dstIIP.getLatestSnapshot();
                final INodeReference.DstReference ref = new INodeReference.DstReference(
                        dstIIP.getINode(-2).asDirectory(), withCount,
                        dstSnapshot == null ? Snapshot.INVALID_ID : dstSnapshot.getId());
                toDst = ref;
            }

            // add src as dst to complete rename
            if (addLastINodeNoQuotaCheck(dstIIP, toDst)) {
                undoRemoveSrc = false;
                if (NameNode.stateChangeLog.isDebugEnabled()) {
                    NameNode.stateChangeLog.debug(
                            "DIR* FSDirectory.unprotectedRenameTo: " + src
                                    + " is renamed to " + dst);
                }

                final INode srcParent = srcIIP.getINode(-2);
                srcParent.updateFileModificationTime(timestamp, srcIIP.getLatestSnapshot(),
                        inodeMap);
                dstParent = dstIIP.getINode(-2);
                dstParent.updateFileModificationTime(timestamp, dstIIP.getLatestSnapshot(),
                        inodeMap);
                // update moved lease with new filename
                getFSNamesystem().unprotectedChangeLease(src, dst);

                // Collect the blocks and remove the lease for previous dst
                long filesDeleted = -1;
                if (removedDst != null) {
                    undoRemoveDst = false;
                    INode.BlocksMapUpdateInfo collectedBlocks = new INode.BlocksMapUpdateInfo();
                    List<INode> removedINodes = new ArrayList<INode>();
                    filesDeleted = removedDst.cleanSubtree(null,
                            dstIIP.getLatestSnapshot(), collectedBlocks, removedINodes, true)
                            .get(Quota.NAMESPACE);
                    getFSNamesystem().removePathAndBlocks(src, collectedBlocks,
                            removedINodes);
                }

                if (snapshottableDirs.size() > 0) {
                    // There are snapshottable directories (without snapshots) to be
                    // deleted. Need to update the SnapshotManager.
                    namesystem.removeSnapshottableDirs(snapshottableDirs);
                }

                // update the quota usage in src tree
                if (isSrcInSnapshot) {
                    // get the counts after rename
                    Quota.Counts newSrcCounts = srcChild.computeQuotaUsage(
                            Quota.Counts.newInstance(), false, Snapshot.INVALID_ID);
                    newSrcCounts.subtract(oldSrcCounts);
                    srcParent.addSpaceConsumed(newSrcCounts.get(Quota.NAMESPACE),
                            newSrcCounts.get(Quota.DISKSPACE), false);
                }

                return filesDeleted >= 0;
            }
        } finally {
            if (undoRemoveSrc) {
                // Rename failed - restore src
                final INodeDirectory srcParent = srcIIP.getINode(-2).asDirectory();
                final INode oldSrcChild = srcChild;
                // put it back
                if (withCount == null) {
                    srcChild.setLocalName(srcChildName);
                } else if (!srcChildIsReference) { // src must be in snapshot
                    // the withCount node will no longer be used thus no need to update
                    // its reference number here
                    final INode originalChild = withCount.getReferredINode();
                    srcChild = originalChild;
                    srcChild.setLocalName(srcChildName);
                } else {
                    withCount.removeReference(oldSrcChild.asReference());
                    final INodeReference originalRef = new INodeReference.DstReference(
                            srcParent, withCount, srcRefDstSnapshot);
                    srcChild = originalRef;
                    withCount.getReferredINode().setLocalName(srcChildName);
                }

                if (srcParent instanceof INodeDirectoryWithSnapshot) {
                    ((INodeDirectoryWithSnapshot) srcParent).undoRename4ScrParent(
                            oldSrcChild.asReference(), srcChild, srcIIP.getLatestSnapshot());
                } else {
                    // srcParent is not an INodeDirectoryWithSnapshot, we only need to add
                    // the srcChild back
                    addLastINodeNoQuotaCheck(srcIIP, srcChild);
                }
            }
            if (undoRemoveDst) {
                // Rename failed - restore dst
                if (dstParent instanceof INodeDirectoryWithSnapshot) {
                    ((INodeDirectoryWithSnapshot) dstParent).undoRename4DstParent(
                            removedDst, dstIIP.getLatestSnapshot());
                } else {
                    addLastINodeNoQuotaCheck(dstIIP, removedDst);
                }
                if (removedDst.isReference()) {
                    final INodeReference removedDstRef = removedDst.asReference();
                    final INodeReference.WithCount wc =
                            (INodeReference.WithCount) removedDstRef.getReferredINode().asReference();
                    wc.addReference(removedDstRef);
                }
            }
        }
        NameNodeTest.stateChangeLog.warn("DIR* FSDirectory.unprotectedRenameTo: "
                + "failed to rename " + src + " to " + dst);
        throw new IOException("rename from " + src + " to " + dst + " failed.");
    }

    boolean isDirMutable(String src) throws UnresolvedLinkException,
            SnapshotAccessControlException {
        src = normalizePath(src);
        readLock();
        try {
            INode node = rootDir.getINode4Write(src, false);
            return node != null && node.isDirectory();
        } finally {
            readUnlock();
        }
    }

    DirectoryListing getListing(String src, byte[] startAfter,
                                boolean needLocation) throws UnresolvedLinkException, IOException {
        String srcs = normalizePath(src);

        readLock();
        try {
            if (srcs.endsWith(HdfsConstants.SEPARATOR_DOT_SNAPSHOT_DIR)) {
                return getSnapshotsListing(srcs, startAfter);
            }
            final FileINodesInPath inodesInPath = rootDir.getLastINodeInPath(srcs, true);
            final Snapshot snapshot = inodesInPath.getPathSnapshot();
            final INode targetNode = inodesInPath.getINode(0);
            if (targetNode == null)
                return null;

            if (!targetNode.isDirectory()) {
                return new DirectoryListing(
                        new HdfsFileStatus[]{createFileStatus(HdfsFileStatus.EMPTY_NAME,
                                targetNode, needLocation, snapshot)}, 0);
            }

            final INodeDirectory dirInode = targetNode.asDirectory();
            final ReadOnlyList<INode> contents = dirInode.getChildrenList(snapshot);
            int startChild = FileINodeDirectory.nextChild(contents, startAfter);
            int totalNumChildren = contents.size();
            int numOfListing = Math.min(totalNumChildren-startChild, this.lsLimit);
            HdfsFileStatus listing[] = new HdfsFileStatus[numOfListing];
            for (int i=0; i<numOfListing; i++) {
                INode cur = contents.get(startChild+i);
                listing[i] = createFileStatus(cur.getLocalNameBytes(), cur,
                        needLocation, snapshot);
            }
            return new DirectoryListing(
                    listing, totalNumChildren-startChild-numOfListing);
        } finally {
            readUnlock();
        }
    }
    private DirectoryListing getSnapshotsListing(String src, byte[] startAfter)
            throws UnresolvedLinkException, IOException {
        Preconditions.checkState(hasReadLock());
        Preconditions.checkArgument(
                src.endsWith(HdfsConstants.SEPARATOR_DOT_SNAPSHOT_DIR),
                "%s does not end with %s", src, HdfsConstants.SEPARATOR_DOT_SNAPSHOT_DIR);

        final String dirPath = normalizePath(src.substring(0,
                src.length() - HdfsConstants.DOT_SNAPSHOT_DIR.length()));

        final INode node = this.getINode(dirPath);
        final INodeDirectorySnapshottable dirNode = INodeDirectorySnapshottable
                .valueOf(node, dirPath);
        final ReadOnlyList<Snapshot> snapshots = dirNode.getSnapshotList();
        int skipSize = ReadOnlyList.Util.binarySearch(snapshots, startAfter);
        skipSize = skipSize < 0 ? -skipSize - 1 : skipSize + 1;
        int numOfListing = Math.min(snapshots.size() - skipSize, this.lsLimit);
        final HdfsFileStatus listing[] = new HdfsFileStatus[numOfListing];
        for (int i = 0; i < numOfListing; i++) {
            Snapshot.Root sRoot = snapshots.get(i + skipSize).getRoot();
            listing[i] = createFileStatus(sRoot.getLocalNameBytes(), sRoot, null);
        }
        return new DirectoryListing(
                listing, snapshots.size() - skipSize - numOfListing);
    }

    long getPreferredBlockSize(String path) throws UnresolvedLinkException,
            FileNotFoundException, IOException {
        readLock();
        try {
            return INodeFile.valueOf(rootDir.getNode(path, false), path
            ).getPreferredBlockSize();
        } finally {
            readUnlock();
        }
    }
    static String getFullPathName(INode inode) {
        INode[] inodes = getFullPathINodes(inode);
        return getFullPathName(inodes, inodes.length - 1);
    }
    private static INode[] getFullPathINodes(INode inode) {
        return getRelativePathINodes(inode, null);
    }

    /**
     * @return the relative path of an inode from one of its ancestors,
     *         represented by an array of inodes.
     */
    private static INode[] getRelativePathINodes(INode inode, INode ancestor) {
        // calculate the depth of this inode from the ancestor
        int depth = 0;
        for (INode i = inode; i != null && !i.equals(ancestor); i = i.getParent()) {
            depth++;
        }
        INode[] inodes = new INode[depth];

        // fill up the inodes in the path from this inode to root
        for (int i = 0; i < depth; i++) {
            if (inode == null) {
                NameNode.stateChangeLog.warn("Could not get full path."
                        + " Corresponding file might have deleted already.");
                return null;
            }
            inodes[depth-i-1] = inode;
            inode = inode.getParent();
        }
        return inodes;
    }
    ContentSummary getContentSummary(String src)
            throws FileNotFoundException, UnresolvedLinkException {
        String srcs = normalizePath(src);
        readLock();
        try {
            INode targetNode = rootDir.getNode(srcs, false);
            if (targetNode == null) {
                throw new FileNotFoundException("File does not exist: " + srcs);
            }
            else {
                return targetNode.computeContentSummary();
            }
        } finally {
            readUnlock();
        }
    }
    void setQuota(String src, long nsQuota, long dsQuota)
            throws FileNotFoundException, PathIsNotDirectoryException,
            QuotaExceededException, UnresolvedLinkException,
            SnapshotAccessControlException {
        writeLock();
        try {
            FileINodeDirectory dir = unprotectedSetQuota(src, nsQuota, dsQuota);
            if (dir != null) {
                fsImage.getEditLog().logSetQuota(src, dir.getNsQuota(),
                        dir.getDsQuota());
            }
        } finally {
            writeUnlock();
        }
    }
    /**
     * See {@link ClientProtocol#setQuota(String, long, long)} for the contract.
     * Sets quota for for a directory.
     * @returns INodeDirectory if any of the quotas have changed. null other wise.
     * @throws FileNotFoundException if the path does not exist.
     * @throws PathIsNotDirectoryException if the path is not a directory.
     * @throws QuotaExceededException if the directory tree size is
     *                                greater than the given quota
     * @throws UnresolvedLinkException if a symlink is encountered in src.
     * @throws SnapshotAccessControlException if path is in RO snapshot
     */
    FileINodeDirectory unprotectedSetQuota(String src, long nsQuota, long dsQuota)
            throws FileNotFoundException, PathIsNotDirectoryException,
            QuotaExceededException, UnresolvedLinkException,
            SnapshotAccessControlException {
        assert hasWriteLock();
        // sanity check
        if ((nsQuota < 0 && nsQuota != HdfsConstants.QUOTA_DONT_SET &&
                nsQuota < HdfsConstants.QUOTA_RESET) ||
                (dsQuota < 0 && dsQuota != HdfsConstants.QUOTA_DONT_SET &&
                        dsQuota < HdfsConstants.QUOTA_RESET)) {
            throw new IllegalArgumentException("Illegal value for nsQuota or " +
                    "dsQuota : " + nsQuota + " and " +
                    dsQuota);
        }

        String srcs = normalizePath(src);
        final FileINodesInPath iip = rootDir.getINodesInPath4Write(srcs, true);
        FileINodeDirectory dirNode = FileINodeDirectory.valueOf(iip.getLastINode(), srcs);
        if (dirNode.isRoot() && nsQuota == HdfsConstants.QUOTA_RESET) {
            throw new IllegalArgumentException("Cannot clear namespace quota on root.");
        } else { // a directory inode
            long oldNsQuota = dirNode.getNsQuota();
            long oldDsQuota = dirNode.getDsQuota();
            if (nsQuota == HdfsConstants.QUOTA_DONT_SET) {
                nsQuota = oldNsQuota;
            }
            if (dsQuota == HdfsConstants.QUOTA_DONT_SET) {
                dsQuota = oldDsQuota;
            }

            final Snapshot latest = iip.getLatestSnapshot();
            if (dirNode instanceof FileINodeDirectoryWithQuota) {
                FileINodeDirectoryWithQuota quotaNode = (FileINodeDirectoryWithQuota) dirNode;
                Quota.Counts counts = null;
                if (!quotaNode.isQuotaSet()) {
                    // dirNode must be an INodeDirectoryWithSnapshot whose quota has not
                    // been set yet
                    counts = quotaNode.computeQuotaUsage();
                }
                // a directory with quota; so set the quota to the new value
                quotaNode.setQuota(nsQuota, dsQuota);
                if (quotaNode.isQuotaSet() && counts != null) {
                    quotaNode.setSpaceConsumed(counts.get(Quota.NAMESPACE),
                            counts.get(Quota.DISKSPACE));
                } else if (!quotaNode.isQuotaSet() && latest == null) {
                    // do not replace the node if the node is a snapshottable directory
                    // without snapshots
                    if (!(quotaNode instanceof FileINodeDirectoryWithSnapshot)) {
                        // will not come here for root because root is snapshottable and
                        // root's nsQuota is always set
                        return quotaNode.replaceSelf4INodeDirectory(inodeMap);
                    }
                }
            } else {
                // a non-quota directory; so replace it with a directory with quota
                return dirNode.replaceSelf4Quota(latest, nsQuota, dsQuota, inodeMap);
            }
            return (oldNsQuota != nsQuota || oldDsQuota != dsQuota) ? dirNode : null;
        }
    }

    boolean isValidToCreate(String src) throws UnresolvedLinkException,
            SnapshotAccessControlException {
        String srcs = normalizePath(src);
        readLock();
        try {
            if (srcs.startsWith("/") && !srcs.endsWith("/")
                    && rootDir.getINode4Write(srcs, false) == null) {
                return true;
            } else {
                return false;
            }
        } finally {
            readUnlock();
        }
    }

    INodeSymlink addSymlink(String path, String target,
                            PermissionStatus dirPerms, boolean createParent, boolean logRetryCache)
            throws UnresolvedLinkException, FileAlreadyExistsException,
            QuotaExceededException, SnapshotAccessControlException {
        waitForReady();

        final long modTime = now();
        if (createParent) {
            final String parent = new Path(path).getParent().toString();
            if (!mkdirs(parent, dirPerms, true, modTime)) {
                return null;
            }
        }
        final String userName = dirPerms.getUserName();
        INodeSymlink newNode  = null;
        long id = namesystem.allocateNewInodeId();
        writeLock();
        try {
            newNode = unprotectedAddSymlink(id, path, target, modTime, modTime,
                    new PermissionStatus(userName, null, FsPermission.getDefault()));
        } finally {
            writeUnlock();
        }
        if (newNode == null) {
            NameNode.stateChangeLog.info("DIR* addSymlink: failed to add " + path);
            return null;
        }
        fsImage.getEditLog().logSymlink(path, target, modTime, modTime, newNode,
                logRetryCache);

        if(NameNode.stateChangeLog.isDebugEnabled()) {
            NameNode.stateChangeLog.debug("DIR* addSymlink: " + path + " is added");
        }
        return newNode;
    }

    INodeSymlink unprotectedAddSymlink(long id, String path, String target,
                                       long mtime, long atime, PermissionStatus perm)
            throws UnresolvedLinkException, QuotaExceededException {
        assert hasWriteLock();
        final INodeSymlink symlink = new INodeSymlink(id, null, perm, mtime, atime,
                target);
        return addINode(path, symlink) ? symlink : null;
    }
    /** Verify if the snapshot name is legal. */
    void verifySnapshotName(String snapshotName, String path)
            throws FSLimitException.PathComponentTooLongException {
        if (snapshotName.contains(Path.SEPARATOR)) {
            throw new HadoopIllegalArgumentException(
                    "Snapshot name cannot contain \"" + Path.SEPARATOR + "\"");
        }
        final byte[] bytes = DFSUtil.string2Bytes(snapshotName);
        verifyINodeName(bytes);
        verifyMaxComponentLength(bytes, path, 0);
    }
    public FileINodeDirectoryWithQuota getRoot() {
        return rootDir;
    }

}
