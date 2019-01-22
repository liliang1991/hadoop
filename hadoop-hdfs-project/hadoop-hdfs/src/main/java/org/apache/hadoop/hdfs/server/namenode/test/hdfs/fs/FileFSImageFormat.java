package org.apache.hadoop.hdfs.server.namenode.test.hdfs.fs;//package org.apache.hadoop.hdfs.server.namenode.test.hdfs.fs;
//
//import org.apache.commons.logging.Log;
//import org.apache.hadoop.HadoopIllegalArgumentException;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.fs.PathIsNotDirectoryException;
//import org.apache.hadoop.fs.UnresolvedLinkException;
//import org.apache.hadoop.fs.permission.PermissionStatus;
//import org.apache.hadoop.hdfs.protocol.LayoutVersion;
//import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
//import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
//import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;
//import org.apache.hadoop.hdfs.server.namenode.*;
//import org.apache.hadoop.hdfs.server.namenode.snapshot.*;
//import org.apache.hadoop.hdfs.server.namenode.startupprogress.Phase;
//import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress;
//import org.apache.hadoop.hdfs.server.namenode.startupprogress.Step;
//import org.apache.hadoop.hdfs.server.namenode.startupprogress.StepType;
//import org.apache.hadoop.hdfs.server.namenode.test.hdfs.block.FileINodeDirectorySnapshottable;
//import org.apache.hadoop.io.MD5Hash;
//import org.apache.hadoop.io.Text;
//
//import java.io.*;
//import java.security.DigestInputStream;
//import java.security.MessageDigest;
//import java.util.Arrays;
//import java.util.Map;
//
//import static org.apache.hadoop.util.Time.now;
//
//public class FileFSImageFormat {
//    private static final Log LOG = FileSystenImage.LOG;
//
//    public static class Loader {
//        private final Configuration conf;
//        /** which namesystem this loader is working for */
//        private final FileNamesystem namesystem;
//
//        /** Set to true once a file has been loaded using this loader. */
//        private boolean loaded = false;
//
//        /** The transaction ID of the last edit represented by the loaded file */
//        private long imgTxId;
//        /** The MD5 sum of the loaded file */
//        private MD5Hash imgDigest;
//
//        private Map<Integer, Snapshot> snapshotMap = null;
//        private final SnapshotFSImageFormat.ReferenceMap referenceMap = new SnapshotFSImageFormat.ReferenceMap();
//
//        Loader(Configuration conf, FileNamesystem namesystem) {
//            this.conf = conf;
//            this.namesystem = namesystem;
//        }
//
//        /**
//         * Return the MD5 checksum of the image that has been loaded.
//         * @throws IllegalStateException if load() has not yet been called.
//         */
//        MD5Hash getLoadedImageMd5() {
//            checkLoaded();
//            return imgDigest;
//        }
//
//        long getLoadedImageTxId() {
//            checkLoaded();
//            return imgTxId;
//        }
//
//        /**
//         * Throw IllegalStateException if load() has not yet been called.
//         */
//        private void checkLoaded() {
//            if (!loaded) {
//                throw new IllegalStateException("Image not yet loaded!");
//            }
//        }
//
//        /**
//         * Throw IllegalStateException if load() has already been called.
//         */
//        private void checkNotLoaded() {
//            if (loaded) {
//                throw new IllegalStateException("Image already loaded!");
//            }
//        }
//
//        void load(File curFile) throws IOException {
//            checkNotLoaded();
//            assert curFile != null : "curFile is null";
//
//            StartupProgress prog = NameNode.getStartupProgress();
//            Step step = new Step(StepType.INODES);
//            prog.beginStep(Phase.LOADING_FSIMAGE, step);
//            long startTime = now();
//
//            //
//            // Load in bits
//            //
//            MessageDigest digester = MD5Hash.getDigester();
//            DigestInputStream fin = new DigestInputStream(
//                    new FileInputStream(curFile), digester);
//
//            DataInputStream in = new DataInputStream(fin);
//            try {
//                // read image version: first appeared in version -1
//                int imgVersion = in.readInt();
//                if (getLayoutVersion() != imgVersion) {
//                    throw new InconsistentFSStateException(curFile,
//                            "imgVersion " + imgVersion +
//                                    " expected to be " + getLayoutVersion());
//                }
//                boolean supportSnapshot = LayoutVersion.supports(LayoutVersion.Feature.SNAPSHOT,
//                        imgVersion);
//
//                // read namespaceID: first appeared in version -2
//                in.readInt();
//
//                long numFiles = in.readLong();
//
//                // read in the last generation stamp for legacy blocks.
//                long genstamp = in.readLong();
//                namesystem.setGenerationStampV1(genstamp);
//
//                if (LayoutVersion.supports(LayoutVersion.Feature.SEQUENTIAL_BLOCK_ID, imgVersion)) {
//                    // read the starting generation stamp for sequential block IDs
//                    genstamp = in.readLong();
//                    namesystem.setGenerationStampV2(genstamp);
//
//                    // read the last generation stamp for blocks created after
//                    // the switch to sequential block IDs.
//                    long stampAtIdSwitch = in.readLong();
//                    namesystem.setGenerationStampV1Limit(stampAtIdSwitch);
//
//                    // read the max sequential block ID.
//                    long maxSequentialBlockId = in.readLong();
//                    namesystem.setLastAllocatedBlockId(maxSequentialBlockId);
//                } else {
//                    long startingGenStamp = namesystem.upgradeGenerationStampToV2();
//                    // This is an upgrade.
//                    LOG.info("Upgrading to sequential block IDs. Generation stamp " +
//                            "for new blocks set to " + startingGenStamp);
//                }
//
//                // read the transaction ID of the last edit represented by
//                // this image
//                if (LayoutVersion.supports(LayoutVersion.Feature.STORED_TXIDS, imgVersion)) {
//                    imgTxId = in.readLong();
//                } else {
//                    imgTxId = 0;
//                }
//
//                // read the last allocated inode id in the fsimage
//                if (LayoutVersion.supports(LayoutVersion.Feature.ADD_INODE_ID, imgVersion)) {
//                    long lastInodeId = in.readLong();
//                    namesystem.resetLastInodeId(lastInodeId);
//                    if (LOG.isDebugEnabled()) {
//                        LOG.debug("load last allocated InodeId from fsimage:" + lastInodeId);
//                    }
//                } else {
//                    if (LOG.isDebugEnabled()) {
//                        LOG.debug("Old layout version doesn't have inode id."
//                                + " Will assign new id for each inode.");
//                    }
//                }
//
//                if (supportSnapshot) {
//                    snapshotMap = namesystem.getSnapshotManager().read(in, this);
//                }
//
//                // read compression related info
//                FileSystemImageCompression compression;
//                if (LayoutVersion.supports(LayoutVersion.Feature.FSIMAGE_COMPRESSION, imgVersion)) {
//                    compression =  FileSystemImageCompression.readCompressionHeader(conf, in);
//                } else {
//                    compression =  FileSystemImageCompression.createNoopCompression();
//                }
//                in = compression.unwrapInputStream(fin);
//
//                LOG.info("Loading image file " + curFile + " using " + compression);
//
//                // load all inodes
//                LOG.info("Number of files = " + numFiles);
//                prog.setTotal(Phase.LOADING_FSIMAGE, step, numFiles);
//                StartupProgress.Counter counter = prog.getCounter(Phase.LOADING_FSIMAGE, step);
//                if (LayoutVersion.supports(LayoutVersion.Feature.FSIMAGE_NAME_OPTIMIZATION,
//                        imgVersion)) {
//                    if (supportSnapshot) {
//                        loadLocalNameINodesWithSnapshot(numFiles, in, counter);
//                    } else {
//                        loadLocalNameINodes(numFiles, in, counter);
//                    }
//                } else {
//                    loadFullNameINodes(numFiles, in, counter);
//                }
//
//                loadFilesUnderConstruction(in, supportSnapshot, counter);
//                prog.endStep(Phase.LOADING_FSIMAGE, step);
//                // Now that the step is finished, set counter equal to total to adjust
//                // for possible under-counting due to reference inodes.
//                prog.setCount(Phase.LOADING_FSIMAGE, step, numFiles);
//
//                loadSecretManagerState(in);
//
//                // make sure to read to the end of file
//                boolean eof = (in.read() == -1);
//                assert eof : "Should have reached the end of image file " + curFile;
//            } finally {
//                in.close();
//            }
//
//            imgDigest = new MD5Hash(digester.digest());
//            loaded = true;
//
//            LOG.info("Image file " + curFile + " of size " + curFile.length() +
//                    " bytes loaded in " + (now() - startTime)/1000 + " seconds.");
//        }
//        byte[][] getParent(byte[][] path) {
//            byte[][] result = new byte[path.length - 1][];
//            for (int i = 0; i < result.length; i++) {
//                result[i] = new byte[path[i].length];
//                System.arraycopy(path[i], 0, result[i], 0, path[i].length);
//            }
//            return result;
//        }
//
//        public Snapshot getSnapshot(DataInput in) throws IOException {
//            return snapshotMap.get(in.readInt());
//        }
//        private int getLayoutVersion() {
//            return namesystem.getFSImage().getStorage().getLayoutVersion();
//        }
//        private void loadLocalNameINodesWithSnapshot(long numFiles, DataInput in,
//                                                     StartupProgress.Counter counter) throws IOException {
//            assert LayoutVersion.supports(LayoutVersion.Feature.FSIMAGE_NAME_OPTIMIZATION,
//                    getLayoutVersion());
//            assert LayoutVersion.supports(LayoutVersion.Feature.SNAPSHOT, getLayoutVersion());
//
//            // load root
//            loadRoot(in, counter);
//            // load rest of the nodes recursively
//            loadDirectoryWithSnapshot(in, counter);
//        }
//        private void loadRoot(DataInput in, StartupProgress.Counter counter)
//                throws IOException {
//            // load root
//            if (in.readShort() != 0) {
//                throw new IOException("First node is not root");
//            }
//            final INodeDirectory root = loadINode(null, false, in, counter)
//                    .asDirectory();
//            // update the root's attributes
//            updateRootAttr(root);
//        }
//
//        INode loadINode(final byte[] localName, boolean isSnapshotINode,
//                        DataInput in, StartupProgress.Counter counter) throws IOException {
//            final int imgVersion = getLayoutVersion();
//            if (LayoutVersion.supports(LayoutVersion.Feature.SNAPSHOT, imgVersion)) {
//                namesystem.getFSDirectory().verifyINodeName(localName);
//            }
//
//            long inodeId = LayoutVersion.supports(LayoutVersion.Feature.ADD_INODE_ID, imgVersion) ?
//                    in.readLong() : namesystem.allocateNewInodeId();
//
//            final short replication = namesystem.getBlockManager().adjustReplication(
//                    in.readShort());
//            final long modificationTime = in.readLong();
//            long atime = 0;
//            if (LayoutVersion.supports(LayoutVersion.Feature.FILE_ACCESS_TIME, imgVersion)) {
//                atime = in.readLong();
//            }
//            final long blockSize = in.readLong();
//            final int numBlocks = in.readInt();
//
//            if (numBlocks >= 0) {
//                // file
//
//                // read blocks
//                BlockInfo[] blocks = null;
//                if (numBlocks >= 0) {
//                    blocks = new BlockInfo[numBlocks];
//                    for (int j = 0; j < numBlocks; j++) {
//                        blocks[j] = new BlockInfo(replication);
//                        blocks[j].readFields(in);
//                    }
//                }
//
//                String clientName = "";
//                String clientMachine = "";
//                boolean underConstruction = false;
//                FileWithSnapshot.FileDiffList fileDiffs = null;
//                if (LayoutVersion.supports(LayoutVersion.Feature.SNAPSHOT, imgVersion)) {
//                    // read diffs
//                    fileDiffs = SnapshotFSImageFormat.loadFileDiffList(in, this);
//
//                    if (isSnapshotINode) {
//                        underConstruction = in.readBoolean();
//                        if (underConstruction) {
//                            clientName = FSImageSerialization.readString(in);
//                            clientMachine = FSImageSerialization.readString(in);
//                        }
//                    }
//                }
//
//                final PermissionStatus permissions = PermissionStatus.read(in);
//
//                // return
//                if (counter != null) {
//                    counter.increment();
//                }
//                final INodeFile file = new INodeFile(inodeId, localName, permissions,
//                        modificationTime, atime, blocks, replication, blockSize);
//                return fileDiffs != null? new INodeFileWithSnapshot(file, fileDiffs)
//                        : underConstruction? new INodeFileUnderConstruction(
//                        file, clientName, clientMachine, null)
//                        : file;
//            } else if (numBlocks == -1) {
//                //directory
//
//                //read quotas
//                final long nsQuota = in.readLong();
//                long dsQuota = -1L;
//                if (LayoutVersion.supports(LayoutVersion.Feature.DISKSPACE_QUOTA, imgVersion)) {
//                    dsQuota = in.readLong();
//                }
//
//                //read snapshot info
//                boolean snapshottable = false;
//                boolean withSnapshot = false;
//                if (LayoutVersion.supports(LayoutVersion.Feature.SNAPSHOT, imgVersion)) {
//                    snapshottable = in.readBoolean();
//                    if (!snapshottable) {
//                        withSnapshot = in.readBoolean();
//                    }
//                }
//
//                final PermissionStatus permissions = PermissionStatus.read(in);
//
//                //return
//                if (counter != null) {
//                    counter.increment();
//                }
//                final INodeDirectory dir = nsQuota >= 0 || dsQuota >= 0?
//                        new INodeDirectoryWithQuota(inodeId, localName, permissions,
//                                modificationTime, nsQuota, dsQuota)
//                        : new INodeDirectory(inodeId, localName, permissions, modificationTime);
//                return snapshottable ? new INodeDirectorySnapshottable(dir)
//                        : withSnapshot ? new INodeDirectoryWithSnapshot(dir)
//                        : dir;
//            } else if (numBlocks == -2) {
//                //symlink
//                if (!FileSystem.isSymlinksEnabled()) {
//                    throw new IOException("Symlinks not supported - please remove symlink before upgrading to this version of HDFS");
//                }
//
//                final String symlink = Text.readString(in);
//                final PermissionStatus permissions = PermissionStatus.read(in);
//                if (counter != null) {
//                    counter.increment();
//                }
//                return new INodeSymlink(inodeId, localName, permissions,
//                        modificationTime, atime, symlink);
//            } else if (numBlocks == -3) {
//                //reference
//                // Intentionally do not increment counter, because it is too difficult at
//                // this point to assess whether or not this is a reference that counts
//                // toward quota.
//
//                final boolean isWithName = in.readBoolean();
//                // lastSnapshotId for WithName node, dstSnapshotId for DstReference node
//                int snapshotId = in.readInt();
//
//                final INodeReference.WithCount withCount
//                        = referenceMap.loadINodeReferenceWithCount(isSnapshotINode, in, this);
//
//                if (isWithName) {
//                    return new INodeReference.WithName(null, withCount, localName,
//                            snapshotId);
//                } else {
//                    final INodeReference ref = new INodeReference.DstReference(null,
//                            withCount, snapshotId);
//                    return ref;
//                }
//            }
//
//            throw new IOException("Unknown inode type: numBlocks=" + numBlocks);
//        }
//
//        private void loadLocalNameINodes(long numFiles, DataInput in, StartupProgress.Counter counter)
//                throws IOException {
//            assert LayoutVersion.supports(LayoutVersion.Feature.FSIMAGE_NAME_OPTIMIZATION,
//                    getLayoutVersion());
//            assert numFiles > 0;
//
//            // load root
//            loadRoot(in, counter);
//            // have loaded the first file (the root)
//            numFiles--;
//
//            // load rest of the nodes directory by directory
//            while (numFiles > 0) {
//                numFiles -= loadDirectory(in, counter);
//            }
//            if (numFiles != 0) {
//                throw new IOException("Read unexpect number of files: " + -numFiles);
//            }
//        }
//        private int loadDirectory(DataInput in, StartupProgress.Counter counter) throws IOException {
//            String parentPath = FSImageSerialization.readString(in);
//            final INodeDirectory parent = INodeDirectory.valueOf(
//                    namesystem.dir.rootDir.getNode(parentPath, true), parentPath);
//            return loadChildren(parent, in, counter);
//        }
//        private int loadChildren(INodeDirectory parent, DataInput in,
//                                 StartupProgress.Counter counter) throws IOException {
//            int numChildren = in.readInt();
//            for (int i = 0; i < numChildren; i++) {
//                // load single inode
//                INode newNode = loadINodeWithLocalName(false, in, true, counter);
//                addToParent(parent, newNode);
//            }
//            return numChildren;
//        }
//        private void loadFullNameINodes(long numFiles, DataInput in, StartupProgress.Counter counter)
//                throws IOException {
//            byte[][] pathComponents;
//            byte[][] parentPath = {{}};
//            FSDirectory fsDir = namesystem.dir;
//            INodeDirectory parentINode = fsDir.rootDir;
//            for (long i = 0; i < numFiles; i++) {
//                pathComponents = FSImageSerialization.readPathComponents(in);
//                final INode newNode = loadINode(
//                        pathComponents[pathComponents.length-1], false, in, counter);
//
//                if (isRoot(pathComponents)) { // it is the root
//                    // update the root's attributes
//                    updateRootAttr(newNode.asDirectory());
//                    continue;
//                }
//                // check if the new inode belongs to the same parent
//                if(!isParent(pathComponents, parentPath)) {
//                    parentINode = getParentINodeDirectory(pathComponents);
//                    parentPath = getParent(pathComponents);
//                }
//
//                // add new inode
//                addToParent(parentINode, newNode);
//            }
//        }
//        private void loadFilesUnderConstruction(DataInput in,
//                                                boolean supportSnapshot, StartupProgress.Counter counter) throws IOException {
//            FSDirectory fsDir = namesystem.dir;
//            int size = in.readInt();
//
//            LOG.info("Number of files under construction = " + size);
//
//            for (int i = 0; i < size; i++) {
//                INodeFileUnderConstruction cons = FSImageSerialization
//                        .readINodeUnderConstruction(in, namesystem, getLayoutVersion());
//                counter.increment();
//
//                // verify that file exists in namespace
//                String path = cons.getLocalName();
//                final INodesInPath iip = fsDir.getLastINodeInPath(path);
//                INodeFile oldnode = INodeFile.valueOf(iip.getINode(0), path);
//                cons.setLocalName(oldnode.getLocalNameBytes());
//                cons.setParent(oldnode.getParent());
//
//                if (oldnode instanceof INodeFileWithSnapshot) {
//                    cons = new INodeFileUnderConstructionWithSnapshot(cons,
//                            ((INodeFileWithSnapshot)oldnode).getDiffs());
//                }
//
//                fsDir.replaceINodeFile(path, oldnode, cons);
//                namesystem.leaseManager.addLease(cons.getClientName(), path);
//            }
//        }
//        private void loadSecretManagerState(DataInput in)
//                throws IOException {
//            int imgVersion = getLayoutVersion();
//
//            if (!LayoutVersion.supports(LayoutVersion.Feature.DELEGATION_TOKEN, imgVersion)) {
//                //SecretManagerState is not available.
//                //This must not happen if security is turned on.
//                return;
//            }
//            namesystem.loadSecretManagerState(in);
//        }
//        public INode loadINodeWithLocalName(boolean isSnapshotINode,
//                                            DataInput in, boolean updateINodeMap, StartupProgress.Counter counter)
//                throws IOException {
//            final byte[] localName = FSImageSerialization.readLocalName(in);
//            INode inode = loadINode(localName, isSnapshotINode, in, counter);
//            if (updateINodeMap
//                    && LayoutVersion.supports(LayoutVersion.Feature.ADD_INODE_ID, getLayoutVersion())) {
//                namesystem.dir.addToInodeMap(inode);
//            }
//            return inode;
//        }
//    }
//
//}
