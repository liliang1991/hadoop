package org.apache.hadoop.hdfs.server.namenode.test.hdfs.fs;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.server.namenode.*;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Phase;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress;
import org.apache.hadoop.hdfs.server.namenode.test.hdfs.MyNameNode;
import org.apache.hadoop.hdfs.server.namenode.test.hdfs.block.FileINodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.test.hdfs.block.FileINodeDirectoryWithQuota;
import org.apache.hadoop.hdfs.server.namenode.test.hdfs.manager.NameNodeStorageRetentionManager;
import org.apache.hadoop.hdfs.server.namenode.test.hdfs.store.NameNodeStorage;
import org.apache.hadoop.hdfs.server.protocol.CheckpointCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.hdfs.util.Canceler;
import org.apache.hadoop.hdfs.util.MD5FileUtils;
import org.apache.hadoop.util.Time;


import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.*;

public class FileSystenImage implements Closeable {
    public static final Log LOG = LogFactory.getLog(FileSystenImage.class.getName());
    protected long lastAppliedTxId = 0;

    Configuration conf;
    NameNodeStorage storage;
    FileSystemEditLog editLog;
    NameNodeStorageRetentionManager archivalManager;
    boolean isUpgradeFinalized;

    public FileSystenImage(Configuration conf,
                           Collection<URI> imageDirs,
                           List<URI> editsDirs) throws IOException {
        this.conf = conf;

        storage = new NameNodeStorage(conf, imageDirs, editsDirs);
        if (conf.getBoolean(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_RESTORE_KEY,
                DFSConfigKeys.DFS_NAMENODE_NAME_DIR_RESTORE_DEFAULT)) {
            storage.restoreFailedStorage = true;
        }
        this.editLog = new FileSystemEditLog(conf, storage, editsDirs);

        archivalManager = new NameNodeStorageRetentionManager(conf, storage, editLog, new NameNodeStorageRetentionManager.DeletionStoragePurger());
    }
  public   boolean isUpgradeFinalized() {
        return isUpgradeFinalized;
    }
    static Collection<URI> getCheckpointDirs(Configuration conf,
                                             String defaultValue) {
        Collection<String> dirNames = conf.getTrimmedStringCollection(
                DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_DIR_KEY);
        if (dirNames.size() == 0 && defaultValue != null) {
            dirNames.add(defaultValue);
        }
        return Util.stringCollectionAsURIs(dirNames);
    }

    static List<URI> getCheckpointEditsDirs(Configuration conf,
                                            String defaultName) {
        Collection<String> dirNames = conf.getTrimmedStringCollection(
                DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY);
        if (dirNames.size() == 0 && defaultName != null) {
            dirNames.add(defaultName);
        }
        return Util.stringCollectionAsURIs(dirNames);
    }
    /**
     * Load image from a checkpoint directory and save it into the current one.
     * @param target the NameSystem to import into
     * @throws IOException
     */
    void doImportCheckpoint(FileNamesystem target) throws IOException {
        Collection<URI> checkpointDirs =
                FileSystenImage.getCheckpointDirs(conf, null);
        List<URI> checkpointEditsDirs =
                FileSystenImage.getCheckpointEditsDirs(conf, null);

        if (checkpointDirs == null || checkpointDirs.isEmpty()) {
            throw new IOException("Cannot import image from a checkpoint. "
                    + "\"dfs.namenode.checkpoint.dir\" is not set." );
        }

        if (checkpointEditsDirs == null || checkpointEditsDirs.isEmpty()) {
            throw new IOException("Cannot import image from a checkpoint. "
                    + "\"dfs.namenode.checkpoint.dir\" is not set." );
        }

        FileSystenImage realImage = target.getFSImage();
        FileSystenImage ckptImage = new FileSystenImage(conf,
                checkpointDirs, checkpointEditsDirs);
        target.dir.fsImage = ckptImage;
        // load from the checkpoint dirs
        try {
            ckptImage.recoverTransitionRead(HdfsServerConstants.StartupOption.REGULAR, target, null);
        } finally {
            ckptImage.close();
        }
        // return back the real image
        realImage.getStorage().setStorageInfo(ckptImage.getStorage());
        realImage.getEditLog().setNextTxId(ckptImage.getEditLog().getLastWrittenTxId()+1);
        realImage.initEditLog();

        target.dir.fsImage = realImage;
        realImage.getStorage().setBlockPoolID(ckptImage.getBlockPoolID());

        // and save it but keep the same checkpointTime
        saveNamespace(target);
        getStorage().writeAll();
    }

    public int getNamespaceID() {
        return storage.getNamespaceID();
    }
    public void initEditLog() {
        Preconditions.checkState(getNamespaceID() != 0,
                "Must know namespace ID before initting edit log");
        String nameserviceId = DFSUtil.getNamenodeNameServiceId(conf);
        if (!HAUtil.isHAEnabled(conf, nameserviceId)) {
            editLog.initJournalsForWrite();
            editLog.recoverUnclosedStreams();
        } else {
            editLog.initSharedJournalsForRead();
        }
    }
    boolean recoverTransitionRead(HdfsServerConstants.StartupOption startOpt, FileNamesystem target,
                                  MetaRecoveryContext recovery) throws IOException {
        // 3. Do transitions
        switch(startOpt) {
            case UPGRADE:
               // doUpgrade(target);
                return false; // upgrade saved image already
            case IMPORT:
                doImportCheckpoint(target);
                return false; // import checkpoint saved image already
            case ROLLBACK:
                //doRollback();
                break;
            case REGULAR:
            default:
                // just load the image
        }
        FSImageStorageInspector inspector = storage.readAndInspectDirs();
        FSImageStorageInspector.FSImageFile imageFile = null;

        isUpgradeFinalized = inspector.isUpgradeFinalized();

        List<FSImageStorageInspector.FSImageFile> imageFiles = inspector.getLatestImages();

        StartupProgress prog = MyNameNode.getStartupProgress();
        prog.beginPhase(Phase.LOADING_FSIMAGE);
        File phaseFile = imageFiles.get(0).getFile();
        prog.setFile(Phase.LOADING_FSIMAGE, phaseFile.getAbsolutePath());
        prog.setSize(Phase.LOADING_FSIMAGE, phaseFile.length());
        boolean needToSave = inspector.needToSave();
        Map<Storage.StorageDirectory, Storage.StorageState> dataDirStates =
                new HashMap<Storage.StorageDirectory, Storage.StorageState>();
        boolean isFormatted = recoverStorageDirs(startOpt, dataDirStates);
        if (!isFormatted && startOpt != HdfsServerConstants.StartupOption.ROLLBACK
                && startOpt != HdfsServerConstants.StartupOption.IMPORT) {
            throw new IOException("NameNode is not formatted.");
        }
        Iterable<EditLogInputStream> editStreams = null;
        editLog.initJournalsForWrite();
        editLog.recoverUnclosedStreams();
        int layoutVersion = storage.getLayoutVersion();

        storage.processStartupOptionsForUpgrade(startOpt, layoutVersion);


        if (LayoutVersion.supports(LayoutVersion.Feature.TXID_BASED_LAYOUT,
                getLayoutVersion())) {
            long toAtLeastTxId = editLog.isOpenForWrite() ? inspector.getMaxSeenTxId() : 0;


            editStreams = editLog.selectInputStreams(
                    imageFiles.get(0).getCheckpointTxId() + 1,
                    toAtLeastTxId, recovery, false);
        } else {
            editStreams = FileSystemImagePreTransactionalStorageInspector
                    .getEditLogStreams(storage);
        }
        int maxOpSize = conf.getInt(DFSConfigKeys.
                        DFS_NAMENODE_MAX_OP_SIZE_KEY,
                DFSConfigKeys.DFS_NAMENODE_MAX_OP_SIZE_DEFAULT);
        for (EditLogInputStream elis : editStreams) {
            elis.setMaxOpSize(maxOpSize);
        }
        for (EditLogInputStream l : editStreams) {
            LOG.debug("Planning to load edit log stream: " + l);
        }
        if (!editStreams.iterator().hasNext()) {
            LOG.info("No edit log streams selected.");
        }
        for (int i = 0; i < imageFiles.size(); i++) {
            try {
                imageFile = imageFiles.get(i);
                loadFSImageFile(target, recovery, imageFile);
                break;
            } catch (IOException ioe) {
                ioe.printStackTrace();
                LOG.error("Failed to load image from " + imageFile, ioe);
                target.clear();
                imageFile = null;
            }
        }
        prog.endPhase(Phase.LOADING_FSIMAGE);
        long txnsAdvanced = loadEdits(editStreams, target, recovery);

        needToSave |= needsResaveBasedOnStaleCheckpoint(imageFile.getFile(),
                txnsAdvanced);
        editLog.setNextTxId(lastAppliedTxId + 1);
        return needToSave;
    }
    private boolean recoverStorageDirs(HdfsServerConstants.StartupOption startOpt,
                                       Map<Storage.StorageDirectory, Storage.StorageState> dataDirStates) throws IOException {
        boolean isFormatted = false;
        for (Iterator<Storage.StorageDirectory> it =
             storage.dirIterator(); it.hasNext();) {
            Storage.StorageDirectory sd = it.next();
            Storage.StorageState curState;
            try {
                curState = sd.analyzeStorage(startOpt, storage);
                String nameserviceId = DFSUtil.getNamenodeNameServiceId(conf);
                if (curState != Storage.StorageState.NORMAL && HAUtil.isHAEnabled(conf, nameserviceId)) {
                    throw new IOException("Cannot start an HA namenode with name dirs " +
                            "that need recovery. Dir: " + sd + " state: " + curState);
                }
                // sd is locked but not opened
                switch(curState) {
                    case NON_EXISTENT:
                        // name-node fails if any of the configured storage dirs are missing
                        throw new InconsistentFSStateException(sd.getRoot(),
                                "storage directory does not exist or is not accessible.");
                    case NOT_FORMATTED:
                        break;
                    case NORMAL:
                        break;
                    default:  // recovery is possible
                        sd.doRecover(curState);
                }
                if (curState != Storage.StorageState.NOT_FORMATTED
                        && startOpt != HdfsServerConstants.StartupOption.ROLLBACK) {
                    // read and verify consistency with other directories
                    storage.readProperties(sd);
                    isFormatted = true;
                }
                if (startOpt == HdfsServerConstants.StartupOption.IMPORT && isFormatted)
                    // import of a checkpoint is allowed only into empty image directories
                    throw new IOException("Cannot import image from a checkpoint. "
                            + " NameNode already contains an image in " + sd.getRoot());
            } catch (IOException ioe) {
                sd.unlock();
                throw ioe;
            }
            dataDirStates.put(sd,curState);
        }
        return isFormatted;
    }
    void loadFSImageFile(FileNamesystem target, MetaRecoveryContext recovery,
                         FSImageStorageInspector.FSImageFile imageFile) throws IOException {
        LOG.debug("Planning to load image :\n" + imageFile);

        Storage.StorageDirectory sdForProperties = imageFile.sd;
        storage.readProperties(sdForProperties);
      /*  StartupProgress prog = MyNameNode.getStartupProgress();
        Iterable<EditLogInputStream> editStreams = null;
        FSImageStorageInspector inspector = storage.readAndInspectDirs();
        boolean needToSave = inspector.needToSave();

        List<FSImageStorageInspector.FSImageFile> imageFiles = inspector.getLatestImages();
        long toAtLeastTxId = editLog.isOpenForWrite() ? inspector.getMaxSeenTxId() : 0;

        editStreams = editLog.selectInputStreams(
                imageFiles.get(0).getCheckpointTxId() + 1,
                toAtLeastTxId, recovery, false);

        // For txid-based layout, we should have a .md5 file
        // next to the image file
        MD5Hash expectedMD5 = MD5FileUtils.readStoredMd5ForFile(imageFile.getFile());*/
        FileImageFormat.Loader loader = new FileImageFormat.Loader(
                conf, target);
        loader.load(imageFile.getFile());
        target.setBlockPoolId(this.getBlockPoolID());
        long txId = loader.getLoadedImageTxId();
        lastAppliedTxId = txId;
        storage.setMostRecentCheckpointInfo(txId, imageFile.getFile().lastModified());
    }

    public long loadEdits(Iterable<EditLogInputStream> editStreams,
                          FileNamesystem target, MetaRecoveryContext recovery) throws IOException {
        StartupProgress prog = MyNameNode.getStartupProgress();
        prog.beginPhase(Phase.LOADING_EDITS);
        long prevLastAppliedTxId = lastAppliedTxId;
        try {
            FileSystemEditLogLoader loader = new FileSystemEditLogLoader(target, lastAppliedTxId);

            // Load latest edits
            for (EditLogInputStream editIn : editStreams) {
                LOG.info("Reading " + editIn + " expecting start txid #" +
                        (lastAppliedTxId + 1));
                try {
                    loader.loadFSEdits(editIn, lastAppliedTxId + 1, recovery);
                } finally {
                    // Update lastAppliedTxId even in case of error, since some ops may
                    // have been successfully applied before the error.
                    lastAppliedTxId = loader.getLastAppliedTxId();
                }
                // If we are in recovery mode, we may have skipped over some txids.
                if (editIn.getLastTxId() != HdfsConstants.INVALID_TXID) {
                    lastAppliedTxId = editIn.getLastTxId();
                }
            }
        } finally {
            FileSystemEditLog.closeAllStreams(editStreams);
            // update the counts
            updateCountForQuota(target.dir.rootDir);
        }
        prog.endPhase(Phase.LOADING_EDITS);
        return lastAppliedTxId - prevLastAppliedTxId;
    }

    public int getLayoutVersion() {
        return storage.getLayoutVersion();
    }

    @Override
   synchronized public void close() throws IOException {
        if (editLog != null) { // 2NN doesn't have any edit log
            getEditLog().close();
        }
        storage.close();
    }

    public String getBlockPoolID() {
        return storage.getBlockPoolID();
    }

    public FileSystemEditLog getEditLog() {
        return editLog;
    }

    static void updateCountForQuota(FileINodeDirectoryWithQuota root) {
        updateCountForQuotaRecursively(root, Quota.Counts.newInstance());
    }

    private static void updateCountForQuotaRecursively(FileINodeDirectory dir,
                                                       Quota.Counts counts) {
        final long parentNamespace = counts.get(Quota.NAMESPACE);
        final long parentDiskspace = counts.get(Quota.DISKSPACE);

        dir.computeQuotaUsage4CurrentDirectory(counts);

        for (INode child : dir.getChildrenList(null)) {
            if (child.isDirectory()) {
                updateCountForQuotaRecursively(child.asFileDirectory(), counts);
            } else {
                // file or symlink: count here to reduce recursive calls.
                child.computeQuotaUsage(counts, false);
            }
        }

        if (dir.isQuotaSet()) {
            // check if quota is violated. It indicates a software bug.
            final long namespace = counts.get(Quota.NAMESPACE) - parentNamespace;
            if (Quota.isViolated(dir.getNsQuota(), namespace)) {
                LOG.error("BUG: Namespace quota violation in image for "
                        + dir.getFullPathName()
                        + " quota = " + dir.getNsQuota() + " < consumed = " + namespace);
            }

            final long diskspace = counts.get(Quota.DISKSPACE) - parentDiskspace;
            if (Quota.isViolated(dir.getDsQuota(), diskspace)) {
                LOG.error("BUG: Diskspace quota violation in image for "
                        + dir.getFullPathName()
                        + " quota = " + dir.getDsQuota() + " < consumed = " + diskspace);
            }

            ((FileINodeDirectoryWithQuota) dir).setSpaceConsumed(namespace, diskspace);
        }
    }

    private boolean needsResaveBasedOnStaleCheckpoint(
            File imageFile, long numEditsLoaded) {
        final long checkpointPeriod = conf.getLong(
                DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_PERIOD_KEY,
                DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_PERIOD_DEFAULT);
        final long checkpointTxnCount = conf.getLong(
                DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_TXNS_KEY,
                DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_TXNS_DEFAULT);
        long checkpointAge = Time.now() - imageFile.lastModified();

        return (checkpointAge > checkpointPeriod * 1000) ||
                (numEditsLoaded > checkpointTxnCount);
    }

    public synchronized void saveNamespace(FileNamesystem source,
                                           Canceler canceler) throws IOException {
        assert editLog != null : "editLog must be initialized";
        storage.attemptRestoreRemovedStorage();

        boolean editLogWasOpen = editLog.isSegmentOpen();

        long imageTxId = getLastAppliedOrWrittenTxId();
        try {
            saveFSImageInAllDirs(source, imageTxId, canceler);
            storage.writeAll();
        } finally {
            if (editLogWasOpen) {
                editLog.startLogSegment(imageTxId + 1, true);
                // Take this opportunity to note the current transaction.
                // Even if the namespace save was cancelled, this marker
                // is only used to determine what transaction ID is required
                // for startup. So, it doesn't hurt to update it unnecessarily.
                storage.writeTransactionIdFileToStorage(imageTxId + 1);
            }
        }
    }

    public long getLastAppliedOrWrittenTxId() {
        return Math.max(lastAppliedTxId,
                editLog != null ? editLog.getLastWrittenTxId() : 0);
    }

    protected synchronized void saveFSImageInAllDirs(FileNamesystem source, long txid,
                                                     Canceler canceler)
            throws IOException {
        StartupProgress prog = NameNode.getStartupProgress();
        prog.beginPhase(Phase.SAVING_CHECKPOINT);
        if (storage.getNumStorageDirs(NameNodeStorage.NameNodeDirType.IMAGE) == 0) {
            throw new IOException("No image directories available!");
        }
        if (canceler == null) {
            canceler = new Canceler();
        }
        SaveNamespaceContext ctx = new SaveNamespaceContext(
                source, txid, canceler);

        try {
            List<Thread> saveThreads = new ArrayList<Thread>();
            // save images into current
            for (Iterator<Storage.StorageDirectory> it
                 = storage.dirIterator(NameNodeStorage.NameNodeDirType.IMAGE); it.hasNext(); ) {
                Storage.StorageDirectory sd = it.next();
                FSImageSaver saver = new FSImageSaver(ctx, sd);
                Thread saveThread = new Thread(saver, saver.toString());
                saveThreads.add(saveThread);
                saveThread.start();
            }
            waitForThreads(saveThreads);
            saveThreads.clear();
            storage.reportErrorsOnDirectories(ctx.getErrorSDs());

            if (storage.getNumStorageDirs(NameNodeStorage.NameNodeDirType.IMAGE) == 0) {
                throw new IOException(
                        "Failed to save in any storage directories while saving namespace.");
            }


            renameCheckpoint(txid);

            // Since we now have a new checkpoint, we can clean up some
            // old edit logs and checkpoints.
            purgeOldStorage();
        } finally {
            // Notify any threads waiting on the checkpoint to be canceled
            // that it is complete.
            ctx.markComplete();
            ctx = null;
        }
        prog.endPhase(Phase.SAVING_CHECKPOINT);
    }

    private class FSImageSaver implements Runnable {
        private final SaveNamespaceContext context;
        private Storage.StorageDirectory sd;

        public FSImageSaver(SaveNamespaceContext context, Storage.StorageDirectory sd) {
            this.context = context;
            this.sd = sd;
        }

        @Override
        public void run() {
            try {
                saveFSImage(context, sd);
            } catch (SaveNamespaceCancelledException snce) {
                LOG.info("Cancelled image saving for " + sd.getRoot() +
                        ": " + snce.getMessage());
                // don't report an error on the storage dir!
            } catch (Throwable t) {
                LOG.error("Unable to save image for " + sd.getRoot(), t);
                context.reportErrorOnStorageDirectory(sd);
            }
        }

        @Override
        public String toString() {
            return "FSImageSaver for " + sd.getRoot() +
                    " of type " + sd.getStorageDirType();
        }
    }

    private void waitForThreads(List<Thread> threads) {
        for (Thread thread : threads) {
            while (thread.isAlive()) {
                try {
                    thread.join();
                } catch (InterruptedException iex) {
                    LOG.error("Caught interrupted exception while waiting for thread " +
                            thread.getName() + " to finish. Retrying join");
                }
            }
        }
    }

    private void renameCheckpoint(long txid) throws IOException {
        ArrayList<Storage.StorageDirectory> al = null;

        for (Storage.StorageDirectory sd : storage.dirIterable(NameNodeStorage.NameNodeDirType.IMAGE)) {
            try {
                renameCheckpointInDir(sd, txid);
            } catch (IOException ioe) {
                LOG.warn("Unable to rename checkpoint in " + sd, ioe);
                if (al == null) {
                    al = Lists.newArrayList();
                }
                al.add(sd);
            }
        }
        if (al != null) storage.reportErrorsOnDirectories(al);
    }

    private void renameCheckpointInDir(Storage.StorageDirectory sd, long txid)
            throws IOException {
        File ckpt = NameNodeStorage.getStorageFile(sd, NameNodeStorage.NameNodeFile.IMAGE_NEW, txid);
        File curFile = NameNodeStorage.getStorageFile(sd, NameNodeStorage.NameNodeFile.IMAGE, txid);
        // renameTo fails on Windows if the destination file
        // already exists.
        if (LOG.isDebugEnabled()) {
            LOG.debug("renaming  " + ckpt.getAbsolutePath()
                    + " to " + curFile.getAbsolutePath());
        }
        if (!ckpt.renameTo(curFile)) {
            if (!curFile.delete() || !ckpt.renameTo(curFile)) {
                throw new IOException("renaming  " + ckpt.getAbsolutePath() + " to " +
                        curFile.getAbsolutePath() + " FAILED");
            }
        }
    }

    public void purgeOldStorage() {
        try {
            archivalManager.purgeOldStorage();
        } catch (Exception e) {
            LOG.warn("Unable to purge old storage", e);
        }
    }

    void saveFSImage(SaveNamespaceContext context, Storage.StorageDirectory sd)
            throws IOException {
        long txid = context.getTxId();
        File newFile = NameNodeStorage.getStorageFile(sd, NameNodeStorage.NameNodeFile.IMAGE_NEW, txid);
        File dstFile = NameNodeStorage.getStorageFile(sd, NameNodeStorage.NameNodeFile.IMAGE, txid);

        FileImageFormat.Saver saver = new FileImageFormat.Saver(context);
        FileSystemImageCompression compression = FileSystemImageCompression.createCompression(conf);
        saver.save(newFile, compression);

        MD5FileUtils.saveMD5File(dstFile, saver.getSavedDigest());
        storage.setMostRecentCheckpointInfo(txid, Time.now());
    }

    void openEditLogForWrite() throws IOException {
        assert editLog != null : "editLog must be initialized";
        editLog.openForWrite();
        storage.writeTransactionIdFileToStorage(editLog.getCurSegmentTxId());
    }

    ;
    public synchronized long getMostRecentCheckpointTxId() {
        return storage.getMostRecentCheckpointTxId();
    }

    public NameNodeStorage getStorage() {
        return storage;
    }
    CheckpointSignature rollEditLog() throws IOException {
        getEditLog().rollEditLog();
        // Record this log segment ID in all of the storage directories, so
        // we won't miss this log segment on a restart if the edits directories
        // go missing.
        storage.writeTransactionIdFileToStorage(getEditLog().getCurSegmentTxId());
        return new CheckpointSignature(this);
    }


    /**
     * Start checkpoint.
     * <p>
     * If backup storage contains image that is newer than or incompatible with
     * what the active name-node has, then the backup node should shutdown.<br>
     * If the backup image is older than the active one then it should
     * be discarded and downloaded from the active node.<br>
     * If the images are the same then the backup image will be used as current.
     *
     * @param bnReg the backup node registration.
     * @param nnReg this (active) name-node registration.
     * @return {@link NamenodeCommand} if backup node should shutdown or
     * {@link CheckpointCommand} prescribing what backup node should
     *         do with its image.
     * @throws IOException
     */
    NamenodeCommand startCheckpoint(NamenodeRegistration bnReg, // backup node
                                    NamenodeRegistration nnReg) // active name-node
            throws IOException {
        LOG.info("Start checkpoint at txid " + getEditLog().getLastWrittenTxId());
        String msg = null;
        // Verify that checkpoint is allowed
        if(bnReg.getNamespaceID() != storage.getNamespaceID())
            msg = "Name node " + bnReg.getAddress()
                    + " has incompatible namespace id: " + bnReg.getNamespaceID()
                    + " expected: " + storage.getNamespaceID();
        else if(bnReg.isRole(HdfsServerConstants.NamenodeRole.NAMENODE))
            msg = "Name node " + bnReg.getAddress()
                    + " role " + bnReg.getRole() + ": checkpoint is not allowed.";
        else if(bnReg.getLayoutVersion() < storage.getLayoutVersion()
                || (bnReg.getLayoutVersion() == storage.getLayoutVersion()
                && bnReg.getCTime() > storage.getCTime()))
            // remote node has newer image age
            msg = "Name node " + bnReg.getAddress()
                    + " has newer image layout version: LV = " +bnReg.getLayoutVersion()
                    + " cTime = " + bnReg.getCTime()
                    + ". Current version: LV = " + storage.getLayoutVersion()
                    + " cTime = " + storage.getCTime();
        if(msg != null) {
            LOG.error(msg);
            return new NamenodeCommand(NamenodeProtocol.ACT_SHUTDOWN);
        }
        boolean needToReturnImg = true;
        if(storage.getNumStorageDirs(NameNodeStorage.NameNodeDirType.IMAGE) == 0)
            // do not return image if there are no image directories
            needToReturnImg = false;
        CheckpointSignature sig = rollEditLog();
        return new CheckpointCommand(sig, needToReturnImg);
    }

    void endCheckpoint(CheckpointSignature sig) throws IOException {
        LOG.info("End checkpoint at txid " + getEditLog().getLastWrittenTxId());
        sig.validateStorageInfo(this);
    }
    public synchronized void saveNamespace(FileNamesystem source)
            throws IOException {
        saveNamespace(source, null);
    }
    public String getClusterID() {
        return storage.getClusterID();
    }
    void finalizeUpgrade() throws IOException {
        for (Iterator<Storage.StorageDirectory> it = storage.dirIterator(); it.hasNext();) {
            Storage.StorageDirectory sd = it.next();
            doFinalize(sd);
        }
    }
    private void doFinalize(Storage.StorageDirectory sd) throws IOException {
        File prevDir = sd.getPreviousDir();
        if (!prevDir.exists()) { // already discarded
            LOG.info("Directory " + prevDir + " does not exist.");
            LOG.info("Finalize upgrade for " + sd.getRoot()+ " is not required.");
            return;
        }
        LOG.info("Finalizing upgrade for storage directory "
                + sd.getRoot() + "."
                + (storage.getLayoutVersion()==0 ? "" :
                "\n   cur LV = " + storage.getLayoutVersion()
                        + "; cur CTime = " + storage.getCTime()));
        assert sd.getCurrentDir().exists() : "Current directory must exist.";
        final File tmpDir = sd.getFinalizedTmp();
        // rename previous to tmp and remove
        NameNodeStorage.rename(prevDir, tmpDir);
        NameNodeStorage.deleteDir(tmpDir);
        isUpgradeFinalized = true;
        LOG.info("Finalize upgrade for " + sd.getRoot()+ " is complete.");
    }
}
