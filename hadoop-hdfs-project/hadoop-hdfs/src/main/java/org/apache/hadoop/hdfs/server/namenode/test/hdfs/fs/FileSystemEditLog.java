package org.apache.hadoop.hdfs.server.namenode.test.hdfs.fs;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.namenode.*;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.hdfs.server.namenode.test.hdfs.NameNodeTest;
import org.apache.hadoop.hdfs.server.namenode.test.hdfs.journal.FileJournalSet;
import org.apache.hadoop.hdfs.server.namenode.test.hdfs.store.NameNodeStorage;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.Server;


import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static org.apache.hadoop.util.ExitUtil.terminate;
import static org.apache.hadoop.util.Time.now;

public class FileSystemEditLog implements LogsPurgeable {
    static final Log LOG = LogFactory.getLog(FileSystemEditLog.class);
    private EditLogOutputStream editLogStream = null;
    private volatile boolean isAutoSyncScheduled = false;
    private long numTransactions;        // number of transactions
    private long synctxid = 0;
    private long numTransactionsBatchedInSync;

    ThreadLocal<TransactionId> myTransactionId = new ThreadLocal<TransactionId>() {
        @Override
        protected synchronized TransactionId initialValue() {
            return new TransactionId(Long.MAX_VALUE);
        }
    };

/*    对于非HA机制的情况下，EditLog应该开始于UNINITIALIZED或者CLOSED状态
     （因为在构造对象时，EditLog的成员变量state默认为State.UNINITIALIZED）。
     初始化完成之后进入BETWEEN_LOG_SEGMENTS状态，表示前一个segment已经关闭，新的还没开始，已经做好准备了。
     在后面打开服务的时候会变成IN_SEGMENT状态，表示可以写EditLog日志了。

    对于HA机制的情况下，EditLog同样应该开始于UNINITIALIZED或者CLOSED状态，
     但是在完成初始化后并不进入BETWEEN_LOG_SEGMENTS状态，而是进入OPEN_FOR_READING状态
     （因为目前NameNode启动的时候都是以Standby模式启动的，然后通过dfsHAAdmin发送命令把其中一个Standby的NameNode转化成Active的）*/
    private enum State {
        UNINITIALIZED,
        BETWEEN_LOG_SEGMENTS,
        IN_SEGMENT,
        OPEN_FOR_READING,
        CLOSED;
    }

    private long txid = 0;
    private long totalTimeTransactions;
    private volatile boolean isSyncRunning;
    Configuration conf;
    NameNodeStorage storage;
    long lastPrintTime;
    NameNodeMetrics metrics;
    List<URI> editsDirs;
    private List<URI> sharedEditsDirs;
    private FileJournalSet journalSet = null;
    private State state = State.UNINITIALIZED;
    long curSegmentTxId;
    private ThreadLocal<FSEditLogOp.OpInstanceCache> cache =
            new ThreadLocal<FSEditLogOp.OpInstanceCache>() {
                @Override
                protected FSEditLogOp.OpInstanceCache initialValue() {
                    return new FSEditLogOp.OpInstanceCache();
                }
            };

    public FileSystemEditLog(Configuration conf, NameNodeStorage storage, List<URI> editsDirs) {
        isSyncRunning = false;
        this.conf = conf;
        this.storage = storage;
        //  metrics = NameNodeTest.getNameNodeMetrics();
        lastPrintTime = now();

        // If this list is empty, an error will be thrown on first use
        // of the editlog, as no journals will exist
        this.editsDirs = Lists.newArrayList(editsDirs);

        this.sharedEditsDirs = FSNamesystem.getSharedEditsDirs(conf);
    }

    @Override
    public void purgeLogsOlderThan(long minTxIdToKeep) throws IOException {

    }

    @Override
    public void selectInputStreams(Collection<EditLogInputStream> streams, long fromTxId, boolean inProgressOk, boolean forReading) throws IOException {

        journalSet.selectInputStreams(streams, fromTxId, inProgressOk, forReading);

    }

    public Collection<EditLogInputStream> selectInputStreams(
            long fromTxId, long toAtLeastTxId, MetaRecoveryContext recovery,
            boolean inProgressOk) throws IOException {
        return selectInputStreams(fromTxId, toAtLeastTxId, recovery, inProgressOk,
                true);
    }

    public synchronized Collection<EditLogInputStream> selectInputStreams(
            long fromTxId, long toAtLeastTxId, MetaRecoveryContext recovery,
            boolean inProgressOk, boolean forReading) throws IOException {
        List<EditLogInputStream> streams = new ArrayList<EditLogInputStream>();
        selectInputStreams(streams, fromTxId, inProgressOk, forReading);

        try {
            checkForGaps(streams, fromTxId, toAtLeastTxId, inProgressOk);
        } catch (IOException e) {
            if (recovery != null) {
                // If recovery mode is enabled, continue loading even if we know we
                // can't load up to toAtLeastTxId.
                LOG.error(e);
            } else {
                closeAllStreams(streams);
                throw e;
            }
        }
        return streams;
    }

    static void closeAllStreams(Iterable<EditLogInputStream> streams) {
        for (EditLogInputStream s : streams) {
            IOUtils.closeStream(s);
        }
    }

    public synchronized void initJournalsForWrite() {
        Preconditions.checkState(state == State.UNINITIALIZED ||
                state == State.CLOSED, "Unexpected state: %s", state);

        initJournals(this.editsDirs);
        state = State.BETWEEN_LOG_SEGMENTS;
    }

    private synchronized void initJournals(List<URI> dirs) {
        int minimumRedundantJournals = conf.getInt(
                DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_MINIMUM_KEY,
                DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_MINIMUM_DEFAULT);

        journalSet = new FileJournalSet(minimumRedundantJournals);

        for (URI u : dirs) {
            boolean required = FSNamesystem.getRequiredNamespaceEditsDirs(conf)
                    .contains(u);
            if (u.getScheme().equals(NameNodeStorage.LOCAL_URI_SCHEME)) {
                Storage.StorageDirectory sd = storage.getStorageDirectory(u);
                if (sd != null) {
                    journalSet.add(new FileJournalManager(conf, sd, storage), required);
                }
            } else {
                journalSet.add(createJournal(u), required);
            }
        }

        if (journalSet.isEmpty()) {
            LOG.error("No edits directories configured!");
        }
    }

    static Class<? extends JournalManager> getJournalClass(Configuration conf,
                                                           String uriScheme) {
        String key
                = DFSConfigKeys.DFS_NAMENODE_EDITS_PLUGIN_PREFIX + "." + uriScheme;
        Class<? extends JournalManager> clazz = null;
        try {
            clazz = conf.getClass(key, null, JournalManager.class);
        } catch (RuntimeException re) {
            throw new IllegalArgumentException(
                    "Invalid class specified for " + uriScheme, re);
        }

        if (clazz == null) {
            LOG.warn("No class configured for " + uriScheme
                    + ", " + key + " is empty");
            throw new IllegalArgumentException(
                    "No class configured for " + uriScheme);
        }
        return clazz;
    }

    private JournalManager createJournal(URI uri) {
        Class<? extends JournalManager> clazz
                = getJournalClass(conf, uri.getScheme());

        try {
            Constructor<? extends JournalManager> cons
                    = clazz.getConstructor(Configuration.class, URI.class,
                    NamespaceInfo.class);
            return cons.newInstance(conf, uri, storage.getNamespaceInfo());
        } catch (Exception e) {
            throw new IllegalArgumentException("Unable to construct journal, "
                    + uri, e);
        }
    }

    synchronized void recoverUnclosedStreams() {
        Preconditions.checkState(
                state == State.BETWEEN_LOG_SEGMENTS,
                "May not recover segments - wrong state: %s", state);
        try {
            journalSet.recoverUnfinalizedSegments();
        } catch (IOException ex) {
            // All journals have failed, it is handled in logSync.
            // TODO: are we sure this is OK?
        }
    }

    private void checkForGaps(List<EditLogInputStream> streams, long fromTxId,
                              long toAtLeastTxId, boolean inProgressOk) throws IOException {
        Iterator<EditLogInputStream> iter = streams.iterator();
        long txId = fromTxId;
        while (true) {
            if (txId > toAtLeastTxId) return;
            if (!iter.hasNext()) break;
            EditLogInputStream elis = iter.next();
            if (elis.getFirstTxId() > txId) break;
            long next = elis.getLastTxId();
            if (next == HdfsConstants.INVALID_TXID) {
                if (!inProgressOk) {
                    throw new RuntimeException("inProgressOk = false, but " +
                            "selectInputStreams returned an in-progress edit " +
                            "log input stream (" + elis + ")");
                }
                // We don't know where the in-progress stream ends.
                // It could certainly go all the way up to toAtLeastTxId.
                return;
            }
            txId = next + 1;
        }
        throw new IOException(String.format("Gap in transactions. Expected to "
                + "be able to read up until at least txid %d but unable to find any "
                + "edit logs containing txid %d", toAtLeastTxId, txId));
    }

    synchronized boolean isOpenForWrite() {
        return state == State.IN_SEGMENT ||
                state == State.BETWEEN_LOG_SEGMENTS;
    }

    public void logOpenFile(String path, INodeFileUnderConstruction newNode,
                            boolean toLogRpcIds) {
        FSEditLogOp.AddOp op = FSEditLogOp.AddOp.getInstance(cache.get())
                .setInodeId(newNode.getId())
                .setPath(path)
                .setReplication(newNode.getFileReplication())
                .setModificationTime(newNode.getModificationTime())
                .setAccessTime(newNode.getAccessTime())
                .setBlockSize(newNode.getPreferredBlockSize())
                .setBlocks(newNode.getBlocks())
                .setPermissionStatus(newNode.getPermissionStatus())
                .setClientName(newNode.getClientName())
                .setClientMachine(newNode.getClientMachine());
        logRpcIds(op, toLogRpcIds);
        logEdit(op);
    }

    private void logRpcIds(FSEditLogOp op, boolean toLogRpcIds) {
        if (toLogRpcIds) {
            op.setRpcClientId(Server.getClientId());
            op.setRpcCallId(Server.getCallId());
        }
    }

    void logEdit(final FSEditLogOp op) {
        synchronized (this) {
            assert isOpenForWrite() :
                    "bad state: " + state;

            // wait if an automatic sync is scheduled
            waitIfAutoSyncScheduled();

            long start = beginTransaction();
            op.setTransactionId(txid);

            try {
                editLogStream.write(op);
            } catch (IOException ex) {
                // All journals failed, it is handled in logSync.
            }

            endTransaction(start);

            // check if it is time to schedule an automatic sync
            if (!shouldForceSync()) {
                return;
            }
            isAutoSyncScheduled = true;
        }

        // sync buffered edit log entries to persistent store
        logSync();
    }

    synchronized void waitIfAutoSyncScheduled() {
        try {
            while (isAutoSyncScheduled) {
                this.wait(1000);
            }
        } catch (InterruptedException e) {
        }
    }

    private long beginTransaction() {
        assert Thread.holdsLock(this);
        // get a new transactionId
        txid++;

        //
        // record the transactionId when new data was written to the edits log
        //
        TransactionId id = myTransactionId.get();
        id.txid = txid;
        return now();
    }

    private void endTransaction(long start) {
        assert Thread.holdsLock(this);

        // update statistics
        long end = now();
        numTransactions++;
        totalTimeTransactions += (end - start);
        if (metrics != null) // Metrics is non-null only when used inside name node
            metrics.addTransaction(end - start);
    }

    private static class TransactionId {
        public long txid;

        TransactionId(long value) {
            this.txid = value;
        }
    }

    private boolean shouldForceSync() {
        return editLogStream.shouldForceSync();
    }

    public void logSync() {
        long syncStart = 0;

        // Fetch the transactionId of this thread.
        long mytxid = myTransactionId.get().txid;

        boolean sync = false;
        try {
            EditLogOutputStream logStream = null;
            synchronized (this) {
                try {
                    printStatistics(false);

                    // if somebody is already syncing, then wait
                    while (mytxid > synctxid && isSyncRunning) {
                        try {
                            wait(1000);
                        } catch (InterruptedException ie) {
                        }
                    }

                    //
                    // If this transaction was already flushed, then nothing to do
                    //
                    if (mytxid <= synctxid) {
                        numTransactionsBatchedInSync++;
                        if (metrics != null) {
                            // Metrics is non-null only when used inside name node
                            metrics.incrTransactionsBatchedInSync();
                        }
                        return;
                    }

                    // now, this thread will do the sync
                    syncStart = txid;
                    isSyncRunning = true;
                    sync = true;

                    // swap buffers
                    try {
                        if (journalSet.isEmpty()) {
                            throw new IOException("No journals available to flush");
                        }
                        editLogStream.setReadyToFlush();
                    } catch (IOException e) {
                        final String msg =
                                "Could not sync enough journals to persistent storage " +
                                        "due to " + e.getMessage() + ". " +
                                        "Unsynced transactions: " + (txid - synctxid);
                        LOG.fatal(msg, new Exception());
                        IOUtils.cleanup(LOG, journalSet);
                        terminate(1, msg);
                    }
                } finally {
                    // Prevent RuntimeException from blocking other log edit write
                    doneWithAutoSyncScheduling();
                }
                //editLogStream may become null,
                //so store a local variable for flush.
                logStream = editLogStream;
            }

            // do the sync
            long start = now();
            try {
                if (logStream != null) {
                    logStream.flush();
                }
            } catch (IOException ex) {
                synchronized (this) {
                    final String msg =
                            "Could not sync enough journals to persistent storage. "
                                    + "Unsynced transactions: " + (txid - synctxid);
                    LOG.fatal(msg, new Exception());
                    IOUtils.cleanup(LOG, journalSet);
                    terminate(1, msg);
                }
            }
            long elapsed = now() - start;

            if (metrics != null) { // Metrics non-null only when used inside name node
                metrics.addSync(elapsed);
            }

        } finally {
            // Prevent RuntimeException from blocking other log edit sync
            synchronized (this) {
                if (sync) {
                    synctxid = syncStart;
                    isSyncRunning = false;
                }
                this.notifyAll();
            }
        }
    }

    private void printStatistics(boolean force) {
        long now = now();
        if (lastPrintTime + 60000 > now && !force) {
            return;
        }
        lastPrintTime = now;
        StringBuilder buf = new StringBuilder();
        buf.append("Number of transactions: ");
        buf.append(numTransactions);
        buf.append(" Total time for transactions(ms): ");
        buf.append(totalTimeTransactions);
        buf.append(" Number of transactions batched in Syncs: ");
        buf.append(numTransactionsBatchedInSync);
        buf.append(" Number of syncs: ");
        buf.append(editLogStream.getNumSync());
        buf.append(" SyncTimes(ms): ");
        buf.append(journalSet.getSyncTimes());
        LOG.info(buf);
    }

    synchronized void doneWithAutoSyncScheduling() {
        if (isAutoSyncScheduled) {
            isAutoSyncScheduled = false;
            notifyAll();
        }
    }

    public synchronized void setNextTxId(long nextTxId) {
        Preconditions.checkArgument(synctxid <= txid &&
                        nextTxId >= txid,
                "May not decrease txid." +
                        " synctxid=%s txid=%s nextTxId=%s",
                synctxid, txid, nextTxId);

        txid = nextTxId - 1;
    }

    public synchronized boolean isSegmentOpen() {
        return state == State.IN_SEGMENT;
    }

    public synchronized long getLastWrittenTxId() {
        return txid;
    }

    public synchronized void startLogSegment(final long segmentTxId,
                                             boolean writeHeaderTxn) throws IOException {
        LOG.info("Starting log segment at " + segmentTxId);
        Preconditions.checkArgument(segmentTxId > 0,
                "Bad txid: %s", segmentTxId);
        Preconditions.checkState(state == State.BETWEEN_LOG_SEGMENTS,
                "Bad state: %s", state);
        Preconditions.checkState(segmentTxId > curSegmentTxId,
                "Cannot start writing to log segment " + segmentTxId +
                        " when previous log segment started at " + curSegmentTxId);
        Preconditions.checkArgument(segmentTxId == txid + 1,
                "Cannot start log segment at txid %s when next expected " +
                        "txid is %s", segmentTxId, txid + 1);

        numTransactions = totalTimeTransactions = numTransactionsBatchedInSync = 0;

        // TODO no need to link this back to storage anymore!
        // See HDFS-2174.
        storage.attemptRestoreRemovedStorage();

        try {
            editLogStream = journalSet.startLogSegment(segmentTxId);
        } catch (IOException ex) {
            throw new IOException("Unable to start log segment " +
                    segmentTxId + ": too few journals successfully started.", ex);
        }

        curSegmentTxId = segmentTxId;
        state = State.IN_SEGMENT;

        if (writeHeaderTxn) {
            logEdit(FSEditLogOp.LogSegmentOp.getInstance(cache.get(),
                    FSEditLogOpCodes.OP_START_LOG_SEGMENT));
            logSync();
        }
    }

    synchronized void openForWrite() throws IOException {
        Preconditions.checkState(state == State.BETWEEN_LOG_SEGMENTS,
                "Bad state: %s", state);

        long segmentTxId = getLastWrittenTxId() + 1;
        // Safety check: we should never start a segment if there are
        // newer txids readable.
        List<EditLogInputStream> streams = new ArrayList<EditLogInputStream>();
        journalSet.selectInputStreams(streams, segmentTxId, true, true);
        if (!streams.isEmpty()) {
            String error = String.format("Cannot start writing at txid %s " +
                            "when there is a stream available for read: %s",
                    segmentTxId, streams.get(0));
            IOUtils.cleanup(LOG, streams.toArray(new EditLogInputStream[0]));
            throw new IllegalStateException(error);
        }

        startLogSegment(segmentTxId, true);
        assert state == State.IN_SEGMENT : "Bad state: " + state;
    }

    public synchronized long getCurSegmentTxId() {
        Preconditions.checkState(isSegmentOpen(),
                "Bad state: %s", state);
        return curSegmentTxId;
    }

    synchronized long rollEditLog() throws IOException {
        LOG.info("Rolling edit logs");
        endCurrentLogSegment(true);

        long nextTxId = getLastWrittenTxId() + 1;
        startLogSegment(nextTxId, true);

        assert curSegmentTxId == nextTxId;
        return nextTxId;
    }

    /**
     * Create (or find if already exists) an edit output stream, which
     * streams journal records (edits) to the specified backup node.<br>
     * <p>
     * The new BackupNode will start receiving edits the next time this
     * NameNode's logs roll.
     *
     * @param bnReg the backup node registration information.
     * @param nnReg this (active) name-node registration.
     * @throws IOException
     */
    synchronized void registerBackupNode(
            NamenodeRegistration bnReg, // backup node
            NamenodeRegistration nnReg) // active name-node
            throws IOException {
        if (bnReg.isRole(HdfsServerConstants.NamenodeRole.CHECKPOINT))
            return; // checkpoint node does not stream edits

        JournalManager jas = findBackupJournal(bnReg);
        if (jas != null) {
            // already registered
            LOG.info("Backup node " + bnReg + " re-registers");
            return;
        }

        LOG.info("Registering new backup node: " + bnReg);
        BackupJournalManager bjm = new BackupJournalManager(bnReg, nnReg);
        journalSet.add(bjm, true);
    }

    private synchronized BackupJournalManager findBackupJournal(
            NamenodeRegistration bnReg) {
        for (JournalManager bjm : journalSet.getJournalManagers()) {
            if ((bjm instanceof BackupJournalManager)
                    && ((BackupJournalManager) bjm).matchesRegistration(bnReg)) {
                return (BackupJournalManager) bjm;
            }
        }
        return null;
    }

    public synchronized RemoteEditLogManifest getEditLogManifest(long fromTxId)
            throws IOException {
        return journalSet.getEditLogManifest(fromTxId, true);
    }

    synchronized void endCurrentLogSegment(boolean writeEndTxn) {
        LOG.info("Ending log segment " + curSegmentTxId);
        Preconditions.checkState(isSegmentOpen(),
                "Bad state: %s", state);

        if (writeEndTxn) {
            logEdit(FSEditLogOp.LogSegmentOp.getInstance(cache.get(),
                    FSEditLogOpCodes.OP_END_LOG_SEGMENT));
            logSync();
        }

        printStatistics(true);

        final long lastTxId = getLastWrittenTxId();

        try {
            journalSet.finalizeLogSegment(curSegmentTxId, lastTxId);
            editLogStream = null;
        } catch (IOException e) {
            //All journals have failed, it will be handled in logSync.
        }

        state = State.BETWEEN_LOG_SEGMENTS;
    }

    void logTimes(String src, long mtime, long atime) {
        FSEditLogOp.TimesOp op = FSEditLogOp.TimesOp.getInstance(cache.get())
                .setPath(src)
                .setModificationTime(mtime)
                .setAccessTime(atime);
        logEdit(op);
    }

    public void logMkDir(String path, INode newNode) {
        FSEditLogOp.MkdirOp op = FSEditLogOp.MkdirOp.getInstance(cache.get())
                .setInodeId(newNode.getId())
                .setPath(path)
                .setTimestamp(newNode.getModificationTime())
                .setPermissionStatus(newNode.getPermissionStatus());
        logEdit(op);
    }

    void logDelete(String src, long timestamp, boolean toLogRpcIds) {
        FSEditLogOp.DeleteOp op = FSEditLogOp.DeleteOp.getInstance(cache.get())
                .setPath(src)
                .setTimestamp(timestamp);
        logRpcIds(op, toLogRpcIds);
        logEdit(op);
    }

    void logReassignLease(String leaseHolder, String src, String newHolder) {
        FSEditLogOp.ReassignLeaseOp op = FSEditLogOp.ReassignLeaseOp.getInstance(cache.get())
                .setLeaseHolder(leaseHolder)
                .setPath(src)
                .setNewHolder(newHolder);
        logEdit(op);
    }

    /**
     * Add close lease record to edit log.
     */
    public void logCloseFile(String path, INodeFile newNode) {
        FSEditLogOp.CloseOp op = FSEditLogOp.CloseOp.getInstance(cache.get())
                .setPath(path)
                .setReplication(newNode.getFileReplication())
                .setModificationTime(newNode.getModificationTime())
                .setAccessTime(newNode.getAccessTime())
                .setBlockSize(newNode.getPreferredBlockSize())
                .setBlocks(newNode.getBlocks())
                .setPermissionStatus(newNode.getPermissionStatus());

        logEdit(op);
    }

    void logGenerationStampV1(long genstamp) {
        FSEditLogOp.SetGenstampV1Op op = FSEditLogOp.SetGenstampV1Op.getInstance(cache.get())
                .setGenerationStamp(genstamp);
        logEdit(op);
    }

    void logGenerationStampV2(long genstamp) {
        FSEditLogOp.SetGenstampV2Op op = FSEditLogOp.SetGenstampV2Op.getInstance(cache.get())
                .setGenerationStamp(genstamp);
        logEdit(op);
    }


    /**
     * Add set replication record to edit log
     */
    void logSetReplication(String src, short replication) {
        FSEditLogOp.SetReplicationOp op = FSEditLogOp.SetReplicationOp.getInstance(cache.get())
                .setPath(src)
                .setReplication(replication);
        logEdit(op);
    }

    /**
     * Add set permissions record to edit log
     */
    void logSetPermissions(String src, FsPermission permissions) {
        FSEditLogOp.SetPermissionsOp op = FSEditLogOp.SetPermissionsOp.getInstance(cache.get())
                .setSource(src)
                .setPermissions(permissions);
        logEdit(op);
    }

    /**
     * Add set owner record to edit log
     */
    void logSetOwner(String src, String username, String groupname) {
        FSEditLogOp.SetOwnerOp op = FSEditLogOp.SetOwnerOp.getInstance(cache.get())
                .setSource(src)
                .setUser(username)
                .setGroup(groupname);
        logEdit(op);
    }

    public void logUpdateBlocks(String path, INodeFileUnderConstruction file,
                                boolean toLogRpcIds) {
        FSEditLogOp.UpdateBlocksOp op = FSEditLogOp.UpdateBlocksOp.getInstance(cache.get())
                .setPath(path)
                .setBlocks(file.getBlocks());
        logRpcIds(op, toLogRpcIds);
        logEdit(op);
    }

    /**
     * Record a newly allocated block ID in the edit log
     */
    void logAllocateBlockId(long blockId) {
        FSEditLogOp.AllocateBlockIdOp op = FSEditLogOp.AllocateBlockIdOp.getInstance(cache.get())
                .setBlockId(blockId);
        logEdit(op);
    }

    void logRename(String src, String dst, long timestamp, boolean toLogRpcIds) {
        FSEditLogOp.RenameOldOp op = FSEditLogOp.RenameOldOp.getInstance(cache.get())
                .setSource(src)
                .setDestination(dst)
                .setTimestamp(timestamp);
        logRpcIds(op, toLogRpcIds);
        logEdit(op);
    }

    void logConcat(String trg, String[] srcs, long timestamp, boolean toLogRpcIds) {
        FSEditLogOp.ConcatDeleteOp op = FSEditLogOp.ConcatDeleteOp.getInstance(cache.get())
                .setTarget(trg)
                .setSources(srcs)
                .setTimestamp(timestamp);
        logRpcIds(op, toLogRpcIds);
        logEdit(op);
    }

    void logRenameSnapshot(String path, String snapOldName, String snapNewName,
                           boolean toLogRpcIds) {
        FSEditLogOp.RenameSnapshotOp op = FSEditLogOp.RenameSnapshotOp.getInstance(cache.get())
                .setSnapshotRoot(path).setSnapshotOldName(snapOldName)
                .setSnapshotNewName(snapNewName);
        logRpcIds(op, toLogRpcIds);
        logEdit(op);
    }

    void logRename(String src, String dst, long timestamp, boolean toLogRpcIds,
                   Options.Rename... options) {
        FSEditLogOp.RenameOp op = FSEditLogOp.RenameOp.getInstance(cache.get())
                .setSource(src)
                .setDestination(dst)
                .setTimestamp(timestamp)
                .setOptions(options);
        logRpcIds(op, toLogRpcIds);
        logEdit(op);
    }

    void logSyncAll() {
        // Record the most recent transaction ID as our own id
        synchronized (this) {
            TransactionId id = myTransactionId.get();
            id.txid = txid;
        }
        // Then make sure we're synced up to this point
        logSync();
    }

    void logSetQuota(String src, long nsQuota, long dsQuota) {
        FSEditLogOp.SetQuotaOp op = FSEditLogOp.SetQuotaOp.getInstance(cache.get())
                .setSource(src)
                .setNSQuota(nsQuota)
                .setDSQuota(dsQuota);
        logEdit(op);
    }

    void logSymlink(String path, String value, long mtime, long atime,
                    INodeSymlink node, boolean toLogRpcIds) {
        FSEditLogOp.SymlinkOp op = FSEditLogOp.SymlinkOp.getInstance(cache.get())
                .setId(node.getId())
                .setPath(path)
                .setValue(value)
                .setModificationTime(mtime)
                .setAccessTime(atime)
                .setPermissionStatus(node.getPermissionStatus());
        logRpcIds(op, toLogRpcIds);
        logEdit(op);
    }

    void logGetDelegationToken(DelegationTokenIdentifier id,
                               long expiryTime) {
        FSEditLogOp.GetDelegationTokenOp op = FSEditLogOp.GetDelegationTokenOp.getInstance(cache.get())
                .setDelegationTokenIdentifier(id)
                .setExpiryTime(expiryTime);
        logEdit(op);
    }

    void logRenewDelegationToken(DelegationTokenIdentifier id,
                                 long expiryTime) {
        FSEditLogOp.RenewDelegationTokenOp op = FSEditLogOp.RenewDelegationTokenOp.getInstance(cache.get())
                .setDelegationTokenIdentifier(id)
                .setExpiryTime(expiryTime);
        logEdit(op);
    }

    void logCancelDelegationToken(DelegationTokenIdentifier id) {
        FSEditLogOp.CancelDelegationTokenOp op = FSEditLogOp.CancelDelegationTokenOp.getInstance(cache.get())
                .setDelegationTokenIdentifier(id);
        logEdit(op);
    }

    void logCreateSnapshot(String snapRoot, String snapName, boolean toLogRpcIds) {
        FSEditLogOp.CreateSnapshotOp op = FSEditLogOp.CreateSnapshotOp.getInstance(cache.get())
                .setSnapshotRoot(snapRoot).setSnapshotName(snapName);
        logRpcIds(op, toLogRpcIds);
        logEdit(op);
    }

    void logDeleteSnapshot(String snapRoot, String snapName, boolean toLogRpcIds) {
        FSEditLogOp.DeleteSnapshotOp op = FSEditLogOp.DeleteSnapshotOp.getInstance(cache.get())
                .setSnapshotRoot(snapRoot).setSnapshotName(snapName);
        logRpcIds(op, toLogRpcIds);
        logEdit(op);
    }

    void logAllowSnapshot(String path) {
        FSEditLogOp.AllowSnapshotOp op = FSEditLogOp.AllowSnapshotOp.getInstance(cache.get())
                .setSnapshotRoot(path);
        logEdit(op);
    }

    void logDisallowSnapshot(String path) {
        FSEditLogOp.DisallowSnapshotOp op = FSEditLogOp.DisallowSnapshotOp.getInstance(cache.get())
                .setSnapshotRoot(path);
        logEdit(op);
    }

    synchronized void releaseBackupStream(NamenodeRegistration registration)
            throws IOException {
        BackupJournalManager bjm = this.findBackupJournal(registration);
        if (bjm != null) {
            LOG.info("Removing backup journal " + bjm);
            journalSet.remove(bjm);
        }
    }
}
