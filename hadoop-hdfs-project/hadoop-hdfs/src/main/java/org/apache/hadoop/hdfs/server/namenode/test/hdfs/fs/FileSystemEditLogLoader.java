/*
package org.apache.hadoop.hdfs.server.namenode.test.hdfs.fs;*/
package org.apache.hadoop.hdfs.server.namenode.test.hdfs.fs;

import com.google.common.base.Joiner;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.namenode.*;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Phase;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Step;
import org.apache.hadoop.hdfs.server.namenode.test.hdfs.block.FileINodesInPath;
import org.apache.hadoop.hdfs.util.Holder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;

import static org.apache.hadoop.util.Time.now;

public class FileSystemEditLogLoader {
    static final Log LOG = LogFactory.getLog(FileSystemEditLogLoader.class.getName());
    static long REPLAY_TRANSACTION_LOG_INTERVAL = 1000; // 1sec
    private final FileNamesystem fsNamesys;
    private long lastAppliedTxId;
    public FileSystemEditLogLoader(FileNamesystem fsNamesys, long lastAppliedTxId) {
        this.fsNamesys = fsNamesys;
        this.lastAppliedTxId = lastAppliedTxId;
    }
    long loadFSEdits(EditLogInputStream edits, long expectedStartingTxId,
                     MetaRecoveryContext recovery) throws IOException {
        StartupProgress prog = NameNode.getStartupProgress();
        Step step = createStartupProgressStep(edits);
        prog.beginStep(Phase.LOADING_EDITS, step);
        fsNamesys.writeLock();
        try {
            long startTime = now();
            FSImage.LOG.info("Start loading edits file " + edits.getName());
            long numEdits = loadEditRecords(edits, false,
                    expectedStartingTxId, recovery);
            FSImage.LOG.info("Edits file " + edits.getName()
                    + " of size " + edits.length() + " edits # " + numEdits
                    + " loaded in " + (now()-startTime)/1000 + " seconds");
            return numEdits;
        } finally {
            edits.close();
            fsNamesys.writeUnlock();
            prog.endStep(Phase.LOADING_EDITS, step);
        }
    }
    private static Step createStartupProgressStep(EditLogInputStream edits)
            throws IOException {
        long length = edits.length();
        String name = edits.getCurrentStreamName();
        return length != -1 ? new Step(name, length) : new Step(name);
    }
    long loadEditRecords(EditLogInputStream in, boolean closeOnExit,
                         long expectedStartingTxId, MetaRecoveryContext recovery)
            throws IOException {
        FileSystemDirectory fsDir = fsNamesys.dir;

        EnumMap<FSEditLogOpCodes, Holder<Integer>> opCounts =
                new EnumMap<FSEditLogOpCodes, Holder<Integer>>(FSEditLogOpCodes.class);

        if (LOG.isTraceEnabled()) {
            LOG.trace("Acquiring write lock to replay edit log");
        }

        fsNamesys.writeLock();
        fsDir.writeLock();

        long recentOpcodeOffsets[] = new long[4];
        Arrays.fill(recentOpcodeOffsets, -1);

        long expectedTxId = expectedStartingTxId;
        long numEdits = 0;
        long lastTxId = in.getLastTxId();
        long numTxns = (lastTxId - expectedStartingTxId) + 1;
        StartupProgress prog = NameNode.getStartupProgress();
        Step step = createStartupProgressStep(in);
        prog.setTotal(Phase.LOADING_EDITS, step, numTxns);
        StartupProgress.Counter counter = prog.getCounter(Phase.LOADING_EDITS, step);
        long lastLogTime = now();
        long lastInodeId = fsNamesys.getLastInodeId();

        try {
            while (true) {
                try {
                    FSEditLogOp op;
                    try {
                        op = in.readOp();
                        if (op == null) {
                            break;
                        }
                    } catch (Throwable e) {
                        // Handle a problem with our input
                        check203UpgradeFailure(in.getVersion(), e);
                        String errorMessage =
                                formatEditLogReplayError(in, recentOpcodeOffsets, expectedTxId);
                        FSImage.LOG.error(errorMessage, e);
                        if (recovery == null) {
                            // We will only try to skip over problematic opcodes when in
                            // recovery mode.
                            throw new EditLogInputException(errorMessage, e, numEdits);
                        }
                        MetaRecoveryContext.editLogLoaderPrompt(
                                "We failed to read txId " + expectedTxId,
                                recovery, "skipping the bad section in the log");
                        in.resync();
                        continue;
                    }
                    recentOpcodeOffsets[(int)(numEdits % recentOpcodeOffsets.length)] =
                            in.getPosition();


                    // Now that the operation has been successfully decoded and
                    // applied, update our bookkeeping.
                    incrOpCount(op.opCode, opCounts, step, counter);
                    if (op.hasTransactionId()) {
                        lastAppliedTxId = op.getTransactionId();
                        expectedTxId = lastAppliedTxId + 1;
                    } else {
                        expectedTxId = lastAppliedTxId = expectedStartingTxId;
                    }
                    // log progress
                    if (op.hasTransactionId()) {
                        long now = now();
                        if (now - lastLogTime > REPLAY_TRANSACTION_LOG_INTERVAL) {
                            long deltaTxId = lastAppliedTxId - expectedStartingTxId + 1;
                            int percent = Math.round((float) deltaTxId / numTxns * 100);
                            LOG.info("replaying edit log: " + deltaTxId + "/" + numTxns
                                    + " transactions completed. (" + percent + "%)");
                            lastLogTime = now;
                        }
                    }
                    numEdits++;
                } catch (MetaRecoveryContext.RequestStopException e) {
                    MetaRecoveryContext.LOG.warn("Stopped reading edit log at " +
                            in.getPosition() + "/"  + in.length());
                    break;
                }
            }
        } finally {
            fsNamesys.resetLastInodeId(lastInodeId);
            if(closeOnExit) {
                in.close();
            }
            fsDir.writeUnlock();
            fsNamesys.writeUnlock();

            if (LOG.isTraceEnabled()) {
                LOG.trace("replaying edit log finished");
            }


        }
        return numEdits;
    }
    private void check203UpgradeFailure(int logVersion, Throwable e)
            throws IOException {
        // 0.20.203 version version has conflicting opcodes with the later releases.
        // The editlog must be emptied by restarting the namenode, before proceeding
        // with the upgrade.
        if (Storage.is203LayoutVersion(logVersion)
                && logVersion != HdfsConstants.LAYOUT_VERSION) {
            String msg = "During upgrade failed to load the editlog version "
                    + logVersion + " from release 0.20.203. Please go back to the old "
                    + " release and restart the namenode. This empties the editlog "
                    + " and saves the namespace. Resume the upgrade after this step.";
            throw new IOException(msg, e);
        }
    }
    private static String formatEditLogReplayError(EditLogInputStream in,
                                                   long recentOpcodeOffsets[], long txid) {
        StringBuilder sb = new StringBuilder();
        sb.append("Error replaying edit log at offset " + in.getPosition());
        sb.append(".  Expected transaction ID was ").append(txid);
        if (recentOpcodeOffsets[0] != -1) {
            Arrays.sort(recentOpcodeOffsets);
            sb.append("\nRecent opcode offsets:");
            for (long offset : recentOpcodeOffsets) {
                if (offset != -1) {
                    sb.append(' ').append(offset);
                }
            }
        }
        return sb.toString();
    }
/*    private long applyEditLogOp(FSEditLogOp op, FileSystemDirectory fsDir,
                                int logVersion, long lastInodeId) throws IOException {
        long inodeId = INodeId.GRANDFATHER_INODE_ID;
        if (LOG.isTraceEnabled()) {
            LOG.trace("replaying edit log: " + op);
        }
        final boolean toAddRetryCache = fsNamesys.hasRetryCache() && op.hasRpcIds();

        switch (op.opCode) {
            case OP_ADD: {
                FSEditLogOp.AddCloseOp addCloseOp = (FSEditLogOp.AddCloseOp)op;
                if (FSNamesystem.LOG.isDebugEnabled()) {
                    FSNamesystem.LOG.debug(op.opCode + ": " + addCloseOp.path +
                            " numblocks : " + addCloseOp.blocks.length +
                            " clientHolder " + addCloseOp.clientName +
                            " clientMachine " + addCloseOp.clientMachine);
                }
                // There three cases here:
                // 1. OP_ADD to create a new file
                // 2. OP_ADD to update file blocks
                // 3. OP_ADD to open file for append

                // See if the file already exists (persistBlocks call)
                final FileINodesInPath iip = fsDir.getLastINodeInPath(addCloseOp.path);
                final INodeFile oldFile = INodeFile.valueOf(
                        iip.getINode(0), addCloseOp.path, true);
                INodeFile newFile = oldFile;
                if (oldFile == null) { // this is OP_ADD on a new file (case 1)
                    // versions > 0 support per file replication
                    // get name and replication
                    final short replication = fsNamesys.getBlockManager()
                            .adjustReplication(addCloseOp.replication);
                    assert addCloseOp.blocks.length == 0;

                    // add to the file tree
                    inodeId = getAndUpdateLastInodeId(addCloseOp.inodeId, logVersion,
                            lastInodeId);
                    newFile = fsDir.unprotectedAddFile(inodeId,
                            addCloseOp.path, addCloseOp.permissions, replication,
                            addCloseOp.mtime, addCloseOp.atime, addCloseOp.blockSize, true,
                            addCloseOp.clientName, addCloseOp.clientMachine);
                    fsNamesys.leaseManager.addLease(addCloseOp.clientName, addCloseOp.path);

                    // add the op into retry cache if necessary
                    if (toAddRetryCache) {
                        HdfsFileStatus stat = fsNamesys.dir.createFileStatus(
                                HdfsFileStatus.EMPTY_NAME, newFile, null);
                        fsNamesys.addCacheEntryWithPayload(addCloseOp.rpcClientId,
                                addCloseOp.rpcCallId, stat);
                    }
                } else { // This is OP_ADD on an existing file
                    if (!oldFile.isUnderConstruction()) {
                        // This is case 3: a call to append() on an already-closed file.
                        if (FSNamesystem.LOG.isDebugEnabled()) {
                            FSNamesystem.LOG.debug("Reopening an already-closed file " +
                                    "for append");
                        }
                        LocatedBlock lb = fsNamesys.prepareFileForWrite(addCloseOp.path,
                                oldFile, addCloseOp.clientName, addCloseOp.clientMachine, null,
                                false, iip.getLatestSnapshot(), false);
                        newFile = INodeFile.valueOf(fsDir.getINode(addCloseOp.path),
                                addCloseOp.path, true);

                        // add the op into retry cache is necessary
                        if (toAddRetryCache) {
                            fsNamesys.addCacheEntryWithPayload(addCloseOp.rpcClientId,
                                    addCloseOp.rpcCallId, lb);
                        }
                    }
                }
                // Fall-through for case 2.
                // Regardless of whether it's a new file or an updated file,
                // update the block list.

                // Update the salient file attributes.
                newFile.setFileAccessTime(addCloseOp.atime, null, fsDir.getINodeMap());
                newFile.setFileModificationTime(addCloseOp.mtime, null, fsDir.getINodeMap());
                updateBlocks(fsDir, addCloseOp, newFile);
                break;
            }
            case OP_CLOSE: {
                FSEditLogOp.AddCloseOp addCloseOp = (FSEditLogOp.AddCloseOp)op;

                if (FSNamesystem.LOG.isDebugEnabled()) {
                    FSNamesystem.LOG.debug(op.opCode + ": " + addCloseOp.path +
                            " numblocks : " + addCloseOp.blocks.length +
                            " clientHolder " + addCloseOp.clientName +
                            " clientMachine " + addCloseOp.clientMachine);
                }

                final FileINodesInPath iip = fsDir.getLastINodeInPath(addCloseOp.path);
                final INodeFile oldFile = INodeFile.valueOf(iip.getINode(0), addCloseOp.path);

                // Update the salient file attributes.
                oldFile.setFileAccessTime(addCloseOp.atime, null, fsDir.getINodeMap());
                oldFile.setFileModificationTime(addCloseOp.mtime, null, fsDir.getINodeMap());
                updateBlocks(fsDir, addCloseOp, oldFile);

                // Now close the file
                if (!oldFile.isUnderConstruction() &&
                        logVersion <= LayoutVersion.BUGFIX_HDFS_2991_VERSION) {
                    // There was a bug (HDFS-2991) in hadoop < 0.23.1 where OP_CLOSE
                    // could show up twice in a row. But after that version, this
                    // should be fixed, so we should treat it as an error.
                    throw new IOException(
                            "File is not under construction: " + addCloseOp.path);
                }
                // One might expect that you could use removeLease(holder, path) here,
                // but OP_CLOSE doesn't serialize the holder. So, remove by path.
                if (oldFile.isUnderConstruction()) {
                    INodeFileUnderConstruction ucFile = (INodeFileUnderConstruction) oldFile;
                    fsNamesys.leaseManager.removeLeaseWithPrefixPath(addCloseOp.path);
                    INodeFile newFile = ucFile.toINodeFile(ucFile.getModificationTime());
                    fsDir.unprotectedReplaceINodeFile(addCloseOp.path, ucFile, newFile);
                }
                break;
            }
            case OP_UPDATE_BLOCKS: {
                FSEditLogOp.UpdateBlocksOp updateOp = (FSEditLogOp.UpdateBlocksOp)op;

                INodeFile oldFile = INodeFile.valueOf(fsDir.getINode(updateOp.path),
                        updateOp.path);
                // Update in-memory data structures
                updateBlocks(fsDir, updateOp, oldFile);

                if (toAddRetryCache) {
                    fsNamesys.addCacheEntry(updateOp.rpcClientId, updateOp.rpcCallId);
                }
                break;
            }

            case OP_SET_REPLICATION: {
                FSEditLogOp.SetReplicationOp setReplicationOp = (FSEditLogOp.SetReplicationOp)op;
                short replication = fsNamesys.getBlockManager().adjustReplication(
                        setReplicationOp.replication);
                fsDir.unprotectedSetReplication(setReplicationOp.path,
                        replication, null);
                break;
            }
            case OP_CONCAT_DELETE: {
                FSEditLogOp.ConcatDeleteOp concatDeleteOp = (FSEditLogOp.ConcatDeleteOp)op;
                fsDir.unprotectedConcat(concatDeleteOp.trg, concatDeleteOp.srcs,
                        concatDeleteOp.timestamp);

                if (toAddRetryCache) {
                    fsNamesys.addCacheEntry(concatDeleteOp.rpcClientId,
                            concatDeleteOp.rpcCallId);
                }
                break;
            }
            case OP_RENAME_OLD: {
                FSEditLogOp.RenameOldOp renameOp = (FSEditLogOp.RenameOldOp)op;
                fsDir.unprotectedRenameTo(renameOp.src, renameOp.dst,
                        renameOp.timestamp);

                if (toAddRetryCache) {
                    fsNamesys.addCacheEntry(renameOp.rpcClientId, renameOp.rpcCallId);
                }
                break;
            }
            case OP_DELETE: {
                FSEditLogOp.DeleteOp deleteOp = (FSEditLogOp.DeleteOp)op;
                fsDir.unprotectedDelete(deleteOp.path, deleteOp.timestamp);

                if (toAddRetryCache) {
                    fsNamesys.addCacheEntry(deleteOp.rpcClientId, deleteOp.rpcCallId);
                }
                break;
            }
            case OP_MKDIR: {
                FSEditLogOp.MkdirOp mkdirOp = (FSEditLogOp.MkdirOp)op;
                inodeId = getAndUpdateLastInodeId(mkdirOp.inodeId, logVersion,
                        lastInodeId);
                fsDir.unprotectedMkdir(inodeId, mkdirOp.path, mkdirOp.permissions,
                        mkdirOp.timestamp);
                break;
            }
            case OP_SET_GENSTAMP_V1: {
                FSEditLogOp.SetGenstampV1Op setGenstampV1Op = (FSEditLogOp.SetGenstampV1Op)op;
                fsNamesys.setGenerationStampV1(setGenstampV1Op.genStampV1);
                break;
            }
            case OP_SET_PERMISSIONS: {
                FSEditLogOp.SetPermissionsOp setPermissionsOp = (FSEditLogOp.SetPermissionsOp)op;
                fsDir.unprotectedSetPermission(setPermissionsOp.src,
                        setPermissionsOp.permissions);
                break;
            }
            case OP_SET_OWNER: {
                FSEditLogOp.SetOwnerOp setOwnerOp = (FSEditLogOp.SetOwnerOp)op;
                fsDir.unprotectedSetOwner(setOwnerOp.src, setOwnerOp.username,
                        setOwnerOp.groupname);
                break;
            }
            case OP_SET_NS_QUOTA: {
                FSEditLogOp.SetNSQuotaOp setNSQuotaOp = (FSEditLogOp.SetNSQuotaOp)op;
                fsDir.unprotectedSetQuota(setNSQuotaOp.src,
                        setNSQuotaOp.nsQuota,
                        HdfsConstants.QUOTA_DONT_SET);
                break;
            }
            case OP_CLEAR_NS_QUOTA: {
                FSEditLogOp.ClearNSQuotaOp clearNSQuotaOp = (FSEditLogOp.ClearNSQuotaOp)op;
                fsDir.unprotectedSetQuota(clearNSQuotaOp.src,
                        HdfsConstants.QUOTA_RESET,
                        HdfsConstants.QUOTA_DONT_SET);
                break;
            }

            case OP_SET_QUOTA:
                FSEditLogOp.SetQuotaOp setQuotaOp = (FSEditLogOp.SetQuotaOp)op;
                fsDir.unprotectedSetQuota(setQuotaOp.src,
                        setQuotaOp.nsQuota,
                        setQuotaOp.dsQuota);
                break;

            case OP_TIMES: {
                FSEditLogOp.TimesOp timesOp = (FSEditLogOp.TimesOp)op;

                fsDir.unprotectedSetTimes(timesOp.path,
                        timesOp.mtime,
                        timesOp.atime, true);
                break;
            }
            case OP_SYMLINK: {
                if (!FileSystem.isSymlinksEnabled()) {
                    throw new IOException("Symlinks not supported - please remove symlink before upgrading to this version of HDFS");
                }
                FSEditLogOp.SymlinkOp symlinkOp = (FSEditLogOp.SymlinkOp)op;
                inodeId = getAndUpdateLastInodeId(symlinkOp.inodeId, logVersion,
                        lastInodeId);
                fsDir.unprotectedAddSymlink(inodeId, symlinkOp.path,
                        symlinkOp.value, symlinkOp.mtime,
                        symlinkOp.atime, symlinkOp.permissionStatus);

                if (toAddRetryCache) {
                    fsNamesys.addCacheEntry(symlinkOp.rpcClientId, symlinkOp.rpcCallId);
                }
                break;
            }
            case OP_RENAME: {
                FSEditLogOp.RenameOp renameOp = (FSEditLogOp.RenameOp)op;
                fsDir.unprotectedRenameTo(renameOp.src, renameOp.dst,
                        renameOp.timestamp, renameOp.options);

                if (toAddRetryCache) {
                    fsNamesys.addCacheEntry(renameOp.rpcClientId, renameOp.rpcCallId);
                }
                break;
            }
            case OP_GET_DELEGATION_TOKEN: {
                FSEditLogOp.GetDelegationTokenOp getDelegationTokenOp
                        = (FSEditLogOp.GetDelegationTokenOp)op;

                fsNamesys.getDelegationTokenSecretManager()
                        .addPersistedDelegationToken(getDelegationTokenOp.token,
                                getDelegationTokenOp.expiryTime);
                break;
            }
            case OP_RENEW_DELEGATION_TOKEN: {
                FSEditLogOp.RenewDelegationTokenOp renewDelegationTokenOp
                        = (FSEditLogOp.RenewDelegationTokenOp)op;
                fsNamesys.getDelegationTokenSecretManager()
                        .updatePersistedTokenRenewal(renewDelegationTokenOp.token,
                                renewDelegationTokenOp.expiryTime);
                break;
            }
            case OP_CANCEL_DELEGATION_TOKEN: {
                FSEditLogOp.CancelDelegationTokenOp cancelDelegationTokenOp
                        = (FSEditLogOp.CancelDelegationTokenOp)op;
                fsNamesys.getDelegationTokenSecretManager()
                        .updatePersistedTokenCancellation(
                                cancelDelegationTokenOp.token);
                break;
            }
            case OP_UPDATE_MASTER_KEY: {
                FSEditLogOp.UpdateMasterKeyOp updateMasterKeyOp = (FSEditLogOp.UpdateMasterKeyOp)op;
                fsNamesys.getDelegationTokenSecretManager()
                        .updatePersistedMasterKey(updateMasterKeyOp.key);
                break;
            }
            case OP_REASSIGN_LEASE: {
                FSEditLogOp.ReassignLeaseOp reassignLeaseOp = (FSEditLogOp.ReassignLeaseOp)op;

                LeaseManager.Lease lease = fsNamesys.leaseManager.getLease(
                        reassignLeaseOp.leaseHolder);
                INodeFileUnderConstruction pendingFile =
                        INodeFileUnderConstruction.valueOf(
                                fsDir.getINode(reassignLeaseOp.path), reassignLeaseOp.path);
                fsNamesys.reassignLeaseInternal(lease,
                        reassignLeaseOp.path, reassignLeaseOp.newHolder, pendingFile);
                break;
            }
            case OP_START_LOG_SEGMENT:
            case OP_END_LOG_SEGMENT: {
                // no data in here currently.
                break;
            }
            case OP_CREATE_SNAPSHOT: {
                FSEditLogOp.CreateSnapshotOp createSnapshotOp = (FSEditLogOp.CreateSnapshotOp) op;
                String path = fsNamesys.getSnapshotManager().createSnapshot(
                        createSnapshotOp.snapshotRoot, createSnapshotOp.snapshotName);
                if (toAddRetryCache) {
                    fsNamesys.addCacheEntryWithPayload(createSnapshotOp.rpcClientId,
                            createSnapshotOp.rpcCallId, path);
                }
                break;
            }
            case OP_DELETE_SNAPSHOT: {
                FSEditLogOp.DeleteSnapshotOp deleteSnapshotOp = (FSEditLogOp.DeleteSnapshotOp) op;
                INode.BlocksMapUpdateInfo collectedBlocks = new INode.BlocksMapUpdateInfo();
                List<INode> removedINodes = new ArrayList<INode>();
                fsNamesys.getSnapshotManager().deleteSnapshot(
                        deleteSnapshotOp.snapshotRoot, deleteSnapshotOp.snapshotName,
                        collectedBlocks, removedINodes);
                fsNamesys.removeBlocks(collectedBlocks);
                collectedBlocks.clear();
                fsNamesys.dir.removeFromInodeMap(removedINodes);
                removedINodes.clear();

                if (toAddRetryCache) {
                    fsNamesys.addCacheEntry(deleteSnapshotOp.rpcClientId,
                            deleteSnapshotOp.rpcCallId);
                }
                break;
            }
            case OP_RENAME_SNAPSHOT: {
                FSEditLogOp.RenameSnapshotOp renameSnapshotOp = (FSEditLogOp.RenameSnapshotOp) op;
                fsNamesys.getSnapshotManager().renameSnapshot(
                        renameSnapshotOp.snapshotRoot, renameSnapshotOp.snapshotOldName,
                        renameSnapshotOp.snapshotNewName);

                if (toAddRetryCache) {
                    fsNamesys.addCacheEntry(renameSnapshotOp.rpcClientId,
                            renameSnapshotOp.rpcCallId);
                }
                break;
            }
            case OP_ALLOW_SNAPSHOT: {
                FSEditLogOp.AllowSnapshotOp allowSnapshotOp = (FSEditLogOp.AllowSnapshotOp) op;
                fsNamesys.getSnapshotManager().setSnapshottable(
                        allowSnapshotOp.snapshotRoot, false);
                break;
            }
            case OP_DISALLOW_SNAPSHOT: {
                FSEditLogOp.DisallowSnapshotOp disallowSnapshotOp = (FSEditLogOp.DisallowSnapshotOp) op;
                fsNamesys.getSnapshotManager().resetSnapshottable(
                        disallowSnapshotOp.snapshotRoot);
                break;
            }
            case OP_SET_GENSTAMP_V2: {
                FSEditLogOp.SetGenstampV2Op setGenstampV2Op = (FSEditLogOp.SetGenstampV2Op) op;
                fsNamesys.setGenerationStampV2(setGenstampV2Op.genStampV2);
                break;
            }
            case OP_ALLOCATE_BLOCK_ID: {
                FSEditLogOp.AllocateBlockIdOp allocateBlockIdOp = (FSEditLogOp.AllocateBlockIdOp) op;
                fsNamesys.setLastAllocatedBlockId(allocateBlockIdOp.blockId);
                break;
            }
            default:
                throw new IOException("Invalid operation read " + op.opCode);
        }
        return inodeId;
    }*/
    private void incrOpCount(FSEditLogOpCodes opCode,
                             EnumMap<FSEditLogOpCodes, Holder<Integer>> opCounts, Step step,
                             StartupProgress.Counter counter) {
        Holder<Integer> holder = opCounts.get(opCode);
        if (holder == null) {
            holder = new Holder<Integer>(1);
            opCounts.put(opCode, holder);
        } else {
            holder.held++;
        }
        counter.increment();
    }
    private static void dumpOpCounts(
            EnumMap<FSEditLogOpCodes, Holder<Integer>> opCounts) {
        StringBuilder sb = new StringBuilder();
        sb.append("Summary of operations loaded from edit log:\n  ");
        Joiner.on("\n  ").withKeyValueSeparator("=").appendTo(sb, opCounts);
        FSImage.LOG.debug(sb.toString());
    }
 /*   private long getAndUpdateLastInodeId(long inodeIdFromOp, int logVersion,
                                         long lastInodeId) throws IOException {
        long inodeId = inodeIdFromOp;

        if (inodeId == INodeId.GRANDFATHER_INODE_ID) {
            if (LayoutVersion.supports(LayoutVersion.Feature.ADD_INODE_ID, logVersion)) {
                throw new IOException("The layout version " + logVersion
                        + " supports inodeId but gave bogus inodeId");
            }
            inodeId = fsNamesys.allocateNewInodeId();
        } else {
            // need to reset lastInodeId. fsnamesys gets lastInodeId firstly from
            // fsimage but editlog captures more recent inodeId allocations
            if (inodeId > lastInodeId) {
                fsNamesys.resetLastInodeId(inodeId);
            }
        }
        return inodeId;
    }*/
 /*   private void updateBlocks(FileSystemDirectory fsDir, FSEditLogOp.BlockListUpdatingOp op,
                              INodeFile file) throws IOException {
        // Update its block list
        BlockInfo[] oldBlocks = file.getBlocks();
        Block[] newBlocks = op.getBlocks();
        String path = op.getPath();

        // Are we only updating the last block's gen stamp.
        boolean isGenStampUpdate = oldBlocks.length == newBlocks.length;

        // First, update blocks in common
        for (int i = 0; i < oldBlocks.length && i < newBlocks.length; i++) {
            BlockInfo oldBlock = oldBlocks[i];
            Block newBlock = newBlocks[i];

            boolean isLastBlock = i == newBlocks.length - 1;
            if (oldBlock.getBlockId() != newBlock.getBlockId() ||
                    (oldBlock.getGenerationStamp() != newBlock.getGenerationStamp() &&
                            !(isGenStampUpdate && isLastBlock))) {
                throw new IOException("Mismatched block IDs or generation stamps, " +
                        "attempting to replace block " + oldBlock + " with " + newBlock +
                        " as block # " + i + "/" + newBlocks.length + " of " +
                        path);
            }

            oldBlock.setNumBytes(newBlock.getNumBytes());
            boolean changeMade =
                    oldBlock.getGenerationStamp() != newBlock.getGenerationStamp();
            oldBlock.setGenerationStamp(newBlock.getGenerationStamp());

            if (oldBlock instanceof BlockInfoUnderConstruction &&
                    (!isLastBlock || op.shouldCompleteLastBlock())) {
                changeMade = true;
                fsNamesys.getBlockManager().forceCompleteBlock(
                        (INodeFileUnderConstruction)file,
                        (BlockInfoUnderConstruction)oldBlock);
            }
            if (changeMade) {
                // The state or gen-stamp of the block has changed. So, we may be
                // able to process some messages from datanodes that we previously
                // were unable to process.
                fsNamesys.getBlockManager().processQueuedMessagesForBlock(newBlock);
            }
        }

        if (newBlocks.length < oldBlocks.length) {
            // We're removing a block from the file, e.g. abandonBlock(...)
            if (!file.isUnderConstruction()) {
                throw new IOException("Trying to remove a block from file " +
                        path + " which is not under construction.");
            }
            if (newBlocks.length != oldBlocks.length - 1) {
                throw new IOException("Trying to remove more than one block from file "
                        + path);
            }
            Block oldBlock = oldBlocks[oldBlocks.length - 1];
            boolean removed = fsDir.unprotectedRemoveBlock(path,
                    (INodeFileUnderConstruction) file, oldBlock);
            if (!removed && !(op instanceof FSEditLogOp.UpdateBlocksOp)) {
                throw new IOException("Trying to delete non-existant block " + oldBlock);
            }
        } else if (newBlocks.length > oldBlocks.length) {
            // We're adding blocks
            for (int i = oldBlocks.length; i < newBlocks.length; i++) {
                Block newBlock = newBlocks[i];
                BlockInfo newBI;
                if (!op.shouldCompleteLastBlock()) {
                    // TODO: shouldn't this only be true for the last block?
                    // what about an old-version fsync() where fsync isn't called
                    // until several blocks in?
                    newBI = new BlockInfoUnderConstruction(
                            newBlock, file.getBlockReplication());
                } else {
                    // OP_CLOSE should add finalized blocks. This code path
                    // is only executed when loading edits written by prior
                    // versions of Hadoop. Current versions always log
                    // OP_ADD operations as each block is allocated.
                    newBI = new BlockInfo(newBlock, file.getBlockReplication());
                }
                fsNamesys.getBlockManager().addBlockCollection(newBI, file);
                file.addBlock(newBI);
                fsNamesys.getBlockManager().processQueuedMessagesForBlock(newBlock);
            }
        }
    }*/
 public long getLastAppliedTxId() {
     return lastAppliedTxId;
 }
}

