package org.apache.hadoop.hdfs.server.namenode.test.hdfs.manager;

import com.google.common.base.Preconditions;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.*;
import org.apache.hadoop.hdfs.server.namenode.test.hdfs.fs.FileSystemImageTransactionalStorageInspector;
import org.apache.hadoop.hdfs.server.namenode.test.hdfs.store.NameNodeStorage;
import org.apache.hadoop.hdfs.util.MD5FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class NameNodeStorageRetentionManager {
    int numCheckpointsToRetain;
    long numExtraEditsToRetain;
    int maxExtraEditsSegmentsToRetain;
    LogsPurgeable purgeableLogs;
    NameNodeStorage storage;
    StoragePurger purger;
    Log LOG = LogFactory.getLog(
            NNStorageRetentionManager.class);

    public NameNodeStorageRetentionManager(
            Configuration conf,
            NameNodeStorage storage,
            LogsPurgeable purgeableLogs,
            StoragePurger purger) {
        this.numCheckpointsToRetain = conf.getInt(
                DFSConfigKeys.DFS_NAMENODE_NUM_CHECKPOINTS_RETAINED_KEY,
                DFSConfigKeys.DFS_NAMENODE_NUM_CHECKPOINTS_RETAINED_DEFAULT);
        this.numExtraEditsToRetain = conf.getLong(
                DFSConfigKeys.DFS_NAMENODE_NUM_EXTRA_EDITS_RETAINED_KEY,
                DFSConfigKeys.DFS_NAMENODE_NUM_EXTRA_EDITS_RETAINED_DEFAULT);
        this.maxExtraEditsSegmentsToRetain = conf.getInt(
                DFSConfigKeys.DFS_NAMENODE_MAX_EXTRA_EDITS_SEGMENTS_RETAINED_KEY,
                DFSConfigKeys.DFS_NAMENODE_MAX_EXTRA_EDITS_SEGMENTS_RETAINED_DEFAULT);
        Preconditions.checkArgument(numCheckpointsToRetain > 0,
                "Must retain at least one checkpoint");
        Preconditions.checkArgument(numExtraEditsToRetain >= 0,
                DFSConfigKeys.DFS_NAMENODE_NUM_EXTRA_EDITS_RETAINED_KEY +
                        " must not be negative");

        this.storage = storage;
        this.purgeableLogs = purgeableLogs;
        this.purger = purger;
    }

    static interface StoragePurger {
        void purgeLog(FileJournalManager.EditLogFile log);

        void purgeImage(FSImageStorageInspector.FSImageFile image);
    }

    public static class DeletionStoragePurger implements StoragePurger {
        @Override
        public void purgeLog(FileJournalManager.EditLogFile log) {
            deleteOrWarn(log.getFile());
        }

        @Override
        public void purgeImage(FSImageStorageInspector.FSImageFile image) {
            deleteOrWarn(image.getFile());
            deleteOrWarn(MD5FileUtils.getDigestFileForFile(image.getFile()));
        }

        private static void deleteOrWarn(File file) {
            if (!file.delete()) {
                // It's OK if we fail to delete something -- we'll catch it
                // next time we swing through this directory.
            }
        }
    }
    public void purgeOldStorage() throws IOException {
        FileSystemImageTransactionalStorageInspector inspector =
                new FileSystemImageTransactionalStorageInspector();
        storage.inspectStorageDirs(inspector);

        long minImageTxId = getImageTxIdToRetain(inspector);
        purgeCheckpointsOlderThan(inspector, minImageTxId);
        // If fsimage_N is the image we want to keep, then we need to keep
        // all txns > N. We can remove anything < N+1, since fsimage_N
        // reflects the state up to and including N. However, we also
        // provide a "cushion" of older txns that we keep, which is
        // handy for HA, where a remote node may not have as many
        // new images.
        //
        // First, determine the target number of extra transactions to retain based
        // on the configured amount.
        long minimumRequiredTxId = minImageTxId + 1;
        long purgeLogsFrom = Math.max(0, minimumRequiredTxId - numExtraEditsToRetain);

        ArrayList<EditLogInputStream> editLogs = new ArrayList<EditLogInputStream>();
        purgeableLogs.selectInputStreams(editLogs, purgeLogsFrom, false, false);
        Collections.sort(editLogs, new Comparator<EditLogInputStream>() {
            @Override
            public int compare(EditLogInputStream a, EditLogInputStream b) {
                return ComparisonChain.start()
                        .compare(a.getFirstTxId(), b.getFirstTxId())
                        .compare(a.getLastTxId(), b.getLastTxId())
                        .result();
            }
        });

        // Remove from consideration any edit logs that are in fact required.
        while (editLogs.size() > 0 &&
                editLogs.get(editLogs.size() - 1).getFirstTxId() >= minimumRequiredTxId) {
            editLogs.remove(editLogs.size() - 1);
        }

        // Next, adjust the number of transactions to retain if doing so would mean
        // keeping too many segments around.
        while (editLogs.size() > maxExtraEditsSegmentsToRetain) {
            purgeLogsFrom = editLogs.get(0).getLastTxId() + 1;
            editLogs.remove(0);
        }

        // Finally, ensure that we're not trying to purge any transactions that we
        // actually need.
        if (purgeLogsFrom > minimumRequiredTxId) {
            throw new AssertionError("Should not purge more edits than required to "
                    + "restore: " + purgeLogsFrom + " should be <= "
                    + minimumRequiredTxId);
        }

        purgeableLogs.purgeLogsOlderThan(purgeLogsFrom);
    }
    public long getImageTxIdToRetain(FileSystemImageTransactionalStorageInspector inspector) {

        List<FSImageStorageInspector.FSImageFile> images = inspector.getFoundImages();
        TreeSet<Long> imageTxIds = Sets.newTreeSet();
        for (FSImageStorageInspector.FSImageFile image : images) {
            imageTxIds.add(image.getCheckpointTxId());
        }

        List<Long> imageTxIdsList = Lists.newArrayList(imageTxIds);
        if (imageTxIdsList.isEmpty()) {
            return 0;
        }

        Collections.reverse(imageTxIdsList);
        int toRetain = Math.min(numCheckpointsToRetain, imageTxIdsList.size());
        long minTxId = imageTxIdsList.get(toRetain - 1);
        LOG.info("Going to retain " + toRetain + " images with txid >= " +
                minTxId);
        return minTxId;
    }

    private void purgeCheckpointsOlderThan(
            FileSystemImageTransactionalStorageInspector inspector,
            long minTxId) {
        for (FileSystemImageTransactionalStorageInspector.FSImageFile image : inspector.getFoundImages()) {
            if (image.getCheckpointTxId() < minTxId) {
                purger.purgeImage(image);
            }
        }
    }

}
