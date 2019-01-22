package org.apache.hadoop.hdfs.server.namenode.test.hdfs.fs;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.namenode.*;
import org.apache.hadoop.hdfs.server.namenode.test.hdfs.store.NameNodeStorage;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class FileSystemImagePreTransactionalStorageInspector extends FSImageStorageInspector {
    private static final Log LOG =
            LogFactory.getLog(FileSystemImagePreTransactionalStorageInspector.class);
    private long latestNameCheckpointTime = Long.MIN_VALUE;
    private long latestEditsCheckpointTime = Long.MIN_VALUE;
    Storage.StorageDirectory latestEditsSD = null;
    @Override
    public void inspectDirectory(Storage.StorageDirectory sd) throws IOException {

    }

    @Override
    public boolean isUpgradeFinalized() {
        return false;
    }

    @Override
    public List<FSImageFile> getLatestImages() throws IOException {
        return null;
    }

    @Override
    public long getMaxSeenTxId() {
        return 0;
    }

    @Override
    public boolean needToSave() {
        return false;
    }

    static Iterable<EditLogInputStream> getEditLogStreams(NameNodeStorage storage)
            throws IOException {
        FileSystemImagePreTransactionalStorageInspector inspector
                = new FileSystemImagePreTransactionalStorageInspector();
        storage.inspectStorageDirs(inspector);

        List<EditLogInputStream> editStreams = new ArrayList<EditLogInputStream>();
        for (File f : inspector.getLatestEditsFiles()) {
            editStreams.add(new EditLogFileInputStream(f));
        }
        return editStreams;
    }

    private List<File> getLatestEditsFiles() {
        if (latestNameCheckpointTime > latestEditsCheckpointTime) {
            // the image is already current, discard edits
            LOG.debug(
                    "Name checkpoint time is newer than edits, not loading edits.");
            return Collections.<File>emptyList();
        }

        return getEditsInStorageDir(latestEditsSD);
    }

    static List<File> getEditsInStorageDir(Storage.StorageDirectory sd) {
        ArrayList<File> files = new ArrayList<File>();
        File edits = NameNodeStorage.getStorageFile(sd, NameNodeStorage.NameNodeFile.EDITS);
        assert edits.exists() : "Expected edits file at " + edits;
        files.add(edits);
        File editsNew = NameNodeStorage.getStorageFile(sd, NameNodeStorage.NameNodeFile.EDITS_NEW);
        if (editsNew.exists()) {
            files.add(editsNew);
        }
        return files;
    }
}
