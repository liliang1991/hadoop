package org.apache.hadoop.hdfs.server.namenode.test.hdfs.fs;

import com.google.common.collect.ImmutableList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSImageStorageInspector;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.apache.hadoop.hdfs.server.namenode.test.hdfs.store.NameNodeStorage;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FileSystemImageTransactionalStorageInspector extends FSImageStorageInspector {
    private boolean needToSave = false;
    private long maxSeenTxId = 0;
    private boolean isUpgradeFinalized = true;
    List<FSImageFile> foundImages = new ArrayList<FSImageFile>();

    public static final Log LOG = LogFactory.getLog(
            FileSystemImageTransactionalStorageInspector.class);
    private static final Pattern IMAGE_REGEX = Pattern.compile(
           NameNodeStorage.NameNodeFile.IMAGE.getName() + "_(\\d+)");
    @Override
    public void inspectDirectory(StorageDirectory sd) throws IOException {
        // Was the directory just formatted?
        if (!sd.getVersionFile().exists()) {
            needToSave |= true;
            return;
        }

        // Check for a seen_txid file, which marks a minimum transaction ID that
        // must be included in our load plan.
        try {
            maxSeenTxId = Math.max(maxSeenTxId,  NameNodeStorage.readTransactionIdFile(sd));
        } catch (IOException ioe) {
            LOG.warn("Unable to determine the max transaction ID seen by " + sd, ioe);
            return;
        }

        File currentDir = sd.getCurrentDir();
        File filesInStorage[];
        try {
            filesInStorage = FileUtil.listFiles(currentDir);
        } catch (IOException ioe) {
            LOG.warn("Unable to inspect storage directory " + currentDir,
                    ioe);
            return;
        }

        for (File f : filesInStorage) {
            LOG.debug("Checking file " + f);
            String name = f.getName();

            // Check for fsimage_*
            Matcher imageMatch = IMAGE_REGEX.matcher(name);
            if (imageMatch.matches()) {
                if (sd.getStorageDirType().isOfType(NameNodeStorage.NameNodeDirType.IMAGE)) {
                    try {
                        long txid = Long.valueOf(imageMatch.group(1));
                        foundImages.add(new FSImageFile(sd, f, txid));
                    } catch (NumberFormatException nfe) {
                        LOG.error("Image file " + f + " has improperly formatted " +
                                "transaction ID");
                        // skip
                    }
                } else {
                    LOG.warn("Found image file at " + f + " but storage directory is " +
                            "not configured to contain images.");
                }
            }
        }

        // set finalized flag
        isUpgradeFinalized = isUpgradeFinalized && !sd.getPreviousDir().exists();
    }
    @Override
    public boolean isUpgradeFinalized() {
        return isUpgradeFinalized;
    }
    @Override
    public boolean needToSave() {
        return needToSave;
    }

    @Override
   public long getMaxSeenTxId() {
        return maxSeenTxId;
    }
    @Override
    public List<FSImageFile> getLatestImages()throws IOException {
            LinkedList<FSImageFile> ret = new LinkedList<FSImageFile>();
        for (FSImageFile img : foundImages) {
            if (ret.isEmpty()) {
                ret.add(img);
            } else {
                FSImageFile cur = ret.getFirst();
                if (cur.txId == img.txId) {
                    ret.add(img);
                } else if (cur.txId < img.txId) {
                    ret.clear();
                    ret.add(img);
                }
            }
        }
        if (ret.isEmpty()) {
            throw new FileNotFoundException("No valid image files found");
        }
        return ret;
    }
    public List<FSImageFile> getFoundImages() {
        return ImmutableList.copyOf(foundImages);
    }

}
