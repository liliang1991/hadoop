package org.apache.hadoop.hdfs.server.namenode.test.hdfs.fs;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DF;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.server.namenode.*;
import org.apache.hadoop.hdfs.server.namenode.test.hdfs.store.NameNodeStorage;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class FileNameNodeResourceChecker {
    private Map<String, CheckedVolume> volumes;
    long duReserved;
    Configuration conf;
    int minimumRedundantVolumes;
    private static final Log LOG = LogFactory.getLog(FileNameNodeResourceChecker.class.getName());

    public FileNameNodeResourceChecker(Configuration conf) throws IOException {

        {

            this.conf = conf;
            volumes = new HashMap<String, CheckedVolume>();

            duReserved = conf.getLong(DFSConfigKeys.DFS_NAMENODE_DU_RESERVED_KEY,
                    DFSConfigKeys.DFS_NAMENODE_DU_RESERVED_DEFAULT);

            Collection<URI> extraCheckedVolumes = Util.stringCollectionAsURIs(conf
                    .getTrimmedStringCollection(DFSConfigKeys.DFS_NAMENODE_CHECKED_VOLUMES_KEY));

            Collection<URI> localEditDirs = Collections2.filter(
                    FSNamesystem.getNamespaceEditsDirs(conf),
                    new Predicate<URI>() {
                        @Override
                        public boolean apply(URI input) {
                            if (input.getScheme().equals(NameNodeStorage.LOCAL_URI_SCHEME)) {
                                return true;
                            }
                            return false;
                        }
                    });

            // Add all the local edits dirs, marking some as required if they are
            // configured as such.
            for (URI editsDirToCheck : localEditDirs) {
                addDirToCheck(editsDirToCheck,
                        FSNamesystem.getRequiredNamespaceEditsDirs(conf).contains(
                                editsDirToCheck));
            }

            // All extra checked volumes are marked "required"
            for (URI extraDirToCheck : extraCheckedVolumes) {
                addDirToCheck(extraDirToCheck, true);
            }

            minimumRedundantVolumes = conf.getInt(
                    DFSConfigKeys.DFS_NAMENODE_CHECKED_VOLUMES_MINIMUM_KEY,
                    DFSConfigKeys.DFS_NAMENODE_CHECKED_VOLUMES_MINIMUM_DEFAULT);
        }
    }

    @VisibleForTesting
    class CheckedVolume implements CheckableNameNodeResource {
        private DF df;
        private boolean required;
        private String volume;

        public CheckedVolume(File dirToCheck, boolean required)
                throws IOException {
            df = new DF(dirToCheck, conf);
            this.required = required;
            volume = df.getFilesystem();
        }

        public String getVolume() {
            return volume;
        }

        @Override
        public boolean isRequired() {
            return required;
        }

        @Override
        public boolean isResourceAvailable() {
            long availableSpace = df.getAvailable();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Space available on volume '" + volume + "' is "
                        + availableSpace);
            }
            if (availableSpace < duReserved) {
                LOG.warn("Space available on volume '" + volume + "' is "
                        + availableSpace +
                        ", which is below the configured reserved amount " + duReserved);
                return false;
            } else {
                return true;
            }
        }

        @Override
        public String toString() {
            return "volume: " + volume + " required: " + required +
                    " resource available: " + isResourceAvailable();
        }
    }

    private void addDirToCheck(URI directoryToCheck, boolean required)
            throws IOException {
        File dir = new File(directoryToCheck.getPath());
        if (!dir.exists()) {
            throw new IOException("Missing directory " + dir.getAbsolutePath());
        }

        CheckedVolume newVolume = new CheckedVolume(dir, required);
        CheckedVolume volume = volumes.get(newVolume.getVolume());
        if (volume == null || !volume.isRequired()) {
            volumes.put(newVolume.getVolume(), newVolume);
        }
    }
    public boolean hasAvailableDiskSpace() {
        return NameNodeResourcePolicy.areResourcesAvailable(volumes.values(),
                minimumRedundantVolumes);
    }
}
