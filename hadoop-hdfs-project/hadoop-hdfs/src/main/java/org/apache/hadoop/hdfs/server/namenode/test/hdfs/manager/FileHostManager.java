package org.apache.hadoop.hdfs.server.namenode.test.hdfs.manager;

import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.server.namenode.HostFileManager;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

public class FileHostManager {
    private EntrySet includes = new EntrySet();
    private EntrySet excludes = new EntrySet();

    public void refresh(String includeFile, String excludeFile)
            throws IOException {

    }
    public synchronized EntrySet getIncludes() {
        return null;
    }
    public synchronized EntrySet getExcludes() {
        return excludes;
    }

    public synchronized boolean isIncluded(DatanodeID dn) {
        if (includes.isEmpty()) {
            // If the includes list is empty, act as if everything is in the
            // includes list.
            return true;
        } else {
            return includes.find(dn) != null;
        }
    }
    public synchronized boolean isExcluded(DatanodeID dn) {
        return excludes.find(dn) != null;
    }
    public synchronized boolean hasIncludes() {
        return !includes.isEmpty();
    }

    public static class EntrySet implements Iterable<HostFileManager.Entry> {
        /**
         * The index.  Each Entry appears in here exactly once.
         *
         * It may be indexed by one of:
         *     ipAddress:port
         *     ipAddress
         *     registeredHostname:port
         *     registeredHostname
         *
         * The different indexing strategies reflect the fact that we may or may
         * not have a port or IP address for each entry.
         */
        TreeMap<String, HostFileManager.Entry> index = new TreeMap<String, HostFileManager.Entry>();

        public boolean isEmpty() {
            return index.isEmpty();
        }

        public HostFileManager.Entry find(DatanodeID datanodeID) {
            HostFileManager.Entry entry;
            int xferPort = datanodeID.getXferPort();
            assert(xferPort > 0);
            String datanodeIpAddr = datanodeID.getIpAddr();
            if (datanodeIpAddr != null) {
                entry = index.get(datanodeIpAddr + ":" + xferPort);
                if (entry != null) {
                    return entry;
                }
                entry = index.get(datanodeIpAddr);
                if (entry != null) {
                    return entry;
                }
            }
            String registeredHostName = datanodeID.getHostName();
            if (registeredHostName != null) {
                entry = index.get(registeredHostName + ":" + xferPort);
                if (entry != null) {
                    return entry;
                }
                entry = index.get(registeredHostName);
                if (entry != null) {
                    return entry;
                }
            }
            return null;
        }

        public HostFileManager.Entry find(HostFileManager.Entry toFind) {
            int port = toFind.getPort();
            if (port != 0) {
                return index.get(toFind.getIdentifier() + ":" + port);
            } else {
                // An Entry with no port matches any entry with the same identifer.
                // In other words, we treat 0 as "any port."
                Map.Entry<String, HostFileManager.Entry> ceil =
                        index.ceilingEntry(toFind.getIdentifier());
                if ((ceil != null) &&
                        (ceil.getValue().getIdentifier().equals(
                                toFind.getIdentifier()))) {
                    return ceil.getValue();
                }
                return null;
            }
        }

        public String toString() {
            StringBuilder bld = new StringBuilder();

            bld.append("HostSet(");
            for (Map.Entry<String, HostFileManager.Entry> entry : index.entrySet()) {
                bld.append("\n\t");
                bld.append(entry.getKey()).append("->").
                        append(entry.getValue().toString());
            }
            bld.append("\n)");
            return bld.toString();
        }

        @Override
        public Iterator<HostFileManager.Entry> iterator() {
            return index.values().iterator();
        }
    }
}
