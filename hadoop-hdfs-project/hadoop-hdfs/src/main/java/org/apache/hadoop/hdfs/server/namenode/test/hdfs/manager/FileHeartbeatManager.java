package org.apache.hadoop.hdfs.server.namenode.test.hdfs.manager;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStatistics;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager;
import org.apache.hadoop.hdfs.server.namenode.test.hdfs.fs.FileNamesystem;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.Time;

import java.util.*;

public class FileHeartbeatManager implements DatanodeStatistics {
    private final List<DatanodeDescriptor> datanodes = new ArrayList<DatanodeDescriptor>();
    private final Daemon heartbeatThread = new Daemon(new Monitor());

    FileNamesystem namesystem;
    FileBlockManager blockManager;
    private final long heartbeatRecheckInterval;
    Stats stats = new Stats();

    FileHeartbeatManager(FileNamesystem namesystem,
                         FileBlockManager blockManager, Configuration conf) {
        this.namesystem = namesystem;
        this.blockManager = blockManager;
        boolean avoidStaleDataNodesForWrite = conf.getBoolean(
                DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_WRITE_KEY,
                DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_WRITE_DEFAULT);
        long recheckInterval = conf.getInt(
                DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
                DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_DEFAULT); // 5 min
        long staleInterval = conf.getLong(
                DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY,
                DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_DEFAULT);// 30s
        this.heartbeatRecheckInterval = recheckInterval;

    }
    synchronized int getLiveDatanodeCount() {
        return datanodes.size();
    }

    @Override
    public long getCapacityTotal() {
        return stats.capacityTotal;
    }

    @Override
    public long getCapacityUsed() {
        return stats.capacityUsed;
    }

    @Override
    public float getCapacityUsedPercent() {
        return DFSUtil.getPercentUsed(stats.capacityUsed, stats.capacityTotal);
    }

    @Override
    public long getCapacityRemaining() {
        return stats.capacityRemaining;
    }

    @Override
    public float getCapacityRemainingPercent() {
        return DFSUtil.getPercentRemaining(
                stats.capacityRemaining, stats.capacityTotal);
    }

    @Override
    public long getBlockPoolUsed() {
        return stats.blockPoolUsed;
    }

    @Override
    public float getPercentBlockPoolUsed() {
        return DFSUtil.getPercentUsed(stats.blockPoolUsed, stats.capacityTotal);
    }

    @Override
    public int getXceiverCount() {
        return stats.xceiverCount;
    }

    @Override
    public long getCapacityUsedNonDFS() {
        final long nonDFSUsed = stats.capacityTotal
                - stats.capacityRemaining - stats.capacityUsed;
        return nonDFSUsed < 0L ? 0L : nonDFSUsed;
    }
    void activate(Configuration conf) {
        heartbeatThread.start();
    }
    @Override
    public long[] getStats() {
        return new long[]{getCapacityTotal(),
                getCapacityUsed(),
                getCapacityRemaining(),
                -1L,
                -1L,
                -1L,
                getBlockPoolUsed()};
    }

    @Override
    public int getExpiredHeartbeats() {
        return stats.expiredHeartbeats;
    }

    private static class Stats {
        private long capacityTotal = 0L;
        private long capacityUsed = 0L;
        private long capacityRemaining = 0L;
        private long blockPoolUsed = 0L;
        private int xceiverCount = 0;

        private int expiredHeartbeats = 0;

        private void add(final DatanodeDescriptor node) {
            capacityUsed += node.getDfsUsed();
            blockPoolUsed += node.getBlockPoolUsed();
            xceiverCount += node.getXceiverCount();
            if (!(node.isDecommissionInProgress() || node.isDecommissioned())) {
                capacityTotal += node.getCapacity();
                capacityRemaining += node.getRemaining();
            } else {
                capacityTotal += node.getDfsUsed();
            }
        }

        private void subtract(final DatanodeDescriptor node) {
            capacityUsed -= node.getDfsUsed();
            blockPoolUsed -= node.getBlockPoolUsed();
            xceiverCount -= node.getXceiverCount();
            if (!(node.isDecommissionInProgress() || node.isDecommissioned())) {
                capacityTotal -= node.getCapacity();
                capacityRemaining -= node.getRemaining();
            } else {
                capacityTotal -= node.getDfsUsed();
            }
        }

        /**
         * Increment expired heartbeat counter.
         */
        private void incrExpiredHeartbeats() {
            expiredHeartbeats++;
        }
    }

    /** Periodically check heartbeat and update block key */
    private class Monitor implements Runnable {
        private long lastHeartbeatCheck;
        private long lastBlockKeyUpdate;

        @Override
        public void run() {
            while(namesystem.isRunning()) {
                try {
                    final long now = Time.now();
                    if (lastHeartbeatCheck + heartbeatRecheckInterval < now) {
                        heartbeatCheck();
                        lastHeartbeatCheck = now;
                    }
                    if (blockManager.shouldUpdateBlockKey(now - lastBlockKeyUpdate)) {
                        synchronized(FileHeartbeatManager.this) {
                            for(DatanodeDescriptor d : datanodes) {
                                d.needKeyUpdate = true;
                            }
                        }
                        lastBlockKeyUpdate = now;
                    }
                } catch (Exception e) {
                }
                try {
                    Thread.sleep(5000);  // 5 seconds
                } catch (InterruptedException ie) {
                }
            }
        }
    }
    /**
     * Check if there are any expired heartbeats, and if so,
     * whether any blocks have to be re-replicated.
     * While removing dead datanodes, make sure that only one datanode is marked
     * dead at a time within the synchronized section. Otherwise, a cascading
     * effect causes more datanodes to be declared dead.
     */
    void heartbeatCheck() {
        final FileDatanodeManager dm = blockManager.getDatanodeManager();
        // It's OK to check safe mode w/o taking the lock here, we re-check
        // for safe mode after taking the lock before removing a datanode.
        if (namesystem.isInStartupSafeMode()) {
            return;
        }
        boolean allAlive = false;
        while (!allAlive) {
            // locate the first dead node.
            DatanodeID dead = null;
            // check the number of stale nodes
            int numOfStaleNodes = 0;
            synchronized(this) {
                for (DatanodeDescriptor d : datanodes) {
                    if (dead == null && dm.isDatanodeDead(d)) {
                        stats.incrExpiredHeartbeats();
                        dead = d;
                    }
                    if (d.isStale(dm.getStaleInterval())) {
                        numOfStaleNodes++;
                    }
                }

                // Set the number of stale nodes in the DatanodeManager
                dm.setNumStaleNodes(numOfStaleNodes);
            }

            allAlive = dead == null;
            if (!allAlive) {
                // acquire the fsnamesystem lock, and then remove the dead node.
                namesystem.writeLock();
                try {
                    if (namesystem.isInStartupSafeMode()) {
                        return;
                    }
                    synchronized(this) {
                        dm.removeDeadDatanode(dead);
                    }
                } finally {
                    namesystem.writeUnlock();
                }
            }
        }
    }
    synchronized void removeDatanode(DatanodeDescriptor node) {
        if (node.isAlive) {
            stats.subtract(node);
            datanodes.remove(node);
            node.isAlive = false;
        }
    }
    synchronized void register(final DatanodeDescriptor d) {
        if (!datanodes.contains(d)) {
            addDatanode(d);

            //update its timestamp
            d.updateHeartbeat(0L, 0L, 0L, 0L, 0, 0);
        }
    }
    synchronized void addDatanode(final DatanodeDescriptor d) {
        datanodes.add(d);
        d.isAlive = true;
    }
    synchronized void startDecommission(final DatanodeDescriptor node) {
        stats.subtract(node);
        node.startDecommission();
        stats.add(node);
    }

    synchronized void updateHeartbeat(final DatanodeDescriptor node,
                                      long capacity, long dfsUsed, long remaining, long blockPoolUsed,
                                      int xceiverCount, int failedVolumes) {
        stats.subtract(node);
        node.updateHeartbeat(capacity, dfsUsed, remaining, blockPoolUsed,
                xceiverCount, failedVolumes);
        stats.add(node);
    }
    synchronized void stopDecommission(final DatanodeDescriptor node) {
        stats.subtract(node);
        node.stopDecommission();
        stats.add(node);
    }
}
