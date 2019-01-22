package org.apache.hadoop.hdfs.server.namenode.test.hdfs.block;

import org.apache.commons.logging.Log;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.namenode.test.hdfs.manager.FileBlockManager;
import org.apache.hadoop.util.Daemon;

import java.io.PrintWriter;
import java.sql.Time;
import java.util.*;

import static org.apache.hadoop.util.Time.now;

public class FilePendingReplicationBlocks {
    private static final Log LOG = FileBlockManager.LOG;

    Map<Block, PendingBlockInfo> pendingReplications;
    Daemon timerThread = null;

    private long timeout = 5 * 60 * 1000;
    private ArrayList<Block> timedOutItems;
     boolean fsRunning = true;

    //
    // It might take anywhere between 5 to 10 minutes before
    // a request is timed out.
    //
    long defaultRecheckInterval = 5 * 60 * 1000;
    public FilePendingReplicationBlocks(long timeoutPeriod) {
        if ( timeoutPeriod > 0 ) {
            this.timeout = timeoutPeriod;
        }
        pendingReplications = new HashMap<Block,PendingBlockInfo>();
        timedOutItems = new ArrayList<Block>();
    }
  public   static class PendingBlockInfo {
        private long timeStamp;
        private final List<DatanodeDescriptor> targets;

        PendingBlockInfo(DatanodeDescriptor[] targets) {
            this.timeStamp = now();
            this.targets = targets == null ? new ArrayList<DatanodeDescriptor>()
                    : new ArrayList<DatanodeDescriptor>(Arrays.asList(targets));
        }

        long getTimeStamp() {
            return timeStamp;
        }

        void setTimeStamp() {
            timeStamp = now();
        }

        void incrementReplicas(DatanodeDescriptor... newTargets) {
            if (newTargets != null) {
                for (DatanodeDescriptor dn : newTargets) {
                    targets.add(dn);
                }
            }
        }

        void decrementReplicas(DatanodeDescriptor dn) {
            targets.remove(dn);
        }

        int getNumReplicas() {
            return targets.size();
        }
    }
  public   void remove(Block block) {
        synchronized (pendingReplications) {
            pendingReplications.remove(block);
        }
    }
   public void start() {
        timerThread = new Daemon(new PendingReplicationMonitor());
        timerThread.start();
    }
    /*
     * A periodic thread that scans for blocks that never finished
     * their replication request.
     */
    class PendingReplicationMonitor implements Runnable {
        @Override
        public void run() {
            while (fsRunning) {
                long period = Math.min(defaultRecheckInterval, timeout);
                try {
                    pendingReplicationCheck();
                    Thread.sleep(period);
                } catch (InterruptedException ie) {
                    if(LOG.isDebugEnabled()) {
                        LOG.debug("PendingReplicationMonitor thread is interrupted.", ie);
                    }
                }
            }
        }

        /**
         * Iterate through all items and detect timed-out items
         */
        void pendingReplicationCheck() {
            synchronized (pendingReplications) {
                Iterator<Map.Entry<Block, PendingBlockInfo>> iter =
                        pendingReplications.entrySet().iterator();
                long now = now();
                if(LOG.isDebugEnabled()) {
                    LOG.debug("PendingReplicationMonitor checking Q");
                }
                while (iter.hasNext()) {
                    Map.Entry<Block, PendingBlockInfo> entry = iter.next();
                    PendingBlockInfo pendingBlock = entry.getValue();
                    if (now > pendingBlock.getTimeStamp() + timeout) {
                        Block block = entry.getKey();
                        synchronized (timedOutItems) {
                            timedOutItems.add(block);
                        }
                        LOG.warn("PendingReplicationMonitor timed out " + block);
                        iter.remove();
                    }
                }
            }
        }
    }
  public   int getNumReplicas(Block block) {
        synchronized (pendingReplications) {
            PendingBlockInfo found = pendingReplications.get(block);
            if (found != null) {
                return found.getNumReplicas();
            }
        }
        return 0;
    }

    /**
     * Iterate through all items and print them.
     */
  public   void metaSave(PrintWriter out) {
        synchronized (pendingReplications) {
            out.println("Metasave: Blocks being replicated: " +
                    pendingReplications.size());
            Iterator<Map.Entry<Block, PendingBlockInfo>> iter =
                    pendingReplications.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<Block, PendingBlockInfo> entry = iter.next();
                PendingBlockInfo pendingBlock = entry.getValue();
                Block block = entry.getKey();
                out.println(block +
                        " StartTime: " + new Time(pendingBlock.timeStamp) +
                        " NumReplicaInProgress: " +
                        pendingBlock.getNumReplicas());
            }
        }
    }

}
