package org.apache.hadoop.hdfs.server.namenode.test.hdfs.block;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.test.hdfs.manager.FileDatanodeManager;
import org.apache.hadoop.hdfs.util.LightWeightHashSet;

import java.io.PrintWriter;
import java.util.Map;
import java.util.TreeMap;

public class FileInvalidateBlocks {
    FileDatanodeManager datanodeManager;
    private final Map<String, LightWeightHashSet<Block>> node2blocks =
            new TreeMap<String, LightWeightHashSet<Block>>();
    private long numBlocks = 0L;

   public synchronized void remove(final String storageID) {
        final LightWeightHashSet<Block> blocks = node2blocks.remove(storageID);
        if (blocks != null) {
            numBlocks -= blocks.size();
        }
    }
   public synchronized void remove(final String storageID, final Block block) {
        final LightWeightHashSet<Block> v = node2blocks.get(storageID);
        if (v != null && v.remove(block)) {
            numBlocks--;
            if (v.isEmpty()) {
                node2blocks.remove(storageID);
            }
        }
    }
    public FileInvalidateBlocks(FileDatanodeManager datanodeManager) {
        this.datanodeManager = datanodeManager;
    }

    /**
     * Add a block to the block collection
     * which will be invalidated on the specified datanode.
     */
  public   synchronized void add(final Block block, final DatanodeInfo datanode,
                          final boolean log) {
        LightWeightHashSet<Block> set = node2blocks.get(datanode.getStorageID());
        if (set == null) {
            set = new LightWeightHashSet<Block>();
            node2blocks.put(datanode.getStorageID(), set);
        }
        if (set.add(block)) {
            numBlocks++;
            if (log) {
                NameNode.blockStateChangeLog.info("BLOCK* " + getClass().getSimpleName()
                        + ": add " + block + " to " + datanode);
            }
        }
    }
  public   synchronized boolean contains(final String storageID, final Block block) {
        final LightWeightHashSet<Block> s = node2blocks.get(storageID);
        if (s == null) {
            return false; // no invalidate blocks for this storage ID
        }
        Block blockInSet = s.getElement(block);
        return blockInSet != null &&
                block.getGenerationStamp() == blockInSet.getGenerationStamp();
    }

    /** Print the contents to out. */
  public   synchronized void dump(final PrintWriter out) {
        final int size = node2blocks.values().size();
        out.println("Metasave: Blocks " + numBlocks
                + " waiting deletion from " + size + " datanodes.");
        if (size == 0) {
            return;
        }

        for(Map.Entry<String,LightWeightHashSet<Block>> entry : node2blocks.entrySet()) {
            final LightWeightHashSet<Block> blocks = entry.getValue();
            if (blocks.size() > 0) {
                out.println(datanodeManager.getDatanode(entry.getKey()));
                out.println(blocks);
            }
        }
    }
}
