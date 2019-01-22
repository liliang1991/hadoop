package org.apache.hadoop.hdfs.server.namenode.test.hdfs.block;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockCollection;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.util.GSet;
import org.apache.hadoop.util.LightWeightGSet;

import java.util.Iterator;

public class FileBlocksMap {private static class NodeIterator implements Iterator<DatanodeDescriptor> {
    private BlockInfo blockInfo;
    private int nextIdx = 0;

    NodeIterator(BlockInfo blkInfo) {
        this.blockInfo = blkInfo;
    }


    @Override
    public boolean hasNext() {
        return blockInfo != null && nextIdx < blockInfo.getCapacity()
                && blockInfo.getDatanode(nextIdx) != null;
    }

    @Override
    public DatanodeDescriptor next() {
        return blockInfo.getDatanode(nextIdx++);
    }

    @Override
    public void remove()  {
        throw new UnsupportedOperationException("Sorry. can't remove.");
    }
}

    private final int capacity;

    private volatile GSet<Block, BlockInfo> blocks;
    public   FileBlocksMap(final float loadFactor) {
        // Use 2% of total memory to size the GSet capacity
        this.capacity = LightWeightGSet.computeCapacity(2.0, "BlocksMap");
        this.blocks = new LightWeightGSet<Block, BlockInfo>(capacity);
    }
  public   int numNodes(Block b) {
        BlockInfo info = blocks.get(b);
        return info == null ? 0 : info.numNodes();
    }
    public   Iterable<BlockInfo> getBlocks() {
        return blocks;
    }
   public Iterator<DatanodeDescriptor> nodeIterator(Block b) {
        return nodeIterator(blocks.get(b));
    }
  public   BlockInfo addBlockCollection(BlockInfo b, BlockCollection bc) {
        BlockInfo info = blocks.get(b);
        if (info != b) {
            info = b;
            blocks.put(info);
        }
        info.setBlockCollection(bc);
        return info;
    }
   public BlockInfo getStoredBlock(Block b) {
        return blocks.get(b);
    }
   public BlockInfo replaceBlock(BlockInfo newBlock) {
        BlockInfo currentBlock = blocks.get(newBlock);
        assert currentBlock != null : "the block if not in blocksMap";
        // replace block in data-node lists
        for(int idx = currentBlock.numNodes()-1; idx >= 0; idx--) {
            DatanodeDescriptor dn = currentBlock.getDatanode(idx);
            dn.replaceBlock(currentBlock, newBlock);
        }
        // replace block in the map itself
        blocks.put(newBlock);
        return newBlock;
    }
  public   BlockCollection getBlockCollection(Block b) {
        BlockInfo info = blocks.get(b);
        return (info != null) ? info.getBlockCollection() : null;
    }
   public int size() {
        return blocks.size();
    }
    /**
     * Remove data-node reference from the block.
     * Remove the block from the block map
     * only if it does not belong to any file and data-nodes.
     */
 public   boolean removeNode(Block b, DatanodeDescriptor node) {
        BlockInfo info = blocks.get(b);
        if (info == null)
            return false;

        // remove block from the data-node list and the node from the block info
        boolean removed = node.removeBlock(info);

        if (info.getDatanode(0) == null     // no datanodes left
                && info.getBlockCollection() == null) {  // does not belong to a file
            blocks.remove(b);  // remove block from the map
        }
        return removed;
    }
   public void removeBlock(Block block) {
        BlockInfo blockInfo = blocks.remove(block);
        if (blockInfo == null)
            return;

        blockInfo.setBlockCollection(null);
        for(int idx = blockInfo.numNodes()-1; idx >= 0; idx--) {
            DatanodeDescriptor dn = blockInfo.getDatanode(idx);
            dn.removeBlock(blockInfo); // remove from the list and wipe the location
        }
    }
}
