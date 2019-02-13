package org.apache.hadoop.hdfs.server.namenode.test.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TestFileBlock {

    public static void getFileLocation() throws Exception {
        Configuration conf = new Configuration();
        Path fpath = new Path("" + "hadoop-2.7.1.tar.gz");
        FileSystem hdfs = fpath.getFileSystem(conf);
        FileStatus filestatus = hdfs.getFileStatus(fpath);
        BlockLocation[] blkLocations = hdfs.getFileBlockLocations(filestatus, 0, filestatus.getLen());
        System.out.println("total block num:" + blkLocations.length);
        for (int i = 0; i < blkLocations.length; i++) {
            System.out.println(blkLocations[i].toString());
            System.out.println("文件在block中的偏移量" + blkLocations[i].getOffset()
                    + ", 长度" + blkLocations[i].getLength());
        }
    }
}

