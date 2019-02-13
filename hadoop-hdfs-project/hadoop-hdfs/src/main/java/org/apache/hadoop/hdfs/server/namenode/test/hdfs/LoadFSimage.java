package org.apache.hadoop.hdfs.server.namenode.test.hdfs;

import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.server.namenode.FSImageSerialization;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.io.MD5Hash;

import java.io.*;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.util.concurrent.atomic.AtomicInteger;

/*
1．imgVersion(int)：当前image的版本信息

2．namespaceID(int)：用来确保别的HDFS instance中的datanode不会误连上当前NN。

3．numFiles(long)：整个文件系统中包含有多少文件和目录

4．genStamp(long)：生成该image时的时间戳信息
 */
public class LoadFSimage {
    AtomicInteger numSnapshots = new AtomicInteger();

    public void read(String pathname) {
        try {


            File curFile = new File(pathname);
            MessageDigest digester = MD5Hash.getDigester();
            DigestInputStream fin = new DigestInputStream(
                    new FileInputStream(curFile), digester);
            DataInputStream in = new DataInputStream(fin);

            int imgVersion = in.readInt();
            int namespaceID = in.readInt();
            long numFiles = in.readLong();
            long genStamp = in.readLong();
            genStamp = in.readLong();
            //  读取后创建的块的上一代标记切换到顺序块ID。
            long stampAtIdSwitch = in.readLong();
            long maxSequentialBlockId = in.readLong();
            long transactionID = in.readLong();

            //读取fsimage中最后分配的inode id
            long lastInodeId = in.readLong();

            //readFile
            int snapshotCounter = in.readInt();
            numSnapshots.set(in.readInt());
            //compression
            boolean isCompressed = in.readBoolean();


            in = new DataInputStream(new BufferedInputStream(in));
            if (in.readShort() != 0) {
                throw new IOException("First node is not root");
            }

            boolean supportSnapshot = LayoutVersion.supports(LayoutVersion.Feature.SNAPSHOT,
                    imgVersion);
            //loadLocalName
            if (supportSnapshot) {
                //load root
            /*    if (in.readShort() != 0) {
                    throw new IOException("First node is not root");
                }*/
                //inode
                long inodeId =
                        in.readLong();

                short replication =
                        in.readShort();
                replication = 1;
                long modificationTime = in.readLong();
                long atime = in.readLong();
                long blockSize = in.readLong();
                int numBlocks = in.readInt();
                long nsQuota = in.readLong();
                long dsQuota = in.readLong();
                boolean snapshottable = in.readBoolean();
                if (!snapshottable) {
                    boolean withSnapshot = in.readBoolean();
                }
                PermissionStatus permissions = PermissionStatus.read(in);

                System.out.println("replication=====" + replication);
                System.out.println("snapshottable====" + snapshottable);
            }
        //    int namelen = in.readShort();
/*
            byte filepath=in.readByte();
            short replications=in.readShort();*/
            System.out.println("imageversion====" + imgVersion);
            System.out.println("namespaceID====" + namespaceID);
            System.out.println("namespaceID===" + numFiles);
            System.out.println("genStamp====" + genStamp);
            System.out.println("transactionID===" + transactionID);
            //   System.out.println("filepath======"+filepath);
            //loadDirectoryWithSnapshot
            loadDirectoryWithSnapshot(in);
    /*        System.out.println("namelen==="+namelen);
            System.out.println("replications===="+replications);*/

        } catch (IOException e) {

        }
    }

    public void loadDirectoryWithSnapshot(DataInputStream in) {
        try {
            long inodeId = in.readLong();
            int numSnapshots = in.readInt();
            int snapshotQuota = in.readInt();


            //loadChildren
            int numChildren = in.readInt();
            for (int i = 0; i < numChildren; i++) {
                // load single inode
                final byte[] localName = FSImageSerialization.readLocalName(in);
            ;                String pathName = new String(localName);
                System.out.println("pathName==="+pathName);


            }
            //loadFileDirectoryDiffList(
            int size = in.readInt();
            int numSubTree = in.readInt();
      /*      for (int i = 0; i < numSubTree; i++) {
                loadDirectoryWithSnapshot(in, counter);
            }*/

            //loadFilesUnderConstruction
            System.out.println("inodeId====" + inodeId);
            System.out.println("numChildren======" + numChildren);
            System.out.println("numSubTree=====" + numSubTree);
        } catch (IOException e) {

        }


    }

    public void loadFilesUnderConstruction(DataInput in) {
        try {
            int size = in.readInt();
//                loadSecretManagerState(in);
            int currentId = in.readInt();
            int numberOfKeys = in.readInt();
            int numberOfTokens = in.readInt();

        } catch (IOException e) {

        }

    }

    public static void main(String[] args) {
        String pathname = "/home/moon/hadoop/tmp/dfs/name/current/fsimage_0000000000000000013";
        LoadFSimage loadFSimage = new LoadFSimage();
        loadFSimage.read(pathname);
/*        byte[] name={116,101,115,116,49};
        String s=new String(name);
        System.out.println(s);*/

    }
}
