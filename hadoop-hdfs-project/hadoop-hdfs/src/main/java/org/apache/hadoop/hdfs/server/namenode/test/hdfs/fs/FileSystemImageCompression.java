package org.apache.hadoop.hdfs.server.namenode.test.hdfs.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import java.io.*;

public class FileSystemImageCompression {
    private CompressionCodec imageCodec;
    private FileSystemImageCompression() {
    }
    private FileSystemImageCompression(CompressionCodec codec) {
        imageCodec = codec;
    }
    static FileSystemImageCompression createNoopCompression() {
        return new FileSystemImageCompression();
    }
    static FileSystemImageCompression readCompressionHeader(
            Configuration conf, DataInput in) throws IOException
    {
        boolean isCompressed = in.readBoolean();

        if (!isCompressed) {
            return createNoopCompression();
        } else {
            String codecClassName = Text.readString(in);
            return createCompression(conf, codecClassName);
        }
    }
    static FileSystemImageCompression createCompression(Configuration conf)
            throws IOException {
        boolean compressImage = conf.getBoolean(
                DFSConfigKeys.DFS_IMAGE_COMPRESS_KEY,
                DFSConfigKeys.DFS_IMAGE_COMPRESS_DEFAULT);

        if (!compressImage) {
            return createNoopCompression();
        }

        String codecClassName = conf.get(
                DFSConfigKeys.DFS_IMAGE_COMPRESSION_CODEC_KEY,
                DFSConfigKeys.DFS_IMAGE_COMPRESSION_CODEC_DEFAULT);
        return createCompression(conf, codecClassName);
    }
    private static FileSystemImageCompression createCompression(Configuration conf,
                                                        String codecClassName)
            throws IOException {

        CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        CompressionCodec codec = factory.getCodecByClassName(codecClassName);
        if (codec == null) {
            throw new IOException("Not a supported codec: " + codecClassName);
        }

        return new FileSystemImageCompression(codec);
    }
    DataInputStream unwrapInputStream(InputStream is) throws IOException {
        if (imageCodec != null) {
            return new DataInputStream(imageCodec.createInputStream(is));
        } else {
            return new DataInputStream(new BufferedInputStream(is));
        }
    }
   public DataOutputStream writeHeaderAndWrapStream(OutputStream os)
            throws IOException {
        DataOutputStream dos = new DataOutputStream(os);

        dos.writeBoolean(imageCodec != null);

        if (imageCodec != null) {
            String codecClassName = imageCodec.getClass().getCanonicalName();
            Text.writeString(dos, codecClassName);

            return new DataOutputStream(imageCodec.createOutputStream(os));
        } else {
            // use a buffered output stream
            return new DataOutputStream(new BufferedOutputStream(os));
        }
    }
}
