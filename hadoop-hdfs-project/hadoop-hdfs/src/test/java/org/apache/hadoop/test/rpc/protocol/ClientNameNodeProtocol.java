package org.apache.hadoop.test.rpc.protocol;

public interface ClientNameNodeProtocol {
    public static final long versionID=1L;
    public String getMetaData(String Path);

    }
