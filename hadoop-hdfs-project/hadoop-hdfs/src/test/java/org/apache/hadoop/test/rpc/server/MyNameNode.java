package org.apache.hadoop.test.rpc.server;

import org.apache.hadoop.test.rpc.protocol.ClientNameNodeProtocol;

public class MyNameNode implements ClientNameNodeProtocol{
    public String getMetaData(String Path){
        return "path==="+"bl1,bl2";
    }
}
