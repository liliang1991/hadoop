package org.apache.hadoop.test.rpc.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.test.rpc.protocol.ClientNameNodeProtocol;

import java.net.InetSocketAddress;

public class MyHdfsClient {
    public static void main(String[] args)throws Exception {
      ClientNameNodeProtocol clientNameNodeProtocol= RPC.getProxy(ClientNameNodeProtocol.class,1L,new InetSocketAddress("localhost",8881),new Configuration());
        System.out.println(clientNameNodeProtocol.getMetaData("/home/moon"));
    }
}
