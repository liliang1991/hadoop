package org.apache.hadoop.test.rpc.server;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Builder;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.test.rpc.protocol.ClientNameNodeProtocol;

public class ServiceUtil {
    public static void main(String[] args)throws Exception {
        Builder builder=new RPC.Builder(new Configuration());
        builder.setBindAddress("localhost").setPort(8888).setProtocol(ClientNameNodeProtocol.class)
                .setInstance(new MyNameNode());
        Server server= builder.build();
        server.start();
    }
}
