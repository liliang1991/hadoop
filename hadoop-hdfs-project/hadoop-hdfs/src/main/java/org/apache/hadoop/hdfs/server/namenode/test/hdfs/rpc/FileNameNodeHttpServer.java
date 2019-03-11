package org.apache.hadoop.hdfs.server.namenode.test.hdfs.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.hdfs.server.namenode.*;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress;
import org.apache.hadoop.hdfs.server.namenode.test.hdfs.MyNameNode;
import org.apache.hadoop.hdfs.server.namenode.test.hdfs.fs.FileSystenImage;
import org.apache.hadoop.hdfs.server.namenode.web.resources.NamenodeWebHdfsMethods;
import org.apache.hadoop.hdfs.web.AuthFilter;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.hdfs.web.resources.Param;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;

import javax.servlet.ServletContext;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ADMIN;

public class FileNameNodeHttpServer {
    private MyNameNode nn;

    HttpServer httpServer;
    private InetSocketAddress httpAddress;
    private InetSocketAddress bindAddress;
    private  Configuration conf;
    public static final String NAMENODE_ADDRESS_ATTRIBUTE_KEY = "name.node.address";
    public static final String FSIMAGE_ATTRIBUTE_KEY = "name.system.image";
    protected static final String NAMENODE_ATTRIBUTE_KEY = "name.node";
    public static final String STARTUP_PROGRESS_ATTRIBUTE_KEY = "startup.progress";

    public FileNameNodeHttpServer(
            Configuration conf,
            MyNameNode nn,
            InetSocketAddress bindAddress) {
        this.conf = conf;
        this.nn = nn;
        this.bindAddress = bindAddress;
    }
    public void setNameNodeAddress(InetSocketAddress nameNodeAddress) {
        httpServer.setAttribute(NAMENODE_ADDRESS_ATTRIBUTE_KEY,
                NetUtils.getConnectAddress(nameNodeAddress));
    }
    public static MyNameNode getNameNodeFromContext(ServletContext context) {
        return (MyNameNode)context.getAttribute(NAMENODE_ATTRIBUTE_KEY);
    }
    public InetSocketAddress getHttpAddress() {
        return httpAddress;
    }
    public void setFSImage(FileSystenImage fsImage) {
        httpServer.setAttribute(FSIMAGE_ATTRIBUTE_KEY, fsImage);
    }
    public void start() throws IOException {
        final String infoHost = bindAddress.getHostName();
        int infoPort = bindAddress.getPort();

        httpServer = new HttpServer("hdfs", infoHost, infoPort,
                infoPort == 0, conf,
                new AccessControlList(conf.get(DFS_ADMIN, " "))) {
            {
                // Add SPNEGO support to NameNode
                if (UserGroupInformation.isSecurityEnabled()) {
                    initSpnego(conf,
                            DFSConfigKeys.DFS_NAMENODE_INTERNAL_SPNEGO_USER_NAME_KEY,
                            DFSUtil.getSpnegoKeytabKey(conf,
                                    DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY));
                }
                if (WebHdfsFileSystem.isEnabled(conf, LOG)) {
                    //add SPNEGO authentication filter for webhdfs
                    final String name = "SPNEGO";
                    final String classname = AuthFilter.class.getName();
                    final String pathSpec = WebHdfsFileSystem.PATH_PREFIX + "/*";
                    Map<String, String> params = getAuthFilterParams(conf);
                    defineFilter(webAppContext, name, classname, params,
                            new String[]{pathSpec});
                    LOG.info("Added filter '" + name + "' (class=" + classname + ")");

                    // add webhdfs packages
                    addJerseyResourcePackage(
                            NamenodeWebHdfsMethods.class.getPackage().getName()
                                    + ";" + Param.class.getPackage().getName(), pathSpec);
                }
            }

            private Map<String, String> getAuthFilterParams(Configuration conf)
                    throws IOException {
                Map<String, String> params = new HashMap<String, String>();
                String principalInConf = conf
                        .get(DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY);
                if (principalInConf != null && !principalInConf.isEmpty()) {
                    params
                            .put(
                                    DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY,
                                    SecurityUtil.getServerPrincipal(principalInConf,
                                            bindAddress.getHostName()));
                } else if (UserGroupInformation.isSecurityEnabled()) {
                    LOG.error("WebHDFS and security are enabled, but configuration property '" +
                            DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY +
                            "' is not set.");
                }
                String httpKeytab = conf.get(DFSUtil.getSpnegoKeytabKey(conf,
                        DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY));
                if (httpKeytab != null && !httpKeytab.isEmpty()) {
                    params.put(
                            DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_KEYTAB_KEY,
                            httpKeytab);
                } else if (UserGroupInformation.isSecurityEnabled()) {
                    LOG.error("WebHDFS and security are enabled, but configuration property '" +
                            DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_KEYTAB_KEY +
                            "' is not set.");
                }
                return params;
            }
        };

        boolean certSSL = conf.getBoolean(DFSConfigKeys.DFS_HTTPS_ENABLE_KEY, false);
        if (certSSL) {
            boolean needClientAuth = conf.getBoolean("dfs.https.need.client.auth", false);
            InetSocketAddress secInfoSocAddr = NetUtils.createSocketAddr(infoHost + ":" + conf.get(
                    DFSConfigKeys.DFS_NAMENODE_HTTPS_PORT_KEY, "0"));
            Configuration sslConf = new Configuration(false);
            if (certSSL) {
                sslConf.addResource(conf.get(DFSConfigKeys.DFS_SERVER_HTTPS_KEYSTORE_RESOURCE_KEY,
                        "ssl-server.xml"));
            }
            httpServer.addSslListener(secInfoSocAddr, sslConf, needClientAuth);
            // assume same ssl port for all datanodes
            InetSocketAddress datanodeSslPort = NetUtils.createSocketAddr(conf.get(
                    DFSConfigKeys.DFS_DATANODE_HTTPS_ADDRESS_KEY, infoHost + ":" + 50475));
            httpServer.setAttribute(DFSConfigKeys.DFS_DATANODE_HTTPS_PORT_KEY, datanodeSslPort
                    .getPort());
        }
        httpServer.setAttribute(NAMENODE_ATTRIBUTE_KEY, nn);
        httpServer.setAttribute(JspHelper.CURRENT_CONF, conf);
        setupServlets(httpServer, conf);
        httpServer.start();
        httpAddress = new InetSocketAddress(bindAddress.getAddress(), httpServer.getPort());
    }

    public void setStartupProgress(StartupProgress prog) {
        httpServer.setAttribute(STARTUP_PROGRESS_ATTRIBUTE_KEY, prog);
    }
    private static void setupServlets(HttpServer httpServer, Configuration conf) {
        httpServer.addInternalServlet("startupProgress",
                StartupProgressServlet.PATH_SPEC, StartupProgressServlet.class);
        httpServer.addInternalServlet("getDelegationToken",
                GetDelegationTokenServlet.PATH_SPEC,
                GetDelegationTokenServlet.class, true);
        httpServer.addInternalServlet("renewDelegationToken",
                RenewDelegationTokenServlet.PATH_SPEC,
                RenewDelegationTokenServlet.class, true);
        httpServer.addInternalServlet("cancelDelegationToken",
                CancelDelegationTokenServlet.PATH_SPEC,
                CancelDelegationTokenServlet.class, true);
        httpServer.addInternalServlet("fsck", "/fsck", FsckServlet.class,
                true);
        httpServer.addInternalServlet("getimage", "/getimage",
                GetImageServlet.class, true);
        httpServer.addInternalServlet("listPaths", "/listPaths/*",
                ListPathsServlet.class, false);
        httpServer.addInternalServlet("data", "/data/*",
                FileDataServlet.class, false);
        httpServer.addInternalServlet("checksum", "/fileChecksum/*",
                FileChecksumServlets.RedirectServlet.class, false);
        httpServer.addInternalServlet("contentSummary", "/contentSummary/*",
                ContentSummaryServlet.class, false);
    }
}
