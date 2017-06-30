package com.github.wuzguo.es;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

public class TransportTest {

    public static void main(String[] args) throws UnknownHostException {

        // on startup
            Settings settings = Settings.builder()
                .put("cluster.name", "es-hl-test").build();

        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9301))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9302));

        List<DiscoveryNode> nodeList = client.connectedNodes();
        for (DiscoveryNode node : nodeList) {
            System.out.println(" node: " + node.getName());
        }

        // on shutdown
        client.close();
    }

}
