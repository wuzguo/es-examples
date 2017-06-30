package com.github.wuzguo.es;

import com.google.gson.Gson;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class IndexTest {

    public static void main(String[] args) throws UnknownHostException {

        // set the cluster name if you use one different than "elasticsearch"
        Settings settings = Settings.builder()
                .put("cluster.name", "es-hl-test").build();

        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9301))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9302));
         indexGet(client);
        // indexCreate(client);
        // indexDelete(client, "tweet_1");
        // indexUpdate(client);

        // indexBulkUpdate(client);
        // on shutdown
        client.close();
    }

    public static void indexGet(final Client client) {
        // 三个参数对应 索引、类型、已经文档ID
        GetResponse response = client.prepareGet("bank", "account", "6").get();
        System.out.println(response);
    }

    public static void indexCreate(final Client client) {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("title", "title-01");
        map.put("body", "body test");
        map.put("publish", "publish_date");
        String s = new Gson().toJson(map);
        IndexResponse res = null;
        res = client.prepareIndex("twitter", "tweet", "tweet_1").setSource(s, XContentType.JSON).execute().actionGet();
        System.out.println(res);
    }

    public static void indexDelete(final Client client, final String id) {

        DeleteResponse res = null;
        res = client.prepareDelete("twitter", "tweet", id).execute().actionGet();
        System.out.println(res.status().getStatus());
    }


    public static void indexUpdate(final Client client) {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("title", "title-01-02");
        map.put("body", "body test modify");
        map.put("publish", "publish_date modify");

        UpdateRequest req = new UpdateRequest();
        req.index("twitter");
        req.type("tweet");
        req.id("tweet_1");
        req.doc(new Gson().toJson(map), XContentType.JSON);
        try {
            client.update(req).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    public static void indexBulkUpdate(final Client client) {
        BulkRequestBuilder bulkRequest = client.prepareBulk();

        // either use client#prepare, or use Requests# to directly build index/delete requests
        try {
            bulkRequest.add(client.prepareIndex("twitter", "tweet", "1")
                    .setSource(jsonBuilder()
                            .startObject()
                            .field("user", "kimchy")
                            .field("postDate", new Date())
                            .field("message", "trying out Elasticsearch")
                            .endObject()
                    )
            );

            bulkRequest.add(client.prepareIndex("twitter", "tweet", "2")
                    .setSource(jsonBuilder()
                            .startObject()
                            .field("user", "kimchy")
                            .field("postDate", new Date())
                            .field("message", "another post")
                            .endObject()
                    )
            );
        } catch (IOException e) {
            e.printStackTrace();
        }

        BulkResponse bulkResponse = bulkRequest.get();
        if (bulkResponse.hasFailures()) {
            // process failures by iterating through each bulk response item
        }
        System.out.println(bulkResponse.hasFailures());
    }
}
