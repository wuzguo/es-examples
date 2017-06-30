package com.github.wuzguo.es;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;

// 复合查询
public class CompoundQueryTest {

    public static void main(String[] args) throws UnknownHostException {
        // set the cluster name if you use one different than "elasticsearch"
        Settings settings = Settings.builder()
                .put("cluster.name", "es-hl-test").build();

        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9301))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9302));

        // constantScoreQuery(client);
        // booQuery(client);
        disMaxQuery(client);
        client.close();
    }

    // 不计算相关性的query, 沿用index过程中指定的score
    public static void constantScoreQuery(final Client client) {
        SearchResponse res = null;
        QueryBuilder qb = QueryBuilders
                .constantScoreQuery(QueryBuilders.termQuery("gender", "m"))
                .boost(2.0f);

        res = client.prepareSearch("bank")
                .setTypes("account")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(qb)
                .setFrom(0)
                .setSize(10)
                .execute().actionGet();
        for (SearchHit hit : res.getHits().getHits()) {
            System.out.println(hit.getScore() + "," + hit.getSourceAsString());
        }

    }

    public static void booQuery(final Client client) {
        SearchResponse res = null;
        QueryBuilder qb = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("gender", "f"))
                .mustNot(QueryBuilders.termQuery("age", "36"))
                // 如果满足这些语句中的任意语句，将增加 _score ，否则，无任何影响。它们主要用于修正每个文档的相关性得分。
                .should(QueryBuilders.termQuery("firstname", "effie"))
                // 必须 匹配，但它以不评分、过滤模式来进行。这些语句对评分没有贡献，只是根据过滤标准来排除或包含文档。
                .filter(QueryBuilders.termQuery("lastname", "gates"));

        res = client.prepareSearch("bank")
                .setTypes("account")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(qb)
                .setFrom(0)
                .setSize(10)
                .execute().actionGet();
        for (SearchHit hit : res.getHits().getHits()) {
            System.out.println(hit.getScore() + ", " + hit.getSourceAsString());
        }
    }

    // 使用最佳匹配查询子句得到的_score
    // http://blog.csdn.net/dm_vincent/article/details/41820537
    public static void disMaxQuery(final Client client) {
        SearchResponse res = null;
        QueryBuilder qb = QueryBuilders
                .disMaxQuery()
                .add(QueryBuilders.termQuery("address", "street"))
                // tie_breaker参数会让dis_max查询的行为更像是dis_max和bool的一种折中
                // boost 提升查询条件权重
                .add(QueryBuilders.termQuery("email", "quility")).boost(1.2f).tieBreaker(0.7f);

        res = client.prepareSearch("bank")
                .setTypes("account")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(qb)
                .setFrom(0)
                .setSize(10)
                .execute().actionGet();
        for (SearchHit hit : res.getHits().getHits()) {
            System.out.println(hit.getScore() + "," + hit.getSourceAsString());
        }

    }

}
