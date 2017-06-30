package com.github.wuzguo.es;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

public class QueryTest {

    public static void main(String[] args) throws UnknownHostException {
        // set the cluster name if you use one different than "elasticsearch"
        Settings settings = Settings.builder()
                .put("cluster.name", "es-hl-test").build();

        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9301))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9302));

        // matchAllQuery(client);
        /* full text queries */
        // matchQuery(client);
        // multiMatchQuery(client);
        // commonTermQuery(client);
        /* end of full text queries */
        /* term query */
        // rangeQuery(client);
        // termQuery(client);
        othersQuery(client);
        /* end of term query*/

        // on shutdown
      //  client.close();
    }

    public static void matchAllQuery(final Client client) {
        SearchResponse res = null;
        QueryBuilder qb = QueryBuilders.matchAllQuery();

        res = client.prepareSearch("bank")
                .setTypes("account")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(qb)
                .setFrom(0)
                .setSize(10)
                .execute().actionGet();
        for (SearchHit hit : res.getHits().getHits()) {
            System.out.println(hit.getSourceAsString());
        }
    }

    public static void matchQuery(final Client client) {
        SearchResponse res = null;
        QueryBuilder qb = QueryBuilders.matchQuery("text_entry", "Jerusalem");

        res = client.prepareSearch("shakespeare")
                .setTypes("line")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(qb)
                .setFrom(0)
                .setSize(10)
                .execute().actionGet();
        for (SearchHit hit : res.getHits().getHits()) {
            System.out.println(hit.getSourceAsString());
        }
    }

    public static void multiMatchQuery(final Client client) {
        SearchResponse res = null;
        QueryBuilder qb = QueryBuilders
                .multiMatchQuery("Street", "address", "employer", "city");

        res = client.prepareSearch("bank")
                .setTypes("account")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(qb)
                .setFrom(0)
                .setSize(10)
                .execute().actionGet();
        for (SearchHit hit : res.getHits().getHits()) {
            System.out.println(hit.getSourceAsString());
        }

    }

    public static void commonTermQuery(Client client) {
        SearchResponse res = null;
        QueryBuilder qb = QueryBuilders
                .commonTermsQuery("text_entry", "HENRY");

        res = client.prepareSearch("shakespeare")
                .setTypes("line")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(qb)
                .setFrom(0)
                .setSize(10)
                .execute().actionGet();
        for (SearchHit hit : res.getHits().getHits()) {
            System.out.println(hit.getSourceAsString());
        }

        //common terms query
    }

    public static void termQuery(Client client) {
        SearchResponse res = null;
        QueryBuilder qb = QueryBuilders
                .termQuery("text_entry", "fourteen");

//		QueryBuilder qb = QueryBuilders
//				.termsQuery("title","article","relevence");

        HighlightBuilder highlightBuilder = new HighlightBuilder().field("*").requireFieldMatch(false);
        highlightBuilder.preTags("<span style=\"color:red\">");
        highlightBuilder.postTags("</span>");

        res = client.prepareSearch("shakespeare")
                .setTypes("line")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(qb)
                .highlighter(highlightBuilder)
                .setFrom(0)
                .setSize(10)
                .execute().actionGet();
        for (SearchHit hit : res.getHits()) {
            Map<String, HighlightField> result = hit.highlightFields();
            // 获取高亮字段
            HighlightField highlightedSummary = result.get("text_entry");
            Text[] titleTexts = highlightedSummary.getFragments();
            String allFragments = "";
            for (Text text : titleTexts) {
                allFragments += text;
            }
            System.out.println(result);
            System.out.println(allFragments);
        }

        //common terms query
    }

    public static void rangeQuery(Client client) {
        SearchResponse res = null;

        QueryBuilder qb = QueryBuilders
                .rangeQuery("speech_number")
                .gte(5)
                .lt(7);


//		QueryBuilder qb = QueryBuilders
//				.rangeQuery("like")
//				.from(5)
//				.to(7)
//				.includeLower(true)
//				.includeUpper(false);


        res = client.prepareSearch("shakespeare")
                .setTypes("line")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(qb)
                .setFrom(0)
                .setSize(10)
                .execute().actionGet();
        for (SearchHit hit : res.getHits().getHits()) {
            System.out.println(hit.getSourceAsString());
        }

        //common terms query
    }

    public static void othersQuery(Client client) {
        SearchResponse res = null;

        // QueryBuilder qb = QueryBuilders.existsQuery("email");
        // 前缀
        // QueryBuilder qb = QueryBuilders.prefixQuery("email", "burtonmeyers");
        // 通配符
        // QueryBuilder qb = QueryBuilders.wildcardQuery("user", "k?mc*");
        // 正则表达式
        // QueryBuilder qb = QueryBuilders.regexpQuery("user", "k.*y");
        // 模糊查询
        // QueryBuilder qb = QueryBuilders.fuzzyQuery("name", "kimzhy");
        // type查询
        // QueryBuilder qb = QueryBuilders.typeQuery("account");
        //
        QueryBuilder qb = QueryBuilders.idsQuery("account","line").addIds("1","2","5");


        res = client.prepareSearch("bank")
                .setTypes("account")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(qb)
                .setFrom(0)
                .setSize(10)
                .execute().actionGet();
        for (SearchHit hit : res.getHits().getHits()) {
            System.out.println(hit.getSourceAsString());
        }

        //common terms query
    }

}
