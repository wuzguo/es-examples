
package com.github.wuzguo.monitor;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.avg.InternalAvg;
import org.elasticsearch.search.aggregations.metrics.cardinality.InternalCardinality;
import org.elasticsearch.search.aggregations.metrics.sum.InternalSum;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * TODO 日志统计功能
 *
 * @author wuzguo at 2017/6/12 16:28
 */
public class LogStats {

    public static void main(String[] args) throws UnknownHostException {

        // on startup
//        Settings settings = Settings.builder()
//                .put("cluster.name", "es-hongling-test").build();

        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9303));

        List<DiscoveryNode> nodeList = client.listedNodes();
        for (DiscoveryNode node : nodeList) {
            System.out.println(" node: " + node.getName());
        }

        //matchAllQuery(client);
        //countRegistUser(client);
        countRegistUser1(client);
        //countRegistUser2(client);
        //queryRateCurve(client);
        //queryDepositCount(client);
        //queryOnlineUser(client);
        // on shutdown
        client.close();
    }

    public static void matchAllQuery(final Client client) {
        SearchResponse res = null;
        QueryBuilder qb = QueryBuilders.matchAllQuery();

        res = client.prepareSearch("logstash-2017.06.14")
                .setTypes("log")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(qb)
                .setFrom(0)
                .setSize(10)
                .execute().actionGet();
        for (SearchHit hit : res.getHits().getHits()) {
            System.out.println(hit.getSourceAsString());
        }
    }

    public static void countRegistUser(final Client client) {
        SearchResponse res = null;
        QueryBuilder qb = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("businessName", "register_business"))
                .must(QueryBuilders.termQuery("resultStatus", "1"));

        AggregationBuilder agg =
                AggregationBuilders
                        .dateHistogram("histogram")
                        .field("@timestamp")
                        // 字段名称都是小写
                        // https://www.elastic.co/guide/en/elasticsearch/reference/5.4/fielddata.html
                        .subAggregation(AggregationBuilders.terms("source_count").field("fromSource.keyword"))
                        // .interval(1 * 60 * 60 * 1000);
                        .dateHistogramInterval(DateHistogramInterval.HOUR);

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy.MM.dd");
        String strDate = dateFormat.format(new Date());

        res = client.prepareSearch("logstash-" + strDate)
                .setTypes("log")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .addAggregation(agg)
                .setQuery(qb)
                .setFrom(0)
                .setSize(10)
                .execute().actionGet();

        for (SearchHit hit : res.getHits().getHits()) {
            System.out.println(hit.getSourceAsString());
        }

        Map<String, Aggregation> aggs = res.getAggregations().asMap();
        //取到聚合数据
        InternalDateHistogram teamAgg = (InternalDateHistogram) aggs.get("histogram");

        Iterator<InternalDateHistogram.Bucket> teamBucketIt = teamAgg.getBuckets().iterator();
        while (teamBucketIt.hasNext()) {
            InternalDateHistogram.Bucket bucket = teamBucketIt.next();
            DateTime time = ((DateTime) bucket.getKey());

            System.out.println("key: " + time.toString("HH") + ", value: " + bucket.getDocCount());

            Map<String, Aggregation> bucketAggregations = bucket.getAggregations().asMap();
            //取到聚合数据
            StringTerms sourceAgg = (StringTerms) bucketAggregations.get("source_count");
            Iterator<Terms.Bucket> sourceIterator = sourceAgg.getBuckets().iterator();
            while (sourceIterator.hasNext()) {
                Terms.Bucket next = sourceIterator.next();
                System.out.println("sourceIterator:" + next.getKey() + ", " + next.getDocCount());
            }
        }
    }


    public static void countRegistUser1(final Client client) {
//        String[] strings = TimeZone.getAvailableIDs();
//        for (int i = 0; i < strings.length; i ++) {
//            System.out.println(strings[i]);
//        }

        SearchResponse res = null;
        QueryBuilder qb = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("businessName", "register_business"))
                .must(QueryBuilders.termQuery("resultStatus", "1"));

        AggregationBuilder agg =
                AggregationBuilders
                        .terms("source_count")
                        .field("fromSource.keyword")
                        .subAggregation(AggregationBuilders
                                .dateHistogram("histogram")
                                .field("@timestamp")
                                .dateHistogramInterval(DateHistogramInterval.HOUR));

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy.MM.dd");
        String strDate = dateFormat.format(new Date());

        res = client.prepareSearch("logstash-2017.06.27" /*+ strDate*/)
              //  .setTypes("log")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .addAggregation(agg)
                .setQuery(qb)
                .execute().actionGet();

        Map<String, Aggregation> aggs = res.getAggregations().asMap();
        //取到聚合数据
        StringTerms sourceAgg = (StringTerms) aggs.get("source_count");
        Iterator<Terms.Bucket> sourceIterator = sourceAgg.getBuckets().iterator();
        while (sourceIterator.hasNext()) {
            Terms.Bucket next = sourceIterator.next();
            System.out.println("sourceIterator:" + next.getKey() + ", " + next.getDocCount());

            Map<String, Aggregation> bucketAggregations = next.getAggregations().asMap();
            InternalDateHistogram dateHistogram = (InternalDateHistogram) bucketAggregations.get("histogram");
            Iterator<InternalDateHistogram.Bucket> teamBucketIt = dateHistogram.getBuckets().iterator();
            while (teamBucketIt.hasNext()) {
                InternalDateHistogram.Bucket bucket = teamBucketIt.next();
                DateTime time = ((DateTime) bucket.getKey()).withZone(DateTimeZone.forID("PRC"));
                time.withZoneRetainFields(DateTimeZone.forTimeZone(TimeZone.getTimeZone("Etc/GMT+8")));
                System.out.println("key: " + time.toString("HH") + ", value: " + bucket.getDocCount());
                System.out.println(" time.getHourOfDay(): " + time.getHourOfDay() + ", timeKey: " + time.toString("HH"));
            }
        }
    }

    public static void countRegistUser2(final Client client) {
        SearchResponse res = null;
        QueryBuilder qb = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("businessName", "register_business"))
                .must(QueryBuilders.termQuery("resultStatus", "1"));

        AggregationBuilder agg =
                AggregationBuilders
                        .terms("source_count")
                        .field("fromSource.keyword");
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy.MM.dd");
        String strDate = dateFormat.format(new Date());

        res = client.prepareSearch("logstash-2017.06.19" /*+ strDate*/)
                .setTypes("log")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .addAggregation(agg)
                .setQuery(qb)
                .execute().actionGet();

        Map<String, Aggregation> aggs = res.getAggregations().asMap();
        //取到聚合数据
        StringTerms sourceAgg = (StringTerms) aggs.get("source_count");
        Iterator<Terms.Bucket> sourceIterator = sourceAgg.getBuckets().iterator();
        while (sourceIterator.hasNext()) {
            Terms.Bucket next = sourceIterator.next();
            System.out.println("sourceIterator:" + next.getKey() + ", " + next.getDocCount());
        }
    }

    public static void queryRateCurve(final Client client) {
        SearchResponse res = null;
        QueryBuilder qb = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("businessName", "subject_rate_business"));

        AggregationBuilder agg =
                AggregationBuilders
                        .dateHistogram("histogram")
                        .field("@timestamp")
                        .dateHistogramInterval(DateHistogramInterval.MINUTE)
                        .subAggregation(AggregationBuilders.avg("rate_agg").field("rate"));

        //    AvgAggregationBuilder aggregationBuilders = AggregationBuilders.avg("rate").field("avg").subAggregation(agg);
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy.MM.dd");
        String strDate = dateFormat.format(new Date());

        res = client.prepareSearch("logstash-" + strDate)
                .setTypes("log")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .addAggregation(agg)
                .setQuery(qb)
                .execute().actionGet();

        //  Aggregations aggs = res.getAggregations();

        Map<String, Aggregation> aggs = res.getAggregations().asMap();
        //取到聚合数据
        InternalDateHistogram dateHistogram = (InternalDateHistogram) aggs.get("histogram");
        Iterator<InternalDateHistogram.Bucket> teamBucketIt = dateHistogram.getBuckets().iterator();
        while (teamBucketIt.hasNext()) {
            InternalDateHistogram.Bucket bucket = teamBucketIt.next();
            DateTime time = ((DateTime) bucket.getKey());

            Aggregations subAgg = bucket.getAggregations();

            //取到聚合数据
            InternalAvg avgHistogram = subAgg.get("rate_agg");
            Double value = Double.isNaN(avgHistogram.getValue()) || Double.isInfinite(avgHistogram.getValue()) ? 0.0 : avgHistogram.getValue();
            System.out.println("bytekey: " + value);

            // For each entry
            System.out.println(time.toString("HH:mm") + ", bytekey: " + Double.toString(value));
            System.out.println(time.getMinuteOfDay() + ", bytekey: " + Double.toString(value));

        }


//        //取到聚合数据
//        InternalDateHistogram histogram = aggs.get("histogram");
//        // For each entry
//        for (Histogram.Bucket entry : histogram.getBuckets()) {
//
//            DateTime time = ((DateTime) entry.getKey());
//
//            //     Calendar time = Calendar.getInstance();
//            //     time.setTimeInMillis(time2);
//            //    System.out.println("time: " + simpleDateFormat.format(time.getTime()));
//            System.out.println("key: " + time.toString("yyyy-MM-dd HH:mm") + ", value: " + entry.getDocCount());
//
//
//            Aggregations subAgg = entry.getAggregations();
//
//            //取到聚合数据
//            InternalAvg avgHistogram = subAgg.get("rate_agg");
//            // For each entry
//            System.out.println("bytekey: " + avgHistogram.getValue());
//
//        }
    }


    public static void queryDepositCount(final Client client) {
        SearchResponse searchResponse = null;
        QueryBuilder builder = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("businessName", "deposit_business"));

        // https://www.elastic.co/guide/en/elasticsearch/reference/5.4/fielddata.html
        AggregationBuilder agg = AggregationBuilders
                .dateHistogram("histogram")
                .field("@timestamp")
                .dateHistogramInterval(DateHistogramInterval.HOUR)
                .subAggregation(AggregationBuilders
                        .terms("depositType_count")
                        .field("depositType.keyword")
                        .subAggregation(
                                AggregationBuilders.
                                        sum("total_Amount")
                                        .field("accessData")
                        )
                );

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy.MM.dd");
        String strDate = dateFormat.format(new Date());

        searchResponse = client.prepareSearch("logstash-" + strDate)
                .setTypes("log")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .addAggregation(agg)
                .setQuery(builder)
                .execute().actionGet();

        Aggregations aggs = searchResponse.getAggregations();
        //取到聚合数据
        InternalDateHistogram histogram = aggs.get("histogram");
        // For each entry
        List<InternalDateHistogram.Bucket> bucketList = histogram.getBuckets();

        //   Map<Integer, String> mapValue = null;
        if (bucketList != null && bucketList.size() > 0) {
            // 防止在填充数据的过程中自动扩充容量
            //    mapValue = new HashMap<Integer, String>(bucketList.size() * 3 / 2 + 1);
            for (Histogram.Bucket entry : bucketList) {
                DateTime time = ((DateTime) entry.getKey());

                Aggregations subAgg = entry.getAggregations();
                //取到聚合数据
                StringTerms sourceAgg = (StringTerms) subAgg.get("depositType_count");
                Iterator<Terms.Bucket> sourceIterator = sourceAgg.getBuckets().iterator();

                // 将结果放在以小时为键的Map中
                int hour = time.getHourOfDay();
                System.out.println("hour: " + hour);
                while (sourceIterator.hasNext()) {
                    Terms.Bucket next = sourceIterator.next();

                    Aggregations totalAgg = next.getAggregations();
                    //取到聚合数据
                    InternalSum internalSum = totalAgg.get("total_Amount");
                    Double value = Double.isNaN(internalSum.getValue()) || Double.isInfinite(internalSum.getValue()) ? 0.0 : internalSum.getValue();
                    String key = (String) next.getKey();
                    System.out.println("key: " + key + ", value: " + value.intValue());
                }


            }
        }
    }


    public static void queryOnlineUser(final Client client) {
        SearchResponse searchResponse = null;
//        QueryBuilder builder = QueryBuilders.boolQuery()
//                .must(QueryBuilders.termQuery("logType", "0"));
        //    .must(QueryBuilders.existsQuery("userId.keyword"));

        QueryBuilder builder = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("logType", "0"))
                .must(QueryBuilders.existsQuery("userId"));
        // https://www.elastic.co/guide/en/elasticsearch/reference/5.4/fielddata.html
        AggregationBuilder agg = AggregationBuilders
                .dateHistogram("histogram")
                .field("@timestamp")
                .dateHistogramInterval(DateHistogramInterval.HOUR)
                // 去重
                .subAggregation(AggregationBuilders.cardinality("user_cardinality").field("userId.keyword"));

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy.MM.dd");
        String strDate = dateFormat.format(new Date());

        searchResponse = client.prepareSearch("logstash-"+ strDate)
                //.setTypes("log")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .addAggregation(agg)
                .setQuery(builder)
                .execute().actionGet();

        Aggregations aggs = searchResponse.getAggregations();

        //取到聚合数据
        InternalDateHistogram histogram = aggs.get("histogram");
        // For each entry
        List<InternalDateHistogram.Bucket> bucketList = histogram.getBuckets();

        //   Map<Integer, String> mapValue = null;
        if (bucketList != null && bucketList.size() > 0) {
            // 防止在填充数据的过程中自动扩充容量
            //    mapValue = new HashMap<Integer, String>(bucketList.size() * 3 / 2 + 1);
            long count = 0;
            for (Histogram.Bucket entry : bucketList) {
                DateTime time = ((DateTime) entry.getKey());

                Aggregations subAgg = entry.getAggregations();
                //取到聚合数据
                InternalCardinality sourceAgg = (InternalCardinality) subAgg.get("user_cardinality");

                count += entry.getDocCount();
                // 将结果放在以小时为键的Map中
                int hour = time.getHourOfDay();
                System.out.println("hour: " + hour + ", value: " + entry.getDocCount() + ", count: " + count + ", sourceAgg: " + sourceAgg.getValue());
            }
        }
    }
}
