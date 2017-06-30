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
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.elasticsearch.search.aggregations.bucket.range.InternalRange;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.avg.InternalAvg;
import org.elasticsearch.search.aggregations.metrics.max.InternalMax;
import org.elasticsearch.search.aggregations.metrics.min.InternalMin;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentile;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentileRanks;
import org.elasticsearch.search.aggregations.metrics.percentiles.tdigest.InternalTDigestPercentiles;
import org.elasticsearch.search.aggregations.metrics.stats.extended.InternalExtendedStats;
import org.elasticsearch.search.aggregations.metrics.valuecount.InternalValueCount;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.joda.time.DateTime;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

// 聚合查询
public class AggsQueryTest {

    public static void main(String[] args) throws UnknownHostException {
        // set the cluster name if you use one different than "elasticsearch"
        Settings settings = Settings.builder()
                .put("cluster.name", "es-hl-test").build();

        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9301))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9302));

        //avgQuery(client);
        //minQuery(client);
        //maxQuery(client);
        //valueCountQuery(client);
        //extendedStatsQuery(client);
        //percentileQuery(client);
        //percentileRankQuery(client);
        //rangeQuery(client);
        //histogramQuery(client);
        //dateHistogramQuery(client);
        //queryRateCurve(client);
        countRegistUser(client);
        // on shutdown
        client.close();
    }

    public static void avgQuery(final Client client) {
        SearchResponse res = null;
        AggregationBuilder agg = AggregationBuilders
                .avg("avg_num")
                .field("age");

        res = client.prepareSearch("bank")
                .setTypes("account")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .addAggregation(agg)
                .setFrom(0)
                .setSize(10)
                .execute().actionGet();
        for (SearchHit hit : res.getHits().getHits()) {
            System.out.println(hit.getScore() + "," + hit.getSourceAsString());
        }

        Aggregations aggs = res.getAggregations();
        //取到聚合数据
        InternalAvg types = aggs.get("avg_num");
        System.out.println(types.getValue());
    }

    public static void minQuery(final Client client) {
        SearchResponse res = null;
        AggregationBuilder agg = AggregationBuilders
                .min("min_num")
                .field("age");

        res = client.prepareSearch("bank")
                .setTypes("account")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .addAggregation(agg)
                .setFrom(0)
                .setSize(10)
                .execute().actionGet();

        for (SearchHit hit : res.getHits().getHits()) {
            System.out.println(hit.getScore() + "," + hit.getSourceAsString());
        }

        Aggregations aggs = res.getAggregations();
        //取到聚合数据
        InternalMin types = aggs.get("min_num");
        System.out.println(types.getValue());

    }

    public static void maxQuery(final Client client) {
        SearchResponse res = null;
        AggregationBuilder agg = AggregationBuilders
                .max("max_num")
                .field("balance");

        res = client.prepareSearch("bank")
                .setTypes("account")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .addAggregation(agg)
                .setFrom(0)
                .setSize(10)
                .execute().actionGet();


        Aggregations aggs = res.getAggregations();
        //取到聚合数据
        InternalMax types = aggs.get("max_num");
        System.out.println(types.getValue());

    }

    //扩展统计
    public static void extendedStatsQuery(final Client client) {
        SearchResponse res = null;
        AggregationBuilder agg = AggregationBuilders
                .extendedStats("extended_stats_num")
                .field("balance");

        res = client.prepareSearch("bank")
                .setTypes("account")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .addAggregation(agg)
                .setFrom(0)
                .setSize(10)
                .execute().actionGet();


        Aggregations aggs = res.getAggregations();
        //取到聚合数据
        InternalExtendedStats extendedStats = aggs.get("extended_stats_num");

        System.out.println(extendedStats.getStdDeviationAsString());
        System.out.println(extendedStats.getSumOfSquaresAsString());
        System.out.println(extendedStats.getVarianceAsString());
        System.out.println(extendedStats.getAvgAsString());
        System.out.println(extendedStats.getCountAsString());
        System.out.println(extendedStats.getMaxAsString());
        System.out.println(extendedStats.getMinAsString());
    }


    public static void valueCountQuery(final Client client) {
        SearchResponse res = null;
        AggregationBuilder agg =
                AggregationBuilders
                        .count("count")
                        .field("balance");

        res = client.prepareSearch("bank")
                .setTypes("account")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .addAggregation(agg)
                .setFrom(0)
                .setSize(10)
                .execute().actionGet();

        Aggregations aggs = res.getAggregations();
        //取到聚合数据
        InternalValueCount types = aggs.get("count");
        System.out.println(types.getValue());
    }

    public static void percentileQuery(final Client client) {
        SearchResponse res = null;
        AggregationBuilder agg = AggregationBuilders
                .percentiles("percentile_num")
                .field("age")
                .percentiles(20, 30, 40);

        res = client.prepareSearch("bank")
                .setTypes("account")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .addAggregation(agg)
                .setFrom(0)
                .setSize(10)
                .execute().actionGet();

        Aggregations aggs = res.getAggregations();
        //取到聚合数据
        InternalTDigestPercentiles types = aggs.get("percentile_num");
        Iterator<Percentile> iterator = types.iterator();
        while (iterator.hasNext()) {
            Percentile percentile = iterator.next();
            System.out.println(percentile.getPercent() + ", " + percentile.getValue());
        }

    }

    public static void percentileRankQuery(final Client client) {
        SearchResponse res = null;
        AggregationBuilder agg = AggregationBuilders
                .percentileRanks("percentile_rank_num")
                .field("age")
                .values(20, 30, 40);

        res = client.prepareSearch("bank")
                .setTypes("account")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .addAggregation(agg)
                .setFrom(0)
                .setSize(10)
                .execute().actionGet();

        // sr is here your SearchResponse object
        PercentileRanks ranks = res.getAggregations().get("percentile_rank_num");
        // For each entry
        for (Percentile entry : ranks) {
            System.out.println("percent: " + entry.getPercent() + ", value: " + entry.getValue());
        }
    }

    public static void rangeQuery(final Client client) {
        SearchResponse res = null;
        AggregationBuilder agg =
                AggregationBuilders
                        .range("range")
                        .field("age")
                        .addUnboundedTo(20)
                        .addRange(20, 30)
                        .addRange(30, 35)
                        .addRange(35, 40)
                        .addUnboundedFrom(40);
        res = client.prepareSearch("bank")
                .setTypes("account")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .addAggregation(agg)
                .setFrom(0)
                .setSize(0)
                .execute().actionGet();

        Aggregations aggs = res.getAggregations();
        //取到聚合数据
        InternalRange internalRange = aggs.get("range");

        // For each entry
        for (Object entry : internalRange.getBuckets()) {
            InternalRange.Bucket internalBucket = (InternalRange.Bucket) entry;

            System.out.println("key: " + internalBucket.getKey() + ", value: " + internalBucket.getDocCount());
        }

    }

    // 直方图聚合
    public static void histogramQuery(final Client client) {
        SearchResponse res = null;

        AggregationBuilder agg =
                AggregationBuilders
                        .histogram("histogram")
                        .field("age")
                        .interval(1);
        res = client.prepareSearch("bank")
                .setTypes("account")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .addAggregation(agg)
                .setFrom(0)
                .setSize(0)
                .execute().actionGet();

        Aggregations aggs = res.getAggregations();
        //取到聚合数据
        InternalHistogram histogram = aggs.get("histogram");

        // For each entry
        for (Histogram.Bucket entry : histogram.getBuckets()) {
            System.out.println("key: " + entry.getKey() + ", value: " + entry.getDocCount());
        }
    }

    // 日期直方图聚合
    public static void dateHistogramQuery(final Client client) {
        SearchResponse res = null;

        AggregationBuilder agg =
                AggregationBuilders
                        .dateHistogram("histogram")
                        .field("utc_time")
                        .interval(1)
                        .minDocCount(1);
        res = client.prepareSearch("logstash-logs")
                .setTypes("log")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .addAggregation(agg)
                .setFrom(0)
                .setSize(100)
                .execute().actionGet();

        Aggregations aggs = res.getAggregations();
        //取到聚合数据
        InternalDateHistogram histogram = aggs.get("histogram");

        // For each entry
        for (Histogram.Bucket entry : histogram.getBuckets()) {
            System.out.println("key: " + entry.getKey() + ", value: " + entry.getDocCount());
        }
    }


    public static void queryRateCurve(final Client client) {
        SearchResponse res = null;
        AggregationBuilder agg =
                AggregationBuilders
                        .dateHistogram("histogram")
                        .field("utc_time")
                        .dateHistogramInterval(DateHistogramInterval.YEAR)
                        .subAggregation(AggregationBuilders.avg("byte_agg").field("bytes"));

        //    AvgAggregationBuilder aggregationBuilders = AggregationBuilders.avg("rate").field("avg").subAggregation(agg);
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy.MM.dd");
        String strDate = dateFormat.format(new Date());

        res = client.prepareSearch("logstash-logs")
                .setTypes("log")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .addAggregation(agg)
                //  .setQuery(qb)
                .setFrom(0)
                .setSize(10)
                .execute().actionGet();

        for (SearchHit hit : res.getHits().getHits()) {
            System.out.println(hit.getSourceAsString());
        }

        Aggregations aggs = res.getAggregations();
        //取到聚合数据
        InternalDateHistogram histogram = aggs.get("histogram");
        // For each entry
        for (Histogram.Bucket entry : histogram.getBuckets()) {

            //   long time2 = ((Double) entry.getKey()).longValue();

            //    Calendar time = Calendar.getInstance();
            //    time.setTimeInMillis(time2);

            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");

            //   System.out.println("time: " + simpleDateFormat.format(time.getTime()));
            System.out.println("key: " + entry.getKey() + ", value: " + entry.getDocCount());

            Aggregations subAgg = entry.getAggregations();

            //取到聚合数据
            InternalAvg avgHistogram = subAgg.get("byte_agg");
            // For each entry
            System.out.println("bytekey: " + avgHistogram.getValue());
        }


        SearchResponse res2 = null;
        AggregationBuilder agg2 = AggregationBuilders
                .avg("avg_num")
                .field("bytes");

        res2 = client.prepareSearch("logstash-logs")
                .setTypes("log")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .addAggregation(agg2)
                .execute().actionGet();
        Aggregations aggs2 = res2.getAggregations();
        //取到聚合数据
        InternalAvg types = aggs2.get("avg_num");
        System.out.println("aggs2: " + types.getValue());

    }


    public static void countRegistUser(final Client client) {
        SearchResponse res = null;
        QueryBuilder qb = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("response", "200"));

        AggregationBuilder agg =
                AggregationBuilders
                        .dateHistogram("histogram")
                        .field("utc_time")
                        // 字段名称都是小写
                        .subAggregation(AggregationBuilders.terms("source_histogram").field("extension.keyword"))
                        // .interval(1 * 60 * 60 * 1000);
                        .dateHistogramInterval(DateHistogramInterval.HOUR);

        //    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy.MM.dd");
        //    String strDate = dateFormat.format(new Date());

        res = client.prepareSearch("logstash-logs")
                .setTypes("log")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .addAggregation(agg)
                .setQuery(qb)
                .setFrom(0)
                .setSize(10)
                .execute().actionGet();

//        for (SearchHit hit : res.getHits().getHits()) {
//            System.out.println(hit.getSourceAsString());
//        }

        Aggregations aggs = res.getAggregations();
        //取到聚合数据
        InternalDateHistogram histogram = aggs.get("histogram");

        // For each entry
        for (Histogram.Bucket entry : histogram.getBuckets()) {

            // long time2 = ((Double) entry.getKey()).longValue();
            DateTime time = ((DateTime) entry.getKey());
            System.out.println("key: " + time.toString("HH") + ", value: " + entry.getDocCount());

            StringTerms classTerms = (StringTerms) entry.getAggregations().asMap().get("source_histogram");
            Iterator<Terms.Bucket> bucketIterator = classTerms.getBuckets().iterator();

            while (bucketIterator.hasNext()) {
                Terms.Bucket bucket = bucketIterator.next();
                System.out.println("bucket:" + bucket.getKey() + ", " + bucket.getDocCount());
            }
        }
    }
}
