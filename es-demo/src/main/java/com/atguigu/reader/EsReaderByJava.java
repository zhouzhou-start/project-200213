package com.atguigu.reader;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import javax.crypto.spec.IvParameterSpec;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class EsReaderByJava {
    public static void main(String[] args) throws IOException {
        JestClientFactory jestClientFactory = new JestClientFactory();
        HttpClientConfig build = new HttpClientConfig.Builder("http://hadoop105:9200").build();
        jestClientFactory.setHttpClientConfig(build);
        JestClient object = jestClientFactory.getObject();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        TermQueryBuilder termQueryBuilder = new TermQueryBuilder("sex", "male");
        boolQueryBuilder.filter(termQueryBuilder);
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("favo", "球");
        boolQueryBuilder.must(matchQueryBuilder);
        searchSourceBuilder.query(boolQueryBuilder);
        Search build1 = new Search.Builder(searchSourceBuilder.toString()).addIndex("student1").addType("_doc").build();
        SearchResult execute = object.execute(build1);
        System.out.println(execute.getTotal());
        for (SearchResult.Hit<Map, Void> hit : execute.getHits(Map.class)) {
            Map source = hit.source;
            for (Object o : source.keySet()) {
                System.out.println("key"+o+",value"+source.get(o));
            }
            System.out.println("----------------------------");
        }
        MetricAggregation aggregations = execute.getAggregations();
        TermsAggregation group_by_sex = aggregations.getTermsAggregation("group_by_sex");
        for (TermsAggregation.Entry bucket : group_by_sex.getBuckets()) {
            System.out.println("key"+bucket.getKey()+",doc"+bucket.getCount());
        }


    }
}
