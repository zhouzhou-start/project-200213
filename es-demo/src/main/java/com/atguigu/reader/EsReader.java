package com.atguigu.reader;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class EsReader {
    public static void main(String[] args) throws IOException {
//        //获取客户端
//        JestClientFactory jestClientFactory = new JestClientFactory();
//        //创建配置文件
//        HttpClientConfig config = new HttpClientConfig.Builder("http://hadoop105:9200").build();
//        //设置客户端
//        jestClientFactory.setHttpClientConfig(config);
//        //创建对象
//        JestClient object = jestClientFactory.getObject();
//        Search build = new Search.Builder("{\n" +
//                "  \"query\": {\n" +
//                "    \"bool\": {\n" +
//                "      \"filter\": {\n" +
//                "        \"term\": {\n" +
//                "          \"sex\": \"male\"\n" +
//                "        }\n" +
//                "      },\n" +
//                "      \"must\": [\n" +
//                "        {\n" +
//                "          \"match\": {\n" +
//                "            \"favo\": \"球\"\n" +
//                "          }\n" +
//                "        }\n" +
//                "      ]\n" +
//                "    }\n" +
//                "  },\n" +
//                "  \"aggs\": {\n" +
//                "    \"group_by_sex\": {\n" +
//                "      \"terms\": {\n" +
//                "        \"field\": \"sex\",\n" +
//                "        \"size\": 10\n" +
//                "      }\n" +
//                "    }\n" +
//                "  },\n" +
//                "  \"from\": 0,\n" +
//                "  \"size\": 20\n" +
//                "}").addIndex("student1")
//                .addType("_doc")
//                .build();
//        //执行对象
//        SearchResult execute = object.execute(build);
//        //获取相关数据
//        System.out.println(execute.getTotal());
//        System.out.println(execute.getMaxScore());
//        List<SearchResult.Hit<Map, Void>> hits = execute.getHits(Map.class);
//        for (SearchResult.Hit<Map, Void> hit : hits) {
//            Map source = hit.source;
//            for (Object o : source.keySet()) {
//                System.out.println("key"+o+",value"+source.get(o));
//            }
//            System.out.println("----------------------------------");
//        }
//        MetricAggregation aggregations = execute.getAggregations();
//        TermsAggregation group_by_sex = aggregations.getTermsAggregation("group_by_sex");
//        for (TermsAggregation.Entry bucket : group_by_sex.getBuckets()) {
//            System.out.println("key"+bucket.getKey()+",doc_count"+bucket.getCount());
//            System.out.println("-----------------------");
//        }
        JestClientFactory jestClientFactory = new JestClientFactory();
        HttpClientConfig build = new HttpClientConfig.Builder("http://hadoop105:9200").build();
        jestClientFactory.setHttpClientConfig(build);
        JestClient object = jestClientFactory.getObject();
        Search build1 = new Search.Builder("{\\n\" +\n" +
                "                \"  \\\"query\\\": {\\n\" +\n" +
                "                \"    \\\"bool\\\": {\\n\" +\n" +
                "                \"      \\\"filter\\\": {\\n\" +\n" +
                "                \"        \\\"term\\\": {\\n\" +\n" +
                "                \"          \\\"sex\\\": \\\"male\\\"\\n\" +\n" +
                "                \"        }\\n\" +\n" +
                "                \"      },\\n\" +\n" +
                "                \"      \\\"must\\\": [\\n\" +\n" +
                "                \"        {\\n\" +\n" +
                "                \"          \\\"match\\\": {\\n\" +\n" +
                "                \"            \\\"favo\\\": \\\"球\\\"\\n\" +\n" +
                "                \"          }\\n\" +\n" +
                "                \"        }\\n\" +\n" +
                "                \"      ]\\n\" +\n" +
                "                \"    }\\n\" +\n" +
                "                \"  },\\n\" +\n" +
                "                \"  \\\"aggs\\\": {\\n\" +\n" +
                "                \"    \\\"group_by_sex\\\": {\\n\" +\n" +
                "                \"      \\\"terms\\\": {\\n\" +\n" +
                "                \"        \\\"field\\\": \\\"sex\\\",\\n\" +\n" +
                "                \"        \\\"size\\\": 10\\n\" +\n" +
                "                \"      }\\n\" +\n" +
                "                \"    }\\n\" +\n" +
                "                \"  },\\n\" +\n" +
                "                \"  \\\"from\\\": 0,\\n\" +\n" +
                "                \"  \\\"size\\\": 20\\n\" +\n" +
                "                \"}").addIndex("student")
                .addType("_doc")
                .build();
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
