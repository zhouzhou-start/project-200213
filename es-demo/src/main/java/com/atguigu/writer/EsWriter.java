package com.atguigu.writer;

import com.atguigu.bean.student1;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.DocumentResult;
import io.searchbox.core.Index;

import java.io.IOException;

public class EsWriter {
    public static void main(String[] args) throws IOException {
        JestClientFactory jestClientFactory = new JestClientFactory();
        HttpClientConfig build = new HttpClientConfig.Builder("http://hadoop105:9200").build();
        jestClientFactory.setHttpClientConfig(build);
        JestClient object = jestClientFactory.getObject();
        student1 student1 = new student1("006", "wulala", 25, "傻笑");
        Index build1 = new Index.Builder(student1).index("student1").type("_doc").id("1006")
                .build();
        object.execute(build1);
        object.close();

    }
}
