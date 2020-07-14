package com.atguigu.writer;


import com.atguigu.bean.student1;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;

import java.io.IOException;

public class EsWriterByBulk {
    public static void main(String[] args) throws IOException {
        JestClientFactory jestClientFactory = new JestClientFactory();
        HttpClientConfig build = new HttpClientConfig.Builder("http://hadoop105:9200").build();
        jestClientFactory.setHttpClientConfig(build);
        JestClient object = jestClientFactory.getObject();
        student1 student1 = new student1("007", "huahua", 25, "刷抖音");
        student1 student2 = new student1("008", "wangjing", 23, "视频");
        student1 student3 = new student1("009", "chenxiang", 26, "敲代码");
        Index build1 = new Index.Builder(student1).id("1007").build();
        Index build2 = new Index.Builder(student2).id("1008").build();
        Index build3 = new Index.Builder(student3).id("1009").build();
        Bulk build4 = new Bulk.Builder()
                .addAction(build1)
                .addAction(build2)
                .addAction(build3)
                .defaultIndex("student1")
                .defaultType("_doc")
                .build();
        object.execute(build4);
        object.close();
    }
}
