package com.aquarius.wizard.study.sparklauncher;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 应用入口。
 * 这个模块既承载学习文档，也承载一个可继续演进的 Spark 提交网关代码骨架。
 */
@SpringBootApplication
public class HadoopYarnFederationStudyApplication {

    public static void main(String[] args) {
        SpringApplication.run(HadoopYarnFederationStudyApplication.class, args);
    }
}
