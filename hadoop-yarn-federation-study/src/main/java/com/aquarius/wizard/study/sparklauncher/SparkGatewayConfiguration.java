package com.aquarius.wizard.study.sparklauncher;

import org.apache.hadoop.conf.Configuration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import java.nio.file.Files;
import java.nio.file.Path;

@org.springframework.context.annotation.Configuration
@EnableConfigurationProperties(SparkGatewayProperties.class)
public class SparkGatewayConfiguration {

    @Bean
    public SubmissionRecordRepository submissionRecordRepository() {
        return new InMemorySubmissionRecordRepository();
    }

    @Bean
    public SparkLauncherSubmissionService sparkLauncherSubmissionService(
            SparkGatewayProperties properties,
            SubmissionRecordRepository submissionRecordRepository
    ) {
        return new SparkLauncherSubmissionService(properties, submissionRecordRepository);
    }

    @Bean
    public YarnApplicationStatusService yarnApplicationStatusService(SparkGatewayProperties properties) {
        Configuration configuration = new Configuration(false);
        addResourceIfExists(configuration, properties.getHadoopConfDir().resolve("core-site.xml"));
        addResourceIfExists(configuration, properties.getHadoopConfDir().resolve("hdfs-site.xml"));
        addResourceIfExists(configuration, properties.getHadoopConfDir().resolve("mapred-site.xml"));
        addResourceIfExists(configuration, properties.getHadoopConfDir().resolve("yarn-site.xml"));
        return new YarnApplicationStatusService(configuration);
    }

    private static void addResourceIfExists(Configuration configuration, Path file) {
        if (Files.exists(file)) {
            configuration.addResource(new org.apache.hadoop.fs.Path(file.toUri()));
        }
    }
}
