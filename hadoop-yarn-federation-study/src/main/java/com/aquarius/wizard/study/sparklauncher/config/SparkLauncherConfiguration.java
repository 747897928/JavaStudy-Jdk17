package com.aquarius.wizard.study.sparklauncher.config;

import com.aquarius.wizard.study.sparklauncher.repository.SubmissionRecordRepository;
import com.aquarius.wizard.study.sparklauncher.repository.memory.InMemorySubmissionRecordRepository;
import com.aquarius.wizard.study.sparklauncher.service.SparkLauncherSubmissionService;
import com.aquarius.wizard.study.sparklauncher.service.YarnApplicationStatusService;
import com.aquarius.wizard.study.sparklauncher.support.YarnClientPool;
import org.apache.hadoop.conf.Configuration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import java.nio.file.Files;
import java.nio.file.Path;

@org.springframework.context.annotation.Configuration
@EnableConfigurationProperties(SparkLauncherProperties.class)
/**
 * 中文说明：
 * 这里集中管理 Spark 提交网关依赖的 Bean。
 * Hadoop Configuration 只在应用启动时加载一次，避免每次请求重复读 XML 配置文件。
 */
public class SparkLauncherConfiguration {

    @Bean
    public SubmissionRecordRepository submissionRecordRepository() {
        return new InMemorySubmissionRecordRepository();
    }

    @Bean
    public Configuration hadoopConfiguration(SparkLauncherProperties properties) {
        Configuration configuration = new Configuration(false);
        addResourceIfExists(configuration, properties.getHadoopConfDir().resolve("core-site.xml"));
        addResourceIfExists(configuration, properties.getHadoopConfDir().resolve("hdfs-site.xml"));
        addResourceIfExists(configuration, properties.getHadoopConfDir().resolve("mapred-site.xml"));
        addResourceIfExists(configuration, properties.getHadoopConfDir().resolve("yarn-site.xml"));
        return configuration;
    }

    @Bean(initMethod = "start", destroyMethod = "close")
    public YarnClientPool yarnClientPool(Configuration hadoopConfiguration, SparkLauncherProperties properties) {
        return new YarnClientPool(
                hadoopConfiguration,
                properties.getStatusClientPoolSize(),
                properties.getStatusClientMaxIdleTime()
        );
    }

    @Bean
    public SparkLauncherSubmissionService sparkLauncherSubmissionService(
            SparkLauncherProperties properties,
            SubmissionRecordRepository submissionRecordRepository
    ) {
        return new SparkLauncherSubmissionService(properties, submissionRecordRepository);
    }

    @Bean
    public YarnApplicationStatusService yarnApplicationStatusService(
            SubmissionRecordRepository submissionRecordRepository,
            YarnClientPool yarnClientPool,
            Configuration hadoopConfiguration,
            SparkLauncherProperties properties
    ) {
        return new YarnApplicationStatusService(
                submissionRecordRepository,
                yarnClientPool,
                hadoopConfiguration,
                properties
        );
    }

    private static void addResourceIfExists(Configuration configuration, Path file) {
        if (Files.exists(file)) {
            configuration.addResource(new org.apache.hadoop.fs.Path(file.toUri()));
        }
    }
}
