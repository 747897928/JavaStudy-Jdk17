package com.aquarius.wizard.study.sparklauncher.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.nio.file.Path;
import java.time.Duration;

@ConfigurationProperties(prefix = "study.spark")
/**
 * Spark 提交网关的配置项。
 * 统一管理 Spark/YARN/Kerberos 以及状态查询连接池相关参数。
 */
public class SparkLauncherProperties {

    private Path sparkHome = Path.of("/opt/spark");
    private Path hadoopConfDir = Path.of("/etc/hadoop-router-client");
    private String principal = "sparkuser@EXAMPLE.COM";
    private Path keytab = Path.of("/etc/security/keytabs/sparkuser.keytab");
    private Duration appIdWaitTimeout = Duration.ofSeconds(8);
    private String defaultQueue = "root.batch";
    private String defaultDriverMemory = "2g";
    private String defaultExecutorMemory = "4g";
    private Integer defaultExecutorInstances = 2;
    private String deployMode = "cluster";
    private String routerBaseUrl;
    private Integer statusClientPoolSize = 4;
    private Duration statusClientBorrowTimeout = Duration.ofSeconds(3);
    private Duration statusClientMaxIdleTime = Duration.ofMinutes(5);

    public Path getSparkHome() {
        return sparkHome;
    }

    public void setSparkHome(Path sparkHome) {
        this.sparkHome = sparkHome;
    }

    public Path getHadoopConfDir() {
        return hadoopConfDir;
    }

    public void setHadoopConfDir(Path hadoopConfDir) {
        this.hadoopConfDir = hadoopConfDir;
    }

    public String getPrincipal() {
        return principal;
    }

    public void setPrincipal(String principal) {
        this.principal = principal;
    }

    public Path getKeytab() {
        return keytab;
    }

    public void setKeytab(Path keytab) {
        this.keytab = keytab;
    }

    public Duration getAppIdWaitTimeout() {
        return appIdWaitTimeout;
    }

    public void setAppIdWaitTimeout(Duration appIdWaitTimeout) {
        this.appIdWaitTimeout = appIdWaitTimeout;
    }

    public String getDefaultQueue() {
        return defaultQueue;
    }

    public void setDefaultQueue(String defaultQueue) {
        this.defaultQueue = defaultQueue;
    }

    public String getDefaultDriverMemory() {
        return defaultDriverMemory;
    }

    public void setDefaultDriverMemory(String defaultDriverMemory) {
        this.defaultDriverMemory = defaultDriverMemory;
    }

    public String getDefaultExecutorMemory() {
        return defaultExecutorMemory;
    }

    public void setDefaultExecutorMemory(String defaultExecutorMemory) {
        this.defaultExecutorMemory = defaultExecutorMemory;
    }

    public Integer getDefaultExecutorInstances() {
        return defaultExecutorInstances;
    }

    public void setDefaultExecutorInstances(Integer defaultExecutorInstances) {
        this.defaultExecutorInstances = defaultExecutorInstances;
    }

    public String getDeployMode() {
        return deployMode;
    }

    public void setDeployMode(String deployMode) {
        this.deployMode = deployMode;
    }

    public String getRouterBaseUrl() {
        return routerBaseUrl;
    }

    public void setRouterBaseUrl(String routerBaseUrl) {
        this.routerBaseUrl = routerBaseUrl;
    }

    public Integer getStatusClientPoolSize() {
        return statusClientPoolSize;
    }

    public void setStatusClientPoolSize(Integer statusClientPoolSize) {
        this.statusClientPoolSize = statusClientPoolSize;
    }

    public Duration getStatusClientBorrowTimeout() {
        return statusClientBorrowTimeout;
    }

    public void setStatusClientBorrowTimeout(Duration statusClientBorrowTimeout) {
        this.statusClientBorrowTimeout = statusClientBorrowTimeout;
    }

    public Duration getStatusClientMaxIdleTime() {
        return statusClientMaxIdleTime;
    }

    public void setStatusClientMaxIdleTime(Duration statusClientMaxIdleTime) {
        this.statusClientMaxIdleTime = statusClientMaxIdleTime;
    }
}
