package com.aquarius.wizard.study.sparklauncher;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.nio.file.Path;
import java.time.Duration;

@ConfigurationProperties(prefix = "study.spark")
public class SparkGatewayProperties {

    private Path sparkHome = Path.of("/opt/spark");
    private Path hadoopConfDir = Path.of("/etc/hadoop-router-client");
    private String principal = "sparkuser@EXAMPLE.COM";
    private Path keytab = Path.of("/etc/security/keytabs/sparkuser.keytab");
    private Duration appIdWaitTimeout = Duration.ofSeconds(8);

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
}
