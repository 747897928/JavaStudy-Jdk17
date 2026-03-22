package com.aquarius.wizard.springdatar2dbcdemo.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

/**
 * 数据库配置项。
 * <p>
 * 学习时重点看：
 * <p>
 * - writer-url / reader-url 如何区分读写职责
 * - pool 里的生命周期参数如何影响主从切换后的连接刷新速度
 */
@Validated
@ConfigurationProperties(prefix = "demo.database")
public class DemoDatabaseProperties {

    @NotBlank
    private String writerUrl;

    @NotBlank
    private String readerUrl;

    @NotBlank
    private String username;

    @NotBlank
    private String password;

    private boolean initializeSchema = true;

    @Valid
    private Pool pool = new Pool();

    public String getWriterUrl() {
        return writerUrl;
    }

    public void setWriterUrl(String writerUrl) {
        this.writerUrl = writerUrl;
    }

    public String getReaderUrl() {
        return readerUrl;
    }

    public void setReaderUrl(String readerUrl) {
        this.readerUrl = readerUrl;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public boolean isInitializeSchema() {
        return initializeSchema;
    }

    public void setInitializeSchema(boolean initializeSchema) {
        this.initializeSchema = initializeSchema;
    }

    public Pool getPool() {
        return pool;
    }

    public void setPool(Pool pool) {
        this.pool = pool;
    }

    /**
     * R2DBC 连接池参数。
     * <p>
     * 这些参数不只是性能调优项，在主从切换场景下也影响旧连接被淘汰的速度。
     */
    public static class Pool {

        @Min(0)
        private int initialSize = 2;

        @Min(1)
        private int maxSize = 20;

        @Min(1)
        private int acquireRetry = 3;

        private Duration maxIdleTime = Duration.ofSeconds(30);

        private Duration maxLifeTime = Duration.ofMinutes(2);

        private Duration backgroundEvictionInterval = Duration.ofSeconds(15);

        private Duration maxAcquireTime = Duration.ofSeconds(10);

        private Duration maxCreateConnectionTime = Duration.ofSeconds(5);

        private Duration maxValidationTime = Duration.ofSeconds(5);

        private boolean validateWriterRoleOnAcquire = true;

        @NotBlank
        private String validationQuery = "SELECT 1";

        public int getInitialSize() {
            return initialSize;
        }

        public void setInitialSize(int initialSize) {
            this.initialSize = initialSize;
        }

        public int getMaxSize() {
            return maxSize;
        }

        public void setMaxSize(int maxSize) {
            this.maxSize = maxSize;
        }

        public int getAcquireRetry() {
            return acquireRetry;
        }

        public void setAcquireRetry(int acquireRetry) {
            this.acquireRetry = acquireRetry;
        }

        public Duration getMaxIdleTime() {
            return maxIdleTime;
        }

        public void setMaxIdleTime(Duration maxIdleTime) {
            this.maxIdleTime = maxIdleTime;
        }

        public Duration getMaxLifeTime() {
            return maxLifeTime;
        }

        public void setMaxLifeTime(Duration maxLifeTime) {
            this.maxLifeTime = maxLifeTime;
        }

        public Duration getBackgroundEvictionInterval() {
            return backgroundEvictionInterval;
        }

        public void setBackgroundEvictionInterval(Duration backgroundEvictionInterval) {
            this.backgroundEvictionInterval = backgroundEvictionInterval;
        }

        public Duration getMaxAcquireTime() {
            return maxAcquireTime;
        }

        public void setMaxAcquireTime(Duration maxAcquireTime) {
            this.maxAcquireTime = maxAcquireTime;
        }

        public Duration getMaxCreateConnectionTime() {
            return maxCreateConnectionTime;
        }

        public void setMaxCreateConnectionTime(Duration maxCreateConnectionTime) {
            this.maxCreateConnectionTime = maxCreateConnectionTime;
        }

        public Duration getMaxValidationTime() {
            return maxValidationTime;
        }

        public void setMaxValidationTime(Duration maxValidationTime) {
            this.maxValidationTime = maxValidationTime;
        }

        public boolean isValidateWriterRoleOnAcquire() {
            return validateWriterRoleOnAcquire;
        }

        public void setValidateWriterRoleOnAcquire(boolean validateWriterRoleOnAcquire) {
            this.validateWriterRoleOnAcquire = validateWriterRoleOnAcquire;
        }

        public String getValidationQuery() {
            return validationQuery;
        }

        public void setValidationQuery(String validationQuery) {
            this.validationQuery = validationQuery;
        }
    }
}
