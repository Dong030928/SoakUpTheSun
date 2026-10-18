package org.hgc.suts.gateway.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.RedisClient;

import java.net.URI;
import java.time.Duration;

/** Both the native vector store and migration command use the application's Redis DB. */
@Configuration(proxyBeanMethods = false)
public class MemoryVectorConfiguration {
    @Bean(destroyMethod = "close")
    public RedisClient memoryRedisClient(
            @Value("${spring.data.redis.host:localhost}") String host,
            @Value("${spring.data.redis.port:6379}") int port,
            @Value("${spring.data.redis.username:}") String username,
            @Value("${spring.data.redis.password:}") String password,
            @Value("${spring.data.redis.database:0}") int database,
            @Value("${spring.data.redis.ssl.enabled:false}") boolean ssl,
            @Value("${spring.data.redis.url:}") String url,
            @Value("${spring.data.redis.timeout:5s}") Duration timeout,
            @Value("${spring.data.redis.connect-timeout:5s}") Duration connectTimeout) {
        Assert.isTrue(timeout.toMillis() > 0 && timeout.toMillis() <= Integer.MAX_VALUE,
                "Redis socket timeout must be positive and finite");
        Assert.isTrue(connectTimeout.toMillis() > 0 && connectTimeout.toMillis() <= Integer.MAX_VALUE,
                "Redis connect timeout must be positive and finite");
        var builder = DefaultJedisClientConfig.builder();
        if (StringUtils.hasText(url)) {
            URI uri;
            try { uri = URI.create(url); }
            catch (IllegalArgumentException ex) { throw new IllegalArgumentException("Redis URL is invalid"); }
            Assert.isTrue(("redis".equals(uri.getScheme()) || "rediss".equals(uri.getScheme())) && uri.getHost() != null,
                    "Redis URL must use redis:// or rediss://");
            builder = DefaultJedisClientConfig.builder(uri);
            host = uri.getHost();
            port = uri.getPort() > 0 ? uri.getPort() : 6379;
        } else {
            builder.database(database).ssl(ssl);
            if (StringUtils.hasText(username)) builder.user(username);
            if (StringUtils.hasText(password)) builder.password(password);
        }
        builder.socketTimeoutMillis((int) timeout.toMillis()).connectionTimeoutMillis((int) connectTimeout.toMillis());
        return RedisClient.builder().hostAndPort(host, port).clientConfig(builder.build()).build();
    }
}
