package org.hgc.suts.gateway.config;

import com.alibaba.ttl.threadpool.TtlExecutors;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;

@Configuration
public class ThreadPoolConfig {

    @Bean("aiWorkerPool")
    public ThreadPoolTaskExecutor aiWorkerPool() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();

        // 核心线程
        executor.setCorePoolSize(200);
        // 最大线程
        executor.setMaxPoolSize(200);
        // 队列容量
        executor.setQueueCapacity(100);
        executor.setThreadNamePrefix("suts-gateway-thread-");

        // 2. 拒绝策略：直接抛出异常
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy());

        // Spring initializes and shuts down the underlying pool with the application.
        return executor;
    }

    @Bean("aiTaskExecutor")
    @Primary
    public Executor aiTaskExecutor(@Qualifier("aiWorkerPool") ThreadPoolTaskExecutor executor) {
        // Preserve authentication propagation without hiding the pool's lifecycle from Spring.
        return TtlExecutors.getTtlExecutor(executor);
    }
}
