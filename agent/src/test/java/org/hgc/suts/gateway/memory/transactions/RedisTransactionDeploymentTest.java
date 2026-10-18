package org.hgc.suts.gateway.memory.transactions;

import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisClusterConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class RedisTransactionDeploymentTest {
    @Test void actualClusterConnectionCannotBypassTheConfigurationGuardThroughIndexedProperties() {
        var redis = mock(StringRedisTemplate.class);
        var values = mock(ValueOperations.class);
        var factory = mock(RedisConnectionFactory.class);
        var connection = mock(RedisClusterConnection.class);
        when(redis.opsForValue()).thenReturn(values);
        when(values.get(RedisTransactionLedger.GENERATION_KEY)).thenReturn("agent-transactions-integration-only");
        when(redis.getConnectionFactory()).thenReturn(factory);
        when(factory.getConnection()).thenReturn(connection);
        var ledger = new RedisTransactionLedger(redis, TransactionTestConfig.create(), new TransactionMetrics());
        assertThatThrownBy(ledger::verifyDeployment).isInstanceOf(IllegalStateException.class).hasMessageContaining("standalone Redis");
        verify(connection).close();
    }
}
