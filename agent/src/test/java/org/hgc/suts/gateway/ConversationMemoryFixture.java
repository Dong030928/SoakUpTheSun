package org.hgc.suts.gateway;

import org.hgc.suts.gateway.memory.RedisConversationMemoryRepository;
import org.springframework.ai.chat.memory.InMemoryChatMemoryRepository;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Each configured target owns its own history; requests to that target share only that history. */
public final class ConversationMemoryFixture {
    private ConversationMemoryFixture() { }

    public static RedisConversationMemoryRepository fake() {
        RedisConversationMemoryRepository target = mock(RedisConversationMemoryRepository.class);
        configure(target);
        return target;
    }

    public static void configure(RedisConversationMemoryRepository target) {
        InMemoryChatMemoryRepository storage = new InMemoryChatMemoryRepository();
        when(target.open(anyString())).thenAnswer(invocation -> {
            RedisConversationMemoryRepository.Lease lease = mock(RedisConversationMemoryRepository.Lease.class);
            when(lease.repository()).thenReturn(storage);
            when(lease.commit(org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.anyString(),
                    org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.anyList())).thenCallRealMethod();
            String id = invocation.getArgument(0);
            when(lease.hasStoredSnapshot()).thenAnswer(ignored -> !storage.findByConversationId(id).isEmpty());
            return lease;
        });
    }
}
