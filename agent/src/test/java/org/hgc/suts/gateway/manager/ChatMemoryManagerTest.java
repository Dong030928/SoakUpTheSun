package org.hgc.suts.gateway.manager;

import org.hgc.suts.gateway.common.biz.user.UserContext;
import org.hgc.suts.gateway.common.biz.user.UserInfoDTO;
import org.hgc.suts.gateway.config.MemoryProperties;
import org.hgc.suts.gateway.config.MemoryVectorProperties;
import org.hgc.suts.gateway.config.ThreadPoolConfig;
import org.hgc.suts.gateway.service.VectorStorageService;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class ChatMemoryManagerTest {
    private final VectorStorageService vectors = mock(VectorStorageService.class);
    private final MemoryProperties properties = new MemoryProperties();
    private final MemoryVectorProperties vectorProperties = new MemoryVectorProperties();

    @Test
    void recallDeduplicatesAndHonorsItsCharacterBudget() {
        properties.setLongTermChars(7);
        when(vectors.searchMemory(42L, "query", 6)).thenReturn(List.of("abc", "abc", "defghi"));
        var manager = new ChatMemoryManager(vectors, Runnable::run, properties, vectorProperties);
        assertThat(manager.getRelatedHistory(42L, "query")).isEqualTo("abc\ndef");
    }

    @Test
    void rejectedMemoryWriteDoesNotThrowIntoTheCompletedConversation() {
        var manager = new ChatMemoryManager(vectors, task -> { throw new RejectedExecutionException(); }, properties, vectorProperties);
        assertThatCode(() -> manager.saveHistory(42L, "hello", "answer")).doesNotThrowAnyException();
        verifyNoInteractions(vectors);
    }

    @Test
    void writesAreActuallyAsynchronousAndDoNotCarryLoginState() throws Exception {
        var worker = new ThreadPoolConfig().memoryTaskExecutor();
        worker.initialize();
        var started = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var finished = new CountDownLatch(1);
        var token = new AtomicReference<String>();
        var user = new AtomicReference<Long>();
        var separateThread = new AtomicBoolean();
        Thread caller = Thread.currentThread();
        doAnswer(invocation -> {
            token.set(UserContext.getToken());
            user.set(UserContext.getUserId());
            separateThread.set(Thread.currentThread() != caller);
            started.countDown();
            try { release.await(5, TimeUnit.SECONDS); }
            finally { finished.countDown(); }
            return null;
        }).when(vectors).saveMemory(eq(42L), anyString());
        UserContext.setUser(UserInfoDTO.builder().id(42L).build());
        UserContext.setToken("test-login-token");
        try {
            var manager = new ChatMemoryManager(vectors, worker, properties, vectorProperties);
            manager.saveHistory(42L, "hello", "answer");
            assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(finished.getCount()).isEqualTo(1);
            assertThat(token.get()).isNull();
            assertThat(user.get()).isNull();
            assertThat(separateThread.get()).isTrue();
        } finally {
            release.countDown();
            finished.await(5, TimeUnit.SECONDS);
            worker.shutdown();
            UserContext.remove();
        }
    }

    @Test
    void documentBudgetAndFrozenWritesAreEnforcedBeforeEnqueueing() {
        vectorProperties.setMaxDocumentChars(40);
        var manager = new ChatMemoryManager(vectors, Runnable::run, properties, vectorProperties);
        manager.saveHistory(42L, "question".repeat(20), "answer".repeat(20));
        verify(vectors).saveMemory(eq(42L), argThat(text -> text.length() <= 40));
        clearInvocations(vectors);
        properties.setWritesEnabled(false);
        manager.saveHistory(42L, "question", "answer");
        verifyNoInteractions(vectors);
    }
}
