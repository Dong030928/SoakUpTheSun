package org.hgc.suts.gateway.memory.transactions;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.hgc.suts.gateway.config.MemoryTransactionProperties;

import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/** Classic remoting transaction protocol, verified with client/Broker 5.4.0 on Java 17 and 21. */
@Slf4j
public final class RocketMqTransactionTransport implements TransactionTransport {
    public static final String TAG = "conversation-turn-v1";
    private final TransactionMQProducer producer;
    private final DefaultMQPushConsumer consumer;
    private final MemoryTransactionProperties config;
    private final RedisTransactionLedger ledger;
    private final TransactionMetrics metrics;
    private final ThreadPoolExecutor checks;
    private final java.util.concurrent.atomic.AtomicBoolean closed = new java.util.concurrent.atomic.AtomicBoolean();

    public RocketMqTransactionTransport(MemoryTransactionProperties config, RedisTransactionLedger ledger,
            ArchiveMessageConsumer listener, TransactionMetrics metrics) {
        this.config = config; this.ledger = ledger; this.metrics = metrics;
        String instance = "agent-memory-" + UUID.randomUUID();
        producer = new TransactionMQProducer(config.getProducerGroup());
        producer.setNamesrvAddr(config.getNameServer());
        producer.setInstanceName(instance);
        producer.setVipChannelEnabled(false);
        producer.setSendMsgTimeout(config.getSendTimeoutMs());
        producer.setRetryTimesWhenSendFailed(0);
        producer.setRetryTimesWhenSendAsyncFailed(0);
        producer.setRetryAnotherBrokerWhenNotStoreOK(false);
        producer.setMaxMessageSize(config.getMaxMessageBytes());
        producer.setClientCallbackExecutorThreads(2);
        checks = new ThreadPoolExecutor(2, 4, 60, TimeUnit.SECONDS, new ArrayBlockingQueue<>(100), r -> {
            Thread thread = new Thread(r, "agent-memory-transaction-check"); thread.setDaemon(true); return thread;
        }, new ThreadPoolExecutor.AbortPolicy());
        producer.setExecutorService(checks);
        producer.setTransactionListener(new TransactionListener() {
            @Override public LocalTransactionState executeLocalTransaction(Message message, Object argument) {
                try {
                    ArchiveEvent event = decode(message);
                    if (!(argument instanceof LocalWork work) || !work.event.equals(event)) return LocalTransactionState.UNKNOW;
                    return wire(work.callback.get());
                } catch (RuntimeException ex) {
                    metrics.unknown.increment();
                    return LocalTransactionState.UNKNOW;
                }
            }
            @Override public LocalTransactionState checkLocalTransaction(MessageExt message) {
                metrics.checks.increment();
                try { return wire(ledger.check(decode(message))); }
                catch (RuntimeException ex) { metrics.unknown.increment(); return LocalTransactionState.UNKNOW; }
            }
        });
        consumer = new DefaultMQPushConsumer(config.getConsumerGroup());
        consumer.setNamesrvAddr(config.getNameServer());
        consumer.setInstanceName(instance);
        consumer.setVipChannelEnabled(false);
        consumer.setClientCallbackExecutorThreads(2);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.setConsumeThreadMin(2);
        consumer.setConsumeThreadMax(4);
        consumer.setConsumeMessageBatchMaxSize(1);
        consumer.setMaxReconsumeTimes(config.getMaxReconsumeTimes());
        consumer.setAwaitTerminationMillisWhenShutdown(5000);
        consumer.registerMessageListener((MessageListenerConcurrently) (messages, context) -> {
            try {
                for (MessageExt message : messages) listener.consume(message.getBody(), message.getReconsumeTimes());
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            } catch (RuntimeException ex) { return ConsumeConcurrentlyStatus.RECONSUME_LATER; }
        });
    }

    public void start() {
        try {
            ledger.verifyDeployment();
            producer.start();
            // Register this fresh instance with the topic's Brokers even before its first foreground send.
            // Otherwise another node's transaction cannot be checked here until a later publish/route refresh.
            var factory = producer.getDefaultMQProducerImpl().getmQClientFactory();
            factory.updateTopicRouteInfoFromNameServer(config.getTopic());
            // A concurrent scheduled heartbeat may hold the try-lock; it will register the same producer set.
            factory.sendHeartbeatToAllBrokerWithLock();
            preflight();
            if (config.isConsumerEnabled()) {
                startConsumer();
            }
        } catch (Exception ex) {
            close();
            // Client exception messages may contain topology or credentials; retain only the exception class.
            throw new IllegalStateException("Transaction infrastructure startup failed: " + ex.getClass().getSimpleName());
        }
    }

    void startConsumer() throws Exception {
        consumer.subscribe(config.getTopic(), TAG);
        consumer.start();
    }

    java.util.List<String> brokerNames() throws Exception {
        return producer.getDefaultMQProducerImpl().getmQClientFactory().getMQClientAPIImpl()
                .getTopicRouteInfoFromNameServer(config.getTopic(), config.getSendTimeoutMs()).getBrokerDatas()
                .stream().map(org.apache.rocketmq.remoting.protocol.route.BrokerData::getBrokerName).toList();
    }

    @Override public void preflight() {
        try {
            var api = producer.getDefaultMQProducerImpl().getmQClientFactory().getMQClientAPIImpl();
            var route = api.getTopicRouteInfoFromNameServer(config.getTopic(), config.getSendTimeoutMs());
            String broker = route.getBrokerDatas().stream().map(data -> data.getBrokerAddrs().get(0L))
                    .filter(java.util.Objects::nonNull).findFirst().orElseThrow();
            // An actual Broker RPC detects an unavailable Broker before tools, not merely a cached name-server route.
            api.getBrokerRuntimeInfo(broker, config.getSendTimeoutMs());
        } catch (Exception ex) { throw new MemoryTransactionsUnavailableException(); }
    }

    @Override public State send(ArchiveEvent event, Supplier<State> localTransaction) {
        Message message = new Message(config.getTopic(), TAG, event.turn().eventId(), event.encode(config.getMaxMessageBytes()));
        try {
            var result = producer.sendMessageInTransaction(message, new LocalWork(event, localTransaction));
            if (result.getSendStatus() != SendStatus.SEND_OK) return State.UNKNOWN;
            return state(result.getLocalTransactionState());
        } catch (Exception ex) {
            throw new IllegalStateException("Transaction send result uncertain: " + ex.getClass().getSimpleName());
        }
    }
    private ArchiveEvent decode(Message message) {
        return ArchiveEvent.decode(message.getBody(), config.getMaxMessageBytes(), config.getGeneration());
    }
    private static LocalTransactionState wire(State state) {
        return switch (state) {
            case COMMITTED -> LocalTransactionState.COMMIT_MESSAGE;
            case ABORTED -> LocalTransactionState.ROLLBACK_MESSAGE;
            case UNKNOWN -> LocalTransactionState.UNKNOW;
        };
    }
    private static State state(LocalTransactionState state) {
        return switch (state) {
            case COMMIT_MESSAGE -> State.COMMITTED;
            case ROLLBACK_MESSAGE -> State.ABORTED;
            case UNKNOW -> State.UNKNOWN;
        };
    }
    @Override public void close() {
        if (closed.compareAndSet(false, true)) { consumer.shutdown(); producer.shutdown(); checks.shutdownNow(); }
    }
    private record LocalWork(ArchiveEvent event, Supplier<State> callback) { }
}
