package com.notification_system.stream;

import com.notification_system.config.JsonSerde;
import com.notification_system.model.NotificationEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Set;

@Configuration
public class NotificationStreamProcessor {

    private static final Logger log = LoggerFactory.getLogger(NotificationStreamProcessor.class);

    // fallback rule (still useful)
    private static final Set<String> IMMEDIATE_TYPES = Set.of("comment", "follow");

    @Bean
    public KStream<String, NotificationEvent> process(StreamsBuilder builder) {

        log.info("ðŸš€ Notification stream processor started");

        // ðŸ”¥ MULTI-TOPIC SOURCE (AI + normal)
        KStream<String, NotificationEvent> source = builder.stream(
                List.of("notifications", "notifications-high", "notifications-low"),
                Consumed.with(Serdes.String(), new JsonSerde<>(NotificationEvent.class))
        );

        // âœ… VALIDATION + NORMALIZATION
        KStream<String, NotificationEvent> valid = source
                .filter((key, event) -> {
                    boolean ok = event != null
                            && event.getUserId() != null
                            && !event.getUserId().isBlank()
                            && event.getType() != null
                            && !event.getType().isBlank();

                    if (!ok) {
                        log.warn("âŒ Dropping invalid event: {}", event);
                    }
                    return ok;
                })
                .mapValues(this::normalizeEvent)
                .peek((k, v) -> log.info(
                        "ðŸ“© Incoming â†’ user={}, type={}, priority={}",
                        v.getUserId(), v.getType(), v.getPriority()
                ));

        // ============================================
        // ðŸ”¥ PRIORITY-BASED ROUTING (AI CORE LOGIC)
        // ============================================

        KStream<String, NotificationEvent>[] priorityBranches = valid.branch(
                (key, event) -> "HIGH".equalsIgnoreCase(event.getPriority()),
                (key, event) -> "LOW".equalsIgnoreCase(event.getPriority()),
                (key, event) -> true
        );

        KStream<String, NotificationEvent> highPriority = priorityBranches[0];
        KStream<String, NotificationEvent> lowPriority = priorityBranches[1];
        KStream<String, NotificationEvent> normalPriority = priorityBranches[2];

        // ============================================
        // ðŸ”¥ HIGH PRIORITY â†’ DIRECT DELIVERY (NO BATCH)
        // ============================================

        highPriority
                .peek((k, v) -> log.info("ðŸ”¥ HIGH PRIORITY â†’ instant send: {}", v))
                .to("aggregated-notifications",
                        Produced.with(Serdes.String(), new JsonSerde<>(NotificationEvent.class)));

        // ============================================
        // âš¡ NORMAL + LOW â†’ CONTINUE PIPELINE
        // ============================================

        KStream<String, NotificationEvent> processingStream =
                normalPriority.merge(lowPriority);

        // ============================================
        // ðŸ” EXISTING LOGIC (IMMEDIATE + AGGREGATION)
        // ============================================

        var branches = processingStream.split(Named.as("route-"))
                .branch((key, event) ->
                                IMMEDIATE_TYPES.contains(event.getType()),
                        Branched.as("immediate"))
                .defaultBranch(Branched.as("aggregate"));

        KStream<String, NotificationEvent> immediateStream =
                branches.get("route-immediate");

        KStream<String, NotificationEvent> aggregateStream =
                branches.get("route-aggregate");

        // ============================================
        // âš¡ IMMEDIATE EVENTS
        // ============================================

        immediateStream
                .map((key, event) -> {
                    NotificationEvent out = new NotificationEvent();
                    out.setUserId(event.getUserId());
                    out.setType(event.getType());
                    out.setPriority(event.getPriority()); // âœ… preserve priority
                    out.setCount(1);
                    out.setMessage(buildImmediateMessage(event.getType()));
                    out.setTimestamp(System.currentTimeMillis());
                    return new KeyValue<>(event.getUserId(), out);
                })
                .peek((k, v) -> log.info(
                        "âš¡ Immediate notification â†’ user={}, type={}, priority={}",
                        v.getUserId(), v.getType(), v.getPriority()
                ))
                .to("aggregated-notifications",
                        Produced.with(Serdes.String(), new JsonSerde<>(NotificationEvent.class)));

        // ============================================
        // ðŸ“Š AGGREGATION (WINDOW)
        // ============================================

        KGroupedStream<String, NotificationEvent> grouped =
                aggregateStream.groupBy(
                        (key, event) -> event.getUserId(),
                        Grouped.with(Serdes.String(), new JsonSerde<>(NotificationEvent.class))
                );

        grouped
                .windowedBy(TimeWindows.ofSizeAndGrace(
                        Duration.ofSeconds(5),
                        Duration.ofSeconds(2)))
                .emitStrategy(EmitStrategy.onWindowClose())
                .aggregate(
                        () -> {
                            NotificationEvent e = new NotificationEvent();
                            e.setCount(0);
                            return e;
                        },
                        (userId, newEvent, aggEvent) -> {
                            aggEvent.setUserId(userId);
                            aggEvent.setType(newEvent.getType());
                            aggEvent.setPriority(newEvent.getPriority()); // âœ… keep priority
                            aggEvent.setCount(aggEvent.getCount() + 1);
                            aggEvent.setTimestamp(System.currentTimeMillis());
                            return aggEvent;
                        },
                        Materialized.with(
                                Serdes.String(),
                                new JsonSerde<>(NotificationEvent.class))
                )
                .toStream()
                .map((windowedKey, event) -> {
                    String userId = windowedKey.key();

                    NotificationEvent out = new NotificationEvent();
                    out.setUserId(userId);
                    out.setType(event.getType());
                    out.setPriority(event.getPriority());
                    out.setCount(event.getCount());
                    out.setMessage(event.getCount() + " " + event.getType() + " in last 5 seconds");
                    out.setTimestamp(System.currentTimeMillis());

                    return new KeyValue<>(userId, out);
                })
                .filter((key, event) -> event.getCount() >= 1)
                .peek((k, v) -> log.info(
                        "ðŸ“Š Aggregated â†’ user={}, type={}, count={}, priority={}",
                        v.getUserId(), v.getType(), v.getCount(), v.getPriority()
                ))
                .to("aggregated-notifications",
                        Produced.with(Serdes.String(), new JsonSerde<>(NotificationEvent.class)));

        return source;
    }

    private NotificationEvent normalizeEvent(NotificationEvent event) {
        event.setType(event.getType().trim().toLowerCase(Locale.ROOT));
        event.setUserId(event.getUserId().trim());
        return event;
    }

    private String buildImmediateMessage(String type) {
        if ("follow".equals(type)) return "New follower";
        if ("comment".equals(type)) return "New comment";
        return "New notification";
    }
}
