package com.notification_system.consumer;

import com.notification_system.model.NotificationDLQEntity;
import com.notification_system.model.NotificationEntity;
import com.notification_system.model.NotificationEvent;
import com.notification_system.repository.NotificationDLQRepository;
import com.notification_system.repository.NotificationRepository;
import com.notification_system.service.AIService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Service;

@Service
public class NotificationConsumer {

    private static final Logger log = LoggerFactory.getLogger(NotificationConsumer.class);

    private final NotificationRepository repository;
    private final NotificationDLQRepository dlqRepository;
    private final AIService aiService;
    private final SimpMessagingTemplate messagingTemplate;

    public NotificationConsumer(NotificationRepository repository,
                                NotificationDLQRepository dlqRepository,
                                AIService aiService,
                                SimpMessagingTemplate messagingTemplate) {
        this.repository = repository;
        this.dlqRepository = dlqRepository;
        this.aiService = aiService;
        this.messagingTemplate = messagingTemplate;
    }

    // ============================================
    // 🔁 MAIN CONSUMER WITH AI RETRY LOGIC
    // ============================================

    @RetryableTopic(
            attempts = "3",
            backoff = @Backoff(delay = 2000, multiplier = 2),
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
            dltTopicSuffix = "-dlq"
    )
    @KafkaListener(
            topics = "aggregated-notifications",
            groupId = "notification-group"
    )
    public void consume(NotificationEvent event) {

        log.info("📩 Received notification event: {}", event);

        // ============================================
        // ❌ VALIDATION + AI DECISION
        // ============================================

        if (event == null || event.getType() == null) {

            log.warn("⚠️ Invalid event detected → invoking AI decision");

            String decision = aiService.shouldRetry(
                    event != null ? event : new NotificationEvent()
            );

            log.info("🤖 AI Decision: {}", decision);

            if ("DROP".equalsIgnoreCase(decision)) {

                log.error("❌ Dropping event → sending to DLQ");

                dlqRepository.save(mapToDLQEntity(event));
                return;
            }

            throw new RuntimeException("Retrying based on AI decision");
        }

        // ============================================
        // 🔥 NORMAL FLOW
        // ============================================

        NotificationEntity entity = mapToEntity(event);
        repository.save(entity);

        // 🔥 WebSocket push
        messagingTemplate.convertAndSend("/topic/notifications", event);

        log.info(
                "✅ Notification sent → userId={}, type={}, priority={}",
                event.getUserId(),
                event.getType(),
                event.getPriority()
        );
    }

    // ============================================
    // 🧱 ENTITY MAPPING
    // ============================================

    private NotificationEntity mapToEntity(NotificationEvent event) {
        NotificationEntity entity = new NotificationEntity();
        entity.setUserId(event.getUserId());
        entity.setType(event.getType());
        entity.setMessage(event.getMessage());
        entity.setCreatedAt(System.currentTimeMillis());
        return entity;
    }

    private NotificationDLQEntity mapToDLQEntity(NotificationEvent event) {
        NotificationDLQEntity entity = new NotificationDLQEntity();

        if (event != null) {
            entity.setUserId(event.getUserId());
            entity.setType(event.getType());
            entity.setMessage(event.getMessage());
        } else {
            entity.setMessage("NULL EVENT");
        }

        entity.setCreatedAt(System.currentTimeMillis());
        return entity;
    }

    // ============================================
    // 💀 DLQ CONSUMER
    // ============================================

    @KafkaListener(
            topics = "aggregated-notifications-dlq",
            groupId = "dlq-group"
    )
    public void consumeDLQ(NotificationEvent event) {

        log.error("💀 DLQ event received: {}", event);

        NotificationDLQEntity entity = mapToDLQEntity(event);
        dlqRepository.save(entity);
    }

    // ============================================
    // 🚨 FINAL FAILURE HANDLER
    // ============================================

    @DltHandler
    public void handleDLT(NotificationEvent event) {
        log.error("🚨 FINAL FAILURE → DLT handler triggered: {}", event);
    }
}
