package com.notification_system.producer;

import com.notification_system.model.NotificationEvent;
import com.notification_system.service.AIService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducerService {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public KafkaProducerService(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }
    @Autowired
    private  AIService aiService;

//    public void sendEvent(NotificationEvent event) {
//        System.out.println("🔥 SENDING: " + event.getUserId());
//        kafkaTemplate.send("notifications", event.getUserId(), event);
//    }

    public void sendEvent(NotificationEvent event) {

        String priority = aiService.getPriority(event);

        // 🔥 ADD THIS LINE
        event.setPriority(priority);
        String topic = switch (priority) {
            case "HIGH" -> "notifications-high";
            case "LOW" -> "notifications-low";
            default -> "notifications";
        };

        kafkaTemplate.send(topic, event.getUserId(), event);
    }
}
