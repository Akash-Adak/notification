package com.notification_system.controller;

import com.notification_system.model.NotificationEvent;
import com.notification_system.producer.KafkaProducerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/events")
public class EventController {

    private static final Logger log = LoggerFactory.getLogger(EventController.class);

    private final KafkaProducerService producer;

    public EventController(KafkaProducerService producer) {
        this.producer = producer;
    }

    @PostMapping
    public ResponseEntity<String> send(@RequestBody NotificationEvent event) {

        if (event.getUserId() == null || event.getType() == null) {
            return ResponseEntity.badRequest().body("Invalid event ❌");
        }

        log.info("📥 Received event: {}", event);

        event.setTimestamp(System.currentTimeMillis());
        producer.sendEvent(event);

        return ResponseEntity.ok("Event sent successfully 🚀");
    }
}
