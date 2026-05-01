package com.notification_system.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic notificationsTopic() {
        return new NewTopic("notifications", 12, (short) 1);
    }

    @Bean
    public NewTopic aggregatedNotificationsTopic() {
        return new NewTopic("aggregated-notifications", 12, (short) 1);
    }
}
