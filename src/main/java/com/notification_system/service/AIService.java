package com.notification_system.service;

import com.notification_system.model.NotificationEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.*;

@Service
public class AIService {

    private final RestTemplate restTemplate = new RestTemplate();

    // ✅ Read API key from application.properties
    @Value("${gemini.api.key}")
    private String apiKey;

    private static final String GEMINI_URL =
            "https://generativelanguage.googleapis.com/v1/models/gemini-2.5-flash:generateContent?key=";
    // ================================
    // 🔥 PRIORITY CLASSIFICATION
    // ================================
    public String getPriority(NotificationEvent event) {

        String prompt = """
                Classify this notification into one word ONLY:
                HIGH, MEDIUM, or LOW.

                Rules:
                - HIGH: financial, security, urgent
                - MEDIUM: important but not urgent
                - LOW: promotional or informational

                Notification:
                """ + event.getMessage();

        String response = callGemini(prompt);

        if (response.contains("HIGH")) return "HIGH";
        if (response.contains("LOW")) return "LOW";
        return "MEDIUM";
    }

    // ================================
    // 🔁 SMART RETRY DECISION
    // ================================
    public String shouldRetry(NotificationEvent event) {

        String prompt = """
                A notification failed.

                Decide ONE option:
                RETRY or DROP

                Notification:
                """ + event.getMessage();

        String response = callGemini(prompt);

        if (response.contains("DROP")) return "DROP";
        return "RETRY";
    }

    // ================================
    // ✍️ MESSAGE ENHANCEMENT (OPTIONAL)
    // ================================
    public String enhanceMessage(String message) {

        String prompt = """
                Improve this notification message to be more user-friendly and clear:

                """ + message;

        return callGemini(prompt);
    }

    // ================================
    // 🔥 CORE GEMINI CALL
    // ================================
    private String callGemini(String prompt) {

        String url = GEMINI_URL + apiKey;

        Map<String, Object> body = new HashMap<>();
        body.put("contents", List.of(
                Map.of("parts", List.of(
                        Map.of("text", prompt)
                ))
        ));

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        HttpEntity<Map<String, Object>> request =
                new HttpEntity<>(body, headers);

        ResponseEntity<Map> response = restTemplate.postForEntity(
                url,
                request,
                Map.class
        );

        try {
            Map candidate = (Map) ((List) response.getBody().get("candidates")).get(0);
            Map content = (Map) candidate.get("content");
            Map part = (Map) ((List) content.get("parts")).get(0);

            return part.get("text").toString().toUpperCase().trim();

        } catch (Exception e) {
            return "MEDIUM"; // fallback safe default
        }
    }
}
