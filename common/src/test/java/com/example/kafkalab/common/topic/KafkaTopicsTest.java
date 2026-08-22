package com.example.kafkalab.common.topic;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaTopicsTest {

    @Test
    void everyTopicNameIsUniqueAndUsesTheDemoNamingConvention() throws IllegalAccessException {
        Set<String> topics = new HashSet<>();
        for (Field field : KafkaTopics.class.getDeclaredFields()) {
            if (field.getType() == String.class && Modifier.isStatic(field.getModifiers())) {
                String topic = (String) field.get(null);
                assertFalse(topic.isBlank(), () -> field.getName() + " must not be blank");
                assertTrue(topics.add(topic), () -> "Duplicate topic: " + topic);
            }
        }
    }
}
