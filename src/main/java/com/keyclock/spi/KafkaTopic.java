package com.keyclock.spi;

public enum KafkaTopic {
    OTP("otp-topic"),
    USER_EVENTS("keycloakEvents"),
    ADMIN_EVENTS("keycloakAdminEvents"),
    POLICY_ENFORCER("policy-enforcer");

    private final String topicName;

    KafkaTopic(String topicName) {
        this.topicName = topicName;
    }

    public String topicName() {
        return topicName;
    }
}