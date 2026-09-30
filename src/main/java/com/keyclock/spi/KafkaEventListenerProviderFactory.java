package com.keyclock.spi;

import org.apache.kafka.clients.CommonClientConfigs;
import org.jboss.logging.Logger;
import org.keycloak.Config.Scope;
import org.keycloak.events.EventListenerProvider;
import org.keycloak.events.EventListenerProviderFactory;
import org.keycloak.events.EventType;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakSessionFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class KafkaEventListenerProviderFactory implements EventListenerProviderFactory {

    private static final Logger LOG = Logger.getLogger(KafkaEventListenerProviderFactory.class);
    private static final String ID = "kafka";
    private static final String EVENTS_KEY = "events";

    private static final String BOOTSTRAP_SERVERS_ENV = "KAFKA_BOOTSTRAP_SERVERS";
    private static final String EVENTS_ENV = "KAFKA_EVENTS";

    private KafkaEventListenerProvider instance;

    private String bootstrapServers;
    private List<EventType> events;
    private Map<String, Object> kafkaProperties;

    @Override
    public EventListenerProvider create(KeycloakSession session) {
        LOG.debug("starting listener factory");
        if (instance == null) {
            instance = new KafkaEventListenerProvider(
                    new KafkaEventProducer(bootstrapServers, kafkaProperties),
                    new KafkaEventConsumer(bootstrapServers, kafkaProperties,
                            List.of(KafkaTopic.POLICY_ENFORCER.topicName())),
                  events
            );
        }

        return instance;
    }

    @Override
    public String getId() {
        return ID;
    }

    @Override
    public void init(Scope config) {
        LOG.info("Init kafka module ...");
        bootstrapServers = config.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, System.getenv(BOOTSTRAP_SERVERS_ENV));
		// if you want to listen to specific events passed in the config. pass this provider constructor otherwise listen to all events

		Objects.requireNonNull(bootstrapServers, "bootstrapServers must not be null");

        String eventsString = config.get(EVENTS_KEY, System.getenv(EVENTS_ENV));

        if (eventsString != null && !eventsString.isBlank()) {
            events = Arrays.stream(eventsString.split(","))
                    .map(String::trim)
                    .map(EventType::valueOf)
                    .toList();
        }

        kafkaProperties = KafkaConfig.init(config);
    }

    @Override
    public void postInit(KeycloakSessionFactory arg0) {
        // ignore
    }

    @Override
    public void close() {
        if (instance != null) {
            LOG.info("Shutting down Kafka consumer");
            instance.shutDownConsumer();
        }
    }
}
