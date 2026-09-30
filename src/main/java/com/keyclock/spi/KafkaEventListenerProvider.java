package com.keyclock.spi;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jboss.logging.Logger;
import org.keycloak.events.Event;
import org.keycloak.events.EventListenerProvider;
import org.keycloak.events.EventType;
import org.keycloak.events.admin.AdminEvent;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class KafkaEventListenerProvider implements EventListenerProvider {

    private static final Logger LOG = Logger.getLogger(KafkaEventListenerProvider.class);

    private final List<EventType> events;

    private final KafkaEventProducer kafkaEventProducer;
    private final KafkaEventConsumer kafkaEventConsumer;
    private final ObjectMapper mapper;

    public KafkaEventListenerProvider(KafkaEventProducer kafkaEventProducer,
                                      KafkaEventConsumer kafkaEventConsumer,
                                      List<EventType> events) {
        LOG.debug("starting listener provider");
		// if you want to listen to all events. otherwise get events in constructor
        if(events==null ||events.isEmpty()) {
            this.events = Arrays.stream(EventType.values())
                    .collect(Collectors.toList());
        }else{
            this.events=events;
        }

        mapper = new ObjectMapper();
        this.kafkaEventProducer = kafkaEventProducer;
        this.kafkaEventConsumer = kafkaEventConsumer;
        // starting new thread for kafka consumer
        new Thread(this.kafkaEventConsumer).start();
    }



    @Override
    public void onEvent(Event event) {
        if (events.contains(event.getType())) {
            publish(event,KafkaTopic.USER_EVENTS.topicName());
        }
    }

    @Override
    public void onEvent(AdminEvent event, boolean includeRepresentation) {
        publish(event, KafkaTopic.ADMIN_EVENTS.topicName());
    }

    private void publish(Object event, String topic) {
        try {
            String json = mapper.writeValueAsString(event);
            kafkaEventProducer.publishEvent(json, topic);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Interrupted while publishing to " + topic, e);
        } catch (JsonProcessingException | ExecutionException | TimeoutException e) {
            LOG.error("Could not publish to " + topic, e);
        }
    }
    @Override
    public void close() {
        // ignore
    }

    public void shutDownConsumer() {
        if (kafkaEventConsumer != null) {
            LOG.info("shutting down the consumer");
            kafkaEventConsumer.shutDown();
        }
    }
}
