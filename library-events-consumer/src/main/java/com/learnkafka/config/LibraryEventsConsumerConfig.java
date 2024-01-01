package com.learnkafka.config;

import com.learnkafka.service.FailureService;
import com.learnkafka.service.LibraryEventsService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.*;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.util.backoff.FixedBackOff;

import java.util.List;

@Configuration
@EnableKafka
@Slf4j
public class LibraryEventsConsumerConfig {

    public static final String RETRY = "RETRY";
    public static final String SUCCESS = "SUCCESS";
    public static final String DEAD = "DEAD";
    @Autowired
    LibraryEventsService libraryEventsService;

    @Autowired
    KafkaProperties kafkaProperties;

    @Autowired
    FailureService failureService;

    @Autowired
    KafkaTemplate kafkaTemplate;

    @Value("${topics.retry:library-events.RETRY}")
    private String retryTopic;

    @Value("${topics.dlt:library-events.DLT}")
    private String deadLetterTopic;


    public DeadLetterPublishingRecoverer publishingRecoverer() {
        //Publishing failed records to differents topic to retry them or to send them to a dead letter queue (for tracking purposes)

        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate
                , (r, e) -> {
            log.error("Exception in publishingRecoverer : {} ", e.getMessage(), e);
            if (e.getCause() instanceof RecoverableDataAccessException) {
                return new TopicPartition(retryTopic, r.partition());
            } else {
                return new TopicPartition(deadLetterTopic, r.partition());
            }
        }
        );

        return recoverer;

    }

    ConsumerRecordRecoverer consumerRecordRecoverer = (record, exception) -> {
        log.error("Exception is : {} Failed Record : {} ", exception, record);
        if (exception.getCause() instanceof RecoverableDataAccessException) {
            log.info("Inside the recoverable logic");
            //Add any Recovery Code here.
            //failureService.saveFailedRecord((ConsumerRecord<Integer, String>) record, exception, RETRY);

        } else {
            log.info("Inside the non recoverable logic and skipping the record : {}", record);

        }
    };

    public DefaultErrorHandler errorHandler() {

        //Algoritmo di backoff esponenziale binario per pilotare i tempi di retry
        ExponentialBackOffWithMaxRetries expBackOff = new ExponentialBackOffWithMaxRetries(2);
        expBackOff.setInitialInterval(1_000L);
        expBackOff.setMultiplier(2.0);
        expBackOff.setMaxInterval(2_000L);

        var fixedBackOff = new FixedBackOff(1000L, 2L); //max 2 retry attempts for each failed record, wait 1 sec between each retry

        /**
         * Just the Custom Error Handler
         */
       // var defaultErrorHandler =  new DefaultErrorHandler(fixedBackOff);

        /**
         * Error Handler with the BackOff, Exceptions to Ignore, RetryListener
         */

        var defaultErrorHandler = new DefaultErrorHandler(
                //consumerRecordRecoverer
                publishingRecoverer() //defining the recoverer to send the failed records to a different topic
                ,
                //select one between the fixedBackOff and expBackOff
                fixedBackOff
                //expBackOff
        );

        //Adding Exception List to avoid retry
        var exceptiopnToIgnorelist = List.of(
                IllegalArgumentException.class
        );

        exceptiopnToIgnorelist.forEach(defaultErrorHandler::addNotRetryableExceptions);

        //Adding Exception List to retry, commented to handle all other exceptions
        //var exceptiopnToRetrylist = List.of(
        //        RecoverableDataAccessException.class
        //);
        //
        //exceptiopnToRetrylist.forEach(defaultErrorHandler::addRetryableExceptions);

        //For each retry attempt, this listener will be called and print the exception and deliveryAttempt
        defaultErrorHandler.setRetryListeners(
                (record, ex, deliveryAttempt) ->
                        log.info("Failed Record in Retry Listener  exception : {} , deliveryAttempt : {} ", ex.getMessage(), deliveryAttempt)
        );

        return defaultErrorHandler;
    }

    @Bean
    @ConditionalOnMissingBean(name = "kafkaListenerContainerFactory")
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ObjectProvider<ConsumerFactory<Object, Object>> kafkaConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, kafkaConsumerFactory
                .getIfAvailable(() -> new DefaultKafkaConsumerFactory<>(this.kafkaProperties.buildConsumerProperties())));
        factory.setConcurrency(3); //necessary only in non-cloud-like or non-kubernetes environment
        factory.setCommonErrorHandler(errorHandler()); //Error Handler
        return factory;
    }

    // Manual Commit configuration
    //@Bean
    //ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
    //        ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
    //        ConsumerFactory<Object, Object> kafkaConsumerFactory) {
    //    ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
    //    configurer.configure(factory, kafkaConsumerFactory);
    //    factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
    //    return factory;
    //}
}