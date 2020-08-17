package com.learnkafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
public class LibraryEventProducer {

  @Autowired
  private KafkaTemplate<Integer, String> kafkaTemplate;

  @Autowired
  private ObjectMapper objectMapper;

  private String topic = "library-events";

  public void sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException {

    Integer key = libraryEvent.getLibraryEventId();
    String value = objectMapper.writeValueAsString(libraryEvent);

    ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.sendDefault(key, value);
    listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
      @Override
      public void onFailure(Throwable exception) {
        handleFailure(key, value, exception);
      }

      @Override
      public void onSuccess(SendResult<Integer, String> result) {
        handleSuccess(key, value, result);
      }
    });
  }

  public ListenableFuture<SendResult<Integer, String>> sendLibraryEvent_Approach2(LibraryEvent libraryEvent) throws JsonProcessingException {

    Integer key = libraryEvent.getLibraryEventId();
    String value = objectMapper.writeValueAsString(libraryEvent);

    ProducerRecord<Integer, String> producerRecord = buildProducerRecord(key, value, topic);
    ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.send(producerRecord);
    listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
      @Override
      public void onFailure(Throwable exception) {
        handleFailure(key, value, exception);
      }

      @Override
      public void onSuccess(SendResult<Integer, String> result) {
        handleSuccess(key, value, result);
      }
    });

    return listenableFuture;
  }

  private ProducerRecord<Integer, String> buildProducerRecord(Integer key, String value, String topic) {

    List<Header> recordHeaders = List.of(new RecordHeader("event-source", "scanner".getBytes()));
    return new ProducerRecord<>(topic, null, key, value, recordHeaders);
  }

  public SendResult<Integer, String> sendLibraryEventSynchronous(LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException {

    Integer key = libraryEvent.getLibraryEventId();
    String value = objectMapper.writeValueAsString(libraryEvent);
    SendResult<Integer, String> sendResult = null;

    try {
      sendResult = kafkaTemplate.sendDefault(key, value).get(1, TimeUnit.SECONDS);
    } catch (ExecutionException | InterruptedException exception) {
      log.error("ExecutionException/InterruptedException sending the message and the exception is: {}", exception.getMessage());
      throw exception;
    } catch (Exception exception) {
      log.error("Exception sending the message and the exception is: {}", exception.getMessage());
      exception.printStackTrace();
    }

    return sendResult;
  }

  private void handleFailure(Integer key, String value, Throwable exception) {
    log.error("Error sending the message and the exception is: {}", exception.getMessage());

    try {
      throw exception;
    } catch (Throwable throwable) {
      log.error("Error in OnFailure: {}", throwable.getMessage());
    }

  }

  private void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {
    log.info("Message sent successfully for the key: {} and the value is: {}, partition is: {}", key, value,
        result.getRecordMetadata().partition());
  }
}
