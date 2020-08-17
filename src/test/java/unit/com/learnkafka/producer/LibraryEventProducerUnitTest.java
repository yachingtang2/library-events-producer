package com.learnkafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.Book;
import com.learnkafka.domain.LibraryEvent;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.SettableListenableFuture;

import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class LibraryEventProducerUnitTest {

  @InjectMocks
  private LibraryEventProducer libraryEventProducer;

  @Mock
  private KafkaTemplate<Integer, String> kafkaTemplate;

  @Spy
  private ObjectMapper objectMapper = new ObjectMapper();

  @Test
  void sendLibraryEvent_Approach2_failure() {

    Book book = Book.builder()
        .bookId(123)
        .bookAuthor("John")
        .bookName("Kafka using Spring Boot")
        .build();
    LibraryEvent libraryEvent = LibraryEvent.builder()
        .libraryEventId(null)
        .book(book)
        .build();

    SettableListenableFuture<SendResult<Integer, String>> future = new SettableListenableFuture<>();
    future.setException(new RuntimeException("Exception calling Kafka"));

    when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(future);

    assertThrows(Exception.class, () -> libraryEventProducer.sendLibraryEvent_Approach2(libraryEvent).get());
  }

  @Test
  void sendLibraryEvent_Approach2_success() throws JsonProcessingException, ExecutionException, InterruptedException {

    Book book = Book.builder()
        .bookId(123)
        .bookAuthor("John")
        .bookName("Kafka using Spring Boot")
        .build();
    LibraryEvent libraryEvent = LibraryEvent.builder()
        .libraryEventId(null)
        .book(book)
        .build();

    String record = objectMapper.writeValueAsString(libraryEvent);
    SettableListenableFuture<SendResult<Integer, String>> future = new SettableListenableFuture<>();
    ProducerRecord<Integer, String> producerRecord = new ProducerRecord("library-events",
        libraryEvent.getLibraryEventId(), record);
    RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition("library-events", 1), 1, 1, 342,
        System.currentTimeMillis(), 1, 2);
    SendResult<Integer, String> sendResult = new SendResult<>(producerRecord, recordMetadata);
    future.set(sendResult);

    when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(future);

    ListenableFuture<SendResult<Integer, String>> listenableFuture =
        libraryEventProducer.sendLibraryEvent_Approach2(libraryEvent);

    SendResult<Integer, String> sendResult1 = listenableFuture.get();

    assert sendResult1.getRecordMetadata().partition() == 1;
    assertEquals(1, sendResult1.getRecordMetadata().partition());
  }
}
