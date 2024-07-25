package org.example;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.example.batch.BufferedSqsClient;
import org.example.batch.SqsQueueProducer;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

public class Main {

  public static void main(String[] args) {
    SqsClient sqsClient = SqsClientProvider.createSqsClient();
    BufferedSqsClient bufferedSqsClient = SqsClientProvider.createBufferedSqsClient();
    String queueUrl = "http://localhost:4567/000000000000/sample-queue.fifo";

    SqsQueueProducer producer = new SqsQueueProducer(bufferedSqsClient, queueUrl);

    List<Future<SendMessageResponse>> futures = new ArrayList<>();
    for (int i = 0; i < 937; i++) {
      Future<SendMessageResponse> future = producer.sendMessage("Message " + i);
      futures.add(future);
    }

    List<String> sentMessageIds = new ArrayList<>();
    for (Future<SendMessageResponse> future : futures) {
      try {
        SendMessageResponse response = future.get();
        System.out.println("Message sent: " + response.messageId());
        sentMessageIds.add(response.messageId());
      } catch (Exception e) {
        System.out.println("Failed to send message: " + e.getMessage());
      }
    }

    SqsQueueConsumer consumer = new SqsQueueConsumer(sqsClient, queueUrl);
    List<String> consumedMessageIds = consumer.consume().stream()
        .map(Message::messageId)
        .toList();

    if (sentMessageIds.equals(consumedMessageIds)) {
      System.out.println("모든 메시지가 성공적으로 송수신되었습니다.");
    }
  }
}
