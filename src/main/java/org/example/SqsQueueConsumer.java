package org.example;

import java.util.ArrayList;
import java.util.List;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

public class SqsQueueConsumer {

  private final SqsClient sqsClient;
  private final String queueUrl;

  public SqsQueueConsumer(SqsClient sqsClient, String queueUrl) {
    this.sqsClient = sqsClient;
    this.queueUrl = queueUrl;
  }

  public List<Message> consume() {
    List<Message> messages = new ArrayList<>();
    while (true) {
      ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
          .queueUrl(queueUrl)
          .maxNumberOfMessages(10) // 배치로 최대 10개의 메시지를 수신
          .waitTimeSeconds(1) // Long polling 대기 시간 (초)
          .build();

      ReceiveMessageResponse receiveMessageResponse = sqsClient.receiveMessage(receiveMessageRequest);
      System.out.println("Received " + receiveMessageResponse.messages().size() + " messages.");

      if (receiveMessageResponse.messages().isEmpty()) {
        System.out.println("No more messages to receive.");
        break;
      }

      for (Message message : receiveMessageResponse.messages()) {
        messages.add(message);

        String messageGroupId = message.attributes().get(MessageSystemAttributeName.MESSAGE_GROUP_ID);
        System.out.printf(
            "Received message. ID: %s, MessageGroupId: %s, Body: %s%n",
            message.messageId(), messageGroupId, message.body()
        );

        String messageReceiptHandle = message.receiptHandle();
        DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
            .queueUrl(queueUrl)
            .receiptHandle(messageReceiptHandle)
            .build();

        sqsClient.deleteMessage(deleteMessageRequest);
      }
    }
    return messages;
  }
}
