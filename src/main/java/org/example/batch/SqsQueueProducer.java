package org.example.batch;


import java.util.UUID;
import java.util.concurrent.Future;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

public class SqsQueueProducer {

  private final BufferedSqsClient sqsClient;
  private final String queueUrl;


  public SqsQueueProducer(BufferedSqsClient sqsClient, String queueUrl) {
    this.sqsClient = sqsClient;
    this.queueUrl = queueUrl;
  }

  public Future<SendMessageResponse> sendMessage(String messageBody) {
    SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
        .queueUrl(queueUrl)
        .messageBody(messageBody)
        .messageGroupId("same-message-group-id")
        .messageDeduplicationId(UUID.randomUUID().toString())
        .build();

    return sqsClient.sendMessageAsync(sendMessageRequest);
  }
}
