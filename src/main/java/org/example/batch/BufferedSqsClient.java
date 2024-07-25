package org.example.batch;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

public class BufferedSqsClient {

  private final Map<String, QueueBuffer> queueUrlToBuffer = new ConcurrentHashMap<>();
  private final QueueBufferConfig queueBufferConfig;
  private final SqsClient sqsClient;

  public BufferedSqsClient(
      QueueBufferConfig queueBufferConfig,
      SqsClient sqsClient
  ) {
    this.queueBufferConfig = queueBufferConfig;
    this.sqsClient = sqsClient;
  }

  public Future<SendMessageResponse> sendMessageAsync(SendMessageRequest sendMessageRequest) {
    QueueBuffer buffer = getQueueBuffer(sendMessageRequest.queueUrl());
    return buffer.sendMessage(sendMessageRequest);
  }

  private QueueBuffer getQueueBuffer(String queueUrl) {
    return queueUrlToBuffer.computeIfAbsent(
        queueUrl, url -> new QueueBuffer(sqsClient, url, queueBufferConfig)
    );
  }
}
