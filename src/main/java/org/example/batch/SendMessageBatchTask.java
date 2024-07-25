package org.example.batch;

import java.util.UUID;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

public class SendMessageBatchTask {

  private final String id;
  private final QueueBufferFuture<SendMessageResponse> future;
  private final SendMessageRequest request;

  public SendMessageBatchTask(
      QueueBufferFuture<SendMessageResponse> future,
      SendMessageRequest request
  ) {
    this.id = UUID.randomUUID().toString();
    this.future = future;
    this.request = request;
  }

  public String getId() {
    return id;
  }

  public SendMessageRequest getRequest() {
    return request;
  }

  public void succeedFuture(SendMessageResponse response) {
    future.setSuccess(response);
  }

  public void failFuture(Exception e) {
    future.setFailure(e);
  }
}
