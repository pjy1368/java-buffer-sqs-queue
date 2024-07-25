package org.example.batch;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

public class QueueBuffer {

  private static final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

  private final SqsClient sqsClient;
  private final String queueUrl;
  private final QueueBufferConfig bufferConfig;
  private final BlockingQueue<SendMessageBatchTask> buffer = new LinkedBlockingQueue<>();

  public QueueBuffer(SqsClient sqsClient, String queueUrl, QueueBufferConfig bufferConfig) {
    this.sqsClient = sqsClient;
    this.queueUrl = queueUrl;
    this.bufferConfig = bufferConfig;

    startBatchSender();
  }

  private void startBatchSender() {
    scheduler.scheduleAtFixedRate(
        new BatchSender(),
        bufferConfig.getMaxBatchOpenMs(),
        bufferConfig.getMaxBatchOpenMs(),
        TimeUnit.MILLISECONDS
    );
  }

  public Future<SendMessageResponse> sendMessage(SendMessageRequest sendMessageRequest) {
    QueueBufferFuture<SendMessageResponse> future = new QueueBufferFuture<>();

    boolean offered = buffer.offer(new SendMessageBatchTask(future, sendMessageRequest));
    if (!offered) {
      future.setFailure(new RuntimeException("Failed to add message to buffer"));
    }

    return future;
  }

  public class BatchSender implements Runnable {

    @Override
    public void run() {
      List<SendMessageBatchTask> tasks = new ArrayList<>();
      buffer.drainTo(tasks);
      int maxBatchSize = bufferConfig.getMaxBatchSize();

      for (int i = 0; i < tasks.size(); i += maxBatchSize) {
        List<SendMessageBatchTask> subTasks = new ArrayList<>(
            tasks.subList(i, Math.min(i + maxBatchSize, tasks.size()))
        );

        if (!subTasks.isEmpty()) {
          batchSend(subTasks);
        }
      }
    }

    private void batchSend(List<SendMessageBatchTask> tasks) {
      List<SendMessageBatchRequestEntry> entries = tasks.stream()
          .map(task -> SendMessageBatchRequestEntry.builder()
              .id(task.getId())
              .messageBody(task.getRequest().messageBody())
              .messageGroupId(task.getRequest().messageGroupId())
              .messageDeduplicationId(task.getRequest().messageDeduplicationId())
              .build())
          .collect(Collectors.toList());

      SendMessageBatchRequest batchRequest = SendMessageBatchRequest.builder()
          .queueUrl(queueUrl)
          .entries(entries)
          .build();

      try {
        SendMessageBatchResponse batchResponse = sqsClient.sendMessageBatch(batchRequest);
        completeFuturesFromBatchResponse(tasks, batchResponse);
      } catch (Exception e) {
        for (SendMessageBatchTask task : tasks) {
          task.failFuture(e);
        }
      }
    }

    private void completeFuturesFromBatchResponse(
        List<SendMessageBatchTask> tasks,
        SendMessageBatchResponse batchResponse
    ) {
      batchResponse.successful().forEach(entry -> {
        for (SendMessageBatchTask task : tasks) {
          if (task.getId().equals(entry.id())) {
            SendMessageResponse response = SendMessageResponse.builder()
                .messageId(entry.messageId())
                .build();
            task.succeedFuture(response);
          }
        }
      });

      batchResponse.failed().forEach(entry -> {
        for (SendMessageBatchTask batchTask : tasks) {
          if (batchTask.getId().equals(entry.id())) {
            batchTask.failFuture(new RuntimeException("Failed to send message: " + entry.message()));
          }
        }
      });
    }
  }
}
