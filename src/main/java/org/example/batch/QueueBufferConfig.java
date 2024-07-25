package org.example.batch;

import java.util.concurrent.TimeUnit;

public class QueueBufferConfig {

  private static final int MAX_BATCH_SIZE_DEFAULT = 10;
  private static final long MAX_BATCH_OPEN_MS_DEFAULT = TimeUnit.MILLISECONDS.convert(5, TimeUnit.SECONDS);
  //private static final long MAX_BATCH_OPEN_MS_DEFAULT = 100;

  private final long maxBatchOpenMs;
  private final int maxBatchSize;

  public QueueBufferConfig(long maxBatchOpenMs, int maxBatchSize) {
    this.maxBatchOpenMs = maxBatchOpenMs;
    this.maxBatchSize = maxBatchSize;
  }

  public QueueBufferConfig() {
    this(MAX_BATCH_OPEN_MS_DEFAULT, MAX_BATCH_SIZE_DEFAULT);
  }

  public long getMaxBatchOpenMs() {
    return maxBatchOpenMs;
  }

  public int getMaxBatchSize() {
    return maxBatchSize;
  }
}
