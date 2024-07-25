package org.example;

import java.net.URI;
import org.example.batch.BufferedSqsClient;
import org.example.batch.QueueBufferConfig;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;

public class SqsClientProvider {

  public static SqsClient createSqsClient() {
    return SqsClient.builder()
        .endpointOverride(URI.create("http://localhost:4567"))
        .region(Region.US_EAST_1)
        .credentialsProvider(StaticCredentialsProvider.create(
            AwsBasicCredentials.create("xxx", "xxx")
        ))
        .build();
  }

  public static BufferedSqsClient createBufferedSqsClient() {
    return new BufferedSqsClient(createQueueBufferConfig(), createSqsClient());
  }

  private static QueueBufferConfig createQueueBufferConfig() {
    return new QueueBufferConfig();
  }
}
