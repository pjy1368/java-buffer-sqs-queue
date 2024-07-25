package org.example.batch;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import software.amazon.awssdk.annotations.NotNull;

public class QueueBufferFuture<T> implements Future<T> {

  private final AtomicReference<Result<T>> resultRef = new AtomicReference<>();
  private final Object lock = new Object();

  public void setSuccess(T result) {
    if (resultRef.compareAndSet(null, new Result<>(result, null))) {
      synchronized (lock) {
        lock.notifyAll();
      }
    }
  }

  public void setFailure(Exception exception) {
    if (resultRef.compareAndSet(null, new Result<>(null, exception))) {
      synchronized (lock) {
        lock.notifyAll();
      }
    }
  }

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    // not cancellable
    return false;
  }

  @Override
  public T get() throws InterruptedException, ExecutionException {
    while (true) {
      try {
        return get(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
      } catch (TimeoutException te) {
        // shouldn't really happen, since we're specifying a very-very
        // long wait. but if it does, just loop
        // and wait more.
      }
    }
  }

  @Override
  public T get(long timeout, @NotNull TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    long endTime = System.currentTimeMillis() + TimeUnit.MILLISECONDS.convert(timeout, unit);

    synchronized (lock) {
      while (resultRef.get() == null) {
        long waitTime = endTime - System.currentTimeMillis();
        if (waitTime <= 0) {
          throw new TimeoutException("Operation timed out");
        }
        lock.wait(waitTime);
      }
    }

    Result<T> result = resultRef.get();
    if (result.exception != null) {
      throw new ExecutionException(result.exception);
    }
    return result.value;
  }

  @Override
  public boolean isCancelled() {
    // not cancellable
    return false;
  }

  @Override
  public boolean isDone() {
    return resultRef.get() != null;
  }

  private record Result<Res>(Res value, Exception exception) {

  }
}
