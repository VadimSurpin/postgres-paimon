// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import java.util.concurrent.CountDownLatch;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;

/** Runs replication on the main thread; a fatal failure propagates to Spring Boot. */
public final class WriterRunner implements ApplicationRunner, DisposableBean {

  private final @NotNull WriterBootstrap bootstrap;
  private final @NotNull SlotConsumer consumer;
  private final @NotNull CountDownLatch completed = new CountDownLatch(1);
  private @Nullable Thread runThread;
  private boolean stopping;

  public WriterRunner(
      @NotNull WriterBootstrap bootstrap,
      @NotNull SlotConsumer consumer
  ) {
    this.bootstrap = bootstrap;
    this.consumer = consumer;
  }

  @Override
  public void run(
      @NotNull ApplicationArguments args
  ) throws Exception {
    synchronized (this) {
      if (stopping) {
        return;
      }
      runThread = Thread.currentThread();
    }
    try {
      long resume = bootstrap.initialize();
      consumer.run(resume);
    } finally {
      synchronized (this) {
        runThread = null;
        completed.countDown();
      }
    }
  }

  @EventListener(ContextClosedEvent.class)
  public void onContextClosed() throws InterruptedException {
    destroy();
  }

  @Override
  public void destroy() throws InterruptedException {
    Thread active;
    synchronized (this) {
      stopping = true;
      consumer.stop();
      active = runThread;
    }
    if (active != null && active != Thread.currentThread()) {
      // Drain backfill or an in-flight commit before Spring closes the sink and catalog.
      // Closing them on a timeout would race an unfinished write.
      completed.await();
    }
  }
}
