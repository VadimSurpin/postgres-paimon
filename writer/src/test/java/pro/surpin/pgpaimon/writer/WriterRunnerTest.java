// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;
import org.springframework.boot.DefaultApplicationArguments;
import pro.surpin.pgpaimon.writer.dto.*;

class WriterRunnerTest {

  @Test
  void bootstrapCompletesBeforeReplicationStartsAtItsResumePosition() throws Exception {
    WriterBootstrap bootstrap = mock(WriterBootstrap.class);
    SlotConsumer consumer = mock(SlotConsumer.class);
    when(bootstrap.initialize()).thenReturn(123L);
    new WriterRunner(bootstrap, consumer).run(new DefaultApplicationArguments());
    var order = inOrder(bootstrap, consumer);
    order.verify(bootstrap).initialize();
    order.verify(consumer).run(123L);
  }

  @Test
  void bootstrapFailurePreventsStreamingAndPropagates() throws Exception {
    WriterBootstrap bootstrap = mock(WriterBootstrap.class);
    SlotConsumer consumer = mock(SlotConsumer.class);
    IOException failure = new IOException("bootstrap failed");
    when(bootstrap.initialize()).thenThrow(failure);
    assertSame(failure, assertThrows(IOException.class,
        () -> new WriterRunner(bootstrap, consumer).run(new DefaultApplicationArguments())));
    verifyNoInteractions(consumer);
  }

  @Test
  void fatalReplicationFailurePropagatesAndCanBeClosedOnTheSameThread() throws Exception {
    WriterBootstrap bootstrap = mock(WriterBootstrap.class);
    SlotConsumer consumer = mock(SlotConsumer.class);
    when(bootstrap.initialize()).thenReturn(123L);
    IOException failure = new IOException("commit failed");
    doThrow(failure).when(consumer).run(123L);
    WriterRunner runner = new WriterRunner(bootstrap, consumer);
    assertSame(failure,
        assertThrows(IOException.class, () -> runner.run(new DefaultApplicationArguments())));
    runner.destroy();
    verify(consumer).stop();
  }

  @Test
  void shutdownWaitsForInFlightWorkBeforeAllowingResourceDestruction() throws Exception {
    WriterBootstrap bootstrap = mock(WriterBootstrap.class);
    SlotConsumer consumer = mock(SlotConsumer.class);
    CountDownLatch streaming = new CountDownLatch(1);
    CountDownLatch stopRequested = new CountDownLatch(1);
    CountDownLatch commitFinished = new CountDownLatch(1);
    doAnswer(invocation -> {
      streaming.countDown();
      assertTrue(commitFinished.await(5, TimeUnit.SECONDS));
      return null;
    }).when(consumer).run(0L);
    doAnswer(invocation -> {
      stopRequested.countDown();
      return null;
    }).when(consumer).stop();
    WriterRunner runner = new WriterRunner(bootstrap, consumer);
    try (var executor = Executors.newFixedThreadPool(2)) {
      var run = executor.submit(() -> {
        runner.run(new DefaultApplicationArguments());
        return null;
      });
      try {
        assertTrue(streaming.await(5, TimeUnit.SECONDS));
        var close = executor.submit(() -> {
          runner.destroy();
          return null;
        });
        assertTrue(stopRequested.await(5, TimeUnit.SECONDS));
        assertThrows(TimeoutException.class, () -> close.get(100, TimeUnit.MILLISECONDS));
        commitFinished.countDown();
        run.get(5, TimeUnit.SECONDS);
        close.get(5, TimeUnit.SECONDS);
      } finally {
        commitFinished.countDown();
      }
    }
  }

  @Test
  void shutdownBeforeStartupPreventsBootstrap() throws Exception {
    WriterBootstrap bootstrap = mock(WriterBootstrap.class);
    SlotConsumer consumer = mock(SlotConsumer.class);
    WriterRunner runner = new WriterRunner(bootstrap, consumer);
    runner.destroy();
    runner.run(new DefaultApplicationArguments());
    verifyNoInteractions(bootstrap);
    verify(consumer).stop();
    verify(consumer, never()).run(anyLong());
  }
}
