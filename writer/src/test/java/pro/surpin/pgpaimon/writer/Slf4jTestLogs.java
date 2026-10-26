// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.util.List;
import org.slf4j.LoggerFactory;

final class Slf4jTestLogs {

  private Slf4jTestLogs() {}

  static List<ILoggingEvent> capture(
      Class<?> source,
      Runnable action
  ) {
    Logger logger = (Logger) LoggerFactory.getLogger(source);
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.start();
    logger.addAppender(appender);
    try {
      action.run();
      return List.copyOf(appender.list);
    } finally {
      logger.detachAppender(appender);
      appender.stop();
    }
  }
}
