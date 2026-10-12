// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.concurrent.atomic.AtomicReference;
import org.apache.paimon.catalog.Catalog;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

class WriterConfigurationTest {

  private final ApplicationContextRunner contexts =
      new ApplicationContextRunner().withUserConfiguration(TestConfiguration.class);

  @Test
  void wiresPipelineFromSpringPropertiesAndClosesSinkBeforeCatalog() throws Exception {
    AtomicReference<Catalog> catalog = new AtomicReference<>();
    AtomicReference<PaimonSink> sink = new AtomicReference<>();
    contexts
        .withPropertyValues("pgpaimon.jdbc-url=jdbc:postgresql://localhost/paimon",
            "pgpaimon.warehouse=/tmp/spring-writer-test", "pgpaimon.slot=custom-slot")
        .run(context -> {
          assertNull(context.getStartupFailure());
          WriterConfig config = context.getBean(WriterConfig.class);
          assertEquals("custom-slot", config.slotName());
          assertEquals("paimon-writer-custom-slot", config.commitUser());
          assertEquals("jdbc:postgresql://localhost/paimon", config.jdbcUrl());
          assertNotNull(context.getBean(SlotConsumer.class));
          assertNotNull(context.getBean(TxnBatcher.class));
          assertNotNull(context.getBean(WriterRunner.class));
          verifyNoInteractions(context.getBean(WriterBootstrap.class));
          catalog.set(context.getBean(Catalog.class));
          sink.set(context.getBean(PaimonSink.class));
        });
    var order = inOrder(sink.get(), catalog.get());
    order.verify(sink.get()).close();
    order.verify(catalog.get()).close();
  }

  @Test
  void acceptsExistingPgpaimonEnvironmentNames() {
    contexts
        .withPropertyValues("PGPAIMON_JDBC_URL=jdbc:postgresql://localhost/paimon",
            "PGPAIMON_WAREHOUSE=/tmp/spring-writer-test", "PGPAIMON_SKIP_LSN=0/100")
        .run(context -> {
          assertNull(context.getStartupFailure());
          assertEquals(256L, context.getBean(WriterConfig.class).skipLsn());
        });
  }

  @Test
  void missingRequiredConfigurationFailsContextStartup() {
    contexts.run(context -> {
      assertNotNull(context.getStartupFailure());
      Throwable root = context.getStartupFailure();
      while (root.getCause() != null) {
        root = root.getCause();
      }
      assertEquals("PGPAIMON_JDBC_URL is required", root.getMessage());
    });
  }

  @Configuration(proxyBeanMethods = false)
  static class TestConfiguration extends WriterConfiguration {
    @Bean(destroyMethod = "close")
    @Override
    Catalog catalog(
        WriterConfig config
    ) {
      return mock(Catalog.class);
    }

    @Bean(destroyMethod = "close")
    @Override
    PaimonSink paimonSink(
        SchemaRegistry registry,
        WriterConfig config
    ) {
      return mock(PaimonSink.class);
    }

    @Bean
    @Override
    WriterBootstrap writerBootstrap(
        WriterConfig config,
        SchemaRegistry registry,
        PaimonSink sink,
        OffsetStore offsets
    ) {
      return mock(WriterBootstrap.class);
    }
  }
}
