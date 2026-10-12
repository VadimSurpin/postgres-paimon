// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.options.Options;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import pro.surpin.pgpaimon.writer.decoder.PgOutputDecoder;
import pro.surpin.pgpaimon.writer.decoder.PgOutputMessageDecoder;
import pro.surpin.pgpaimon.writer.dto.*;
import pro.surpin.pgpaimon.writer.dto.RelationMeta;

/** Spring owns the pipeline components and closes their resources in dependency order. */
@Configuration(proxyBeanMethods = false)
@ComponentScan(basePackageClasses = PgOutputMessageDecoder.class)
public class WriterConfiguration {

  @Bean
  @NotNull
  WriterConfig writerConfig(
      @NotNull Environment environment
  ) {
    var values = new HashMap<String, String>();
    for (String key : List.of("PGPAIMON_COMMIT_USER", "PGPAIMON_BATCH_MAX_ROWS",
        "PGPAIMON_BATCH_MAX_BYTES", "PGPAIMON_BATCH_MAX_INTERVAL_MS", "PGPAIMON_HEALTH_INTERVAL_MS",
        "PGPAIMON_TXN_MAX_BYTES", "PGPAIMON_ACCEPT_SYSTEM_IDENTIFIER", "PGPAIMON_S3_ENDPOINT",
        "PGPAIMON_S3_ACCESS_KEY", "PGPAIMON_S3_SECRET_KEY", "PGPAIMON_FLUSH_MAX_RECORDS",
        "PGPAIMON_JDBC_URL", "PGPAIMON_MESSAGE_PREFIX", "PGPAIMON_OP_SEQ_FIELD",
        "PGPAIMON_PASSWORD", "PGPAIMON_PUBLICATION", "PGPAIMON_SEQ_FIELD", "PGPAIMON_SKIP_LSN",
        "PGPAIMON_SLOT", "PGPAIMON_USER", "PGPAIMON_WAREHOUSE", "PGPAIMON_XID_FIELD")) {
      String property = "pgpaimon."
          + key.substring("PGPAIMON_".length()).toLowerCase(Locale.ROOT).replace('_', '-');
      String value = environment.getProperty(property);
      if (value == null) {
        value = environment.getProperty(key);
      }
      if (value != null) {
        values.put(key, value);
      }
    }
    return WriterConfig.fromEnv(values);
  }

  @Bean(destroyMethod = "close")
  @NotNull
  Catalog catalog(
      @NotNull WriterConfig config
  ) {
    return CatalogFactory
        .createCatalog(CatalogContext.create(Options.fromMap(config.catalogOptions())));
  }

  @Bean
  @NotNull
  SchemaRegistry schemaRegistry(
      @NotNull Catalog catalog,
      @NotNull WriterConfig config
  ) {
    return new SchemaRegistry(catalog, config);
  }

  @Bean(destroyMethod = "close")
  @NotNull
  PaimonSink paimonSink(
      @NotNull SchemaRegistry registry,
      @NotNull WriterConfig config
  ) {
    return new PaimonSink(registry, config);
  }

  @Bean
  @NotNull
  OffsetStore offsetStore(
      @NotNull SchemaRegistry registry,
      @NotNull WriterConfig config
  ) {
    return new OffsetStore(registry, config);
  }

  @Bean
  @NotNull
  WriterBootstrap writerBootstrap(
      @NotNull WriterConfig config,
      @NotNull SchemaRegistry registry,
      @NotNull PaimonSink sink,
      @NotNull OffsetStore offsets
  ) {
    return new WriterBootstrap(config, registry, sink, offsets);
  }

  @Bean
  @NotNull
  PgOutputDecoder pgOutputDecoder(
      @NotNull List<PgOutputMessageDecoder> pgOutputMessageDecoders
  ) {
    return new PgOutputDecoder(pgOutputMessageDecoders);
  }

  @Bean
  @NotNull
  ToastResolver toastResolver() {
    return new ToastResolver();
  }

  @Bean
  @NotNull
  @org.springframework.context.annotation.Lazy
  DdlMessageHandler ddlMessageHandler(
      @NotNull SchemaRegistry registry,
      @NotNull WriterConfig config
  ) {
    java.util.function.Supplier<String> token = () -> {
      try (var conn = WriterBootstrap.plainConnection(config)) {
        return WriterBootstrap.readToken(conn);
      } catch (java.sql.SQLException e) {
        throw WriterBootstrap.missingGrant(e);
      }
    };
    return new DdlMessageHandler(registry, config.messagePrefix(), token.get(), config.skipLsn(),
        token);
  }

  @Bean
  MiniBatcher miniBatcher(
      PaimonSink sink,
      OffsetStore offsets,
      WriterConfig config,
      ObjectProvider<SlotConsumer> consumer
  ) {
    var limits = new MiniBatcher.Limits(config.batchMaxRows(), config.batchMaxBytes(),
        config.batchMaxIntervalMs());
    return new MiniBatcher(
        WriterBootstrap.confirmingFlusher(sink, lsn -> consumer.getObject().confirm(lsn)),
        offsets::highestCommitted, limits);
  }

  @Bean
  HealthCheck healthCheck(
      WriterConfig config
  ) {
    return new HealthCheck(() -> WriterBootstrap.plainConnection(config), config.healthIntervalMs(),
        config.slotName());
  }

  @Bean
  @NotNull
  TxnBatcher txnBatcher(
      @NotNull ToastResolver resolver,
      @NotNull SchemaRegistry registry,
      @NotNull PaimonSink sink,
      @NotNull WriterConfig config,
      @NotNull MiniBatcher mini
  ) {
    return new TxnBatcher(resolver, registry, mini, config.skipLsn(), (
        record,
        meta) -> registry.rowBuilder(record.relid(), meta).build(record))
        .withMaxBytes(config.txnMaxBytes());
  }

  @Bean
  @NotNull
  SlotConsumer.Handler pipelineHandler(
      @NotNull SchemaRegistry registry,
      @NotNull ObjectProvider<DdlMessageHandler> ddl,
      @NotNull PaimonSink sink,
      @NotNull TxnBatcher batcher,
      @NotNull MiniBatcher mini,
      @NotNull HealthCheck health
  ) {
    return new WriterBootstrap.PipelineHandler(registry::tracked, registry::primaryKey,
        m -> ddl.getObject().handle(m), m -> ddl.getObject().isDdl(m),
        new WriterBootstrap.PipelineHandler.Target() {
          @Override
          public void setRelation(
              long relid,
              @NotNull RelationMeta meta
          ) {
            sink.setRelation(relid, meta);
          }

          @Override
          public void truncate(
              long relid,
              long commitLsn
          ) throws Exception {
            sink.truncate(relid, commitLsn);
          }

          @Override
          public boolean overwrites(
              long relid
          ) {
            return !registry.keyless(relid);
          }
        }, batcher, mini, health::maybeCheck);
  }

  @Bean
  @NotNull
  SlotConsumer slotConsumer(
      @NotNull WriterConfig config,
      @NotNull PgOutputDecoder decoder,
      @NotNull SlotConsumer.Handler handler
  ) {
    return new SlotConsumer(config, decoder, handler);
  }

  @Bean
  @NotNull
  WriterRunner writerRunner(
      @NotNull WriterBootstrap bootstrap,
      @NotNull SlotConsumer consumer
  ) {
    return new WriterRunner(bootstrap, consumer);
  }
}
