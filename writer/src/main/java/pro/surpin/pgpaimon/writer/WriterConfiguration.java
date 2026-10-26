// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.options.Options;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import pro.surpin.pgpaimon.writer.dto.*;
import pro.surpin.pgpaimon.writer.dto.RelationMeta;

/** Spring owns the pipeline components and closes their resources in dependency order. */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(WriterConfig.class)
public class WriterConfiguration {

  /** Preserve the original environment names while using Spring constructor binding. */
  @Bean
  static org.springframework.beans.factory.config.BeanFactoryPostProcessor legacyEnvironmentProperties(
      org.springframework.core.env.ConfigurableEnvironment environment
  ) {
    return beanFactory -> {
      java.util.Map<String, Object> aliases = new java.util.LinkedHashMap<>();
      for (String suffix : java.util.List.of("JDBC_URL", "WAREHOUSE", "USER", "PASSWORD", "SLOT",
          "PUBLICATION", "MESSAGE_PREFIX", "COMMIT_USER", "SEQ_FIELD", "OP_SEQ_FIELD", "XID_FIELD",
          "FLUSH_MAX_RECORDS", "SKIP_LSN", "BATCH_MAX_ROWS", "BATCH_MAX_BYTES",
          "BATCH_MAX_INTERVAL_MS", "HEALTH_INTERVAL_MS", "TXN_MAX_BYTES",
          "ACCEPT_SYSTEM_IDENTIFIER", "S3_ENDPOINT", "S3_ACCESS_KEY", "S3_SECRET_KEY")) {
        String value = environment.getProperty("PGPAIMON_" + suffix);
        if (value == null || value.isBlank())
          continue;
        String property = suffix.toLowerCase(java.util.Locale.ROOT).replace('_', '-');
        if (property.startsWith("s3-"))
          property = "s3." + property.substring(3);
        aliases.put("pgpaimon." + property, value);
      }
      environment.getPropertySources()
          .addLast(new org.springframework.core.env.MapPropertySource("legacyPgpaimon", aliases));
    };
  }

  @Bean(destroyMethod = "close")
  @NotNull
  Catalog catalog(
      @NotNull WriterConfig config
  ) {
    return CatalogFactory
        .createCatalog(CatalogContext.create(Options.fromMap(config.getCatalogOptions())));
  }

  @Bean
  MiniBatcher miniBatcher(
      PaimonSink sink,
      OffsetStore offsets,
      WriterConfig config,
      ObjectProvider<SlotConsumer> consumer
  ) {
    var limits = new MiniBatcher.Limits(config.getBatchMaxRows(), config.getBatchMaxBytes(),
        config.getBatchMaxIntervalMs());
    return new MiniBatcher(
        WriterBootstrap.confirmingFlusher(sink, lsn -> consumer.getObject().confirm(lsn)),
        offsets::highestCommitted, limits);
  }

  @Bean
  HealthCheck healthCheck(
      WriterConfig config
  ) {
    return new HealthCheck(() -> WriterBootstrap.plainConnection(config),
        config.getHealthIntervalMs(), config.getSlotName());
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
    return new TxnBatcherImpl(resolver, registry, mini, config.getSkipLsnValue(), (
        record,
        meta) -> registry.rowBuilder(record.relid(), meta).build(record))
        .withMaxBytes(config.getTxnMaxBytes());
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


}
