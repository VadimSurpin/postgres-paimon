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
    for (String key : List.of("PGPAIMON_COMMIT_USER", "PGPAIMON_DATABASE",
        "PGPAIMON_FLUSH_MAX_RECORDS", "PGPAIMON_JDBC_URL", "PGPAIMON_MESSAGE_PREFIX",
        "PGPAIMON_OP_SEQ_FIELD", "PGPAIMON_PASSWORD", "PGPAIMON_PUBLICATION", "PGPAIMON_SEQ_FIELD",
        "PGPAIMON_SKIP_LSN", "PGPAIMON_SLOT", "PGPAIMON_USER", "PGPAIMON_WAREHOUSE",
        "PGPAIMON_XID_FIELD")) {
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
  DdlMessageHandler ddlMessageHandler(
      @NotNull SchemaRegistry registry,
      @NotNull WriterConfig config
  ) {
    return new DdlMessageHandler(registry, config.messagePrefix());
  }

  @Bean
  @NotNull
  TxnBatcher txnBatcher(
      @NotNull ToastResolver resolver,
      @NotNull SchemaRegistry registry,
      @NotNull PaimonSink sink,
      @NotNull WriterConfig config,
      @NotNull ObjectProvider<SlotConsumer> consumer
  ) {
    // Resolve the consumer only on commit, after the pipeline has been constructed.
    return new TxnBatcher(resolver, registry,
        WriterBootstrap.confirmingFlusher(sink, lsn -> consumer.getObject().confirm(lsn)),
        config.skipLsn(), (
            record,
            meta) -> registry.rowBuilder(record.relid(), meta).build(record));
  }

  @Bean
  @NotNull
  SlotConsumer.Handler pipelineHandler(
      @NotNull SchemaRegistry registry,
      @NotNull DdlMessageHandler ddl,
      @NotNull PaimonSink sink,
      @NotNull TxnBatcher batcher
  ) {
    return new WriterBootstrap.PipelineHandler(registry::tracked, registry::primaryKey, ddl::handle,
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
        }, batcher);
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
