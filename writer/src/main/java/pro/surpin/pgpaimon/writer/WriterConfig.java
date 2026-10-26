// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.Getter;
import org.jetbrains.annotations.Nullable;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.ConstructorBinding;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.boot.context.properties.bind.Name;
import org.springframework.validation.annotation.Validated;

/** Writer settings bound by Spring and shared by bootstrap and replication. */
@ConfigurationProperties(prefix = "pgpaimon")
@Validated
@Getter
public class WriterConfig {

  @NotBlank(message = "PGPAIMON_JDBC_URL is required")
  private final String jdbcUrl;
  @NotBlank(message = "PGPAIMON_WAREHOUSE is required")
  private final String warehouse;
  @NotBlank
  private final String user;
  @NotNull
  private final String password;
  @NotBlank
  private final String slotName;
  @NotBlank
  private final String publicationName;
  @NotBlank
  private final String messagePrefix;
  private final String commitUser;
  @NotBlank
  private final String seqField;
  @NotBlank
  private final String opSeqField;
  private final @Nullable String xidField;
  private final int flushMaxRecords;
  private final long skipLsnValue;
  private final String replicationUrl;
  private final Map<String, String> catalogOptions;
  private final long batchMaxRows;
  private final long batchMaxBytes;
  private final long batchMaxIntervalMs;
  private final long healthIntervalMs;
  private final long txnMaxBytes;
  private final String acceptSystemIdentifier;

  @ConstructorBinding
  public WriterConfig(
      String jdbcUrl,
      String warehouse,
      @DefaultValue("postgres") String user,
      @DefaultValue("") String password,
      @Name("slot") @DefaultValue("paimon") String slotName,
      @Name("publication") @DefaultValue("paimon_pub") String publicationName,
      @DefaultValue("paimon") String messagePrefix,
      @Nullable String commitUser,
      @DefaultValue("_seq") String seqField,
      @DefaultValue("_op_seq") String opSeqField,
      @Nullable String xidField,
      @DefaultValue("10000") int flushMaxRecords,
      @Name("skip-lsn") @Nullable String skipLsnText,
      @DefaultValue S3 s3,
      @DefaultValue("10000") long batchMaxRows,
      @DefaultValue("16777216") long batchMaxBytes,
      @DefaultValue("2000") long batchMaxIntervalMs,
      @DefaultValue("60000") long healthIntervalMs,
      @Nullable Long txnMaxBytes,
      @Nullable String acceptSystemIdentifier
  ) {
    if (jdbcUrl == null || jdbcUrl.isBlank())
      throw new IllegalArgumentException("PGPAIMON_JDBC_URL is required");
    if (warehouse == null || warehouse.isBlank())
      throw new IllegalArgumentException("PGPAIMON_WAREHOUSE is required");
    this.jdbcUrl = jdbcUrl;
    this.warehouse = warehouse;
    this.user = user;
    this.password = password;
    this.slotName = slotName;
    this.publicationName = publicationName;
    this.messagePrefix = messagePrefix;
    this.commitUser =
        commitUser == null || commitUser.isBlank() ? "paimon-writer-" + slotName : commitUser;
    this.seqField = seqField;
    this.opSeqField = opSeqField;
    this.xidField = WriterConfigUtils.blankToNull(xidField);
    this.flushMaxRecords = flushMaxRecords;
    this.skipLsnValue = WriterConfigUtils.parseLsn(skipLsnText);

    this.replicationUrl = jdbcUrl + (jdbcUrl.contains("?") ? "&" : "?")
        + "replication=database&preferQueryMode=simple&assumeMinServerVersion=18";

    Map<String, String> options = new LinkedHashMap<>();
    options.put("warehouse", warehouse);
    WriterConfigUtils.put(options, "s3.endpoint", s3.endpoint());
    WriterConfigUtils.put(options, "s3.access-key", s3.accessKey());
    WriterConfigUtils.put(options, "s3.secret-key", s3.secretKey());
    if (WriterConfigUtils.blankToNull(s3.endpoint()) != null) {
      options.put("s3.path.style.access", "true");
    }
    this.catalogOptions = Map.copyOf(options);
    WriterConfigUtils.validateFixed("PGPAIMON_PUBLICATION", publicationName, "paimon_pub");
    WriterConfigUtils.validateFixed("PGPAIMON_MESSAGE_PREFIX", messagePrefix, "paimon");
    WriterConfigUtils.validateMin("PGPAIMON_BATCH_MAX_ROWS", batchMaxRows, 1);
    WriterConfigUtils.validateMin("PGPAIMON_BATCH_MAX_BYTES", batchMaxBytes, 1);
    WriterConfigUtils.validateMin("PGPAIMON_BATCH_MAX_INTERVAL_MS", batchMaxIntervalMs, 0);
    WriterConfigUtils.validateMin("PGPAIMON_HEALTH_INTERVAL_MS", healthIntervalMs, 0);
    this.batchMaxRows = batchMaxRows;
    this.batchMaxBytes = batchMaxBytes;
    this.batchMaxIntervalMs = batchMaxIntervalMs;
    this.healthIntervalMs = healthIntervalMs;
    this.txnMaxBytes = txnMaxBytes == null ? WriterConfigUtils.defaultTxnMaxBytes() : txnMaxBytes;
    WriterConfigUtils.validateMin("PGPAIMON_TXN_MAX_BYTES", this.txnMaxBytes, 1);
    this.acceptSystemIdentifier = WriterConfigUtils.acceptSystemIdentifier(acceptSystemIdentifier);
  }

  public record S3(
      @Nullable String endpoint,
      @Nullable String accessKey,
      @Nullable String secretKey
  ) {
  }
}
