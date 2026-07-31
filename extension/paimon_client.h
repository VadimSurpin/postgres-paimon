// SPDX-License-Identifier: Apache-2.0
#pragma once
/*
 * paimon_client.h — per-backend row accumulator + ring-buffer producer.
 *
 * DML path: rows are buffered per-relation in PaimonRelBuffer until transaction
 * commit.  At commit the entire batch is serialised and pushed into the
 * shared-memory ring buffer (non-blocking; full ring → message dropped).
 *
 * DDL path: messages are sent to the ring immediately (fire-and-forget).
 */

#ifdef __cplusplus

#include <string>
#include <vector>
#include <unordered_map>
#include <cstdint>

#include "paimon_event.h"

struct PaimonColumn {
    uint16_t    attnum;
    PaimonTypeCode type_code;
    uint32_t    typmod;
    bool        not_null;
    std::string name;
};

struct PaimonRow {
    PaimonRowKind kind;
    uint64_t      seq;
    std::vector<uint16_t>        attnums;
    std::vector<PaimonTypeCode>  types;
    std::vector<int32_t>         lengths;
    std::vector<std::string>     values;
};

struct PaimonRelBuffer {
    uint32_t    table_oid    = 0;
    std::string table_name;
    uint32_t    schema_version = 0;
    std::vector<PaimonRow>     rows;
    size_t      byte_size    = 0;
    std::vector<PaimonColumn>  columns;
    std::vector<uint16_t>      pk_attnums;
};

class PaimonClient {
public:
    static PaimonClient& instance();

    /* Returns true if shared memory ring is available. */
    bool connect();
    bool isConnected() const { return connected_; }

    /* Buffer a DML row (accumulates until flushAll). */
    void bufferRow(uint32_t table_oid,
                   const std::string& table_name,
                   uint32_t schema_version,
                   const std::vector<PaimonColumn>& cols,
                   const std::vector<uint16_t>& pk_attnums,
                   PaimonRowKind kind,
                   uint64_t seq,
                   const std::vector<uint16_t>& attnums,
                   const std::vector<PaimonTypeCode>& types,
                   const std::vector<int32_t>& lengths,
                   const std::vector<std::string>& values);

    /*
     * Serialise buffered rows into pending_msgs_ and write a WAL record when
     * paimon_heap.wal_level = 'local'.  Must be called at XACT_EVENT_PRE_COMMIT
     * so the WAL record is included in the transaction's commit flush.
     */
    void prepareCommit();

    /* Push pending_msgs_ to the ring buffer (call at XACT_EVENT_COMMIT). */
    void flushAll();

    /* Discard buffered rows without sending (call at ABORT). */
    void discardAll();

    /* DDL — fire-and-forget to ring. Returns "" on success. */
    std::string ddlCreate(uint32_t table_oid,
                          const std::string& table_name,
                          const std::vector<PaimonColumn>& cols,
                          const std::vector<uint16_t>& pk_attnums);
    std::string ddlAddColumn(uint32_t table_oid, const PaimonColumn& col);
    std::string ddlDropColumn(uint32_t table_oid, uint16_t attnum);
    std::string ddlRenameColumn(uint32_t table_oid, uint16_t attnum,
                                const std::string& new_name);
    std::string ddlTypeChange(uint32_t table_oid, uint16_t attnum,
                              PaimonTypeCode new_type, uint32_t new_typmod);
    std::string ddlTruncate(uint32_t table_oid, const std::string& table_name);
    std::string ddlDropTable(uint32_t table_oid, const std::string& table_name);
    /* PK changes are always blocked (returns non-empty error). */
    std::string ddlPkChange(uint32_t table_oid, const std::string& table_name);

private:
    PaimonClient() = default;
    ~PaimonClient();

    bool sendToRing(const std::string& msg);
    void flushBuffer(PaimonRelBuffer& buf);
    void ensureSchema(PaimonRelBuffer& buf);
    std::string makeSchemaMsg(const PaimonRelBuffer& buf) const;
    std::string makeBatchMsg(const PaimonRelBuffer& buf) const;

    bool connected_ = false;
    std::unordered_map<uint32_t, PaimonRelBuffer> buffers_;
    std::unordered_map<uint32_t, uint32_t>        sent_schema_version_;
    std::vector<std::string>                      pending_msgs_;
};

extern "C" {
#endif /* __cplusplus */

int  paimon_client_ddl_pk_change(uint32_t table_oid, const char* table_name,
                                 char* err_buf, int err_buf_size);
int  paimon_client_ddl_add_col(uint32_t table_oid, uint16_t attnum,
                               uint8_t type_code, uint32_t typmod,
                               uint8_t not_null, const char* col_name);
int  paimon_client_ddl_drop_col(uint32_t table_oid, uint16_t attnum);
int  paimon_client_ddl_rename(uint32_t table_oid, uint16_t attnum,
                              const char* new_name);
int  paimon_client_ddl_type_change(uint32_t table_oid, uint16_t attnum,
                                   uint8_t new_type_code, uint32_t new_typmod);
int  paimon_client_ddl_truncate(uint32_t table_oid, const char* table_name);
int  paimon_client_ddl_drop_table(uint32_t table_oid, const char* table_name);

void paimon_client_prepare_commit(void);
void paimon_client_flush_all(void);
void paimon_client_discard_all(void);

#ifdef __cplusplus
} /* extern "C" */
#endif
