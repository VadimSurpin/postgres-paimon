// SPDX-License-Identifier: Apache-2.0
/*
 * paimon_client.cpp — backend-side row buffer + ring-buffer producer.
 *
 * Replaces the old socket-based client.  Each PostgreSQL backend accumulates
 * DML rows in process-local memory during a transaction.  On COMMIT the
 * entire batch is serialised and pushed into the shared-memory ring buffer
 * for the paimon_bgworker to consume asynchronously.
 *
 * On ABORT: local buffers are discarded — no data reaches the ring.
 *
 * Because rows accumulate until commit the ring write is always transactionally
 * consistent: the bgworker never sees partial transactions.
 *
 * For large transactions (> ring_size) rows that cannot fit are silently
 * dropped (the heap remains authoritative).
 */

#include "paimon_client.h"
#include "paimon_event.h"
#include "paimon_shmem.h"
#include "paimon_ring.h"
#include "paimon_wal.h"

#include <cstring>
#include <cstdlib>

/* ── little-endian serialisation helpers ─────────────────────────── */
namespace {

void put_u8 (std::string& b, uint8_t  v) { b.push_back((char)v); }
void put_u16(std::string& b, uint16_t v) {
    b.push_back((char)(v));
    b.push_back((char)(v >> 8));
}
void put_u32(std::string& b, uint32_t v) {
    b.push_back((char)(v));
    b.push_back((char)(v >>  8));
    b.push_back((char)(v >> 16));
    b.push_back((char)(v >> 24));
}
void put_i32(std::string& b, int32_t v)  { put_u32(b, (uint32_t)v); }
void put_u64(std::string& b, uint64_t v) {
    put_u32(b, (uint32_t)v);
    put_u32(b, (uint32_t)(v >> 32));
}
void put_str(std::string& b, const std::string& s) {
    put_u16(b, (uint16_t)s.size());
    b.append(s);
}
/* Build a complete framed message: [uint32 total_len][uint8 type][payload] */
std::string frame(uint8_t msg_type, const std::string& payload) {
    std::string msg;
    uint32_t total = 4 + 1 + (uint32_t)payload.size();
    put_u32(msg, total);
    put_u8 (msg, msg_type);
    msg.append(payload);
    return msg;
}

} // anonymous namespace

/* ── PaimonClient singleton (per backend process) ─────────────────── */

PaimonClient& PaimonClient::instance() {
    static PaimonClient inst;
    return inst;
}

PaimonClient::~PaimonClient() {}

/* "Connect" just checks that the ring is available. */
bool PaimonClient::connect() {
    if (connected_) return true;
    PaimonRingHdr *hdr = paimon_ring_hdr();
    connected_ = (hdr != NULL && hdr->magic == PAIMON_RING_MAGIC);
    return connected_;
}

/* Write a fully-framed message to the ring buffer. */
bool PaimonClient::sendToRing(const std::string& msg) {
    if (!connect()) return false;
    return paimon_ring_write(msg.data(), msg.size());
}

/* ── Schema bootstrap ─────────────────────────────────────────────── */

void PaimonClient::ensureSchema(PaimonRelBuffer& buf) {
    auto it = sent_schema_version_.find(buf.table_oid);
    if (it != sent_schema_version_.end() &&
        it->second == buf.schema_version)
        return;

    std::string payload;
    put_u32(payload, buf.table_oid);
    put_str(payload, buf.table_name);
    put_u32(payload, (uint32_t)buf.columns.size());
    for (const auto& c : buf.columns) {
        put_u16(payload, c.attnum);
        put_u8 (payload, (uint8_t)c.type_code);
        put_u32(payload, c.typmod);
        put_u8 (payload, c.not_null ? 1 : 0);
        put_str(payload, c.name);
    }
    put_u32(payload, (uint32_t)buf.pk_attnums.size());
    for (uint16_t pk : buf.pk_attnums)
        put_u16(payload, pk);

    sendToRing(frame(PMSG_DDL_CREATE, payload));
    sent_schema_version_[buf.table_oid] = buf.schema_version;
}

/* Build a DDL_CREATE framed message without sending it to the ring. */
std::string PaimonClient::makeSchemaMsg(const PaimonRelBuffer& buf) const {
    std::string payload;
    put_u32(payload, buf.table_oid);
    put_str(payload, buf.table_name);
    put_u32(payload, (uint32_t)buf.columns.size());
    for (const auto& c : buf.columns) {
        put_u16(payload, c.attnum);
        put_u8 (payload, (uint8_t)c.type_code);
        put_u32(payload, c.typmod);
        put_u8 (payload, c.not_null ? 1 : 0);
        put_str(payload, c.name);
    }
    put_u32(payload, (uint32_t)buf.pk_attnums.size());
    for (uint16_t pk : buf.pk_attnums)
        put_u16(payload, pk);
    return frame(PMSG_DDL_CREATE, payload);
}

/* Build a DML_BATCH framed message without sending it to the ring. */
std::string PaimonClient::makeBatchMsg(const PaimonRelBuffer& buf) const {
    std::string payload;
    put_u32(payload, buf.table_oid);
    put_str(payload, buf.table_name);
    put_u32(payload, buf.schema_version);
    put_u32(payload, (uint32_t)buf.rows.size());
    for (const auto& row : buf.rows) {
        put_u8 (payload, (uint8_t)row.kind);
        put_u64(payload, row.seq);
        put_u16(payload, (uint16_t)row.attnums.size());
        for (size_t i = 0; i < row.attnums.size(); ++i) {
            put_u16(payload, row.attnums[i]);
            put_u8 (payload, (uint8_t)row.types[i]);
            put_i32(payload, row.lengths[i]);
            if (row.lengths[i] > 0)
                payload.append(row.values[i]);
        }
    }
    return frame(PMSG_DML_BATCH, payload);
}

/* ── Buffer a DML row ─────────────────────────────────────────────── */

void PaimonClient::bufferRow(uint32_t table_oid,
                             const std::string& table_name,
                             uint32_t schema_version,
                             const std::vector<PaimonColumn>& cols,
                             const std::vector<uint16_t>& pk_attnums,
                             PaimonRowKind kind,
                             uint64_t seq,
                             const std::vector<uint16_t>& attnums,
                             const std::vector<PaimonTypeCode>& types,
                             const std::vector<int32_t>& lengths,
                             const std::vector<std::string>& values) {
    if (!connect()) return;

    PaimonRelBuffer& buf = buffers_[table_oid];
    if (buf.table_oid == 0) {
        buf.table_oid      = table_oid;
        buf.table_name     = table_name;
        buf.schema_version = schema_version;
        buf.columns        = cols;
        buf.pk_attnums     = pk_attnums;
    } else if (buf.schema_version != schema_version) {
        /* Schema changed mid-transaction — flush and update. */
        buf.schema_version = schema_version;
        buf.columns        = cols;
        buf.pk_attnums     = pk_attnums;
    }

    PaimonRow row;
    row.kind    = kind;
    row.seq     = seq;
    row.attnums = attnums;
    row.types   = types;
    row.lengths = lengths;
    row.values  = values;

    size_t row_bytes = 1 + 8 + 2;
    for (size_t i = 0; i < attnums.size(); ++i)
        row_bytes += 2 + 1 + 4 + (lengths[i] > 0 ? lengths[i] : 0);
    buf.byte_size += row_bytes;
    buf.rows.push_back(std::move(row));
}

/* ── Flush one relation buffer to the ring ────────────────────────── */

void PaimonClient::flushBuffer(PaimonRelBuffer& buf) {
    if (buf.rows.empty()) return;
    if (!connect()) { buf.rows.clear(); buf.byte_size = 0; return; }

    ensureSchema(buf);

    std::string payload;
    put_u32(payload, buf.table_oid);
    put_str(payload, buf.table_name);
    put_u32(payload, buf.schema_version);
    put_u32(payload, (uint32_t)buf.rows.size());

    for (const auto& row : buf.rows) {
        put_u8 (payload, (uint8_t)row.kind);
        put_u64(payload, row.seq);
        put_u16(payload, (uint16_t)row.attnums.size());
        for (size_t i = 0; i < row.attnums.size(); ++i) {
            put_u16(payload, row.attnums[i]);
            put_u8 (payload, (uint8_t)row.types[i]);
            put_i32(payload, row.lengths[i]);
            if (row.lengths[i] > 0)
                payload.append(row.values[i]);
        }
    }

    sendToRing(frame(PMSG_DML_BATCH, payload));
    buf.rows.clear();
    buf.byte_size = 0;
}

/*
 * prepareCommit — serialise all buffered rows into pending_msgs_ and, when
 * paimon_heap.wal_level = 'local', write + flush a WAL record so the messages
 * are durable before the transaction commit record hits the WAL.
 *
 * Must be called at XACT_EVENT_PRE_COMMIT (before RecordTransactionCommit)
 * so the WAL flush is absorbed into the commit's fsync at no extra I/O.
 */
void PaimonClient::prepareCommit() {
    pending_msgs_.clear();

    if (!connect()) return;

    bool had_rows = false;
    for (auto& [oid, buf] : buffers_) {
        if (buf.rows.empty()) continue;
        had_rows = true;

        /* Include a schema bootstrap message if this version has not been sent. */
        auto it = sent_schema_version_.find(oid);
        if (it == sent_schema_version_.end() ||
            it->second != buf.schema_version) {
            pending_msgs_.push_back(makeSchemaMsg(buf));
            sent_schema_version_[oid] = buf.schema_version;
        }

        pending_msgs_.push_back(makeBatchMsg(buf));
        buf.rows.clear();
        buf.byte_size = 0;
    }

    if (!had_rows) return;

    pending_msgs_.push_back(frame(PMSG_SYNC, ""));

    /* WAL record: all pending messages in one XLogInsert + XLogFlush. */
    if (paimon_wal_is_local())
        paimon_wal_write(pending_msgs_);
}

/* ── Write pending_msgs_ to ring (called at XACT_EVENT_COMMIT) ───── */

void PaimonClient::flushAll() {
    if (!connect()) { pending_msgs_.clear(); return; }
    for (const auto& msg : pending_msgs_)
        sendToRing(msg);
    pending_msgs_.clear();
}

void PaimonClient::discardAll() {
    for (auto& [oid, buf] : buffers_) {
        buf.rows.clear();
        buf.byte_size = 0;
    }
    pending_msgs_.clear();
}

/* ── DDL helpers ──────────────────────────────────────────────────── */

std::string PaimonClient::ddlCreate(uint32_t table_oid,
                                     const std::string& table_name,
                                     const std::vector<PaimonColumn>& cols,
                                     const std::vector<uint16_t>& pk_attnums) {
    if (!connect()) return "";
    std::string payload;
    put_u32(payload, table_oid);
    put_str(payload, table_name);
    put_u32(payload, (uint32_t)cols.size());
    for (const auto& c : cols) {
        put_u16(payload, c.attnum);
        put_u8 (payload, (uint8_t)c.type_code);
        put_u32(payload, c.typmod);
        put_u8 (payload, c.not_null ? 1 : 0);
        put_str(payload, c.name);
    }
    put_u32(payload, (uint32_t)pk_attnums.size());
    for (uint16_t pk : pk_attnums) put_u16(payload, pk);
    sendToRing(frame(PMSG_DDL_CREATE, payload));
    return "";
}

std::string PaimonClient::ddlAddColumn(uint32_t table_oid,
                                        const PaimonColumn& col) {
    if (!connect()) return "";
    std::string payload;
    put_u32(payload, table_oid);
    put_u16(payload, col.attnum);
    put_u8 (payload, (uint8_t)col.type_code);
    put_u32(payload, col.typmod);
    put_u8 (payload, col.not_null ? 1 : 0);
    put_str(payload, col.name);
    sendToRing(frame(PMSG_DDL_ADD_COL, payload));
    return "";
}

std::string PaimonClient::ddlDropColumn(uint32_t table_oid, uint16_t attnum) {
    if (!connect()) return "";
    std::string payload;
    put_u32(payload, table_oid);
    put_u16(payload, attnum);
    sendToRing(frame(PMSG_DDL_DROP_COL, payload));
    return "";
}

std::string PaimonClient::ddlRenameColumn(uint32_t table_oid, uint16_t attnum,
                                           const std::string& new_name) {
    if (!connect()) return "";
    std::string payload;
    put_u32(payload, table_oid);
    put_u16(payload, attnum);
    put_str(payload, new_name);
    sendToRing(frame(PMSG_DDL_RENAME, payload));
    return "";
}

std::string PaimonClient::ddlTypeChange(uint32_t table_oid, uint16_t attnum,
                                         PaimonTypeCode new_type,
                                         uint32_t new_typmod) {
    if (!connect()) return "";
    std::string payload;
    put_u32(payload, table_oid);
    put_u16(payload, attnum);
    put_u8 (payload, (uint8_t)new_type);
    put_u32(payload, new_typmod);
    sendToRing(frame(PMSG_DDL_TYPE, payload));
    return "";
}

std::string PaimonClient::ddlTruncate(uint32_t table_oid,
                                       const std::string& table_name) {
    if (!connect()) return "";
    std::string payload;
    put_u32(payload, table_oid);
    put_str(payload, table_name);
    sendToRing(frame(PMSG_DDL_TRUNCATE, payload));
    return "";
}

std::string PaimonClient::ddlDropTable(uint32_t table_oid,
                                        const std::string& table_name) {
    if (!connect()) return "";
    std::string payload;
    put_u32(payload, table_oid);
    put_str(payload, table_name);
    sendToRing(frame(PMSG_DDL_DROP_TBL, payload));
    return "";
}

std::string PaimonClient::ddlPkChange(uint32_t /*table_oid*/,
                                       const std::string& table_name) {
    return "Changing primary key of a Paimon-offloaded table is not supported: " +
           table_name;
}

/* ── C-callable wrappers ─────────────────────────────────────────── */

extern "C" {

int paimon_client_ddl_pk_change(uint32_t table_oid, const char* table_name,
                                char* err_buf, int err_buf_size) {
    std::string err = PaimonClient::instance().ddlPkChange(table_oid, table_name);
    if (!err.empty()) {
        strncpy(err_buf, err.c_str(), err_buf_size - 1);
        err_buf[err_buf_size - 1] = '\0';
        return 1;
    }
    return 0;
}

int paimon_client_ddl_add_col(uint32_t table_oid, uint16_t attnum,
                              uint8_t type_code, uint32_t typmod,
                              uint8_t not_null, const char* col_name) {
    PaimonColumn c;
    c.attnum    = attnum;
    c.type_code = (PaimonTypeCode)type_code;
    c.typmod    = typmod;
    c.not_null  = (not_null != 0);
    c.name      = col_name;
    PaimonClient::instance().ddlAddColumn(table_oid, c);
    return 0;
}

int paimon_client_ddl_drop_col(uint32_t table_oid, uint16_t attnum) {
    PaimonClient::instance().ddlDropColumn(table_oid, attnum);
    return 0;
}

int paimon_client_ddl_rename(uint32_t table_oid, uint16_t attnum,
                             const char* new_name) {
    PaimonClient::instance().ddlRenameColumn(table_oid, attnum, new_name);
    return 0;
}

int paimon_client_ddl_type_change(uint32_t table_oid, uint16_t attnum,
                                  uint8_t new_type_code, uint32_t new_typmod) {
    PaimonClient::instance().ddlTypeChange(
        table_oid, attnum, (PaimonTypeCode)new_type_code, new_typmod);
    return 0;
}

int paimon_client_ddl_truncate(uint32_t table_oid, const char* table_name) {
    PaimonClient::instance().ddlTruncate(table_oid, table_name);
    return 0;
}

int paimon_client_ddl_drop_table(uint32_t table_oid, const char* table_name) {
    PaimonClient::instance().ddlDropTable(table_oid, table_name);
    return 0;
}

void paimon_client_prepare_commit(void) {
    PaimonClient::instance().prepareCommit();
}

void paimon_client_flush_all(void) {
    PaimonClient::instance().flushAll();
}

void paimon_client_discard_all(void) {
    PaimonClient::instance().discardAll();
}

} // extern "C"
