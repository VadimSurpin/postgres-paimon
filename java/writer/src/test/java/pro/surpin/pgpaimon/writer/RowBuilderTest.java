// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.types.*;
import org.junit.jupiter.api.Test;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class RowBuilderTest {

    private static RowType paimonSchema() {
        return RowType.of(
            new DataType[] { DataTypes.INT().notNull(), DataTypes.STRING(),
                             DataTypes.BIGINT().notNull(), DataTypes.INT().notNull() },
            new String[]   { "id", "v", "_seq", "_op_seq" });
    }

    @Test
    void mapsPgColumnsByNameAndAppendsMetadata() {
        RowBuilder b = new RowBuilder(paimonSchema(), List.of("id", "v"),
                                      "_seq", "_op_seq", null);
        GenericRow row = b.build(new SinkRecord(16412, RowKind.UPDATE_AFTER,
                                 List.of("7", "hello"), 0x16B374D8L, 3, 4711L));
        assertEquals(4, row.getFieldCount());
        assertEquals(7, row.getInt(0));
        assertEquals("hello", row.getString(1).toString());
        assertEquals(0x16B374D8L, row.getLong(2));
        assertEquals(3, row.getInt(3));
        assertEquals(RowKind.UPDATE_AFTER, row.getRowKind());
    }

    @Test
    void pgColumnOrderDifferentFromPaimonOrderStillMapsCorrectly() {
        RowBuilder b = new RowBuilder(paimonSchema(), List.of("v", "id"),
                                      "_seq", "_op_seq", null);
        GenericRow row = b.build(new SinkRecord(16412, RowKind.INSERT,
                                 List.of("hello", "7"), 1L, 0, 1L));
        assertEquals(7, row.getInt(0));
        assertEquals("hello", row.getString(1).toString());
    }

    @Test
    void xidIsWrittenWhenTheSchemaHasIt() {
        RowType withXid = RowType.of(
            new DataType[] { DataTypes.INT().notNull(), DataTypes.BIGINT().notNull(),
                             DataTypes.INT().notNull(), DataTypes.BIGINT() },
            new String[]   { "id", "_seq", "_op_seq", "_xid" });
        RowBuilder b = new RowBuilder(withXid, List.of("id"), "_seq", "_op_seq", "_xid");
        GenericRow row = b.build(new SinkRecord(1, RowKind.INSERT, List.of("1"), 1L, 0, 4711L));
        assertEquals(4711L, row.getLong(3));
    }

    @Test
    void aPaimonColumnWithNoPgCounterpartIsNull() {
        RowBuilder b = new RowBuilder(paimonSchema(), List.of("id"),
                                      "_seq", "_op_seq", null);
        GenericRow row = b.build(new SinkRecord(1, RowKind.INSERT, List.of("1"), 1L, 0, 1L));
        assertTrue(row.isNullAt(1), "v was dropped in PG but is still in the Paimon schema");
    }

    @Test
    void aPgColumnWithNoPaimonCounterpartIsRejected() {
        RowBuilder b = new RowBuilder(paimonSchema(), List.of("id", "v", "surprise"),
                                      "_seq", "_op_seq", null);
        IllegalStateException e = assertThrows(IllegalStateException.class,
            () -> b.build(new SinkRecord(1, RowKind.INSERT,
                          List.of("1", "x", "y"), 1L, 0, 1L)));
        assertTrue(e.getMessage().contains("surprise"));
    }
}
