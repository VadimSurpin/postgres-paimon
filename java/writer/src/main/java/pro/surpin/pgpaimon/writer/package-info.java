// SPDX-License-Identifier: Apache-2.0
/**
 * Writer value types and configuration.
 *
 * <p><b>OID rule (no exceptions):</b> every PostgreSQL OID (relation id, type oid, ...) is an
 * unsigned 32-bit value (0 .. 4294967295) and is held in a Java {@code long}, never an
 * {@code int}. Normalise it with {@code Integer.toUnsignedLong(buffer.getInt())} from the
 * binary pgoutput protocol, and with {@code Long.parseLong(...)} from DDL-message JSON
 * (jsonb renders an oid as a decimal string). Both routes then give the same non-negative
 * number, and a large OID can never throw or appear negative.
 */
package pro.surpin.pgpaimon.writer;
