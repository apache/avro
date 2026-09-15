/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
using System;
using Avro.IO;

namespace Avro.Generic
{
    /// <summary>
    /// Shared allocation guards for decoding Avro collections (arrays and maps),
    /// used by every generic/specific reader. Avro encodes a collection as one or
    /// more blocks, each prefixed with an element count; a malicious or truncated
    /// input can declare far more elements than the stream could ever hold, driving
    /// an unbounded allocation from a tiny payload. These helpers reject such counts
    /// before anything is allocated. The same logic backs both reader
    /// implementations (<see cref="DefaultReader"/> and
    /// <see cref="PreresolvingDatumReader{T}"/>) so the caps cannot drift apart.
    /// </summary>
    internal static class CollectionBounds
    {
        // Collection allocation limits, guarding against a block-count DoS. Both
        // default to the same values as the other Avro SDKs and can be overridden
        // (to a single value capping both) via the AVRO_MAX_COLLECTION_ITEMS
        // environment variable.
        internal static readonly long MaxCollectionItems = ReadCollectionLimit(10_000_000L);

        // The largest array the runtime can allocate. Mirrors
        // BinaryDecoder.MaxDotNetArrayLength: the readers size .NET arrays from the
        // (cumulative) block count, which throws (OutOfMemoryException/
        // OverflowException) above this length rather than a deterministic
        // AvroException.
#if NETSTANDARD2_0
        private const int MaxDotNetArrayLength = 0x3FFFFFFF;
#else
        private const int MaxDotNetArrayLength = 0x7FFFFFC7;
#endif

        // The structural cap is additionally clamped to the runtime's maximum
        // array length: the callers cast the (cumulative) block count to int to
        // size .NET collections, and a limit above the max array length (e.g. from
        // a large env override, or int.MaxValue itself) would let a collection
        // that passes EnsureCollectionAvailable still fault inside Array.Resize
        // instead of failing deterministically.
        internal static readonly long MaxCollectionStructural =
            Math.Min(ReadCollectionLimit(2147483639L), MaxDotNetArrayLength);

        // Upper bound on how many elements the backing array is grown by in a
        // single step while decoding. The array still grows to hold every element
        // actually read; this only avoids resizing to the full (possibly
        // attacker-declared) block count up front, before any element is read.
        // That matters most for non-seekable streams, where the bytes-available
        // check cannot bound the declared count, so a single resize to the block
        // count could allocate a huge array before the truncated stream is
        // detected.
        internal const int MaxCollectionPrealloc = 1024;

        private static long ReadCollectionLimit(long defaultValue)
        {
            string env = Environment.GetEnvironmentVariable("AVRO_MAX_COLLECTION_ITEMS");
            if (!string.IsNullOrEmpty(env) && long.TryParse(env, out long value) && value >= 0)
            {
                return value;
            }

            return defaultValue;
        }

        // Per-thread, per-datum cumulative count of zero-byte-encoded collection
        // elements (e.g. an array of nulls). Such elements consume no input, so
        // the bytes-remaining check cannot bound them, and a per-collection cap is
        // not enough either: a record's schema can declare many zero-byte
        // collection fields, each block under the limit but jointly unbounded. The
        // budget is therefore cumulative across a whole datum. It is thread-static,
        // not reader instance state, because a resolved reader may be reused or
        // shared among threads (see PreresolvingDatumReader), which such an
        // instance field would make unsafe.
        [ThreadStatic] private static long zeroByteItemsRead;

        // Nesting depth of the active decode scope on this thread. A delegated
        // reader or a skipped writer field decodes within the enclosing datum's
        // scope and accumulates into its budget; only the outermost scope resets
        // the running total.
        [ThreadStatic] private static int scopeDepth;

        /// <summary>
        /// Opens a decode scope bounding the cumulative zero-byte-element
        /// allocation for the current datum. Scopes nest: a nested scope (a
        /// delegated reader or a skipped field) accumulates into the enclosing
        /// datum's budget, and only the outermost scope resets the running total,
        /// so the cap applies across the whole datum rather than per collection.
        /// Dispose the returned scope (via <c>using</c>) once the datum is decoded;
        /// the budget is thread-static, so the scope must be closed on the same
        /// thread, and it is always closed so state cannot leak into later decodes.
        /// </summary>
        internal static Scope EnterScope()
        {
            if (scopeDepth == 0)
            {
                zeroByteItemsRead = 0;
            }

            scopeDepth++;
            return default;
        }

        /// <summary>
        /// The disposable returned by <see cref="EnterScope"/>. A stateless struct
        /// so <c>using</c> incurs no allocation; closing the outermost scope resets
        /// the per-datum budget.
        /// </summary>
        internal readonly struct Scope : IDisposable
        {
            /// <inheritdoc/>
            public void Dispose()
            {
                if (--scopeDepth == 0)
                {
                    zeroByteItemsRead = 0;
                }
            }
        }

        /// <summary>
        /// Minimum number of bytes a single value of the given schema can occupy
        /// on the wire. Used to reject an array/map block count that could not be
        /// backed by the bytes remaining. A type that encodes to zero bytes
        /// returns 0 (not only <c>null</c>, but also composites that encode to
        /// nothing, e.g. a record whose fields are all zero-byte), which disables
        /// the bytes-remaining check for it (so an array of such elements is not
        /// falsely rejected; they are instead bounded by the zero-byte item cap).
        /// A depth limit breaks self-referencing schemas.
        /// </summary>
        internal static int MinBytesPerElement(Schema schema, int depth = 0)
        {
            if (schema == null)
            {
                return 0;
            }

            switch (schema.Tag)
            {
                case Schema.Type.Null:
                    return 0;
                case Schema.Type.Float:
                    return 4;
                case Schema.Type.Double:
                    return 8;
                case Schema.Type.Fixed:
                    return ((FixedSchema)schema).Size;
                case Schema.Type.Record:
                case Schema.Type.Error:
                    if (depth > 64)
                    {
                        // A cyclic or pathologically deep record. Return 1 (not
                        // 0) so the collection check stays enabled; a valid
                        // recursive value always encodes to >= 1 byte. The depth
                        // guard is applied only here, so zero-byte leaf types
                        // such as null still return 0 regardless of depth.
                        return 1;
                    }

                    // Accumulate in a long and clamp so a deeply nested schema
                    // cannot overflow int into a value <= 0, which would disable
                    // the collection check.
                    long total = 0;
                    foreach (Field f in (RecordSchema)schema)
                    {
                        total += MinBytesPerElement(f.Schema, depth + 1);
                        if (total >= int.MaxValue)
                        {
                            return int.MaxValue;
                        }
                    }

                    return (int)total;
                default:
                    // boolean, int, long, bytes, string, enum, union, array, map:
                    // all encode to at least one byte.
                    return 1;
            }
        }

        /// <summary>
        /// Rejects a collection (array or map) block that could drive an unbounded
        /// allocation, before allocating for it. A block whose declared element
        /// count could not be backed by the bytes actually remaining is rejected;
        /// zero-byte element blocks (where the bytes-remaining check does not
        /// apply) are bounded by a cumulative item cap; and every collection is
        /// bounded by a structural cap. Returns the running total across blocks.
        /// </summary>
        /// <param name="d">Decoder the collection is being read from.</param>
        /// <param name="total">Running element total across the blocks decoded so far for this collection.</param>
        /// <param name="count">Element count declared by the current block.</param>
        /// <param name="minBytesPerElement">Minimum on-wire size of one element (see <see cref="MinBytesPerElement"/>).</param>
        internal static long EnsureCollectionAvailable(Decoder d, long total, long count, long minBytesPerElement)
        {
            // A negative count is corrupt/malicious data (it can also arise from
            // long.MinValue overflow when negating a negative block count), and
            // the callers cast the block count to int; reject it explicitly.
            if (count < 0)
            {
                throw new AvroException($"Invalid negative collection block count: {count}");
            }

            // Reject before adding so an oversized block count cannot overflow
            // `total` (wrapping it negative and bypassing the caps below). The
            // running total is always <= MaxCollectionStructural on entry (the
            // invariant this method maintains) and count >= 0, so the subtraction
            // cannot underflow or overflow.
            if (count > MaxCollectionStructural - total)
            {
                throw new AvroException(
                    $"Collection size {total} + {count} exceeds the maximum allowed size of {MaxCollectionStructural}");
            }

            total += count;

            if (minBytesPerElement <= 0)
            {
                // Zero-byte elements (e.g. null) consume no input, so the
                // bytes-remaining check cannot bound them. Cap the cumulative
                // count across the whole datum, not just this collection: a
                // record's schema can declare many zero-byte collection fields,
                // each block under the limit but jointly unbounded.
                zeroByteItemsRead += count;
                if (zeroByteItemsRead > MaxCollectionItems)
                {
                    throw new AvroException(
                        $"Collection of zero-byte elements ({zeroByteItemsRead}) exceeds the maximum allowed size of {MaxCollectionItems}");
                }
            }
            else if (d is BinaryDecoder bd)
            {
                long remaining = bd.RemainingBytes();
                if (remaining >= 0 && count > remaining / minBytesPerElement)
                {
                    throw new AvroException(
                        $"Collection claims {count} elements with at least {minBytesPerElement} bytes each, but only {remaining} bytes are available");
                }
            }

            return total;
        }
    }
}
