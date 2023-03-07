/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
using System.Text;
using Google.Protobuf.WellKnownTypes;
using ImmudbProxy;

namespace ImmuDB;

/// <summary>
/// Represents an ImmuDB Entry value without reference, such as the value of a specific key, plus metadata
/// </summary>
public readonly ref struct EntrySpec
{
    /// <summary>
    /// Gets the key
    /// </summary>
    /// <value></value>
    public ReadOnlySpan<byte> Key { get; }

    /// <summary>
    /// Gets the value
    /// </summary>
    /// <value></value>
    public ReadOnlySpan<byte> Value { get; }

    /// <summary>
    /// Gets the metadata
    /// </summary>
    /// <value></value>
    public KVMetadata? Metadata { get; }

    /// <summary>
    /// Creates an EntrySpec instance
    /// </summary>
    /// <param name="key">The key</param>
    /// <param name="metadata">The metadata</param>
    /// <param name="value">The wrapped reference value</param>
    public EntrySpec(ReadOnlySpan<byte> key, KVMetadata? metadata, ReadOnlySpan<byte> value) {
        Key = key;
        Metadata = metadata;
        Value = value;
    }

    /// <summary>
    /// Wraps a Reference Key
    /// </summary>
    /// <param name="referenceKey">The reference</param>
    /// <param name="atTx">Transaction ID</param>
    public static bool WrapReference(ReadOnlySpan<byte> referenceKey, ulong atTx, Span<byte> result)
    {
        // Prepending 1 byte, Appending a ulong (8 bytes) with a prefix (1 byte) = 10 bytes
        if (result.Length != referenceKey.Length + 10)
            throw new InvalidOperationException($"referenceKey result must be of length: {referenceKey.Length + 10}, given: {result.Length}");

        result[0] = Consts.SET_KEY_PREFIX;
        return Utils.WrapReferenceValueAt(referenceKey, result.Slice(1), atTx);
    }

    /// <summary>
    /// Gets the string representation of the value
    /// </summary>
    /// <returns></returns>
    public override string ToString()
    {
        if((Value == null) || (Value.Length == 0)) {
            return "";
        }
        return Encoding.UTF8.GetString(Value);
    }

    

    /// <summary>
    /// Gets the encoded key
    /// </summary>
    /// <returns></returns>
    public ReadOnlySpan<byte> GetEncodedKey()
    {
        return Utils.WrapWithPrefix(Key, Consts.SET_KEY_PREFIX);
    }

    /// <summary>
    /// Gets the digest for a version
    /// </summary>
    /// <param name="version">The version</param>
    /// <returns></returns>
    public ReadOnlySpan<byte> DigestFor(int version)
    {
        KV kv;
        kv = new KV(
                Utils.WrapWithPrefix(Key, Consts.SET_KEY_PREFIX),
                Metadata,
                Value
        );
        return kv.DigestFor(version);
    }

}
