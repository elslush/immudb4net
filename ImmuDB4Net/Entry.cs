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
using System.Security.Cryptography;
using System.Text;

namespace ImmuDB;

/// <summary>
/// Represents an ImmuDB value, such as the value of a specific key
/// </summary>
public readonly struct Entry
{
    /// <summary>
    /// Gets the transaction ID
    /// </summary>
    /// <value></value>
    public ulong Tx { get; }

    /// <summary>
    /// Gets the key
    /// </summary>
    /// <value></value>
    public ReadOnlyMemory<byte> Key { get; }

    /// <summary>
    /// Gets the value
    /// </summary>
    /// <value></value>
    public ReadOnlyMemory<byte> Value { get; }

    /// <summary>
    /// Gets the metadata
    /// </summary>
    /// <value></value>
    public KVMetadata? Metadata { get; }

    /// <summary>
    /// Gets the reference
    /// </summary>
    /// <value></value>
    public Reference? ReferencedBy { get; }

    private Entry(ulong tx,
        ReadOnlyMemory<byte> key,
        ReadOnlyMemory<byte> value,
        KVMetadata? metadata,
        Reference? referencedBy
    )
    {
        Tx = tx;
        Key = key;
        Value = value;
        Metadata = metadata;
        ReferencedBy = referencedBy;
    }

    /// <summary>
    /// Gets the string representation of the value
    /// </summary>
    /// <returns></returns>
    public override string ToString()
    {
        if(Value.Length == 0)
        {
            return "";
        }
        return Encoding.UTF8.GetString(Value.Span);
    }

    /// <summary>
    /// Converts from a gRPC entry
    /// </summary>
    /// <param name="e"></param>
    /// <returns></returns>
    public static Entry ValueOf(ImmudbProxy.Entry e)
    {
        ImmudbProxy.Entry proxyInst = e ?? ImmudbProxy.Entry.DefaultInstance;
        Entry entry = new(
            proxyInst.Tx,
            proxyInst.Key.Memory,
            proxyInst.Value.Memory,
            proxyInst.Metadata is null ? null : KVMetadata.ValueOf(proxyInst.Metadata),
            proxyInst.ReferencedBy is null ? null : Reference.ValueOf(proxyInst.ReferencedBy)
        );

        return entry;
    }

    /// <summary>
    /// Gets the encoded key
    /// </summary>
    /// <returns>Whether the key was copied successfully</returns>
    public bool GetEncodedKey(Span<byte> result)
    {
        if (ReferencedBy == null)
        {
            return Utils.WrapWithPrefix(Key.Span, result, Consts.SET_KEY_PREFIX);
        }

        return Utils.WrapWithPrefix(ReferencedBy.Value.Key.Span, result, Consts.SET_KEY_PREFIX);
    }

    /// <summary>
    /// Gets the encoded key Length.
    /// </summary>
    /// <returns></returns>
    public int GetEncodedKeyLength => ReferencedBy is null
        ? Key.Length + 1
        : ReferencedBy.Value.Key.Length + 1;

    /// <summary>
    /// Gets the digest for a version
    /// </summary>
    /// <param name="version">The version</param>
    /// <returns></returns>
    public bool DigestFor(in int version, Span<byte> result)
    {
        if (result.Length != Consts.SHA256_SIZE)
            throw new InvalidOperationException($"result must be of length: {Consts.SHA256_SIZE}, given: {result.Length}");

        Span<byte> encodedKey = stackalloc byte[GetEncodedKeyLength];
        if (!GetEncodedKey(encodedKey))
            return false;

        if (ReferencedBy is null)
        {
            Span<byte> value = stackalloc byte[Value.Length + 1];
            if (!Utils.WrapWithPrefix(Value.Span, value, Consts.PLAIN_VALUE_PREFIX))
                return false;

            var kv = new KV(
                    encodedKey,
                    Metadata,
                    value
            );
            return kv.DigestFor(version, result);
        }
        else
        {
            Span<byte> value = stackalloc byte[Value.Length + 10];
            value[0] = Consts.SET_KEY_PREFIX;
            if (!Utils.WrapReferenceValueAt(Key.Span, value, ReferencedBy.Value.AtTx))
                return false;

            var kv = new KV(
                    encodedKey,
                    ReferencedBy.Value.Metadata,
                    value
            );
            return kv.DigestFor(version, result);
        }
    }
}
