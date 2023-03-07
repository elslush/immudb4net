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

using ImmuDB.Crypto;
using ImmudbProxy;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;

namespace ImmuDB;

/// <summary>
/// KV represents a key value pair with metadata
/// </summary>
public readonly ref struct KV
{
    /// <summary>
    /// Gets the key
    /// </summary>
    /// <value></value>
    public ReadOnlySpan<byte> Key { get; }
    /// <summary>
    /// Gets the metadata
    /// </summary>
    /// <value></value>
    public KVMetadata? Metadata { get; }
    /// <summary>
    /// Gets the value
    /// </summary>
    /// <value></value>
    public ReadOnlySpan<byte> Value { get; }

    /// <summary>
    /// Creates a new KV instance
    /// </summary>
    /// <param name="key">The key</param>
    /// <param name="metadata">The metadata</param>
    /// <param name="value">The value</param>
    public KV(ReadOnlySpan<byte> key, KVMetadata? metadata, ReadOnlySpan<byte> value)
    {
        Key = key;
        Metadata = metadata;
        Value = value;
    }
    /// <summary>
    /// Computes the digest for a specific version
    /// </summary>
    /// <param name="version">The version number</param>
    /// <returns></returns>
    public bool DigestFor(in int version, Span<byte> result)
    {
        if (result.Length != 32)
            return false;

        return version switch
        {
            0 => Digest_v0(result),
            1 => Digest_v1(result),
            _ => throw new InvalidOperationException("unsupported tx header version"),
        };
    }

    bool Digest_v0(Span<byte> result)
    {
        if (Metadata != null)
        {
            throw new InvalidOperationException("metadata is unsupported when in 1.1 compatibility mode");
        }

        Span<byte> b = stackalloc byte[Key.Length + SHA256.HashSizeInBytes];
        if (!Key.TryCopyTo(b[..Key.Length]))
            return false;


        if (!CanHash())
            return false;

        if (!Value[..SHA256.HashSizeInBytes].TryCopyTo(b.Slice(Key.Length, SHA256.HashSizeInBytes)))
            return false;

        return CryptoUtils.Sha256Sum(b, result, out int bytesWritten)
            && bytesWritten == SHA256.HashSizeInBytes;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool CanHash()
    {
        Span<byte> hvalue = stackalloc byte[SHA256.HashSizeInBytes];
        return CryptoUtils.Sha256Sum(Value, hvalue, out _);
    }

    bool Digest_v1(Span<byte> result)
    {
        //byte[] mdbs = new byte[] { };
        int mdLen = Metadata is null
            ? 0
            : Metadata.Value.SerializedLength;

        Span<byte> bytes = stackalloc byte[2 + mdLen + 2 + Key.Length + Consts.SHA256_SIZE];
        var min = 0;

        Utils.WriteWithBigEndian(bytes.Slice(min += 2, 2), (short)mdLen);
        if (Metadata is not null)
            Metadata.Value.Serialize(bytes.Slice(min += mdLen, mdLen));
        Utils.WriteWithBigEndian(bytes.Slice(min += 2, 2), (short)Key.Length);
        Key.CopyTo(bytes.Slice(min += Key.Length, Key.Length));
        CryptoUtils.Sha256Sum(Value, bytes.Slice(min, Consts.SHA256_SIZE), out _);
        return CryptoUtils.Sha256Sum(bytes, result, out _);

        //MemoryStream bytes = new MemoryStream(2 + mdLen + 2 + Key.Length + Consts.SHA256_SIZE);
        //using (BinaryWriter bw = new BinaryWriter(bytes))
        //{
        //    Utils.WriteWithBigEndian(bw, (short)mdLen);
        //    if (mdLen > 0)
        //    {
        //        Utils.WriteArray(bw, mdbs);
        //    }
        //    Utils.WriteWithBigEndian(bw, (short)Key.Length);
        //    Utils.WriteArray(bw, Key);
        //    Utils.WriteArray(bw, CryptoUtils.Sha256Sum(Value));
        //}
        //return CryptoUtils.Sha256Sum(bytes.ToArray());
    }
}