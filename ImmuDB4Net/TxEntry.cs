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

using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using ImmuDB.Crypto;
using ImmudbProxy;

namespace ImmuDB;

/// <summary>
/// Represents A transaction entry that belongs to a <see cref="Tx" /> class
/// </summary>
public readonly ref struct TxEntry
{
    /// <summary>
    /// Gets the key
    /// </summary>
    /// <value></value>
    public ReadOnlySpan<byte> Key { get; }
    /// <summary>
    /// The transaction metadata
    /// </summary>
    /// <value></value>
    public KVMetadata? Metadata { get; }
    /// <summary>
    /// The VLength parameter
    /// </summary>
    /// <value></value>
    public int VLength { get; }
    /// <summary>
    /// The hash value
    /// </summary>
    /// <value></value>
    public ReadOnlySpan<byte> HVal { get; }

    private TxEntry(ReadOnlySpan<byte> key, KVMetadata metadata, int vLength, ReadOnlySpan<byte>  hVal)
    {
        //this.Key = new byte[key.Length];
        //Array.Copy(key, 0, this.Key, 0, key.Length);
        this.Key = key;

        this.Metadata = metadata;
        this.VLength = vLength;
        this.HVal = hVal;
    }

    /// <summary>
    /// Converts from a gRPC protobuf TxEntry instance
    /// </summary>
    /// <param name="txe"></param>
    /// <returns></returns>
    public static TxEntry ValueOf(ImmudbProxy.TxEntry txe)
    {
        KVMetadata md = new KVMetadata();

        if (txe.Metadata != null)
        {
            md = KVMetadata.ValueOf(txe.Metadata);
        }

        return new TxEntry(
                        txe.Key.Span,
                        md,
                        txe.VLen,
                        CryptoUtils.DigestFrom(txe.HValue.ToByteArray())
                );
    }

    /// <summary>
    /// Calculates the digest for a specific version
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

    /// <summary>
    /// Calculates the digest for version 0
    /// </summary>
    /// <returns></returns>
    public bool Digest_v0(Span<byte> result)
    {
        if (Metadata != null)
        {
            throw new InvalidOperationException("metadata is unsupported when in 1.1 compatibility mode");
        }

        Span<byte> b = stackalloc byte[Key.Length + Consts.SHA256_SIZE];

        Key.CopyTo(b[..Key.Length]);
        HVal.CopyTo(b.Slice(Key.Length, Consts.SHA256_SIZE));
        //Array.Copy(Key, 0, b, 0, Key.Length);
        //Array.Copy(HVal, 0, b, Key.Length, HVal.Length);

        return CryptoUtils.Sha256Sum(b, result, out _);
    }

    /// <summary>
    /// Calculates the digest for version 1
    /// </summary>
    /// <returns></returns>
    public bool Digest_v1(Span<byte> result)
    {
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
        HVal.CopyTo(bytes.Slice(min, Consts.SHA256_SIZE));
        return CryptoUtils.Sha256Sum(bytes, result, out _);

        //byte[] mdbs = new byte[0];
        //int mdLen = 0;

        //if (Metadata != null)
        //{
        //    mdbs = Metadata.Serialize();
        //    mdLen = mdbs.Length;
        //}

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
        //    Utils.WriteArray(bw, HVal);

        //}
        //return CryptoUtils.Sha256Sum(bytes.ToArray());
    }

}