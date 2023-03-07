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

using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using CommunityToolkit.HighPerformance;
using Google.Protobuf;
using Google.Protobuf.Collections;

namespace ImmuDB.Crypto;

internal static class CryptoUtils
{
    // FYI: Interesting enough, Go returns a fixed value for sha256.Sum256(nil) and this value is:
    // [227 176 196 66 152 252 28 20 154 251 244 200 153 111 185 36 39 174 65 228 100 155 147 76 164 149 153 27 120 82 184 85]
    // whose Base64 encoded value is 47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=.
    // So we treat this case as in Go.
    private static readonly byte[] SHA256_SUM_OF_NULL = Convert.FromBase64String("47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=");

    /// <summary>
    /// This method returns a SHA256 digest of the provided data.
    /// </summary>
    /// <param name="data"></param>
    /// <returns></returns>
    public static bool Sha256Sum(ReadOnlySpan<byte> data, Span<byte> output, out int bytesWritten)
    {
        if ((data == null) || data.Length == 0)
        {
            var isSuccess = SHA256_SUM_OF_NULL.AsSpan().TryCopyTo(output);
            if (isSuccess)
                bytesWritten = SHA256_SUM_OF_NULL.Length;
            else
                bytesWritten = 0;
            return isSuccess;
        }
        
        return SHA256.TryHashData(data, output, out bytesWritten);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void DigestsFrom(in RepeatedField<ByteString> terms, Span2D<byte> result)
    {
        if (terms == null)
            return;

        for (int i = 0; i < terms.Count; i++)
            for (int j = 0; j < SHA256.HashSizeInBytes; j++)
                result[i, j] = terms[i][j];
    }

    /// <summary>
    /// Copies the provided `digest` array into a byte[32] array.
    /// </summary>
    /// <param name="digest"></param>
    /// <returns></returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool DigestFrom(ReadOnlySpan<byte> digest, Span<byte> result)
    {
        if(digest == null || digest.Length != result.Length)
            return false;

        return digest.TryCopyTo(result);
    }

    public static bool VerifyInclusion(Span2D<byte> iProof, ulong i, ulong j, ReadOnlySpan<byte> iLeaf, ReadOnlySpan<byte> jRoot)
    {
        if (i > j || i == 0 || (i < j && iProof.Length == 0))
            return false;

        Span<byte> ciRoot = stackalloc byte[iLeaf.Length];
        iLeaf.CopyTo(ciRoot);

        evalInclusion(iProof, i, j, ciRoot);
        return jRoot.SequenceEqual(ciRoot);
    }

    private static void evalInclusion(Span2D<byte> iProof, ulong i, ulong j,  Span<byte> ciRoot)
    {
        ulong i1 = i - 1;
        ulong j1 = j - 1;

        Span<byte> b = stackalloc byte[1 + SHA256.HashSizeInBytes * 2];
        b[0] = Consts.NODE_PREFIX;

        for (int rowNum = 0; rowNum < iProof.Height; rowNum++)
        {
            Span<byte> h = iProof.GetRowSpan(rowNum);
            if (i1 % 2 == 0 && i1 != j1)
            {
                ciRoot.CopyTo(b.Slice(1));
                h.CopyTo(ciRoot.Slice(SHA256.HashSizeInBytes + 1));
                //Array.Copy(ciRoot, 0, b, 1, ciRoot.Length);
                //Array.Copy(h, 0, b, SHA256.HashSizeInBytes + 1, h.Length);
            }
            else
            {
                h.CopyTo(b.Slice(1));
                ciRoot.CopyTo(b.Slice(SHA256.HashSizeInBytes + 1));
                //Array.Copy(h, 0, b, 1, h.Length);
                //Array.Copy(ciRoot, 0, b, SHA256.HashSizeInBytes + 1, ciRoot.Length);
            }

            Sha256Sum(b, ciRoot, out _);
            i1 >>= 1;
            j1 >>= 1;
        }
    }

    public static bool VerifyInclusion(InclusionProof proof, ReadOnlySpan<byte> digest, ReadOnlySpan<byte> root)
    {
        if (proof == null)
        {
            return false;
        }

        Span<byte> leaf = stackalloc byte[1 + SHA256.HashSizeInBytes];
        leaf[0] = Consts.LEAF_PREFIX;
        digest.CopyTo(leaf.Slice(1));
        //Array.Copy(digest, 0, leaf, 1, digest.Length);

        Span<byte> calcRoot = stackalloc byte[SHA256.HashSizeInBytes];
        Sha256Sum(leaf, calcRoot, out _);
        int i = proof.Leaf;
        int r = proof.Width - 1;

        if (proof.Terms != null)
        {
            Span<byte> b = new byte[1 + 2 * SHA256.HashSizeInBytes];
            b[0] = Consts.NODE_PREFIX;

            for (int j = 0; j < proof.Terms.Length; j++)
            {
                if (i % 2 == 0 && i != r)
                {
                    calcRoot.CopyTo(b.Slice(1));
                    proof.Terms[j].CopyTo(b.Slice(1 + SHA256.HashSizeInBytes));

                    //Array.Copy(calcRoot, 0, b, 1, calcRoot.Length);
                    //Array.Copy(proof.Terms[j], 0, b, 1 + SHA256.HashSizeInBytes, proof.Terms[j].Length);
                }
                else
                {
                    proof.Terms[j].CopyTo(b.Slice(1));
                    calcRoot.CopyTo(b.Slice(1 + SHA256.HashSizeInBytes));

                    //Array.Copy(proof.Terms[j], 0, b, 1, proof.Terms[j].Length);
                    //Array.Copy(calcRoot, 0, b, 1 + SHA256.HashSizeInBytes, calcRoot.Length);
                }

                Sha256Sum(b, calcRoot, out _);
                i /= 2;
                r /= 2;
            }
        }

        return i == r && root.SequenceEqual(calcRoot);
    }

    internal static bool VerifyDualProof(in DualProof proof, in ulong sourceTxId, in ulong targetTxId,
        ReadOnlySpan<byte> sourceAlh, ReadOnlySpan<byte> targetAlh)
    {
        if (proof.SourceTxHeader == null || proof.TargetTxHeader == null
                || proof.SourceTxHeader.Id != sourceTxId
                || proof.TargetTxHeader.Id != targetTxId)
        {
            return false;
        }

        if (proof.SourceTxHeader.Id == 0
                || proof.SourceTxHeader.Id > proof.TargetTxHeader.Id)
        {
            return false;
        }

        if (!sourceAlh.SequenceEqual(proof.SourceTxHeader.Alh())
                || !targetAlh.SequenceEqual(proof.TargetTxHeader.Alh()))
        {
            return false;
        }

        if (sourceTxId < proof.TargetTxHeader.BlTxId)
        {
            if (!CryptoUtils.VerifyInclusion(proof.InclusionProof, sourceTxId,
                    proof.TargetTxHeader.BlTxId, leafFor(sourceAlh),
                    proof.TargetTxHeader.BlRoot))
            {
                return false;
            }
        }

        if (proof.SourceTxHeader.BlTxId > 0)
        {
            if (!CryptoUtils.VerifyConsistency(proof.ConsistencyProof,
                    proof.SourceTxHeader.BlTxId, proof.TargetTxHeader.BlTxId,
                    proof.SourceTxHeader.BlRoot, proof.TargetTxHeader.BlRoot))
            {
                return false;
            }
        }

        if (proof.TargetTxHeader.BlTxId > 0)
        {
            if (!VerifyLastInclusion(proof.LastInclusionProof, proof.TargetTxHeader.BlTxId,
                    leafFor(proof.targetBlTxAlh), proof.TargetTxHeader.BlRoot))
            {
                return false;
            }
        }

        if (sourceTxId < proof.TargetTxHeader.BlTxId)
        {
            return VerifyLinearProof(proof.LinearProof, proof.TargetTxHeader.BlTxId,
                    targetTxId, proof.targetBlTxAlh, targetAlh);
        }

        return VerifyLinearProof(proof.LinearProof, sourceTxId, targetTxId, sourceAlh, targetAlh);
    }

    private static byte[] leafFor(byte[] d)
    {
        byte[] b = new byte[1 + SHA256.HashSizeInBytes];
        b[0] = Consts.LEAF_PREFIX;
        Array.Copy(d, 0, b, 1, d.Length);
        return Sha256Sum(b);
    }

    private static bool VerifyLinearProof(LinearProof proof, ulong sourceTxId, ulong targetTxId,
           byte[] sourceAlh, byte[] targetAlh)
    {

        if (proof == null || proof.SourceTxId != sourceTxId || proof.TargetTxId != targetTxId)
        {
            return false;
        }
        if (proof.SourceTxId == 0 || proof.SourceTxId > proof.TargetTxId || proof.Terms.Length == 0
                || !sourceAlh.SequenceEqual(proof.Terms[0]))
        {
            return false;
        }
        if (proof.Terms.Length != (int)(targetTxId - sourceTxId + 1))
        {
            return false;
        }
        byte[] calculatedAlh = proof.Terms[0];

        for (int i = 1; i < proof.Terms.Length; i++)
        {
            byte[] bs = new byte[Consts.TX_ID_SIZE + 2 * SHA256.HashSizeInBytes];
            Utils.PutUint64(proof.SourceTxId + (ulong)i, bs);
            Array.Copy(calculatedAlh, 0, bs, Consts.TX_ID_SIZE, calculatedAlh.Length);
            Array.Copy(proof.Terms[i], 0, bs, Consts.TX_ID_SIZE + SHA256.HashSizeInBytes, proof.Terms[i].Length);
            calculatedAlh = Sha256Sum(bs);
        }

        return targetAlh.SequenceEqual(calculatedAlh);
    }

    public static bool VerifyLastInclusion(byte[][] iProof, ulong i, byte[] leaf, byte[] root)
    {
        if (i == 0)
        {
            return false;
        }
        return root.SequenceEqual(EvalLastInclusion(iProof, i, leaf));
    }

    private static byte[] EvalLastInclusion(byte[][] iProof, ulong i, byte[] leaf)
    {
        ulong i1 = i - 1;
        byte[] root = leaf;

        byte[] b = new byte[1 + SHA256.HashSizeInBytes * 2];
        b[0] = Consts.NODE_PREFIX;

        foreach (byte[] h in iProof)
        {
            Array.Copy(h, 0, b, 1, h.Length);
            Array.Copy(root, 0, b, SHA256.HashSizeInBytes + 1, root.Length);
            root = Sha256Sum(b);
            i1 >>= 1;
        }
        return root;
    }

    public static bool VerifyConsistency(byte[][] cProof, ulong i, ulong j, byte[] iRoot,
            byte[] jRoot)
    {
        if (i > j || i == 0 || (i < j && cProof.Length == 0))
        {
            return false;
        }

        if (i == j && cProof.Length == 0)
        {
            return iRoot.SequenceEqual(jRoot);
        }

        byte[][] result = EvalConsistency(cProof, i, j);
        byte[] ciRoot = result[0];
        byte[] cjRoot = result[1];

        return iRoot.SequenceEqual(ciRoot) && jRoot.SequenceEqual(cjRoot);
    }

    // Returns a "pair" (two) byte[] values (ciRoot, cjRoot), that's why
    // the returned data is byte[][] just to keep it simple.
    public static byte[][] EvalConsistency(byte[][] cProof, ulong i, ulong j)
    {

        ulong fn = i - 1;
        ulong sn = j - 1;

        while (fn % 2 == 1)
        {
            fn >>= 1;
            sn >>= 1;
        }

        byte[] ciRoot = cProof[0];
        byte[] cjRoot = cProof[0];

        byte[] b = new byte[1 + SHA256.HashSizeInBytes * 2];
        b[0] = Consts.NODE_PREFIX;

        for (int k = 1; k < cProof.Length; k++)
        {
            byte[] h = cProof[k];
            if (fn % 2 == 1 || fn == sn)
            {
                Array.Copy(h, 0, b, 1, h.Length);

                Array.Copy(ciRoot, 0, b, 1 + SHA256.HashSizeInBytes, ciRoot.Length);
                ciRoot = Sha256Sum(b);

                Array.Copy(cjRoot, 0, b, 1 + SHA256.HashSizeInBytes, cjRoot.Length);
                cjRoot = Sha256Sum(b);

                while (fn % 2 == 0 && fn != 0)
                {
                    fn >>= 1;
                    sn >>= 1;
                }
            }
            else
            {
                Array.Copy(cjRoot, 0, b, 1, cjRoot.Length);
                Array.Copy(h, 0, b, 1 + SHA256.HashSizeInBytes, h.Length);
                cjRoot = Sha256Sum(b);
            }
            fn >>= 1;
            sn >>= 1;
        }

        byte[][] result = new byte[2][];
        result[0] = ciRoot;
        result[1] = cjRoot;
        return result;
    }
}