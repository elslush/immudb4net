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

using System;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics.Arm;
using System.Security.Cryptography;
using System.Text;
using CommunityToolkit.HighPerformance;
using Google.Protobuf;
using Google.Protobuf.Collections;
using ImmuDB.Crypto;

namespace ImmuDB;

/// <summary>
/// Utils is a static class that provides helper functions, like converters
/// </summary>
public static class Utils
{
    /// <summary>
    /// Converts a string to gRPC byte string
    /// </summary>
    /// <param name="str">The source string</param>
    /// <returns>The gRPC ByteString </returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ByteString ToByteString(in string str)
    {
        if (str == null)
            return ByteString.Empty;

        return ByteString.CopyFromUtf8(str);
    }

    /// <summary>
    /// Converts a string to gRPC byte string
    /// </summary>
    /// <param name="b">The source byte array</param>
    /// <returns>The gRPC ByteString </returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ByteString ToByteString(in byte[] bytes)
    {
        if (bytes is null || bytes.Length < 1)
            return ByteString.Empty;

        return ByteString.CopyFrom(bytes);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ByteString ToByteString(ReadOnlyMemory<byte> bytes)
    {
        if (bytes.Length < 1)
        {
            return ByteString.Empty;
        }

        return UnsafeByteOperations.UnsafeWrap(bytes);
    }

    /// <summary>
    /// Converts a string to byte array. Checks for null argument.
    /// </summary>
    /// <param name="str">The source string</param>
    /// <returns>The converted byte array </returns>
    //public static byte[] ToByteArray(string str)
    //{
    //    if (str == null)
    //    {
    //        return Array.Empty<byte>()
    //    }

    //    return Encoding.UTF8.GetBytes(str);
    //}

    //public static int ToByteArray(ReadOnlySpan<char> str, Memory<byte> buffer, )
    //{

    //    var bufferSpan = buffer[..byteCount].Span;

    //    return Encoding.UTF8.GetBytes(str, bufferSpan);
    //}

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    internal static bool WrapWithPrefix(ReadOnlySpan<byte> key, Span<byte> result,  byte prefix)
    {
        if (key.Length < 1)
            return false;

        result[0] = prefix;

        return key.TryCopyTo(result.Slice(1, key.Length));
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    internal static bool WrapReferenceValueAt(ReadOnlySpan<byte> key, Span<byte> result, ulong atTx)
    {
        if (result.Length < 9)
            return false;

        result[0] = Consts.REFERENCE_VALUE_PREFIX;

        if (!PutUint64(atTx, result, 1))
            return false;

        return key.TryCopyTo(result.Slice(9, key.Length));
    }

    /// <summary>
    /// Convert the list of SHA256 (32-length) bytes to a primitive byte[][].
    /// </summary>
    /// <param name="data"></param>
    /// <returns></returns>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static bool ConvertSha256ListToBytesArray(in RepeatedField<ByteString> data, Span2D<byte> result)
    {
        if (data == null)
            return false;

        for (int i = 0; i < data.Count; i++)
            if (!data[i].Span[..32].TryCopyTo(result.GetRowSpan(i)))
                return false;

        return true;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static bool PutUint32(in int value, Span<byte> dest, in int destPos)
    {
        // Considering gRPC's generated code that maps Go's uint32 and int32 to C#'s int,
        // this is basically the version of this Go code:
        // binary.BigEndian.PutUint32(target[targetIdx:], value)
        Span<byte> destTemp = stackalloc byte[4];
        if (!BitConverter.TryWriteBytes(destTemp, value))
            return false;

        if (!BitConverter.IsLittleEndian)
            destTemp.Reverse();

        return destTemp.TryCopyTo(dest.Slice(destPos, destTemp.Length));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static bool PutUint64(in ulong value, Span<byte> dest)
    {
        return PutUint64(value, dest, 0);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static bool PutUint64(in ulong value, Span<byte> dest, in int destPos)
    {
        // Considering gRPC's generated code that maps Go's uint64 and int64 to Java's long,
        // this is basically the Java version of this Go code:
        // binary.BigEndian.PutUint64(target[targetIdx:], value)
        Span<byte> destTemp = stackalloc byte[8];
        if (!BitConverter.TryWriteBytes(destTemp, value))
            return false;

        if (!BitConverter.IsLittleEndian)
            destTemp.Reverse();

        return destTemp.TryCopyTo(dest.Slice(destPos, destTemp.Length));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void WriteLittleEndian(Span<byte> result, in ulong item)
    {
        if (BitConverter.TryWriteBytes(result, item) && !BitConverter.IsLittleEndian)
            result.Reverse();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void WriteWithBigEndian(Span<byte> result, in ulong item)
    {
        if (BitConverter.TryWriteBytes(result, item) && BitConverter.IsLittleEndian)
            result.Reverse();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void WriteWithBigEndian(Span<byte> result, in uint item)
    {
        if (BitConverter.TryWriteBytes(result, item) && BitConverter.IsLittleEndian)
            result.Reverse();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void WriteWithBigEndian(Span<byte> result, in ushort item)
    {
        if (BitConverter.TryWriteBytes(result, item) && BitConverter.IsLittleEndian)
            result.Reverse();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void WriteWithBigEndian(Span<byte> result, in long item)
    {
        if (BitConverter.TryWriteBytes(result, item) && BitConverter.IsLittleEndian)
            result.Reverse();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void WriteWithBigEndian(Span<byte> result, in double item)
    {
        if (BitConverter.TryWriteBytes(result, item) && BitConverter.IsLittleEndian)
            result.Reverse();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void WriteWithBigEndian(Span<byte> result, in short item)
    {
        if (BitConverter.TryWriteBytes(result, item) && BitConverter.IsLittleEndian)
            result.Reverse();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static bool WriteArray(Span<byte> result, ReadOnlySpan<byte> item)
    {
        if ((item == null) || (item.Length == 0))
        {
            return false;
        }
        return item.TryCopyTo(result);
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    internal static bool GenerateShortHash(ReadOnlySpan<char> source, Span<char> shortHash)
    {
        Span<char> shortHashTemp = stackalloc char[SHA256.HashSizeInBytes];
        if (!GetSHA256Upper(source, shortHashTemp))
            return false;

        var previousCopyIndex = 0;
        var previousShortHashIndex = 0;
        int i;
        int length;
        for (i = 0; i < shortHashTemp.Length; i++)
        {
            switch (shortHashTemp[i])
            {
                // Change temp character span in place
                case '+':
                    shortHashTemp[i] = '-';
                    break;
                // Change temp character span in place
                case '/':
                    shortHashTemp[i] = '_';
                    break;
                // Copy from temp to destination in bulk using
                // ReadOnlySpan<T>.CopyTo because Memmove is fastest.
                case '=':
                    // Length of short hash array until '=' is detected
                    // (because we want to discard '=')
                    length = i - 1 - previousCopyIndex;

                    // Make sure we don't go over 30 characters in shortHash
                    length += Math.Max(0, 30 - (previousShortHashIndex + length));

                    // Copy
                    shortHashTemp
                        .Slice(previousCopyIndex, length)
                        .CopyTo(shortHash.Slice(previousShortHashIndex, length));

                    previousCopyIndex += i - 1;
                    previousShortHashIndex += i - 1;

                    if (previousShortHashIndex >= 30)
                        return true;
                    break;
                default:
                    break;
            }
        }

        // Bulk Copy remaining
        length = i - previousCopyIndex;
        length += Math.Max(0, 30 - (previousShortHashIndex + length));
        if (length < 30)
        {
            shortHashTemp
                    .Slice(previousCopyIndex, length)
                    .CopyTo(shortHash.Slice(previousShortHashIndex, length));
        }

        return true;

        //using (SHA256 sha256 = SHA256.Create())
        //{
        //    source.r
        //    var result = sha256.ComputeHash(Encoding.UTF8.GetBytes(source));
        //    var hash = Convert.ToBase64String(result).ToUpper()
        //        .Replace("=", "")
        //        .Replace("+", "-")
        //        .Replace("/", "_")
        //        .Substring(0, 30);
        //    return hash;
        //}
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool GetSHA256Upper(ReadOnlySpan<char> source, Span<char> shortHash)
    {
        Span<char> shortHashTemp = stackalloc char[SHA256.HashSizeInBytes];
        if (!GetSHA256Chars(source, shortHashTemp))
            return false;

        ReadOnlySpan<char> shortHashTempReadonly = shortHashTemp;

        var shaCount = shortHashTempReadonly.ToUpper(shortHash, System.Globalization.CultureInfo.InvariantCulture);
        if (shaCount != SHA256.HashSizeInBytes)
            return false;

        return true;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool GetSHA256Chars(ReadOnlySpan<char> source, Span<char> shortHashTemp)
    {
        Span<byte> sha = stackalloc byte[SHA256.HashSizeInBytes];
        int shaCount = GetSHA256(source, sha);
        if (shaCount != SHA256.HashSizeInBytes)
            return false;

        if (!Convert.TryToBase64Chars(sha, shortHashTemp, out int charsWritten))
            return false;

        return true;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int GetSHA256(ReadOnlySpan<char> source, Span<byte> result)
    {
        var byteCount = Encoding.UTF8.GetByteCount(source);
        Span<byte> sourceBytes = stackalloc byte[byteCount];
        byteCount = Encoding.UTF8.GetBytes(source, sourceBytes);

        if (CryptoUtils.Sha256Sum(sourceBytes[..byteCount], result, out byteCount))
            return byteCount;

        return 0;
    }
}