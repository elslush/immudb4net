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

namespace ImmuDB;

/// <summary>
/// Represents the database key-value metadata
/// </summary>
public readonly struct KVMetadata
{

    internal const byte deletedAttrCode = 0;
    internal const byte expiresAtAttrCode = 1;
    internal const byte nonIndexableAttrCode = 2;

    private readonly bool hasDeleted, nonIndexable;
    private readonly long? expirationTime;
    private readonly int serializationLength;

    //private Dictionary<byte, MetadataAttribute> attributes;

    /// <summary>
    /// Creates an empty KV metadata
    /// </summary>
    public KVMetadata(bool hasDeleted, bool nonIndexable, long? expirationTime)
    {
        this.hasDeleted = hasDeleted;
        this.nonIndexable = nonIndexable;
        this.expirationTime = expirationTime;

        var serializationLength = 0;
        if (hasDeleted)
            serializationLength += 1;
        if (nonIndexable)
            serializationLength += 1;
        if (expirationTime is not null)
            serializationLength += 9;

        this.serializationLength = serializationLength;
    }

    /// <summary>
    /// Converts from a gRPC protobuf proxy KVMetadata object
    /// </summary>
    /// <param name="md"></param>
    /// <returns></returns>
    public static KVMetadata ValueOf(ImmudbProxy.KVMetadata md)
    {
        return new(md.Deleted, md.NonIndexable, md.Expiration?.ExpiresAt);
    }

    /// <summary>
    /// Is true if the deleted attribute is present
    /// </summary>
    public bool Deleted => hasDeleted;

    /// <summary>
    /// Is true if the non indexable attribute is set
    /// </summary>
    /// <value></value>
    public bool NonIndexable => nonIndexable;

    /// <summary>
    /// Is true if the attributes contain expiration
    /// </summary>
    /// <value></value>
    public bool HasExpirationTime => expirationTime is not null;

    /// <summary>
    /// Gets the expiration time
    /// </summary>
    /// <value></value>
    public DateTime ExpirationTime
    {
        get
        {
            if (expirationTime is null)
            {
                throw new InvalidOperationException("no expiration time set");
            }
            return DateTimeOffset.FromUnixTimeSeconds((long)expirationTime).DateTime;
        }
    }

    public int SerializedLength => serializationLength;


    /// <summary>
    /// Serializes to byte array
    /// </summary>
    /// <returns></returns>
    public bool Serialize(Span<byte> result)
    {
        if (result.Length != serializationLength)
            return false;

        var i = 0;
        if (hasDeleted)
            result[i++] = deletedAttrCode;

        if (nonIndexable)
            result[i++] = nonIndexableAttrCode;

        if (expirationTime is not null)
        {
            result[i++] = nonIndexableAttrCode;
            Utils.WriteWithBigEndian(result.Slice(i, 8), (long)expirationTime);
        }

        return true;
    }
}