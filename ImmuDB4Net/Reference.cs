namespace ImmuDB;

/// <summary>
/// Represents the reference data fields
/// </summary>
public readonly struct Reference
{
    /// <summary>
    /// The transaction ID
    /// </summary>
    /// <value></value>
    public ulong Tx { get; }
    /// <summary>
    /// The key entry in a database
    /// </summary>
    /// <value></value>
    public ReadOnlyMemory<byte> Key { get; }
    /// <summary>
    /// The transaction it refers to
    /// </summary>
    /// <value></value>
    public ulong AtTx { get; }
    /// <summary>
    /// Gets the associated metadata
    /// </summary>
    /// <value></value>
    public KVMetadata? Metadata { get; }

    private Reference(ulong tx, ReadOnlyMemory<byte> key, ulong atTx, KVMetadata? metadata)
    {
        Tx = tx;
        Key = key;
        AtTx = atTx;
        Metadata = metadata;
    }

    /// <summary>
    /// Converts from a gRPC protobuf Reference object
    /// </summary>
    /// <param name="proxyRef"></param>
    /// <returns></returns>
    public static Reference ValueOf(ImmudbProxy.Reference proxyRef)
    {
        Reference reference = new(
            proxyRef.Tx,
            proxyRef.Key.Memory,
            proxyRef.AtTx,
            proxyRef.Metadata is null ? null : KVMetadata.ValueOf(proxyRef.Metadata)
        ); ;

        return reference;
    }


    /// <summary>
    /// Gets the digest for a version
    /// </summary>
    /// <param name="version">The version</param>
    /// <returns></returns>
    public bool DigestFor(int version, Span<byte> result)
    {
        if (result.Length != Consts.SHA256_SIZE)
            throw new InvalidOperationException($"result must be of length: {Consts.SHA256_SIZE}, given: {result.Length}");

        // Encode key with prefix
        Span<byte> encodedKey = stackalloc byte[Key.Length + 1];
        Utils.WrapWithPrefix(Key.Span, encodedKey, Consts.SET_KEY_PREFIX);

        // Prepending 1 byte, Appending a ulong (8 bytes) with a prefix (1 byte) = 10 bytes
        Span<byte> encodedValue = stackalloc byte[Key.Length + 10];
        encodedValue[0] = Consts.SET_KEY_PREFIX;
        Utils.WrapReferenceValueAt(Key.Span, result.Slice(1), AtTx);

        var kv = new KV(encodedKey, Metadata, encodedValue);

        return kv.DigestFor(version, result);
    }
}