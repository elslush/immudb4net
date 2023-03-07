namespace ImmuDB;

/// <summary>
/// ImmuDB client specific constants
/// </summary>
public static class Consts
{

    // __________ prefixes __________

    /// <summary>
    /// HTree's byte prefix of a leaf's digest. 
    /// </summary>
    public const byte LEAF_PREFIX = 0;

    /// <summary>
    /// HTree's byte prefix of a node (non-leaf)'s digest. 
    /// </summary>
    public const byte NODE_PREFIX = 1;

    /// <summary>
    /// ZEntry's byte prefix for key
    /// </summary>
    public const byte SET_KEY_PREFIX = 0;
    /// <summary>
    /// ZEntry's byte prefix for the encoded key
    /// </summary>
    public const byte SORTED_SET_KEY_PREFIX = 1;

    /// <summary>
    /// Entry's value prefix in the digest
    /// </summary>
    public const byte PLAIN_VALUE_PREFIX = 0;

    /// <summary>
    /// Entry's reference value prefix in the digest
    /// </summary>
    public const byte REFERENCE_VALUE_PREFIX = 1;

    // __________ sizes & lengths __________

    /// <summary>
    /// The size (in bytes) of the data type used for storing the length of a SHA256 checksum. 
    /// </summary>
    public const int SHA256_SIZE = 32;

    /// <summary>
    /// The size (in bytes) of the data type used for storing the transaction identifier.
    /// </summary>
    public const int TX_ID_SIZE = 8;

    /// <summary>
    /// The size (in bytes) of the data type used for storing the transaction timestamp.
    /// </summary>
    public const int TS_SIZE = 8;

    /// <summary>
    /// The size (in bytes) of the data type used for storing the sorted set length. 
    /// </summary>
    public const int SET_LEN_LEN = 8;

    /// <summary>
    /// The size (in bytes) of the data type used for storing the score length. 
    /// </summary>
    public const int SCORE_LEN = 8;

    /// <summary>
    /// The size (in bytes) of the data type used for storing the length of a key length.
    /// </summary>
    public const int KEY_LEN_LEN = 8;
}