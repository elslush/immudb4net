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

using System.Buffers;
using System.Text;
using Google.Protobuf;
using Google.Protobuf.Collections;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using ImmuDB.Crypto;
using ImmuDB.Exceptions;
using ImmuDB.SQL;
using ImmudbProxy;
using Org.BouncyCastle.Crypto;
using System.IO.MemoryMappedFiles;
using System.Runtime.InteropServices;

/// <summary>
/// Class ImmuClient provides the awaitable API for accessing an ImmuDB server. If synchronous support is needed, use <see cref="ImmuClientSync" />
/// </summary>
public partial class ImmuClient
{
    internal const string AUTH_HEADER = "authorization";

    private readonly AsymmetricKeyParameter? serverSigningKey;
    private readonly IImmuStateHolder stateHolder;
    private IConnection connection;
    private string currentDb = "defaultdb";
    private static LibraryWideSettings globalSettings = new LibraryWideSettings();
    private Session? activeSession;
    private TimeSpan heartbeatInterval;
    private ManualResetEvent? heartbeatCloseRequested;
    private Task? heartbeatTask;
    private ReleasedConnection releasedConnection = new ReleasedConnection();

    internal ImmuService.ImmuServiceClient Service { get { return Connection.Service; } }
    internal object connectionSync = new Object();
    internal IConnection Connection
    {
        get
        {
            lock (connectionSync)
            {
                return connection;
            }
        }

        set
        {
            lock (connectionSync)
            {
                connection = value;
            }
        }
    }
    internal IConnectionPool ConnectionPool { get; set; }
    internal ISessionManager SessionManager { get; set; }
    internal object sessionSync = new();
    internal int sessionSetupInProgress = 0;

    internal ManualResetEvent? heartbeatCalled;

    internal Session? ActiveSession
    {
        get
        {
            lock (sessionSync)
            {
                return activeSession;
            }
        }
        set
        {
            lock (sessionSync)
            {
                activeSession = value;
            }
        }
    }
    internal IImmuStateHolder StateHolder => stateHolder;

    /// <summary>
    /// Gets or sets the DeploymentInfoCheck flag. If enabled then a check of server authenticity is perform while establishing a new link with the ImmuDB server.
    /// </summary>
    /// <value>Default: true</value>
    public bool DeploymentInfoCheck
    {
        get
        {
            return stateHolder.DeploymentInfoCheck;
        }

        set
        {

            stateHolder.DeploymentInfoCheck = value;
        }
    }
    /// <summary>
    /// Gets or sets the length of time the <see cref="ImmuClient.Close" /> function is allowed to block before it completes.
    /// </summary>
    /// <value>Default: 2 sec</value>
    public TimeSpan ConnectionShutdownTimeout { get; set; }

    /// <summary>
    /// Gets or sets the length of time interval between periodic checks of idle connections to be closed. 
    /// </summary>
    /// <value></value>
    public TimeSpan IdleConnectionCheckInterval { get; internal set; }

    /// <summary>
    /// Gets the grpc address of the ImmuDB server
    /// </summary>
    /// <value></value>
    /// <remarks>
    /// This value is computed from the server url and server port arguments that come either from 
    /// <see cref="ImmuClientBuilder.Build()" /> or <see cref="ImmuClient(string, int)" /> constructor
    /// </remarks>
    public string GrpcAddress { get; }

    /// <summary>
    /// Gets the <see cref="LibraryWideSettings" /> object with the process wide settings
    /// </summary>
    /// <value></value>
    public static LibraryWideSettings GlobalSettings
    {
        get
        {
            return globalSettings;
        }
    }

    /// <summary>
    /// Creates a new instance of <see cref="ImmuClientBuilder" /> factory object for <see cref="ImmuClient" /> instances
    /// </summary>
    /// <returns>A new instance of <see cref="ImmuClient" /></returns>
    public static ImmuClientBuilder NewBuilder()
    {
        return new ImmuClientBuilder();
    }

    /// <summary>
    /// Initializes a new instance of <see cref="ImmuClient" />. It uses the default value of 'localhost:3322' as ImmuDB server address and port and 'defaultdb' as database name.
    /// </summary>
    /// <returns></returns>
    public ImmuClient() : this(NewBuilder())
    {
        //long offset = 0x10000000; // 256 megabytes
        //long length = 0x20000000; // 512 megabytes
        //using (var mmf = MemoryMappedFile.CreateFromFile(@"c:\ExtremelyLargeImage.data", FileMode.Open, "ImgA"))
        //{
        //    // Create a random access view, from the 256th megabyte (the offset)
        //    // to the 768th megabyte (the offset plus length).
        //    using (var accessor = mmf.CreateViewAccessor(offset, length))
        //    {
        //        accessor.rea<byte>
        //        int colorSize = Marshal.SizeOf(typeof(MyColor));
        //        MyColor color;

        //        // Make changes to the view.
        //        for (long i = 0; i < length; i += colorSize)
        //        {
        //            accessor.Read(i, out color);
        //            color.Brighten(10);
        //            accessor.Write(i, ref color);
        //        }
        //    }
        //}
    }

    /// <summary>
    ///  Initializes a new instance of <see cref="ImmuClient" />. It uses the implicit 'defaultdb' value for the database name.
    /// </summary>
    /// <param name="serverUrl">The ImmuDB server address, e.g. localhost or http://localhost </param>
    /// <param name="serverPort">The port where the ImmuDB server listens</param>
    public ImmuClient(ReadOnlySpan<char> serverUrl, in int serverPort)
        : this(NewBuilder().WithServerUrl(serverUrl).WithServerPort(serverPort))
    {
    }

    internal ImmuClient(ImmuClientBuilder builder)
    {
        ConnectionPool = builder.ConnectionPool;
        GrpcAddress = builder.GetGrpcAddress();
        connection = releasedConnection;
        SessionManager = builder.SessionManager;
        serverSigningKey = builder.ServerSigningKey;
        stateHolder = builder.StateHolder;
        DeploymentInfoCheck = builder.DeploymentInfoCheck;
        heartbeatInterval = builder.HeartbeatInterval;
        ConnectionShutdownTimeout = builder.ConnectionShutdownTimeout;
        stateHolder.DeploymentInfoCheck = builder.DeploymentInfoCheck;
        stateHolder.DeploymentKey = Utils.GenerateShortHash(GrpcAddress);
        stateHolder.DeploymentLabel = GrpcAddress;
    }

    /// <summary>
    /// Releases the resources used by the SDK objects. (e.g. connection pool resources). As best practice, this method should be call just before the existing process ends.
    /// </summary>
    /// <returns></returns>
    public static async Task ReleaseSdkResources()
    {
        await RandomAssignConnectionPool.Instance.ShutdownAsync();
    }

    private void StartHeartbeat()
    {
        heartbeatTask = Task.Factory.StartNew(async () =>
        {
            while (true)
            {
                if (heartbeatCloseRequested!.WaitOne(heartbeatInterval)) return;
                try
                {
                    await Service.KeepAliveAsync(new Empty(), Service.GetHeaders(ActiveSession));
                    heartbeatCalled?.Set();
                }
                catch (RpcException) { }
            }
        });
    }

    private void StopHeartbeat()
    {
        if (heartbeatTask == null)
        {
            return;
        }
        heartbeatCloseRequested?.Set();
        heartbeatTask.Wait();
        heartbeatCloseRequested?.Close();
        heartbeatCalled?.Close();
        heartbeatTask = null;
    }

    private void ValidateLocalState()
    {
        lock (stateSync)
        {
            var localState = stateHolder.GetState(ActiveSession, currentDb);
            if (localState == null)
            {
                localState = ServerCurrentState;
                stateHolder.SetState(ActiveSession!, localState);
            }
            else
            {
                var serverState = Service.CurrentState(new Empty(), Service.GetHeaders(ActiveSession));
                try
                {
                    var verifiableTx = Service.VerifiableTxById(new VerifiableTxRequest
                    {
                        SinceTx = localState.TxId,
                        Tx = serverState.TxId,
                        EntriesSpec = new EntriesSpec
                        {
                            SqlEntriesSpec = new EntryTypeSpec
                            {
                                Action = EntryTypeAction.Exclude,
                            },
                            KvEntriesSpec = new EntryTypeSpec
                            {
                                Action = EntryTypeAction.Exclude,
                            },
                            ZEntriesSpec = new EntryTypeSpec
                            {
                                Action = EntryTypeAction.Exclude,
                            }
                        }
                    }, Service.GetHeaders(ActiveSession));
                    Crypto.DualProof dualProof = Crypto.DualProof.ValueOf(verifiableTx.DualProof);

                    ulong sourceId;
                    ulong targetId;
                    Span<byte> sourceAlh = stackalloc byte[32];
                    byte[] targetAlh;
                    if (localState.TxId <= serverState.TxId)
                    {
                        sourceId = localState.TxId;
                        sourceAlh = CryptoUtils.DigestFrom(localState.TxHash);
                        targetId = serverState.TxId;
                        targetAlh = dualProof.TargetTxHeader.Alh();
                    }
                    else
                    {
                        sourceId = serverState.TxId;
                        sourceAlh = dualProof.SourceTxHeader.Alh();
                        targetId = localState.TxId;
                        targetAlh = CryptoUtils.DigestFrom(localState.TxHash);
                    }
                    if (localState.TxId > 0)
                    {
                        if (!CryptoUtils.VerifyDualProof(
                                Crypto.DualProof.ValueOf(verifiableTx.DualProof),
                                sourceId,
                                targetId,
                                sourceAlh,
                                targetAlh
                        ))
                        {
                            throw new VerificationException("Data is corrupted (dual proof verification failed for the local state).");
                        }
                    }
                }
                catch (RpcException e)
                {
                    if (e.Message.Contains("tx not found"))
                    {
                        throw new VerificationException("The local state validation against the server state failed");
                    }
                }
            }
        }
    }

    /// <summary>
    /// Opens a connection to the ImmuDB server or reuses one from the connection pool. It also initiates a session with the forementioned server.
    /// </summary>
    /// <param name="username">The username</param>
    /// <param name="password">The username's password</param>
    /// <param name="databaseName">The database to use</param>
    /// <returns></returns>
    public async Task Open(string username, string password, string databaseName)
    {
        try
        {
            using (ManualResetEvent mre = new ManualResetEvent(false))
            {
                while (true)
                {
                    if (Interlocked.Exchange(ref sessionSetupInProgress, 1) == 0)
                    {
                        break;
                    }
                    mre.WaitOne(2);
                }
            }
            if (ActiveSession != null)
            {
                throw new InvalidOperationException("please close the existing session before opening a new one");
            }
            Connection = ConnectionPool.Acquire(new ConnectionParameters
            {
                Address = GrpcAddress,
                ShutdownTimeout = ConnectionShutdownTimeout
            });
            ActiveSession = await SessionManager.OpenSessionAsync(Connection, username, password, databaseName);
            currentDb = databaseName;
            ValidateLocalState();
            heartbeatCloseRequested = new ManualResetEvent(false);
            heartbeatCalled = new ManualResetEvent(false);
            StartHeartbeat();
        }
        finally
        {
            Interlocked.Exchange(ref sessionSetupInProgress, 0);
        }
    }

    /// <summary>
    /// Releases the established connection to the connection pool and acquires a new one.
    /// </summary>
    /// <remarks>The active session is not affected.</remarks>
    /// <returns></returns>
    public async Task Reconnect()
    {
        lock (connectionSync)
        {
            ConnectionPool.Release(connection);
            connection = ConnectionPool.Acquire(new ConnectionParameters
            {
                Address = GrpcAddress,
                ShutdownTimeout = ConnectionShutdownTimeout
            });
        }
        await Task.Yield();
    }

    /// <summary>
    /// Releases the established connection back to the connection pool and closes the active session
    /// </summary>
    /// <returns></returns>
    public async Task Close()
    {
        try
        {
            using (ManualResetEvent mre = new ManualResetEvent(false))
            {
                while (true)
                {
                    if (Interlocked.Exchange(ref sessionSetupInProgress, 1) == 0)
                    {
                        break;
                    }
                    mre.WaitOne(2);
                }
            }
            StopHeartbeat();
            await SessionManager.CloseSessionAsync(Connection, ActiveSession);
            ActiveSession = null;
            lock (connectionSync)
            {
                ConnectionPool.Release(connection);
                connection = releasedConnection;
            }
        }
        finally
        {
            Interlocked.Exchange(ref sessionSetupInProgress, 0);
        }
    }

    /// <summary>
    /// Gets the status of the connection
    /// </summary>
    /// <returns></returns>
    public bool IsClosed => Connection.Released;

    private void CheckSessionHasBeenOpened()
    {
        if (ActiveSession == null)
        {
            throw new ArgumentException("Session is null. Make sure you call Open beforehand.");
        }
    }

    private readonly object stateSync = new();

    /// <summary>
    /// Gets the database state data and if not present updates it from the server
    /// </summary>
    /// <returns></returns>
    public ImmuState State
    {
        get
        {
            lock (stateSync)
            {
                ImmuState? state = stateHolder.GetState(ActiveSession, currentDb);
                if (state == null)
                {
                    state = ServerCurrentState;
                    stateHolder.SetState(ActiveSession!, state);
                }
                else
                {
                    CheckSessionHasBeenOpened();
                }
                return state;
            }
        }
    }

    /// <summary>
    /// Get the current database state that exists on the server. It may throw a RuntimeException if server's state signature verification fails.
    /// </summary>
    /// <remarks>The reading of this property does not imply that it also validates against the local state. 
    /// If the server public key is provided then the a verification with this public key is performed</remarks>
    /// <returns>An <see cref="ImmuState" /> instance</returns>
    public ImmuState ServerCurrentState
    {
        get
        {
            CheckSessionHasBeenOpened();
            ImmutableState state = Service.CurrentState(new Empty(), Service.GetHeaders(ActiveSession));
            ImmuState immuState = ImmuState.ValueOf(state);
            if (!immuState.CheckSignature(serverSigningKey))
            {
                throw new VerificationException("State signature verification failed");
            }
            return immuState;
        }
    }

    //
    // ========== DATABASE ==========
    //

    /// <summary>
    /// Creates a new database
    /// </summary>
    /// <param name="database">The database name</param>
    /// <returns></returns>
    public Task CreateDatabase(in string database)
    {
        CheckSessionHasBeenOpened();
        CreateDatabaseRequest db = new()
        {
            Name = database
        };

        return Service.CreateDatabaseV2Async(db, Service.GetHeaders(ActiveSession))
            .ResponseAsync;
    }

    /// <summary>
    /// Changes the selected database
    /// </summary>
    /// <param name="database">The newly selected database. It must be an existing one.</param>
    /// <returns></returns>
    public Task UseDatabase(in string database, CancellationToken cancellationToken = default)
    {
        CheckSessionHasBeenOpened();
        Database db = new()
        {
            DatabaseName = database
        };
        currentDb = database;
        return Service.UseDatabaseAsync(db, Service.GetHeaders(ActiveSession), cancellationToken: cancellationToken)
            .ResponseAsync;
        
    }

    /// <summary>
    /// Gets the server's database list
    /// </summary>
    /// <remarks>The function returns only the name of the databases where the user has access to</remarks>
    /// <returns>The server's database list</returns>
    public async IAsyncEnumerable<string> Databases()
    {
        CheckSessionHasBeenOpened();
        DatabaseListRequestV2 req = new();
        DatabaseListResponseV2 res = await Service.DatabaseListV2Async(req, Service.GetHeaders(ActiveSession));

        foreach (ImmudbProxy.DatabaseWithSettings db in res.Databases)
            yield return db.Name;
    }

    //
    // ========== GET ==========
    //

    /// <summary>
    /// Retrieves the value for a specific key at a transaction ID.
    /// </summary>
    /// <param name="key">The lookup key</param>
    /// <param name="tx">The transaction id</param>
    /// <returns>An <see cref="Entry"/> object. Most often the Value field is used.</returns>
    public async Task<Entry> Get(ReadOnlyMemory<byte> key, ulong tx)
    {
        CheckSessionHasBeenOpened();
        KeyRequest req = new()
        {
            Key = Utils.ToByteString(key),
            AtTx = tx
        };
        try
        {
            var entry = await Service.GetAsync(req, Service.GetHeaders(ActiveSession));
            return Entry.ValueOf(entry);
        }
        catch (RpcException e)
        {
            if (e.Message.Contains("key not found"))
            {
                throw new KeyNotFoundException();
            }

            throw e;
        }
    }

    /// <summary>
    /// Retrieves the value for a specific key and at a transaction ID.
    /// </summary>
    /// <param name="key">The lookup key</param>
    /// <param name="tx">The transaction id</param>
    /// <returns>An <see cref="Entry"/> object. Most often the Value field is used.</returns>
    public Task<Entry> Get(ReadOnlySpan<char> key, in ulong tx)
    {
        var byteCount = Encoding.UTF8.GetByteCount(key);
        IMemoryOwner<byte> owner = MemoryPool<byte>.Shared.Rent(byteCount);
        var memory = owner.Memory;
        var bufferSpan = memory[..byteCount].Span;
        byteCount = Encoding.UTF8.GetBytes(key, bufferSpan);

        return Get(memory[..byteCount], tx);
    }

    /// <summary>
    /// Retrieves the value for a key
    /// </summary>
    /// <param name="key">The lookup key</param>
    /// <returns>An <see cref="Entry"/> object. Most often the Value field is used.</returns>
    public Task<Entry> Get(ReadOnlySpan<char> key)
    {
        return Get(key, 0);
    }

    /// <summary>
    /// Retrieves with authenticity check the value for a specific key
    /// </summary>
    /// <param name="keyReq">The lookup key, it is composed from the string key and the transaction id</param>
    /// <param name="state">The local state. One can get the local state by calling <see cref="State" /> property </param>
    /// <returns>An <see cref="Entry"/> object. Most often the Value field is used.</returns>
    public async Task<Entry> VerifiedGet(ImmudbProxy.KeyRequest keyReq, ImmuState state)
    {
        CheckSessionHasBeenOpened();
        ImmudbProxy.VerifiableGetRequest vGetReq = new ImmudbProxy.VerifiableGetRequest()
        {
            KeyRequest = keyReq,
            ProveSinceTx = state.TxId
        };

        ImmudbProxy.VerifiableEntry vEntry;

        try
        {
            vEntry = await Service.VerifiableGetAsync(vGetReq, Service.GetHeaders(ActiveSession));
        }
        catch (RpcException e)
        {
            if (e.Message.Contains("key not found"))
            {
                throw new KeyNotFoundException(string.Format("The key {0} was not found", keyReq.Key.ToStringUtf8()));
            }

            throw e;
        }

        ImmuDB.Crypto.InclusionProof inclusionProof = ImmuDB.Crypto.InclusionProof.ValueOf(vEntry.InclusionProof);
        ImmuDB.Crypto.DualProof dualProof = ImmuDB.Crypto.DualProof.ValueOf(vEntry.VerifiableTx.DualProof);

        byte[] eh;
        ulong sourceId, targetId;
        byte[] sourceAlh;
        byte[] targetAlh;

        Entry entry = Entry.ValueOf(vEntry.Entry);

        if (entry.ReferencedBy == null && !keyReq.Key.ToByteArray().SequenceEqual(entry.Key))
        {
            throw new VerificationException("Data is corrupted: entry does not belong to specified key");
        }

        if (entry.ReferencedBy != null && !keyReq.Key.ToByteArray().SequenceEqual(entry.ReferencedBy.Key))
        {
            throw new VerificationException("Data is corrupted: entry does not belong to specified key");
        }

        if (entry.Metadata != null && entry.Metadata.Deleted)
        {
            throw new VerificationException("Data is corrupted: entry is marked as deleted");
        }

        if (keyReq.AtTx != 0 && entry.Tx != keyReq.AtTx)
        {
            throw new VerificationException("Data is corrupted: entry does not belong to specified tx");
        }

        if (state.TxId <= entry.Tx)
        {
            byte[] digest = vEntry.VerifiableTx.DualProof.TargetTxHeader.EH.ToByteArray();
            eh = CryptoUtils.DigestFrom(digest);
            sourceId = state.TxId;
            sourceAlh = CryptoUtils.DigestFrom(state.TxHash);
            targetId = entry.Tx;
            targetAlh = dualProof.TargetTxHeader.Alh();
        }
        else
        {
            byte[] digest = vEntry.VerifiableTx.DualProof.SourceTxHeader.EH.ToByteArray();
            eh = CryptoUtils.DigestFrom(digest);
            sourceId = entry.Tx;
            sourceAlh = dualProof.SourceTxHeader.Alh();
            targetId = state.TxId;
            targetAlh = CryptoUtils.DigestFrom(state.TxHash);
        }

        byte[] kvDigest = entry.DigestFor(vEntry.VerifiableTx.Tx.Header.Version);
        if (!CryptoUtils.VerifyInclusion(inclusionProof, kvDigest, eh))
        {
            throw new VerificationException("Inclusion verification failed.");
        }

        if (state.TxId > 0)
        {
            if (!CryptoUtils.VerifyDualProof(
                    dualProof,
                    sourceId,
                    targetId,
                    sourceAlh,
                    targetAlh
            ))
            {
                throw new VerificationException("Dual proof verification failed.");
            }
        }

        ImmuState newState = new ImmuState(
                currentDb,
                targetId,
                targetAlh,
                (vEntry.VerifiableTx.Signature ?? ImmudbProxy.Signature.DefaultInstance).Signature_.ToByteArray());

        if (!newState.CheckSignature(serverSigningKey))
        {
            throw new VerificationException("State signature verification failed");
        }

        UpdateState(newState);
        return Entry.ValueOf(vEntry.Entry);
    }

    /// <summary>
    /// Retrieves with authenticity check the value for a specific key. The assumed default TxID is 0
    /// </summary>
    /// <param name="key">The lookup key</param>
    /// <returns>An <see cref="Entry"/> object. Most often the Value field is used.</returns>
    public Task<Entry> VerifiedGet(string key) => VerifiedGetAtTx(key, 0);

    /// <summary>
    /// Retrieves with authenticity check the value for a specific key. The assumed default TxID is 0
    /// </summary>
    /// <param name="key">The lookup key</param>
    /// <returns>An <see cref="Entry"/> object. Most often the Value field is used.</returns>
    public Task<Entry> VerifiedGet(ReadOnlyMemory<byte> key) => VerifiedGetAtTx(key, 0);

    /// <summary>
    /// Retrieves with authenticity check the value for a specific key
    /// </summary>
    /// <param name="key">The lookup key</param>
    /// <param name="tx">The transaction ID</param>
    /// <returns>An <see cref="Entry"/> object. Most often the Value field is used.</returns>
    public Task<Entry> VerifiedGetAtTx(ReadOnlySpan<char> key, in ulong tx)
    {
        var byteCount = Encoding.UTF8.GetByteCount(key);
        using IMemoryOwner<byte> owner = MemoryPool<byte>.Shared.Rent(byteCount);

        var memory = owner.Memory;

        byteCount = Encoding.UTF8.GetBytes(key, memory.Span);

        return VerifiedGetAtTx(memory[..byteCount], tx);
    }

    /// <summary>
    /// Retrieves with authenticity check the value for a specific key at a transaction ID
    /// </summary>
    /// <param name="key">The lookup key</param>
    /// <param name="tx">The transaction ID</param>
    /// <returns>An <see cref="Entry"/> object. Most often the Value field is used.</returns>
    public Task<Entry> VerifiedGetAtTx(ReadOnlyMemory<byte> key, in ulong tx)
    {
        ImmudbProxy.KeyRequest keyReq = new ImmudbProxy.KeyRequest()
        {
            Key = Utils.ToByteString(key),
            AtTx = tx
        };

        return VerifiedGet(keyReq, State);
    }

    /// <summary>
    /// Retrieves with authenticity check the value for a specific key. Changes to the key after given transaction can be ignored.
    /// </summary>
    /// <param name="key">The lookup key</param>
    /// <param name="tx">The transaction ID after which the server is allowed to ignore changes to the key</param>
    /// <returns>An <see cref="Entry"/> object. Most often the Value field is used.</returns>
    public Task<Entry> VerifiedGetSinceTx(ReadOnlySpan<char> key, in ulong tx)
    {
        var byteCount = Encoding.UTF8.GetByteCount(key);
        using IMemoryOwner<byte> owner = MemoryPool<byte>.Shared.Rent(byteCount);
        var memory = owner.Memory;

        byteCount = Encoding.UTF8.GetBytes(key, memory.Span);
        return VerifiedGetSinceTx(memory[..byteCount], tx);
    }

    /// <summary>
    /// Retrieves with authenticity check the value for a specific key. Changes to the key after given transaction can be ignored.
    /// </summary>
    /// <param name="key">The lookup key</param>
    /// <param name="tx">The transaction ID</param>
    /// <returns>An <see cref="Entry"/> object. Most often the Value field is used.</returns>
    public Task<Entry> VerifiedGetSinceTx(ReadOnlyMemory<byte> key, in ulong tx)
    {
        ImmudbProxy.KeyRequest keyReq = new ImmudbProxy.KeyRequest()
        {
            Key = Utils.ToByteString(key),
            SinceTx = tx
        };

        return VerifiedGet(keyReq, State);
    }

    /// <summary>
    /// Retrieves with authenticity check the value for a specific key at a specific revision
    /// </summary>
    /// <param name="key">The lookup key</param>
    /// <param name="rev">The revision number</param>
    /// <returns>An <see cref="Entry"/> object. Most often the Value field is used.</returns>
    public Task<Entry> VerifiedGetAtRevision(ReadOnlySpan<char> key, in long rev)
    {
        var byteCount = Encoding.UTF8.GetByteCount(key);
        using IMemoryOwner<byte> owner = MemoryPool<byte>.Shared.Rent(byteCount);
        var memory = owner.Memory;

        byteCount = Encoding.UTF8.GetBytes(key, memory.Span);
        return VerifiedGetAtRevision(memory[..byteCount], rev);
    }

    /// <summary>
    /// Retrieves with authenticity check the value for a specific key at a specific revision
    /// </summary>
    /// <param name="key">The lookup key</param>
    /// <param name="rev">The transaction ID</param>
    /// <returns>An <see cref="Entry"/> object. Most often the Value field is used.</returns>
    public Task<Entry> VerifiedGetAtRevision(ReadOnlyMemory<byte> key, in long rev)
    {
        ImmudbProxy.KeyRequest keyReq = new ImmudbProxy.KeyRequest()
        {
            Key = Utils.ToByteString(key),
            AtRevision = rev
        };

        return VerifiedGet(keyReq, State);
    }

    /// <summary>
    /// Retrieves the value for a specific key since a transaction ID
    /// </summary>
    /// <param name="key">The lookup key</param>
    /// <param name="tx">The transaction ID</param>
    /// <returns>An <see cref="Entry"/> object. Most often the Value field is used.</returns>
    public Task<Entry> GetSinceTx(ReadOnlySpan<char> key, in ulong tx)
    {
        var byteCount = Encoding.UTF8.GetByteCount(key);
        using IMemoryOwner<byte> owner = MemoryPool<byte>.Shared.Rent(byteCount);
        var memory = owner.Memory;

        byteCount = Encoding.UTF8.GetBytes(key, memory.Span);
        return GetSinceTx(memory[..byteCount], tx);
    }

    /// <summary>
    /// Retrieves the value for a specific key since a transaction ID
    /// </summary>
    /// <param name="key">The lookup key</param>
    /// <param name="tx">The transaction ID</param>
    /// <returns>An <see cref="Entry"/> object. Most often the Value field is used.</returns>
    public async Task<Entry> GetSinceTx(ReadOnlyMemory<byte> key, ulong tx)
    {
        CheckSessionHasBeenOpened();
        ImmudbProxy.KeyRequest req = new ImmudbProxy.KeyRequest()
        {
            Key = Utils.ToByteString(key),
            SinceTx = tx
        };

        try
        {
            return Entry.ValueOf(await Service.GetAsync(req, Service.GetHeaders(ActiveSession)));
        }
        catch (RpcException e)
        {
            if (e.Message.Contains("key not found"))
            {
                throw new KeyNotFoundException();
            }

            throw e;
        }
    }



    /// <summary>
    /// Retrieves the value for a specific key at a revision number
    /// </summary>
    /// <param name="key">The lookup key</param>
    /// <param name="rev">The revision number</param>
    /// <returns>An <see cref="Entry"/> object. Most often the Value field is used.</returns>
    public async Task<Entry> GetAtRevision(string key, long rev)
    {
        return await GetAtRevision(Utils.ToByteArray(key), rev);
    }

    /// <summary>
    /// Retrieves the value for a specific key at a revision number
    /// </summary>
    /// <param name="key">The lookup key</param>
    /// <param name="rev">The revision number</param>
    /// <returns>An <see cref="Entry"/> object. Most often the Value field is used.</returns>
    public async Task<Entry> GetAtRevision(byte[] key, long rev)
    {
        CheckSessionHasBeenOpened();
        ImmudbProxy.KeyRequest req = new ImmudbProxy.KeyRequest()
        {
            Key = Utils.ToByteString(key),
            AtRevision = rev
        };

        try
        {
            return Entry.ValueOf(await Service.GetAsync(req, Service.GetHeaders(ActiveSession)));
        }
        catch (RpcException e)
        {
            if (e.Message.Contains("key not found"))
            {
                throw new KeyNotFoundException();
            }

            throw e;
        }
    }

    /// <summary>
    /// Retrieves the values for the specified keys. This is a batch equivalent of Get.
    /// </summary>
    /// <param name="keys">The list of lookup keys</param>
    /// <returns>A list of <see cref="Entry"/> objects.</returns>
    public async Task<List<Entry>> GetAll(IEnumerable<string> keys)
    {
        CheckSessionHasBeenOpened();
        KeyListRequest req = new();

        foreach (string key in keys)
        {
            req.Keys.Add(Utils.ToByteString(key));
        }

        
        

        ImmudbProxy.Entries entries = await Service.GetAllAsync(req, Service.GetHeaders(ActiveSession));
        List<Entry> result = new List<Entry>(entries.Entries_.Count);

        foreach (ImmudbProxy.Entry entry in entries.Entries_)
        {
            result.Add(Entry.ValueOf(entry));
        }

        return result;
    }

    //
    // ========== SCAN ==========
    //

    /// <summary>
    /// Iterates over the key/values in the selected database and retrieves the values for the matching criteria
    /// </summary>
    /// <param name="prefix">Prefix for the keys</param>
    /// <param name="seekKey">Optional, initial key for the first entry in the iteration</param>
    /// <param name="endKey">Optional, end key for the scanning range</param>
    /// <param name="inclusiveSeek">Optional, default is false, specifies if initial key's value is included</param>
    /// <param name="inclusiveEnd">Optional, default is false, specifies if end key's value is included</param>
    /// <param name="limit">Optional, maximum number of of returned items. If the value is 0 then the default server-side limit, typically 1000 is used.
    /// If the returned result is larger than the maximum server-side limit then an error is returned</param>
    /// <param name="desc">Optional, specifies the sorting order, defaults to ascending sort</param>
    /// <returns>A list of <see cref="Entry"/> objects.</returns>
    public async Task<List<Entry>> Scan(byte[] prefix, byte[] seekKey, byte[] endKey, bool inclusiveSeek, bool inclusiveEnd,
                            ulong limit, bool desc)
    {
        CheckSessionHasBeenOpened();
        ImmudbProxy.ScanRequest req = new ImmudbProxy.ScanRequest()
        {
            Prefix = Utils.ToByteString(prefix),
            SeekKey = Utils.ToByteString(seekKey),
            EndKey = Utils.ToByteString(endKey),
            InclusiveSeek = inclusiveSeek,
            InclusiveEnd = inclusiveEnd,
            Limit = limit,
            Desc = desc
        };

        ImmudbProxy.Entries entries = await Service.ScanAsync(req, Service.GetHeaders(ActiveSession));
        return BuildList(entries);
    }

    /// <summary>
    /// Iterates over the key/values in the selected database and retrieves the values for the matching criteria
    /// </summary>
    /// <param name="prefix">Prefix of the keys</param>
    /// <returns>A list of <see cref="Entry"/> objects.</returns>
    public async Task<List<Entry>> Scan(string prefix)
    {
        return await Scan(Utils.ToByteArray(prefix));
    }

    /// <summary>
    /// Iterates over the key/values in the selected database and retrieves the values for the matching criteria
    /// </summary>
    /// <param name="prefix">Prefix of the keys</param>
    /// <returns>A list of <see cref="Entry"/> objects.</returns>
    public async Task<List<Entry>> Scan(byte[] prefix)
    {
        return await Scan(prefix, 0, false);
    }

    /// <summary>
    /// Iterates over the key/values in the selected database and retrieves the values for the matching criteria
    /// </summary>
    /// <param name="prefix">Prefix of the keys</param>
    /// <param name="limit">Maximum number of of returned items</param>
    /// <param name="desc">Specifies the sorting order</param>
    /// <returns>A list of <see cref="Entry"/> objects.</returns>
    public async Task<List<Entry>> Scan(string prefix, ulong limit, bool desc)
    {
        return await Scan(Utils.ToByteArray(prefix), limit, desc);
    }

    /// <summary>
    /// Iterates over the key/values in the selected database and retrieves the values for the matching criteria
    /// </summary>
    /// <param name="prefix">Prefix of the keys</param>
    /// <param name="limit">Maximum number of of returned items</param>
    /// <param name="desc">Specifies the sorting order</param>
    /// <returns>A list of <see cref="Entry"/> objects.</returns>
    public async Task<List<Entry>> Scan(byte[] prefix, ulong limit, bool desc)
    {
        return await Scan(prefix, new byte[0], limit, desc);
    }

    /// <summary>
    /// Iterates over the key/values in the selected database and retrieves the values for the matching criteria
    /// </summary>
    /// <param name="prefix">Prefix of the keys</param>
    /// <param name="seekKey">Initial key for the first entry in the iteration</param>
    /// <param name="limit">Maximum number of of returned items</param>
    /// <param name="desc">Specifies if the sorting order is of descending</param>
    /// <returns>A list of <see cref="Entry"/> objects.</returns>
    public async Task<List<Entry>> Scan(string prefix, string seekKey, ulong limit, bool desc)
    {
        return await Scan(Utils.ToByteArray(prefix), Utils.ToByteArray(seekKey), limit, desc);
    }

    /// <summary>
    /// Iterates over the key/values in the selected database and retrieves the values for the matching criteria
    /// </summary>
    /// <param name="prefix">Prefix of the keys</param>
    /// <param name="seekKey">Initial key for the first entry in the iteration</param>
    /// <param name="endKey">End key for the scanning range</param>
    /// <param name="limit">Maximum number of of returned items</param>
    /// <param name="desc">Specifies if the sorting order is of descending</param>
    /// <returns>A list of <see cref="Entry"/> objects.</returns>
    public async Task<List<Entry>> Scan(string prefix, string seekKey, string endKey, ulong limit, bool desc)
    {
        return await Scan(Utils.ToByteArray(prefix), Utils.ToByteArray(seekKey), Utils.ToByteArray(endKey), limit, desc);
    }

    /// <summary>
    /// Iterates over the key/values in the selected database and retrieves the values for the matching criteria
    /// </summary>
    /// <param name="prefix">Prefix of the keys</param>
    /// <param name="seekKey">Initial key for the first entry in the iteration</param>
    /// <param name="limit">Maximum number of of returned items</param>
    /// <param name="desc">Specifies if the sorting order is of descending</param>
    /// <returns>A list of <see cref="Entry"/> objects.</returns>
    public async Task<List<Entry>> Scan(byte[] prefix, byte[] seekKey, ulong limit, bool desc)
    {
        return await Scan(prefix, seekKey, new byte[0], limit, desc);
    }

    /// <summary>
    /// Iterates over the key/values in the selected database and retrieves the values for the matching criteria
    /// </summary>
    /// <param name="prefix">Prefix of the keys</param>
    /// <param name="seekKey">Initial key for the first entry in the iteration</param>
    /// <param name="endKey">End key for the scanning range</param>
    /// <param name="limit">Maximum number of of returned items</param>
    /// <param name="desc">Specifies if the sorting order is of descending</param>
    /// <returns>A list of <see cref="Entry"/> objects.</returns>
    public async Task<List<Entry>> Scan(byte[] prefix, byte[] seekKey, byte[] endKey, ulong limit, bool desc)
    {
        return await Scan(prefix, seekKey, endKey, false, false, limit, desc);
    }

    //
    // ========== SET ==========
    //

    /// <summary>
    /// Adds a key/value pair.
    /// </summary>
    /// <param name="key">The key to be added</param>
    /// <param name="value">The value to be added</param>
    /// <returns>The transaction information</returns>
    public async Task<TxHeader> Set(byte[] key, byte[] value)
    {
        CheckSessionHasBeenOpened();
        ImmudbProxy.KeyValue kv = new ImmudbProxy.KeyValue()
        {
            Key = Utils.ToByteString(key),
            Value = Utils.ToByteString(value)
        };

        ImmudbProxy.SetRequest req = new ImmudbProxy.SetRequest();
        req.KVs.Add(kv);

        ImmudbProxy.TxHeader txHdr = await Service.SetAsync(req, Service.GetHeaders(ActiveSession));

        if (txHdr.Nentries != 1)
        {
            throw new CorruptedDataException();
        }

        return TxHeader.ValueOf(txHdr);
    }

    /// <summary>
    /// Adds a key/value pair.
    /// </summary>
    /// <param name="key">The key to be added</param>
    /// <param name="value">The value to be added</param>
    /// <returns>The transaction information</returns>
    public async Task<TxHeader> Set(string key, byte[] value)
    {
        return await Set(Utils.ToByteArray(key), value);
    }

    /// <summary>
    /// Adds a key/value pair.
    /// </summary>
    /// <param name="key">The key to be added</param>
    /// <param name="value">The value to be added</param>
    /// <returns>The transaction information</returns>
    public async Task<TxHeader> Set(string key, string value)
    {
        return await Set(Utils.ToByteArray(key), Utils.ToByteArray(value));
    }

    /// <summary>
    /// Adds a key/value pair that expires at a specific moment in time.
    /// </summary>
    /// <param name="key">The key to be added</param>
    /// <param name="value">The value to be added</param>
    /// <param name="expiresAt">The datetime when the key expires</param>
    /// <returns>The transaction information</returns>
    public async Task<TxHeader> ExpirableSet(byte[] key, byte[] value, DateTime expiresAt)
    {
        CheckSessionHasBeenOpened();
        ImmudbProxy.KeyValue kv = new ImmudbProxy.KeyValue()
        {
            Key = Utils.ToByteString(key),
            Value = Utils.ToByteString(value),
            Metadata = new ImmudbProxy.KVMetadata
            {
                Expiration = new Expiration
                {
                    ExpiresAt = new DateTimeOffset(expiresAt).ToUnixTimeSeconds()
                }
            }
        };

        ImmudbProxy.SetRequest req = new ImmudbProxy.SetRequest();
        req.KVs.Add(kv);

        ImmudbProxy.TxHeader txHdr = await Service.SetAsync(req, Service.GetHeaders(ActiveSession));

        if (txHdr.Nentries != 1)
        {
            throw new CorruptedDataException();
        }

        return TxHeader.ValueOf(txHdr);
    }

    /// <summary>
    /// Adds a key/value pair that expires at a specific moment in time.
    /// </summary>
    /// <param name="key">The key to be added</param>
    /// <param name="value">The value to be added</param>
    /// <param name="expiresAt">The datetime when the key expires</param>
    /// <returns>The transaction information</returns>
    public async Task<TxHeader> ExpirableSet(string key, byte[] value, DateTime expiresAt)
    {
        return await ExpirableSet(Utils.ToByteArray(key), value, expiresAt);
    }

    /// <summary>
    /// Adds a key/value pair that expires at a specific moment in time.
    /// </summary>
    /// <param name="key">The key to be added</param>
    /// <param name="value">The value to be added</param>
    /// <param name="expiresAt">The datetime when the key expires</param>
    /// <returns>The transaction information</returns>
    public async Task<TxHeader> ExpirableSet(string key, string value, DateTime expiresAt)
    {
        return await ExpirableSet(Utils.ToByteArray(key), Utils.ToByteArray(value), expiresAt);
    }


    /// <summary>
    /// Adds a list of key/value pairs
    /// </summary>
    /// <param name="kvList">The list of pairs to be added</param>
    /// <returns>The transaction information</returns>
    public async Task<TxHeader> SetAll(List<KVPair> kvList)
    {
        CheckSessionHasBeenOpened();
        ImmudbProxy.SetRequest request = new ImmudbProxy.SetRequest();
        foreach (KVPair kv in kvList)
        {
            ImmudbProxy.KeyValue kvProxy = new ImmudbProxy.KeyValue();
            kvProxy.Key = Utils.ToByteString(kv.Key);
            kvProxy.Value = Utils.ToByteString(kv.Value);
            request.KVs.Add(kvProxy);
        }

        ImmudbProxy.TxHeader txHdr = await Service.SetAsync(request, Service.GetHeaders(ActiveSession));

        if (txHdr.Nentries != kvList.Count)
        {
            throw new CorruptedDataException();
        }

        return TxHeader.ValueOf(txHdr);
    }

    /// <summary>
    /// Adds a tag (reference) to a specific key/value element in the selected database
    /// </summary>
    /// <param name="reference">The reference</param>
    /// <param name="referencedKey">The lookup key</param>
    /// <param name="atTx">Transaction ID at which the referenced key will be bound at</param>
    /// <returns>The transaction information</returns>
    public async Task<TxHeader> SetReference(byte[] reference, byte[] referencedKey, ulong atTx)
    {
        CheckSessionHasBeenOpened();
        ImmudbProxy.ReferenceRequest req = new ImmudbProxy.ReferenceRequest()
        {
            Key = Utils.ToByteString(reference),
            ReferencedKey = Utils.ToByteString(referencedKey),
            AtTx = atTx,
            BoundRef = atTx > 0
        };

        ImmudbProxy.TxHeader txHdr = await Service.SetReferenceAsync(req, Service.GetHeaders(ActiveSession));

        if (txHdr.Nentries != 1)
        {
            throw new CorruptedDataException();
        }

        return TxHeader.ValueOf(txHdr);
    }

    /// <summary>
    /// Adds a tag (reference) to a specific key/value element in the selected database
    /// </summary>
    /// <param name="reference">The reference</param>
    /// <param name="referencedKey">The lookup key</param>
    /// <param name="atTx">Transaction ID</param>
    /// <returns>The transaction information</returns>
    public async Task<TxHeader> SetReference(string reference, string referencedKey, ulong atTx)
    {
        return await SetReference(
            Utils.ToByteArray(reference),
            Utils.ToByteArray(referencedKey),
            atTx);
    }

    /// <summary>
    /// Adds a tag (reference) to a specific key/value element in the selected database
    /// </summary>
    /// <param name="reference">The reference</param>
    /// <param name="referencedKey">The lookup key</param>
    /// <returns>The transaction information</returns>
    public async Task<TxHeader> SetReference(string reference, string referencedKey)
    {
        return await SetReference(
            Utils.ToByteArray(reference),
            Utils.ToByteArray(referencedKey),
            0);
    }

    /// <summary>
    /// Adds a tag (reference) to a specific key/value element in the selected database
    /// </summary>
    /// <param name="reference">The reference</param>
    /// <param name="referencedKey">The lookup key</param>
    /// <returns>The transaction information</returns>
    public async Task<TxHeader> SetReference(byte[] reference, byte[] referencedKey)
    {
        return await SetReference(reference, referencedKey, 0);
    }

    /// <summary>
    /// Adds with authenticity check a tag (reference) to a specific key/value element in the selected database
    /// </summary>
    /// <param name="reference">The reference</param>
    /// <param name="referencedKey">The lookup key</param>
    /// <returns>The transaction information</returns>
    public async Task<TxHeader> VerifiedSetReference(string reference, string referencedKey) {
        return await VerifiedSetReference(
            Utils.ToByteArray(reference), 
            Utils.ToByteArray(referencedKey), 
            0);
    }
    
    /// <summary>
    /// Adds with authenticity check a tag (reference) to a specific key/value element in the selected database
    /// </summary>
    /// <param name="reference">The reference</param>
    /// <param name="referencedKey">The lookup key</param>
    /// <returns>The transaction information</returns>
    public async Task<TxHeader> VerifiedSetReference(byte[] reference, byte[] referencedKey) {
        return await VerifiedSetReference(reference, referencedKey, 0);
    }

    /// <summary>
    /// Adds with authenticity check a tag (reference) to a specific key/value element in the selected database
    /// </summary>
    /// <param name="reference">The reference</param>
    /// <param name="referencedKey">The lookup key</param>
    /// <param name="atTx">Transaction ID at which the referenced key will be bound at</param>
    /// <returns>The transaction information</returns>
    public async Task<TxHeader> VerifiedSetReference(byte[] reference, byte[] referencedKey, ulong atTx)
    {
        CheckSessionHasBeenOpened();

        ImmuState state = State;
       
        ImmudbProxy.VerifiableReferenceRequest req = new ImmudbProxy.VerifiableReferenceRequest()
        {
            ReferenceRequest = new ReferenceRequest
            {
                Key = Utils.ToByteString(reference),
                ReferencedKey = Utils.ToByteString(referencedKey),
                AtTx = atTx,
                BoundRef = atTx > 0
            },
            ProveSinceTx = state.TxId
        };

        ImmudbProxy.VerifiableTx verifiableTx = await Service.VerifiableSetReferenceAsync(req, Service.GetHeaders(ActiveSession));

        if (verifiableTx.Tx.Header.Nentries != 1)
        {
            throw new CorruptedDataException();
        }

        return ProcessVerifiedSetReferenceResult(reference, referencedKey, atTx, state, verifiableTx);
    }

    private TxHeader ProcessVerifiedSetReferenceResult(ReadOnlySpan<byte> reference, ReadOnlySpan<byte> referencedKey, in ulong atTx, in ImmuState state, in VerifiableTx verifiableTx)
    {
        Tx tx;
        try
        {
            tx = Tx.ValueOf(verifiableTx.Tx);
        }
        catch (Exception e)
        {
            throw new VerificationException("Failed to extract the transaction.", e);
        }

        TxHeader txHeader = tx.Header;

        Span<byte> wrappedReference = stackalloc byte[referencedKey.Length + 10];
        EntrySpec.WrapReference(referencedKey, atTx, wrappedReference);
        EntrySpec spec = new EntrySpec(reference, null, wrappedReference);
        ImmuDB.Crypto.InclusionProof inclusionProof = tx.Proof(spec.GetEncodedKey());

        if (!CryptoUtils.VerifyInclusion(inclusionProof, spec.DigestFor(txHeader.Version), txHeader.Eh))
        {
            throw new VerificationException("Data is corrupted (verify inclusion failed)");
        }

        ImmuState newState = VerifyDualProof(verifiableTx, tx, state);

        if (!newState.CheckSignature(serverSigningKey))
        {
            throw new VerificationException("State signature verification failed");
        }

        UpdateState(newState);
        return TxHeader.ValueOf(verifiableTx.Tx.Header);
    }

    /// <summary>
    /// Adds a key/value pair.
    /// </summary>
    /// <param name="key">The key to be added</param>
    /// <param name="value">The value to be added</param>
    /// <returns>The transaction information</returns>
    public async Task<TxHeader> VerifiedSet(string key, byte[] value)
    {
        return await VerifiedSet(Utils.ToByteArray(key), value);
    }

    /// <summary>
    /// Adds a key/value pair.
    /// </summary>
    /// <param name="key">The key to be added</param>
    /// <param name="value">The value to be added</param>
    /// <returns>The transaction information</returns>
    public async Task<TxHeader> VerifiedSet(string key, string value)
    {
        return await VerifiedSet(Utils.ToByteArray(key), Utils.ToByteArray(value));
    }

    /// <summary>
    /// Adds a key/value pair.
    /// </summary>
    /// <param name="key">The key to be added</param>
    /// <param name="value">The value to be added</param>
    /// <returns>The transaction information</returns>
    public async Task<TxHeader> VerifiedSet(byte[] key, byte[] value)
    {
        CheckSessionHasBeenOpened();

        ImmuState state = State;
        ImmudbProxy.KeyValue kv = new ImmudbProxy.KeyValue()
        {
            Key = Utils.ToByteString(key),
            Value = Utils.ToByteString(value),
        };

        var setRequest = new ImmudbProxy.SetRequest();
        setRequest.KVs.Add(kv);
        ImmudbProxy.VerifiableSetRequest vSetReq = new ImmudbProxy.VerifiableSetRequest()
        {
            SetRequest = setRequest,
            ProveSinceTx = state.TxId
        };

        // using the awaitable VerifiableSetAsync is not ok here, because in the multithreading case it fails. Switched back to synchronous call in this case.

        var vtx = await Service.VerifiableSetAsync(vSetReq, Service.GetHeaders(ActiveSession));

        int ne = vtx.Tx.Header.Nentries;

        if (ne != 1 || vtx.Tx.Entries.Count != 1)
        {
            throw new VerificationException($"Got back {ne} entries (in tx metadata) instead of 1.");
        }

        Tx tx;
        try
        {
            tx = Tx.ValueOf(vtx.Tx);
        }
        catch (Exception e)
        {
            throw new VerificationException("Failed to extract the transaction.", e);
        }

        TxHeader txHeader = tx.Header;

        Entry entry = Entry.ValueOf(new ImmudbProxy.Entry()
        {
            Key = Utils.ToByteString(key),
            Value = Utils.ToByteString(value),
        });

        ImmuDB.Crypto.InclusionProof inclusionProof = tx.Proof(entry.GetEncodedKey());

        if (!CryptoUtils.VerifyInclusion(inclusionProof, entry.DigestFor(txHeader.Version), txHeader.Eh))
        {
            throw new VerificationException("Data is corrupted (verify inclusion failed)");
        }

        ImmuState newState = VerifyDualProof(vtx, tx, state);

        if (!newState.CheckSignature(serverSigningKey))
        {
            throw new VerificationException("State signature verification failed");
        }

        UpdateState(newState);
        return TxHeader.ValueOf(vtx.Tx.Header);
    }

    private ImmuState VerifyDualProof(ImmudbProxy.VerifiableTx vtx, Tx tx, ImmuState state)
    {
        ulong sourceId = state.TxId;
        ulong targetId = tx.Header.Id;
        byte[] sourceAlh = CryptoUtils.DigestFrom(state.TxHash);
        byte[] targetAlh = tx.Header.Alh();

        if (state.TxId > 0)
        {
            if (!CryptoUtils.VerifyDualProof(
                    Crypto.DualProof.ValueOf(vtx.DualProof),
                    sourceId,
                    targetId,
                    sourceAlh,
                    targetAlh
            ))
            {
                throw new VerificationException("Data is corrupted (dual proof verification failed).");
            }
        }

        return new ImmuState(currentDb, targetId, targetAlh, (vtx.Signature ?? ImmudbProxy.Signature.DefaultInstance).Signature_.ToByteArray());
    }

    //
    // ========== Z ==========
    //

    /// <summary>
    /// Adds a scored key reference to a sorted set
    /// </summary>
    /// <param name="set">The set identifier</param>
    /// <param name="key">The referenced key</param>
    /// <param name="atTx">Optional, the transaction ID at which the key is bound at</param>
    /// <param name="score">The score</param>
    /// <returns>The transaction information</returns>
    public async Task<TxHeader> ZAdd(byte[] set, byte[] key, ulong atTx, double score)
    {
        CheckSessionHasBeenOpened();
        ImmudbProxy.TxHeader txHdr = await Service.ZAddAsync(
                new ImmudbProxy.ZAddRequest()
                {
                    Set = Utils.ToByteString(set),
                    Key = Utils.ToByteString(key),
                    AtTx = atTx,
                    Score = score,
                    BoundRef = atTx > 0
                }, Service.GetHeaders(ActiveSession));

        if (txHdr.Nentries != 1)
        {
            throw new CorruptedDataException();
        }

        return TxHeader.ValueOf(txHdr);
    }


    /// <summary>
    /// Adds a scored key to a set
    /// </summary>
    /// <param name="set">The set identifier</param>
    /// <param name="key">The lookup key</param>
    /// <param name="score">The score</param>
    /// <returns>The transaction information</returns>
    public async Task<TxHeader> ZAdd(string set, string key, double score)
    {
        return await ZAdd(Utils.ToByteArray(set), Utils.ToByteArray(key), score);
    }

    /// <summary>
    /// Adds a scored key to a set
    /// </summary>
    /// <param name="set">The set identifier</param>
    /// <param name="key">The lookup key</param>
    /// <param name="score">The score</param>
    /// <returns>The transaction information</returns>
    public async Task<TxHeader> ZAdd(byte[] set, byte[] key, double score)
    {
        return await ZAdd(set, key, 0, score);
    }

    /// <summary>
    /// Adds with authenticity check a scored key to a set
    /// </summary>
    /// <param name="set">The set identifier</param>
    /// <param name="key">The lookup key</param>
    /// <param name="score">The score</param> 
    /// <param name="atTx">The transaction ID</param>
    /// <returns>The transaction information</returns>
    public async Task<TxHeader> VerifiedZAdd(byte[] set, byte[] key, ulong atTx, double score)
    {
        CheckSessionHasBeenOpened();

        ImmuState state = State;
        ImmudbProxy.ZAddRequest zAddReq = new ImmudbProxy.ZAddRequest()
        {
            Set = Utils.ToByteString(set),
            Key = Utils.ToByteString(key),
            AtTx = atTx,
            Score = score,
            BoundRef = atTx > 0
        };
        ImmudbProxy.VerifiableZAddRequest vZAddReq = new ImmudbProxy.VerifiableZAddRequest()
        {
            ZAddRequest = zAddReq,
            ProveSinceTx = state.TxId
        };

        ImmudbProxy.VerifiableTx vtx = await Service.VerifiableZAddAsync(vZAddReq, Service.GetHeaders(ActiveSession));

        if (vtx.Tx.Header.Nentries != 1)
        {
            throw new VerificationException("Data is corrupted.");
        }

        Tx tx;
        try
        {
            tx = Tx.ValueOf(vtx.Tx);
        }
        catch (Exception e)
        {
            throw new VerificationException("Failed to extract the transaction.", e);
        }

        TxHeader txHeader = tx.Header;

        ZEntry entry = ZEntry.ValueOf(new ImmudbProxy.ZEntry
        {
            Set = Utils.ToByteString(set),
            Key = Utils.ToByteString(key),
            AtTx = atTx,
            Score = score
        });

        Crypto.InclusionProof inclusionProof = tx.Proof(entry.GetEncodedKey());

        if (!CryptoUtils.VerifyInclusion(inclusionProof, entry.DigestFor(txHeader.Version), txHeader.Eh))
        {
            throw new VerificationException("Data is corrupted (inclusion verification failed).");
        }

        if (!txHeader.Eh.SequenceEqual(CryptoUtils.DigestFrom(vtx.DualProof.TargetTxHeader.EH.ToByteArray())))
        {
            throw new VerificationException("Data is corrupted (different digests).");
        }

        ImmuState newState = VerifyDualProof(vtx, tx, state);

        if (!newState.CheckSignature(serverSigningKey))
        {
            throw new VerificationException("State signature verification failed");
        }
        UpdateState(newState);
        return TxHeader.ValueOf(vtx.Tx.Header);
    }

    /// <summary>
    /// Adds with authenticity check a scored key to a set
    /// </summary>
    /// <param name="set">The set identifier</param>
    /// <param name="key">The lookup key</param>
    /// <param name="score">The score</param>
    /// <returns>The transaction information</returns>
    public async Task<TxHeader> VerifiedZAdd(string set, string key, double score)
    {
        return await VerifiedZAdd(Utils.ToByteArray(set), Utils.ToByteArray(key), score);
    }

    /// <summary>
    /// Adds with authenticity check a scored key to a set
    /// </summary>
    /// <param name="set">The set identifier</param>
    /// <param name="key">The lookup key</param>
    /// <param name="score">The score</param>
    /// <returns>The transaction information</returns>
    public async Task<TxHeader> VerifiedZAdd(byte[] set, byte[] key, double score)
    {
        return await VerifiedZAdd(set, key, 0, score);
    }

    /// <summary>
    /// Adds with authenticity check a scored key to a set
    /// </summary>
    /// <param name="set">The set identifier</param>
    /// <param name="key">The lookup key</param>
    /// <param name="score">The score</param>
    /// <param name="atTx"></param>
    /// <returns>The transaction information</returns>
    public async Task<TxHeader> VerifiedZAdd(string set, string key, ulong atTx, double score)
    {
        return await VerifiedZAdd(Utils.ToByteArray(set), Utils.ToByteArray(key), atTx, score);
    }

    /// <summary>
    /// Iterates over the entries added with ZAdd in the selected database and retrieves the values for the matching criteria
    /// </summary>
    /// <param name="set">The set identifier</param>
    /// <param name="limit">Maximum number of entries to return</param>
    /// <param name="reverse">If true, return elements in reversed order</param>
    /// <returns>A list of <see cref="Entry"/> objects.</returns>
    public async Task<List<ZEntry>> ZScan(string set, ulong limit, bool reverse)
    {
        return await ZScan(Utils.ToByteArray(set), limit, reverse);
    }

    /// <summary>
    /// Iterates over the entries added with ZAdd in the selected database and retrieves the values for the matching criteria
    /// </summary>
    /// <param name="set"></param>
    /// <param name="limit"></param>
    /// <param name="reverse"></param>
    /// <returns>A list of <see cref="Entry"/> objects.</returns>
    public async Task<List<ZEntry>> ZScan(byte[] set, ulong limit, bool reverse)
    {
        CheckSessionHasBeenOpened();
        ImmudbProxy.ZScanRequest req = new ImmudbProxy.ZScanRequest()
        {
            Set = Utils.ToByteString(set),
            Limit = limit,
            Desc = reverse
        };

        ImmudbProxy.ZEntries zEntries = await Service.ZScanAsync(req, Service.GetHeaders(ActiveSession));
        return BuildList(zEntries);
    }

    //
    // ========== DELETE ==========
    //

    /// <summary>
    /// Deletes a key/value entry
    /// </summary>
    /// <param name="key">The lookup key</param>
    /// <returns>The transaction information</returns>
    public async Task<TxHeader> Delete(string key)
    {
        return await Delete(Utils.ToByteArray(key));
    }

    /// <summary>
    /// Deletes a key/value entry
    /// </summary>
    /// <param name="key">The lookup key</param>
    /// <returns>The transaction information</returns>
    public async Task<TxHeader> Delete(byte[] key)
    {
        CheckSessionHasBeenOpened();
        try
        {
            ImmudbProxy.DeleteKeysRequest req = new ImmudbProxy.DeleteKeysRequest()
            {
                Keys = { Utils.ToByteString(key) }
            };
            return TxHeader.ValueOf(await Service.DeleteAsync(req, Service.GetHeaders(ActiveSession)));
        }
        catch (RpcException e)
        {
            if (e.Message.Contains("key not found"))
            {

                throw new KeyNotFoundException();
            }

            throw e;
        }
    }

    //
    // ========== TX ==========
    //

    /// <summary>
    /// Gets a transaction information
    /// </summary>
    /// <param name="txId">The Transaction ID</param>
    /// <returns>The transaction information</returns>
    public async Task<Tx> TxById(ulong txId)
    {
        CheckSessionHasBeenOpened();
        try
        {
            ImmudbProxy.Tx tx = await Service.TxByIdAsync(
                new ImmudbProxy.TxRequest()
                {
                    Tx = txId
                }, Service.GetHeaders(ActiveSession));
            return Tx.ValueOf(tx);
        }
        catch (RpcException e)
        {
            if (e.Message.Contains("tx not found"))
            {
                throw new TxNotFoundException();
            }

            throw e;
        }
    }

    /// <summary>
    /// Gets with authenticity check a transaction information
    /// </summary>
    /// <param name="txId">The Transaction ID</param>
    /// <returns>The transaction information</returns>
    public async Task<Tx> VerifiedTxById(ulong txId)
    {
        CheckSessionHasBeenOpened();
        ImmuState state = State;
        ImmudbProxy.VerifiableTxRequest vTxReq = new ImmudbProxy.VerifiableTxRequest()
        {
            Tx = txId,
            ProveSinceTx = state.TxId
        };

        ImmudbProxy.VerifiableTx vtx;

        try
        {
            vtx = await Service.VerifiableTxByIdAsync(vTxReq, Service.GetHeaders(ActiveSession));
        }
        catch (RpcException e)
        {
            if (e.Message.Contains("tx not found"))
            {
                throw new TxNotFoundException();
            }
            throw e;
        }

        Crypto.DualProof dualProof = Crypto.DualProof.ValueOf(vtx.DualProof);
        ulong sourceId;
        ulong targetId;
        byte[] sourceAlh;
        byte[] targetAlh;

        if (state.TxId <= txId)
        {
            sourceId = state.TxId;
            sourceAlh = CryptoUtils.DigestFrom(state.TxHash);
            targetId = txId;
            targetAlh = dualProof.TargetTxHeader.Alh();
        }
        else
        {
            sourceId = txId;
            sourceAlh = dualProof.SourceTxHeader.Alh();
            targetId = state.TxId;
            targetAlh = CryptoUtils.DigestFrom(state.TxHash);
        }

        if (state.TxId > 0)
        {
            if (!CryptoUtils.VerifyDualProof(
                    Crypto.DualProof.ValueOf(vtx.DualProof),
                    sourceId,
                    targetId,
                    sourceAlh,
                    targetAlh
            ))
            {
                throw new VerificationException("Data is corrupted (dual proof verification failed).");
            }
        }

        Tx tx;
        try
        {
            tx = Tx.ValueOf(vtx.Tx);
        }
        catch (Exception e)
        {
            throw new VerificationException("Failed to extract the transaction.", e);
        }

        ImmuState newState = new ImmuState(currentDb, targetId, targetAlh, (vtx.Signature ?? ImmudbProxy.Signature.DefaultInstance).Signature_.ToByteArray());

        if (!newState.CheckSignature(serverSigningKey))
        {
            throw new VerificationException("State signature verification failed");
        }
        UpdateState(newState);
        return tx;
    }

    private void UpdateState(ImmuState newState)
    {
        lock (stateSync)
        {
            stateHolder.SetState(ActiveSession!, newState);
        }
    }

    /// <summary>
    /// Iterates over the transactions 
    /// </summary>
    /// <param name="initialTxId">Initial transaction ID</param>
    /// <returns>A list of transactions information</returns>
    public async Task<List<Tx>> TxScan(ulong initialTxId)
    {
        CheckSessionHasBeenOpened();
        ImmudbProxy.TxScanRequest req = new ImmudbProxy.TxScanRequest()
        {
            InitialTx = initialTxId
        };

        ImmudbProxy.TxList txList = await Service.TxScanAsync(req, Service.GetHeaders(ActiveSession));
        return buildList(txList);
    }

    /// <summary>
    /// Iterates over the transactions 
    /// </summary>
    /// <param name="initialTxId">Initial transaction ID</param>
    /// <param name="limit">The maximum number of transactions</param>
    /// <param name="desc">True for descending order</param>
    /// <returns>A list of transactions information</returns>
    public async Task<List<Tx>> TxScan(ulong initialTxId, uint limit, bool desc)
    {
        ImmudbProxy.TxScanRequest req = new ImmudbProxy.TxScanRequest()
        {
            InitialTx = initialTxId,
            Limit = limit,
            Desc = desc
        };
        ImmudbProxy.TxList txList = await Service.TxScanAsync(req, Service.GetHeaders(ActiveSession));
        return buildList(txList);
    }

    //
    // ========== HEALTH ==========
    //


    /// <summary>
    /// Performs a healthcheck query
    /// </summary>
    /// <returns>true if healthcheck is successful</returns>
    public async Task<bool> HealthCheck()
    {
        var healthResponse = await Service.HealthAsync(new Empty(), Service.GetHeaders(ActiveSession));
        return healthResponse.Status;
    }

    /// <summary>
    /// Gets the Connected status
    /// </summary>
    public bool IsConnected => !Connection.Released;

    //
    // ========== USER MGMT ==========
    //

    /// <summary>
    /// Gets the user list
    /// </summary>
    /// <returns>The user list</returns>
    public async Task<List<Iam.User>> ListUsers()
    {
        CheckSessionHasBeenOpened();
        ImmudbProxy.UserList userList = await Service.ListUsersAsync(new Empty(), Service.GetHeaders(ActiveSession));
        return userList.Users.ToList()
                .Select(u => new Iam.User(
                    u.User_.ToString(System.Text.Encoding.UTF8),
                    BuildPermissions(u.Permissions))
                {
                    Active = u.Active,
                    CreatedAt = u.Createdat,
                    CreatedBy = u.Createdby,
                }).ToList();
    }

    private List<Iam.Permission> BuildPermissions(RepeatedField<ImmudbProxy.Permission> permissionsList)
    {
        return permissionsList.ToList()
                .Select(p => (Iam.Permission)p.Permission_).ToList();
    }

    /// <summary>
    /// Creates a user
    /// </summary>
    /// <param name="username">The username</param>
    /// <param name="password">The username's password</param>
    /// <param name="permission">The <see cref="Iam.Permission"/> object</param>
    /// <param name="database">The database where the user is created</param>
    /// <returns></returns>
    public async Task CreateUser(string username, string password, Iam.Permission permission, string database)
    {
        CheckSessionHasBeenOpened();
        ImmudbProxy.CreateUserRequest createUserRequest = new ImmudbProxy.CreateUserRequest()
        {
            User = Utils.ToByteString(username),
            Password = Utils.ToByteString(password),
            Permission = (uint)permission,
            Database = database
        };

        await Service.CreateUserAsync(createUserRequest, Service.GetHeaders(ActiveSession));
    }

    /// <summary>
    /// Changes a user's password
    /// </summary>
    /// <param name="username">The username</param>
    /// <param name="oldPassword">The username's old password (only needed when changing sysadmin user's password)</param>
    /// <param name="newPassword">The new password</param>
    /// <returns></returns>
    public async Task ChangePassword(string username, string oldPassword, string newPassword)
    {
        CheckSessionHasBeenOpened();
        ImmudbProxy.ChangePasswordRequest changePasswordRequest = new ImmudbProxy.ChangePasswordRequest()
        {
            User = Utils.ToByteString(username),
            OldPassword = Utils.ToByteString(oldPassword),
            NewPassword = Utils.ToByteString(newPassword),
        };

        await Service.ChangePasswordAsync(changePasswordRequest, Service.GetHeaders(ActiveSession));
    }

    //
    // ========== INDEX MGMT ==========
    //

    /// <summary>
    /// Flushes the index
    /// </summary>
    /// <param name="cleanupPercentage">The percentage of index data to scan for unused disk space</param>
    /// <param name="synced">Set true for the index flush operation to be synchronous, this may be slower.</param>
    /// <returns></returns>
    public async Task FlushIndex(float cleanupPercentage, bool synced)
    {
        CheckSessionHasBeenOpened();
        ImmudbProxy.FlushIndexRequest req = new ImmudbProxy.FlushIndexRequest()
        {
            CleanupPercentage = cleanupPercentage,
            Synced = synced
        };

        await Service.FlushIndexAsync(req, Service.GetHeaders(ActiveSession));
    }

    /// <summary>
    /// Compacts the index
    /// </summary>
    /// <returns></returns>
    public async Task CompactIndex()
    {
        CheckSessionHasBeenOpened();
        await Service.CompactIndexAsync(new Empty(), Service.GetHeaders(ActiveSession));
    }

    //
    // ========== HISTORY ==========
    //

    /// <summary>
    /// Retrieves the history of changes of a specific key
    /// </summary>
    /// <param name="key">The lookup key</param>
    /// <param name="limit">The maximum number of returned items</param>
    /// <param name="offset">The starting index</param>
    /// <param name="desc">The sorting order, true for descending</param>
    /// <returns></returns>
    public async Task<List<Entry>> History(string key, int limit, ulong offset, bool desc)
    {
        return await History(Utils.ToByteArray(key), limit, offset, desc);
    }

    /// <summary>
    /// Retrieves the history of changes of a specific key
    /// </summary>
    /// <param name="key">The lookup key</param>
    /// <param name="limit">The maximum number of returned items</param>
    /// <param name="offset">The starting index</param>
    /// <param name="desc">The sorting order, true for descending</param>
    /// <returns></returns>
    public async Task<List<Entry>> History(byte[] key, int limit, ulong offset, bool desc)
    {
        CheckSessionHasBeenOpened();
        try
        {
            ImmudbProxy.Entries entries = await Service.HistoryAsync(new ImmudbProxy.HistoryRequest()
            {
                Key = Utils.ToByteString(key),
                Limit = limit,
                Offset = offset,
                Desc = desc
            }, Service.GetHeaders(ActiveSession));

            return BuildList(entries);
        }
        catch (RpcException e)
        {
            if (e.Message.Contains("key not found"))
            {
                throw new KeyNotFoundException();
            }

            throw e;
        }
    }

    private List<Entry> BuildList(ImmudbProxy.Entries entries)
    {
        List<Entry> result = new List<Entry>(entries.Entries_.Count);
        entries.Entries_.ToList().ForEach(entry => result.Add(Entry.ValueOf(entry)));
        return result;
    }

    private List<ZEntry> BuildList(ImmudbProxy.ZEntries entries)
    {
        List<ZEntry> result = new List<ZEntry>(entries.Entries.Count);
        entries.Entries.ToList()
                .ForEach(entry => result.Add(ZEntry.ValueOf(entry)));
        return result;
    }

    private List<Tx> buildList(ImmudbProxy.TxList txList)
    {
        List<Tx> result = new List<Tx>(txList.Txs.Count);
        txList.Txs.ToList().ForEach(tx =>
        {
            try
            {
                result.Add(Tx.ValueOf(tx));
            }
            catch (Exception e)
            {
                Console.WriteLine("An exception occurred into buildList: {0}", e);
            }
        });
        return result;
    }

    //
    // ========== SQL Exec and SQL Query  ==========
    //



    /// <summary>
    /// Executes an SQL statement against the selected database
    /// </summary>
    /// <param name="sqlStatement">The SQL statement</param>
    /// <param name="parameters">a variable number of SQLParameter values</param>
    /// <returns>A <see cref="SQL.SQLExecResult" /> object containing transaction ids and updated rows count for each transaction</returns>
    public async Task<SQL.SQLExecResult> SQLExec(string sqlStatement, params SQLParameter[] parameters)
    {
        CheckSessionHasBeenOpened();

        var req = new ImmudbProxy.SQLExecRequest
        {
            Sql = sqlStatement,
        };
        if (parameters != null)
        {
            req.Params.Add(SQL.Converters.ToNamedParams(parameters));
        }
        var result = await Service.SQLExecAsync(req, Service.GetHeaders(ActiveSession));
        var sqlResult = new SQL.SQLExecResult();

        foreach (var item in result.Txs)
        {
            if (item.Header == null)
            {
                continue;
            }
            sqlResult.Items.Add(new SQLExecResultItem { TxID = item.Header.Id, UpdatedRowsCount = item.UpdatedRows });
        }
        return sqlResult;
    }

    /// <summary>
    /// Executes an SQL Query against the selected database
    /// </summary>
    /// <param name="sqlStatement"></param>
    /// <param name="parameters"></param>
    /// <returns>A <see cref="SQL.SQLQueryResult" /> object containing the column list and the rows with execution result</returns>
    public async Task<SQL.SQLQueryResult> SQLQuery(string sqlStatement, params SQLParameter[] parameters)
    {
        CheckSessionHasBeenOpened();
        var req = new ImmudbProxy.SQLQueryRequest
        {
            Sql = sqlStatement,
        };
        if (parameters != null)
        {
            req.Params.Add(SQL.Converters.ToNamedParams(parameters));
        }
        var result = await Service.SQLQueryAsync(req, Service.GetHeaders(ActiveSession));
        SQL.SQLQueryResult queryResult = new SQL.SQLQueryResult();
        queryResult.Columns.AddRange(result.Columns.Select(x =>
        {
            var columnName = x.Name.Substring(x.Name.LastIndexOf(".") + 1);
            columnName = columnName.Remove(columnName.Length - 1, 1);
            return new SQL.Column(columnName, x.Type);
        }));
        foreach (var row in result.Rows)
        {
            Dictionary<string, SQL.SQLValue> rowItems = new Dictionary<string, SQL.SQLValue>();
            for (int i = 0; i < row.Columns.Count; i++)
            {
                var columnName = row.Columns[i].Substring(row.Columns[i].LastIndexOf(".") + 1);
                columnName = columnName.Remove(columnName.Length - 1, 1);
                rowItems.Add(columnName, SQL.Converters.FromProxySQLValue(row.Values[i]));
            }
            queryResult.Rows.Add(rowItems);
        }
        return queryResult;
    }
}