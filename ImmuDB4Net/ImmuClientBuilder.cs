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

using System.Runtime.CompilerServices;
using Org.BouncyCastle.Crypto;

/// <summary>
/// Enables the creation of <see cref="ImmuClient" /> instances
/// </summary>
public ref struct ImmuClientBuilder
{
    private const string DefaultSchema = "http://";

    /// <summary>
    /// Gets the server URL, such as localhost or http://localhost
    /// </summary>
    /// <value></value>
    public ReadOnlySpan<char> ServerUrl { get; private set; }
    /// <summary>
    /// Gets the username
    /// </summary>
    /// <value></value>
    public ReadOnlySpan<char> Username { get; private set; }
    /// <summary>
    /// Gets the password
    /// </summary>
    /// <value></value>
    public ReadOnlySpan<char> Password { get; private set; }
    /// <summary>
    /// Gets the database name, such as DefaultDB
    /// </summary>
    /// <value></value>
    public ReadOnlySpan<char> Database { get; private set; }
    /// <summary>
    /// Gets the port number, ex: 3322
    /// </summary>
    /// <value></value>
    public int ServerPort { get; private set; }

    private int serverPortStringLength;
    /// <summary>
    /// Gets the public-private key pair
    /// </summary>
    /// <value></value>
    public AsymmetricKeyParameter? ServerSigningKey { get; private set; }
    /// <summary>
    /// Gets the DeploymentInfoCheck flag. If this flag is set then a check of server authenticity is perform while establishing a new link with the ImmuDB server.
    /// </summary>
    /// <value></value>
    public bool DeploymentInfoCheck { get; private set; }
    /// <summary>
    /// Gets the StateHolder instance. Default is of type <see cref="FileImmuStateHolder" />
    /// </summary>
    /// <value></value>
    public IImmuStateHolder StateHolder { get; private set; }
    /// <summary>
    /// Gets or sets the time interval between heartbeat gRPC calls
    /// </summary>
    /// <value></value>
    public TimeSpan HeartbeatInterval { get; set; }

    internal IConnectionPool ConnectionPool { get; }
    internal ISessionManager SessionManager { get; }

    static ImmuClientBuilder()
    {
        // This is needed for .NET Core 3 and below.
        AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
    }

    /// <summary>
    /// The constructor for ImmuClientBuilder, sets the default values for the fields
    /// </summary>
    public ImmuClientBuilder()
    {
        ServerUrl = "localhost";
        ServerPort = 3322;
        Username = "immudb";
        Password = "immudb";
        Database = "defaultdb";
        StateHolder = new FileImmuStateHolder();
        DeploymentInfoCheck = true;
        HeartbeatInterval = TimeSpan.FromMinutes(1);
        ConnectionPool = RandomAssignConnectionPool.Instance;
        SessionManager = DefaultSessionManager.Instance;
        ConnectionShutdownTimeout = TimeSpan.FromSeconds(2);
    }

    /// <summary>
    /// Gets the GrpcAddress; it is formed from the ServerUrl and ServerPort parameters
    /// </summary>
    /// <value></value>
    public string GetGrpcAddress()
    {
        // Allocate the maximum number of characters on the Stack
        Span<char> grpcAddress = stackalloc char[GrpcAddressLength];

        // Append the grpc address to the char buffer
        GetGrpcAddress(ServerUrl, ServerPort, serverPortStringLength, grpcAddress);

        // Copy char buffer to string on Heap
        return grpcAddress.ToString();
    }

    public int GrpcAddressLength => DefaultSchema.Length + ServerUrl.Length + serverPortStringLength;

    public void GetGrpcAddress(Span<char> grpcAddress)
    {
        if (grpcAddress.Length != GrpcAddressLength)
            throw new InvalidOperationException($"grpcAddress span is not of length: {GrpcAddressLength}, given: {grpcAddress.Length}");

        GetGrpcAddress(ServerUrl, ServerPort, serverPortStringLength, grpcAddress);
    }

    public static void GetGrpcAddress(ReadOnlySpan<char> serverUrl, in int serverPort, Span<char> grpcAddress)
        => GetGrpcAddress(serverUrl, serverPort, IntLength(serverPort), grpcAddress);

    public static void GetGrpcAddress(ReadOnlySpan<char> serverUrl, in int serverPort, in int serverPortStringLength, Span<char> grpcAddress)
    {
        // Append the default schema if necessary
        var min = 0;
        if (!serverUrl.StartsWith("http"))
            DefaultSchema.AsSpan().CopyTo(grpcAddress.Slice(min += DefaultSchema.Length, DefaultSchema.Length));

        // Append the the url in all lowercase
        serverUrl.ToLowerInvariant(grpcAddress.Slice(min += serverUrl.Length, serverUrl.Length));

        // Append the Port
        serverPort.TryFormat(grpcAddress.Slice(min, serverPortStringLength), out _);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int IntLength(in int i)
    {
        if (i < 0)
            throw new ArgumentOutOfRangeException(nameof(i));
        if (i == 0)
            return 1;
        return (int)Math.Floor(Math.Log10(i)) + 1;
    }

    /// <summary>
    /// Gets the length of time the <see cref="ImmuClient.Close" /> function is allowed to block before it completes.
    /// </summary>
    /// <value>Default: 2 sec</value>
    public TimeSpan ConnectionShutdownTimeout { get; internal set; }

    /// <summary>
    /// Sets a stateholder instance. It could be a custom state holder that implements IImmuStateHolder
    /// </summary>
    /// <param name="stateHolder"></param>
    /// <returns></returns>
    public ImmuClientBuilder WithStateHolder(IImmuStateHolder stateHolder)
    {
        StateHolder = stateHolder;
        return this;
    }

    /// <summary>
    /// Sets the CheckDeploymentInfo flag. If this flag is set then a check of server authenticity is perform while establishing a new link with the ImmuDB server.
    /// </summary>
    /// <param name="check"></param>
    /// <returns></returns>
    public ImmuClientBuilder CheckDeploymentInfo(bool check)
    {
        this.DeploymentInfoCheck = check;
        return this;
    }

    /// <summary>
    /// Sets the credentials
    /// </summary>
    /// <param name="username">The username</param>
    /// <param name="password">The password</param>
    /// <returns></returns>
    public ImmuClientBuilder WithCredentials(ReadOnlySpan<char> username, ReadOnlySpan<char> password)
    {
        this.Username = username;
        this.Password = password;
        return this;
    }

    /// <summary>
    /// Sets the database name
    /// </summary>
    /// <param name="databaseName"></param>
    /// <returns></returns>
    public ImmuClientBuilder WithDatabase(ReadOnlySpan<char> databaseName)
    {
        this.Database = databaseName;
        return this;
    }

    /// <summary>
    /// Sets the port number where the ImmuDB listens to
    /// </summary>
    /// <param name="serverPort"></param>
    /// <returns></returns>
    public ImmuClientBuilder WithServerPort(in int serverPort)
    {
        this.ServerPort = serverPort;
        serverPortStringLength = IntLength(serverPort);
        return this;
    }

    /// <summary>
    /// Sets the server URL 
    /// </summary>
    /// <param name="serverUrl"></param>
    /// <returns></returns>
    public ImmuClientBuilder WithServerUrl(ReadOnlySpan<char> serverUrl)
    {
        this.ServerUrl = serverUrl;
        return this;
    }

    /// <summary>
    /// Sets the time interval between heartbeat gRPC calls
    /// </summary>
    /// <param name="heartbeatInterval"></param>
    /// <returns></returns>
    public ImmuClientBuilder WithHeartbeatInterval(TimeSpan heartbeatInterval)
    {
        this.HeartbeatInterval = heartbeatInterval;
        return this;
    }

    /// <summary>
    /// Sets the length of time the <see cref="ImmuClient.Close" /> function is allowed to block before it completes.
    /// </summary>
    /// <param name="timeout"></param>
    /// <returns></returns>
    public ImmuClientBuilder WithConnectionShutdownTimeout(TimeSpan timeout)
    {
        this.ConnectionShutdownTimeout = timeout;
        return this;
    }

    /// <summary>
    /// Sets the file path containing the signing public key of ImmuDB server
    /// </summary>
    /// <param name="publicKeyFileName"></param>
    /// <returns></returns>
    public ImmuClientBuilder WithServerSigningKey(in string publicKeyFileName)
    {
        this.ServerSigningKey = ImmuState.GetPublicKeyFromPemFile(publicKeyFileName);
        return this;
    }

    /// <summary>
    /// Sets the server signing key
    /// </summary>
    /// <param name="key"></param>
    /// <returns></returns>
    public ImmuClientBuilder WithServerSigningKey(AsymmetricKeyParameter? key)
    {
        this.ServerSigningKey = key;
        return this;
    }

    /// <summary>
    /// Creates an <see cref="ImmuClient" /> instance using the parameters defined in the builder. One can use the builder's fluent interface to define these parameters.
    /// </summary>
    /// <returns></returns>
    public ImmuClient Build()
    {
        return new ImmuClient(this);
    }

    /// <summary>
    /// Creates an <see cref="ImmuClient" /> instance using the parameters from the builder instance and opens a connection to the server.
    /// </summary>
    /// <returns></returns>
    public async Task<ImmuClient> Open()
    {
        var immuClient = new ImmuClient(this);
        await immuClient.Open(this.Username, this.Password, this.Database);
        return immuClient;
    }
}
