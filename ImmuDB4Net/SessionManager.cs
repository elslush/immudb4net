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

using System.Buffers;
using System.Collections.Concurrent;
using System.Text;
using Google.Protobuf.WellKnownTypes;
using ImmudbProxy;
using static ImmudbProxy.ImmuService;

namespace ImmuDB;

internal interface ISessionManager
{
    Task<Session> OpenSessionAsync(IConnection connection, ReadOnlySpan<char> username, ReadOnlySpan<char> password, in string initialDbName);
    Session OpenSession(IConnection connection, ReadOnlySpan<char> username, ReadOnlySpan<char> password, in string initialDbName);
    Task CloseSessionAsync(IConnection connection, Session? session);
    void CloseSession(IConnection connection, Session? session);
}

internal class DefaultSessionManager : ISessionManager
{
    internal static DefaultSessionManager _instance = new DefaultSessionManager();
    public static ISessionManager Instance
    {
        get
        {
            return _instance;
        }
    }
    private ConcurrentDictionary<string, Session> sessions = new ConcurrentDictionary<string, Session>();

    public Task<Session> OpenSessionAsync(IConnection connection, ReadOnlySpan<char> username, ReadOnlySpan<char> password, in string initialDbName)
    {
        var usernameByteCount = Encoding.UTF8.GetByteCount(username);
        var passwordByteCount = Encoding.UTF8.GetByteCount(password);

        using IMemoryOwner<byte> owner = MemoryPool<byte>.Shared.Rent(usernameByteCount + passwordByteCount);
        
        Encoding.UTF8.GetBytes(username, owner.Memory.Span[..usernameByteCount]);
        Encoding.UTF8.GetBytes(password, owner.Memory.Span.Slice(usernameByteCount, passwordByteCount));

        OpenSessionRequest openSessionRequest = new()
        {
            Username = Utils.ToByteString(owner.Memory[..usernameByteCount]),
            Password = Utils.ToByteString(owner.Memory.Slice(usernameByteCount, passwordByteCount)),
            DatabaseName = initialDbName
        };

        return OpenSessionAsync(connection, openSessionRequest);
    }

    public async Task<Session> OpenSessionAsync(IConnection connection, OpenSessionRequest openSessionRequest)
    {
        var result = await connection.Service.OpenSessionAsync(openSessionRequest);
        var session = new Session(result.SessionID, result.ServerUUID)
        {
            Kind = TransactionKind.ReadWrite
        };
        sessions[result.SessionID] = session;
        return session;
    }

    public Session OpenSession(IConnection connection, ReadOnlySpan<char> username, ReadOnlySpan<char> password, in string initialDbName)
    {
        var usernameByteCount = Encoding.UTF8.GetByteCount(username);
        var passwordByteCount = Encoding.UTF8.GetByteCount(password);

        using IMemoryOwner<byte> owner = MemoryPool<byte>.Shared.Rent(usernameByteCount + passwordByteCount);

        Encoding.UTF8.GetBytes(username, owner.Memory.Span[..usernameByteCount]);
        Encoding.UTF8.GetBytes(password, owner.Memory.Span.Slice(usernameByteCount, passwordByteCount));

        OpenSessionRequest openSessionRequest = new()
        {
            Username = Utils.ToByteString(owner.Memory[..usernameByteCount]),
            Password = Utils.ToByteString(owner.Memory.Slice(usernameByteCount, passwordByteCount)),
            DatabaseName = initialDbName
        };

        return OpenSession(connection, openSessionRequest);
    }

    public Session OpenSession(IConnection connection, OpenSessionRequest openSessionRequest)
    {
        var result = connection.Service.OpenSession(openSessionRequest);
        var session = new Session(result.SessionID, result.ServerUUID)
        {
            Kind = TransactionKind.ReadWrite
        };
        sessions[result.SessionID] = session;
        return session;
    }

    public async Task CloseSessionAsync(IConnection connection, Session? session)
    {
        if (session?.Id == null)
        {
            return;
        }
        await connection.Service.CloseSessionAsync(new Empty(), ImmuServiceClient.GetHeaders(session));
        sessions.TryRemove(session.Id, out _);
    }
    public void CloseSession(IConnection connection, Session? session)
    {
        if (session?.Id == null)
        {
            return;
        }
        connection.Service.CloseSession(new Empty(), ImmuServiceClient.GetHeaders(session));
        sessions.TryRemove(session.Id, out _);
    }
}

