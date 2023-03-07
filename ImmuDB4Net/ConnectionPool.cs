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


using System.Runtime.InteropServices;

namespace ImmuDB;


internal interface IConnectionPool
{
    int MaxConnectionsPerServer { get; }
    IConnection Acquire(ConnectionParameters cp);
    void Release(IConnection con);
    Task ShutdownAsync();
    void Shutdown();
}



internal class RandomAssignConnectionPool : IConnectionPool
{
    public int MaxConnectionsPerServer { get; private set; }
    public TimeSpan TerminateIdleConnectionTimeout { get; private set; }
    public TimeSpan IdleConnectionCheckInterval { get; private set; }
    private Random random = new Random(Environment.TickCount);
    private readonly Task cleanupIdleConnections;
    private ManualResetEvent shutdownRequested = new ManualResetEvent(false);

    internal static object instanceSync = new();
    internal static RandomAssignConnectionPool? instance = null;
    public static IConnectionPool Instance
    {
        get
        {
            lock (instanceSync)
            {
                if (instance == null)
                {
                    instance = new RandomAssignConnectionPool();
                }
                return instance;
            }
        }
    }

    internal static async Task ResetInstance()
    {
        RandomAssignConnectionPool? localInstance;        
        lock (instanceSync)
        {
            localInstance = instance;
            instance = null;
        }
        if (localInstance != null)
        {
            await localInstance.ShutdownAsync();
        }
    }

    internal class Item
    {
        private int refCount;

        public Item(IConnection con)
        {
            Connection = con;
            RefCount = 0;
            LastChangeTimestamp = DateTime.UtcNow;
        }

        public IConnection Connection { get; set; }
        public DateTime LastChangeTimestamp { get; set; }
        public int RefCount
        {
            get => refCount; set
            {
                if (refCount != value)
                {
                    LastChangeTimestamp = DateTime.UtcNow;
                    refCount = value;
                }
            }
        }

        public bool ShouldBeTerminated(TimeSpan timeout)
        {
            if (refCount > 0)
            {
                return false;
            }
            TimeSpan currentDiff = DateTime.UtcNow.Subtract(LastChangeTimestamp);
            return currentDiff.CompareTo(timeout) >= 0;
        }
    }

    internal RandomAssignConnectionPool()
    {
        MaxConnectionsPerServer = ImmuClient.GlobalSettings.MaxConnectionsPerServer;
        TerminateIdleConnectionTimeout = ImmuClient.GlobalSettings.TerminateIdleConnectionTimeout;
        IdleConnectionCheckInterval = ImmuClient.GlobalSettings.IdleConnectionCheckInterval;


        cleanupIdleConnections = Task.Factory.StartNew(async () =>
        {
            while (true)
            {
                if (shutdownRequested.WaitOne(IdleConnectionCheckInterval))
                {
                    break;
                }
                await CleanupIdleConnections(TerminateIdleConnectionTimeout);
            }
        });
    }

    ~RandomAssignConnectionPool()
    {
        try
        {
            shutdownRequested.Close();
        }
        catch (ObjectDisposedException) { }
    }

    private Task CleanupIdleConnections(TimeSpan timeout)
    {
        Task[] shutdownTasks;
        Tuple<string, Item>[] parentChildList;
        lock (this)
        {
            // Get number of connections to cleanup
            var connectionCount = 0;
            foreach (var addressPool in connections.Values)
                foreach (var item in addressPool)
                    if (item.ShouldBeTerminated(timeout))
                        connectionCount++;

            shutdownTasks = new Task[connectionCount];
            parentChildList = new Tuple<string, Item>[connectionCount];

            var i = 0;
            foreach (var addressPool in connections)
                foreach (var item in CollectionsMarshal.AsSpan(addressPool.Value))
                    if (item.ShouldBeTerminated(timeout))
                    {
                        // Collect the shutdown task
                        shutdownTasks[i] = item.Connection.ShutdownAsync();
                        // Collect the connection to remove from the connection pool.
                        // This must happen outside of the loop because you cannot remove items while iterating.
                        parentChildList[i++] = new Tuple<string, Item>(addressPool.Key, item);
                    }

            foreach (var parentChild in parentChildList.AsSpan())
            {
                connections[parentChild.Item1].Remove(parentChild.Item2);
            }
        }

        return Task.WhenAll(shutdownTasks);
    }

    private readonly Dictionary<string, List<Item>> connections = new Dictionary<string, List<Item>>();

    public IConnection Acquire(ConnectionParameters cp)
    {
        if (cp == null)
        {
            throw new ArgumentException("Acquire: ConnectionParameters argument cannot be null");
        }
        lock (this)
        {
            List<Item>? poolForAddress;
            if (!connections.TryGetValue(cp.Address, out poolForAddress))
            {
                poolForAddress = new List<Item>();
                var conn = new Connection(cp);
                var item = new Item(conn);
                poolForAddress.Add(item);
                connections.Add(cp.Address, poolForAddress);
                return conn;
            }
            if (poolForAddress.Count < MaxConnectionsPerServer)
            {
                var conn = new Connection(cp);
                var item = new Item(conn);
                poolForAddress.Add(item);
                return conn;
            }
            var randomItem = poolForAddress[random.Next(MaxConnectionsPerServer)];
            randomItem.RefCount++;
            return randomItem.Connection;
        }
    }

    ///<summary>
    /// The method <c>Release</c> is called when the connection specified as argument is no more used by an <c>ImmuClient</c> instance, therefore it
    /// can be closed 
    ///</summary>
    public void Release(IConnection con)
    {
        List<Item>? poolForAddress;
        lock (this)
        {
            if (!connections.TryGetValue(con.Address, out poolForAddress))
            {
                return;
            }
            for (int i = 0; i < poolForAddress.Count; i++)
            {
                if ((poolForAddress[i].Connection == con) && (poolForAddress[i].RefCount > 0))
                {
                    poolForAddress[i].RefCount--;
                    break;
                }
            }
        }
    }

    public Task ShutdownAsync()
    {
        try
        {
            shutdownRequested.Set();
            cleanupIdleConnections.Wait();
        }
        catch (ObjectDisposedException) { }

        Task[] shutdownTasks;
        lock (this)
        {
            var connectionCount = 0;
            foreach (var addressPool in connections.Values)
                connectionCount += addressPool.Count;

            shutdownTasks = new Task[connectionCount];

            var i = 0;
            foreach (var addressPool in connections.Values)
                foreach (var item in CollectionsMarshal.AsSpan(addressPool))
                    shutdownTasks[i++] = item.Connection.ShutdownAsync();

            connections.Clear();
        }

        return Task.WhenAll(shutdownTasks);
    }    
    
    public void Shutdown()
    {
        try
        {
            shutdownRequested.Set();
            cleanupIdleConnections.Wait();
        }
        catch (ObjectDisposedException) { }

        IConnection[] connectionsClone;
        lock (this)
        {
            var connectionCount = 0;
            foreach (var addressPool in connections.Values)
                connectionCount += addressPool.Count;

            connectionsClone = new IConnection[connectionCount];

            var i = 0;
            foreach (var addressPool in connections.Values)
                foreach (var item in CollectionsMarshal.AsSpan(addressPool))
                    connectionsClone[i++] = item.Connection;

            connections.Clear();
        }
        foreach (var connection in connectionsClone.AsSpan())
            connection.Shutdown();
    }
}