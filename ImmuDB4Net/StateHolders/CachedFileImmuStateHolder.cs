using System;
using System.Buffers;
using System.IO.MemoryMappedFiles;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using ImmuDB.Exceptions;
using MemoryPack;
using Microsoft.Extensions.Caching.Distributed;

namespace ImmuDB.StateHolders;

public class CachedFileImmuStateHolder : IImmuStateHolder
{
    private static SemaphoreSlim createSemaphore = new(0, 1),
        setStateSemaphore = new(0, 1),
        getStateSemaphore = new(0, 1);

    private readonly string? deploymentKey, deploymentLabel;
    private readonly IDistributedCache cache;

    /// <summary>
    /// The constructor that uses the builder
    /// </summary>
    /// <param name="builder">The builder for FileImmuStateHolder</param>
    public CachedFileImmuStateHolder(IDistributedCache cache)
    {
        this.cache = cache;
    }

    /// <summary>
    /// The constructor that uses the builder
    /// </summary>
    /// <param name="builder">The builder for FileImmuStateHolder</param>
    public CachedFileImmuStateHolder(IDistributedCache cache, string deploymentKey, string deploymentLabel)
    {
        this.cache = cache;
        this.deploymentKey = deploymentKey;
        this.deploymentLabel = deploymentLabel;
    }

    public async Task<bool> ValidateDeploymentInfo(string serverUUID, CancellationToken cancellationToken = default)
    {
        var deploymentServerUUID = await GetDeploymentInfo(cancellationToken);
        return deploymentServerUUID is not null && deploymentServerUUID.SequenceEqual(serverUUID);
    }

    public async Task<string?> GetDeploymentInfo(CancellationToken cancellationToken = default)
    {
        if (deploymentKey is null)
            return null;

        var deploymentKeyBytes = await cache.GetAsync(deploymentKey, cancellationToken);

        if (deploymentKeyBytes is null)
            return null;

        return string.Intern(Encoding.UTF8.GetString(deploymentKeyBytes.AsSpan()));
    }

    public Task CreateDeploymentInfo(ReadOnlySpan<char> serverUUID, CancellationToken cancellationToken = default)
    {
        if (deploymentKey is null)
        {
            throw new InvalidOperationException("you need to set deploymentkey before using GetDeploymentInfo");
        }

        var serverUUIDByteCount = Encoding.UTF8.GetByteCount(serverUUID);

        byte[] serverUUIDBytes = ArrayPool<byte>.Shared.Rent(serverUUIDByteCount);
        try
        {
            Encoding.UTF8.GetBytes(deploymentKey, serverUUIDBytes);
            return cache.SetAsync(deploymentKey, serverUUIDBytes, cancellationToken);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(serverUUIDBytes);
        }
    }

    /// <summary>
    /// Gets the database's state
    /// </summary>
    /// <param name="session"></param>
    /// <param name="dbname"></param>
    /// <returns></returns>
    public async ValueTask<ImmuState?> GetState(string dbname, CancellationToken cancellationToken = default)
    {
        await getStateSemaphore.WaitAsync(cancellationToken);

        try
        {
            string stateFilePath = Path.Combine(StatesFolder, DeploymentKey, string.Format("state_{0}", dbname));
            if (!File.Exists(stateFilePath))
            {
                return null;
            }

            bool mutexCreated;
            Mutex mutex = new Mutex(true, "testmapmutex", out mutexCreated);
            using (var mmf = MemoryMappedFile.CreateFromFile(@"c:\ExtremelyLargeImage.data", FileMode.Open))
            {

            }
            using (FileStream openStream = File.OpenRead(stateFilePath))
                return await JsonSerializer.DeserializeAsync<ImmuState>(openStream, StateSourceGenerationContext.Default.ImmuState, cancellationToken);
        }
        catch (Exception)
        {
            return null;
        }
        finally
        {
            getStateSemaphore.Release();
        }
    }

    /// <summary>
    /// Sets the state
    /// </summary>
    /// <param name="session">The current session</param>
    /// <param name="state">The state</param>
    public async Task SetState(Session session, ImmuState state, CancellationToken cancellationToken = default)
    {
        try
        {
            ImmuState? currentState = await GetState(state.Database, cancellationToken);
            if (currentState != null && currentState.TxId >= state.TxId)
            {
                // if the state to save is older than what is save, just skip it
                return;
            }

            MempryPacl

            string newStateFile = Path.Combine(StatesFolder, string.Format("state_{0}_{1}",
                state.Database,
                Path.GetRandomFileName().Replace(".", "")));
            using (FileStream openStream = File.OpenRead(newStateFile))
            {
                await JsonSerializer.SerializeAsync(openStream, state, StateSourceGenerationContext.Default.ImmuState, cancellationToken);
            }
            //var options = new JsonSerializerOptions { WriteIndented = true };
            //string contents = JsonSerializer.Serialize(state, options);
            //File.WriteAllText(newStateFile, contents);
            // I had to use this workaround because File.Move with overwrite is not available in .NET Standard 2.0. Otherwise is't just a one-liner code.
            var stateHolderFile = Path.Combine(StatesFolder, DeploymentKey, string.Format("state_{0}", state.Database));
            var intermediateMoveStateFile = newStateFile + "_";
            if (File.Exists(stateHolderFile))
            {
                try
                {
                    File.Move(stateHolderFile, intermediateMoveStateFile);
                }
                catch (FileNotFoundException) { }
            }
            try
            {
                File.Move(newStateFile, stateHolderFile);
            }
            catch (IOException) { }
            if (File.Exists(intermediateMoveStateFile))
            {
                File.Delete(intermediateMoveStateFile);
            }
            stateHolderFile = newStateFile;
        }
        catch (IOException e)
        {
            Console.WriteLine($"An IOException occurred: {e.ToString()}.");
            throw new InvalidOperationException("an IO exception occurred", e);
        }
    }
}

