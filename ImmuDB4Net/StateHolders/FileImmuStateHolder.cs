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

namespace ImmuDB.StateHolders;

using System;
using System.Text.Json;
using ImmuDB.Exceptions;

/// <summary>
/// Stores the local state of an ImmuDB database in a file
/// </summary>
public class FileImmuStateHolder : IImmuStateHolder
{
    private static SemaphoreSlim createSemaphore = new(0, 1),
        setStateSemaphore = new(0, 1),
        getStateSemaphore = new(0, 1);

    /// <summary>
    /// The folder where the state info is stored
    /// </summary>
    public string StatesFolder { get; }
    /// <summary>
    /// Gets or sets the deployment key
    /// </summary>
    /// <value></value>
    public string? DeploymentKey { get; set; }
    /// <summary>
    /// Gets or sets the deployment label, usually the address
    /// </summary>
    /// <value></value>
    public string? DeploymentLabel { get; set; }
    /// <summary>
    /// 
    /// </summary>
    /// <value></value>
    public bool DeploymentInfoCheck { get; set; } = true;

    private DeploymentInfoContent? deploymentInfo;

    /// <summary>
    /// The constructor that uses the builder
    /// </summary>
    /// <param name="builder">The builder for FileImmuStateHolder</param>
    public FileImmuStateHolder(string statesFolder)
    {
        StatesFolder = statesFolder;
    }

    /// <summary>
    /// The default constructor that uses builder's default values
    /// </summary>
    /// <returns></returns>
    public FileImmuStateHolder() : this(Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), "immudb4net"))
    {
    }

    /// <summary>
    /// Gets the database's state
    /// </summary>
    /// <param name="session"></param>
    /// <param name="dbname"></param>
    /// <returns></returns>
    public async ValueTask<ImmuState?> GetState(Session? session, string dbname, CancellationToken cancellationToken = default)
    {
        if (DeploymentKey == null)
        {
            throw new InvalidOperationException("you need to set deploymentkey before using GetDeploymentInfo");
        }

        await getStateSemaphore.WaitAsync(cancellationToken);

        try
        {
            if (session == null)
            {
                return null;
            }
            if (deploymentInfo == null)
            {
                deploymentInfo = await GetDeploymentInfo(session.ServerUUID, cancellationToken);
                if (deploymentInfo == null)
                {
                    deploymentInfo = await CreateDeploymentInfo(session, cancellationToken);
                }
                if ((deploymentInfo.ServerUUID != session.ServerUUID) && DeploymentInfoCheck)
                {
                    var deploymentInfoPath = Path.Combine(StatesFolder, DeploymentKey);
                    throw new VerificationException(
                        string.Format("server UUID mismatch. Most likely you connected to a different server instance than previously used at the same address. if you understand the reason and you want to get rid of the problem, you can either delete the folder `{0}` or set CheckDeploymentInfo to false ", deploymentInfoPath));
                }
            }
            var completeStatesFolderPath = Path.Combine(StatesFolder, DeploymentKey);
            if (!Directory.Exists(completeStatesFolderPath))
            {
                Directory.CreateDirectory(completeStatesFolderPath);
            }
            string stateFilePath = Path.Combine(completeStatesFolderPath, string.Format("state_{0}", dbname));
            if (!File.Exists(stateFilePath))
            {
                return null;
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

    internal async Task<DeploymentInfoContent?> GetDeploymentInfo(string? serverUUID, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(DeploymentKey))
        {
            throw new InvalidOperationException("you need to set deploymentkey before using GetDeploymentInfo");
        }

        if (string.IsNullOrEmpty(serverUUID))
        {
            return await ReadDeploymentKeyBasedDeploymentInfo(cancellationToken);
        }

        if (Directory.Exists(StatesFolder))
        {
            var dirs = Directory.EnumerateDirectories(StatesFolder);
            foreach (var dir in dirs)
            {
                var deploymentInfoPath = Path.Combine(dir, "deploymentinfo.json");
                if (!File.Exists(deploymentInfoPath))
                {
                    continue;
                }
                try
                {
                    DeploymentInfoContent? loadedDeploymentInfo;
                    using (FileStream openStream = File.OpenRead(deploymentInfoPath))
                        loadedDeploymentInfo = await JsonSerializer.DeserializeAsync<DeploymentInfoContent>(openStream, StateSourceGenerationContext.Default.DeploymentInfoContent, cancellationToken);

                    if (loadedDeploymentInfo == null)
                    {
                        continue;
                    }
                    if (string.Equals(serverUUID, loadedDeploymentInfo.ServerUUID, StringComparison.InvariantCultureIgnoreCase))
                    {
                        DeploymentKey = Path.GetFileName(dir);
                        return loadedDeploymentInfo;
                    }
                }
                catch (JsonException)
                {
                    continue;
                }
            }
        }

        return await ReadDeploymentKeyBasedDeploymentInfo(cancellationToken);
    }

    private ValueTask<DeploymentInfoContent?> ReadDeploymentKeyBasedDeploymentInfo(CancellationToken cancellationToken = default)
    {
        var completeStatesFolderPath = Path.Combine(StatesFolder, DeploymentKey!);
        var deploymentInfoPath = Path.Combine(completeStatesFolderPath, "deploymentinfo.json");
        if (!File.Exists(deploymentInfoPath))
        {
            return ValueTask.FromResult<DeploymentInfoContent?>(null);
        }

        using FileStream openStream = File.OpenRead(deploymentInfoPath);
        return JsonSerializer.DeserializeAsync<DeploymentInfoContent>(openStream, StateSourceGenerationContext.Default.DeploymentInfoContent, cancellationToken);
    }

    internal async Task<DeploymentInfoContent> CreateDeploymentInfo(Session session, CancellationToken cancellationToken = default)
    {
        if (DeploymentKey == null)
        {
            throw new InvalidOperationException("you need to set deploymentkey before using GetDeploymentInfo");
        }

        await createSemaphore.WaitAsync(cancellationToken);
        try
        {
            var completeStatesFolderPath = Path.Combine(StatesFolder, DeploymentKey);
            if (!Directory.Exists(completeStatesFolderPath))
            {
                Directory.CreateDirectory(completeStatesFolderPath);
            }
            var deploymentInfoPath = Path.Combine(completeStatesFolderPath, "deploymentinfo.json");
            var info = new DeploymentInfoContent { Label = DeploymentLabel, ServerUUID = session.ServerUUID };

            using (FileStream createStream = File.Create(deploymentInfoPath))
            {
                await JsonSerializer.SerializeAsync(createStream, info, StateSourceGenerationContext.Default.DeploymentInfoContent, cancellationToken);
            }
            return info;
        }
        finally
        {
            createSemaphore.Release();
        }
    }

    /// <summary>
    /// Sets the state
    /// </summary>
    /// <param name="session">The current session</param>
    /// <param name="state">The state</param>
    public async Task SetState(Session session, ImmuState state, CancellationToken cancellationToken = default)
    {
        if (DeploymentKey == null)
        {
            throw new InvalidOperationException("you need to set deploymentkey before using GetDeploymentInfo");
        }

        await setStateSemaphore.WaitAsync(cancellationToken);
        try
        {
            ImmuState? currentState = await GetState(session, state.Database, cancellationToken);
            if (currentState != null && currentState.TxId >= state.TxId)
            {
                // if the state to save is older than what is save, just skip it
                return;
            }

            string newStateFile = Path.Combine(StatesFolder, string.Format("state_{0}_{1}",
                state.Database,
                Path.GetRandomFileName().Replace(".", "")));
            using(FileStream openStream = File.OpenRead(newStateFile))
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
        finally
        {
            setStateSemaphore.Release();
        }
    }
}
