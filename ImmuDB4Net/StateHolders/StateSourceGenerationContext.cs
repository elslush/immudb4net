using System;
using ImmuDB;
using ImmuDB.StateHolders;
using System.Text.Json.Serialization;

namespace ImmuDB.StateHolders;

[JsonSerializable(typeof(ImmuState))]
[JsonSerializable(typeof(DeploymentInfoContent))]
internal partial class StateSourceGenerationContext : JsonSerializerContext
{
}

