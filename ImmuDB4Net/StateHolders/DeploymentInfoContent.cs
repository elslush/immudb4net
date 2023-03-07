using System;
using System.Text.Json.Serialization;
using MemoryPack;

namespace ImmuDB.StateHolders;

[MemoryPackable]
internal partial class DeploymentInfoContent
{
    [JsonPropertyName("label")]
    public string? Label { get; set; }
    [JsonPropertyName("serveruuid")]
    public string? ServerUUID { get; set; }
}

