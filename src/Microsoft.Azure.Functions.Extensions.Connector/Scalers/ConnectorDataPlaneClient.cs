// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System.Net.Http.Json;
using Microsoft.Extensions.Logging;

namespace Microsoft.Azure.Functions.Extensions.Connector;

/// <summary>
/// POC HTTP client for the data plane endpoints exposed by MockDataPlaneServer.
/// In production this will call the real BPM data plane hosted behind APIM.
/// Keyed by (connectorNamespace, triggerName) matching the Phase 2 API contract.
/// </summary>
internal static class ConnectorDataPlaneClient
{
    /// <summary>
    /// Env var: base URL of the data plane server.
    /// e.g. CONNECTOR_DATA_PLANE_ENDPOINT=http://localhost:5100
    /// When unset, ConnectorMetricsProvider falls back to the Phase 1 mock.
    /// </summary>
    internal const string EndpointEnvVar = "CONNECTOR_DATA_PLANE_ENDPOINT";

    private static readonly HttpClient Http = new();

    /// <summary>
    /// Calls GET {endpoint}/metrics?namespace={ns}&amp;triggerName={name}
    /// Returns the pendingEventCount from the data plane server.
    /// </summary>
    internal static async Task<ConnectorTriggerMetrics> GetMetricsAsync(
        string endpoint,
        string? connectorNamespace,
        string? triggerName,
        ILogger logger,
        CancellationToken cancellationToken = default)
    {
        var url = $"{endpoint.TrimEnd('/')}/metrics" +
                  $"?namespace={Uri.EscapeDataString(connectorNamespace ?? string.Empty)}" +
                  $"&triggerName={Uri.EscapeDataString(triggerName ?? string.Empty)}";

        try
        {
            var response = await Http.GetFromJsonAsync<MetricsResponse>(url, cancellationToken)
                           .ConfigureAwait(false);

            logger.LogDebug(
                "ConnectorDataPlaneClient GET /metrics ns={Namespace} trigger={Trigger} → pendingEventCount={Count}",
                connectorNamespace, triggerName, response?.PendingEventCount ?? 0);

            return new ConnectorTriggerMetrics
            {
                PendingEvents = response?.PendingEventCount ?? 0,
                SampledAtUtc = DateTime.UtcNow
            };
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex,
                "ConnectorDataPlaneClient failed to GET /metrics for ns={Namespace} trigger={Trigger}. Returning 0.",
                connectorNamespace, triggerName);

            return new ConnectorTriggerMetrics { PendingEvents = 0, SampledAtUtc = DateTime.UtcNow };
        }
    }

    private sealed class MetricsResponse
    {
        public int PendingEventCount { get; set; }
    }
}
