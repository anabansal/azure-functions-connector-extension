// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

namespace MockDataPlaneServer;

/// <summary>
/// In-memory event buffer keyed by (connectorNamespace, triggerName).
/// Simulates the system-managed queue that BPM would maintain in Phase 2.
/// </summary>
public sealed class EventBuffer
{
    private readonly Dictionary<string, Queue<EventEntry>> _buffers = new();
    private readonly object _lock = new();

    /// <summary>
    /// Enqueues an event for the given namespace+trigger.
    /// Called by the webhook ingest endpoint (POST /ingest).
    /// </summary>
    public string Enqueue(string connectorNamespace, string triggerName, string payloadJson)
    {
        var key = MakeKey(connectorNamespace, triggerName);
        var eventId = Guid.NewGuid().ToString();

        var entry = new EventEntry
        {
            EventId = eventId,
            AckToken = $"ack-{eventId[..8]}",
            EnqueuedTimeUtc = DateTime.UtcNow,
            AckDeadlineUtc = DateTime.UtcNow.AddMinutes(5),
            DeliveryCount = 1,
            PayloadJson = payloadJson
        };

        lock (_lock)
        {
            if (!_buffers.TryGetValue(key, out var queue))
            {
                queue = new Queue<EventEntry>();
                _buffers[key] = queue;
            }
            queue.Enqueue(entry);
        }

        return eventId;
    }

    /// <summary>
    /// Returns up to maxMessages events (peek — they stay in the buffer until acknowledged).
    /// </summary>
    public List<EventEntry> Peek(string connectorNamespace, string triggerName, int maxMessages = 10)
    {
        var key = MakeKey(connectorNamespace, triggerName);
        lock (_lock)
        {
            if (!_buffers.TryGetValue(key, out var queue))
                return [];

            return queue.Take(maxMessages).ToList();
        }
    }

    /// <summary>
    /// Acknowledges (permanently removes) events by ackToken.
    /// </summary>
    public AcknowledgeResult Acknowledge(string connectorNamespace, string triggerName, List<AcknowledgeRequest> requests)
    {
        var key = MakeKey(connectorNamespace, triggerName);
        var results = new List<AcknowledgeItemResult>();

        lock (_lock)
        {
            if (!_buffers.TryGetValue(key, out var queue))
            {
                foreach (var r in requests)
                    results.Add(new AcknowledgeItemResult(r.EventId, r.AckToken, "NotFound"));
                return new AcknowledgeResult(0, 0, requests.Count, results);
            }

            var remaining = queue.ToList();
            int acknowledged = 0, notFound = 0;

            foreach (var r in requests)
            {
                var match = remaining.FirstOrDefault(e => e.AckToken == r.AckToken && e.EventId == r.EventId);
                if (match != null)
                {
                    remaining.Remove(match);
                    acknowledged++;
                    results.Add(new AcknowledgeItemResult(r.EventId, r.AckToken, "Acknowledged"));
                }
                else
                {
                    notFound++;
                    results.Add(new AcknowledgeItemResult(r.EventId, r.AckToken, "NotFound"));
                }
            }

            _buffers[key] = new Queue<EventEntry>(remaining);
            return new AcknowledgeResult(acknowledged, 0, notFound, results);
        }
    }

    // ── SM call log ──────────────────────────────────────────────────────────
    private readonly List<SmCallEntry> _smLog = new();

    public void LogSmCall(string connectorNamespace, string triggerName, int pendingCount)
    {
        lock (_lock)
        {
            _smLog.Insert(0, new SmCallEntry(DateTime.UtcNow, connectorNamespace, triggerName, pendingCount));
            if (_smLog.Count > 50) _smLog.RemoveAt(_smLog.Count - 1);
        }
    }

    public List<SmCallEntry> GetSmLog()
    {
        lock (_lock) { return _smLog.ToList(); }
    }

    /// <summary>
    /// Returns the current pending event count for the given namespace+trigger.
    /// </summary>
    public int PendingCount(string connectorNamespace, string triggerName)
    {
        var key = MakeKey(connectorNamespace, triggerName);
        lock (_lock)
        {
            return _buffers.TryGetValue(key, out var queue) ? queue.Count : 0;
        }
    }

    private static string MakeKey(string connectorNamespace, string triggerName)
        => $"{connectorNamespace}::{triggerName}";
}

public sealed record EventEntry
{
    public required string EventId { get; init; }
    public required string AckToken { get; init; }
    public required DateTime EnqueuedTimeUtc { get; init; }
    public required DateTime AckDeadlineUtc { get; init; }
    public required int DeliveryCount { get; init; }
    public required string PayloadJson { get; init; }
}

public sealed record AcknowledgeRequest(string EventId, string AckToken);
public sealed record AcknowledgeItemResult(string EventId, string AckToken, string Status);
public sealed record AcknowledgeResult(int Acknowledged, int Expired, int Failed, List<AcknowledgeItemResult> Results);
public sealed record SmCallEntry(DateTime TimestampUtc, string Namespace, string TriggerName, int PendingCount);
