// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System.Text.Json;
using MockDataPlaneServer;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddSingleton<EventBuffer>();
builder.Services.AddCors();
var app = builder.Build();

app.UseCors(x => x.AllowAnyOrigin().AllowAnyMethod().AllowAnyHeader());
app.UseDefaultFiles();
app.UseStaticFiles();

// ─── POST /ingest ──────────────────────────────────────────────────────────────
// Receives webhook events from BPM (notificationDetails callbackUrl).
// Body: raw connector payload JSON (same shape BPM sends today).
// Query: ?namespace={connectorNamespace}&triggerName={triggerName}
app.MapPost("/ingest", async (
    HttpRequest request,
    string @namespace,
    string triggerName,
    EventBuffer buffer) =>
{
    if (string.IsNullOrWhiteSpace(@namespace) || string.IsNullOrWhiteSpace(triggerName))
        return Results.BadRequest("namespace and triggerName query params are required.");

    using var reader = new StreamReader(request.Body);
    var body = await reader.ReadToEndAsync();

    if (string.IsNullOrWhiteSpace(body))
        return Results.BadRequest("Request body is empty.");

    var eventId = buffer.Enqueue(@namespace, triggerName, body);

    app.Logger.LogInformation(
        "[ingest] ns={Namespace} trigger={Trigger} eventId={EventId} pending={Pending}",
        @namespace, triggerName, eventId, buffer.PendingCount(@namespace, triggerName));

    // Return 202 Accepted — same as what the real extension returns to BPM today.
    // BPM only checks for a 2xx; it does not read the response body.
    return Results.Accepted();
});

// ─── GET /metrics ──────────────────────────────────────────────────────────────
// Called by ConnectorMetricsProvider (Scale Monitor) every 10 seconds.
// Returns pendingEventCount matching the Phase 2 API contract.
app.MapGet("/metrics", (
    HttpRequest request,
    string @namespace,
    string triggerName,
    EventBuffer buffer) =>
{
    if (string.IsNullOrWhiteSpace(@namespace) || string.IsNullOrWhiteSpace(triggerName))
        return Results.BadRequest("namespace and triggerName query params are required.");

    var count = buffer.PendingCount(@namespace, triggerName);

    // Only log as SM call when request comes from the SM (not from the dashboard UI).
    var src = request.Query["_src"].ToString();
    if (src != "ui")
    {
        buffer.LogSmCall(@namespace, triggerName, count);
    }

    app.Logger.LogInformation(
        "[metrics] ns={Namespace} trigger={Trigger} pendingEventCount={Count} src={Src}",
        @namespace, triggerName, count, string.IsNullOrEmpty(src) ? "SM" : src);

    return Results.Ok(new
    {
        pendingEventCount = count,
        oldestEventAgeSeconds = 0,     // simplified for POC
        lastEnqueuedTimeUtc = (DateTime?)null,
        lastDequeuedTimeUtc = (DateTime?)null
    });
});

// ─── GET /events ───────────────────────────────────────────────────────────────
// Called by Function Host to consume events (peek-ack semantics).
// Returns events matching the Phase 2 API contract envelope shape.
app.MapGet("/events", (
    string @namespace,
    string triggerName,
    int maxMessages = 10,
    EventBuffer buffer = null!) =>
{
    if (string.IsNullOrWhiteSpace(@namespace) || string.IsNullOrWhiteSpace(triggerName))
        return Results.BadRequest("namespace and triggerName query params are required.");

    var events = buffer.Peek(@namespace, triggerName, maxMessages);

    app.Logger.LogInformation(
        "[events] ns={Namespace} trigger={Trigger} returned={Count}",
        @namespace, triggerName, events.Count);

    var envelope = events.Select(e => new
    {
        eventId = e.EventId,
        ackToken = e.AckToken,
        deliveryCount = e.DeliveryCount,
        enqueuedTimeUtc = e.EnqueuedTimeUtc,
        ackDeadlineUtc = e.AckDeadlineUtc,
        data = JsonSerializer.Deserialize<object>(e.PayloadJson)
    });

    return Results.Ok(new { value = envelope });
});

// ─── POST /events/acknowledge ─────────────────────────────────────────────────
// Called by Function Host after processing events.
// Body: { "events": [{ "eventId": "...", "ackToken": "..." }] }
app.MapPost("/events/acknowledge", async (
    HttpRequest request,
    string @namespace,
    string triggerName,
    EventBuffer buffer) =>
{
    if (string.IsNullOrWhiteSpace(@namespace) || string.IsNullOrWhiteSpace(triggerName))
        return Results.BadRequest("namespace and triggerName query params are required.");

    using var reader = new StreamReader(request.Body);
    var body = await reader.ReadToEndAsync();
    var doc = JsonSerializer.Deserialize<JsonElement>(body);

    var requests = doc.GetProperty("events")
        .EnumerateArray()
        .Select(e => new AcknowledgeRequest(
            e.GetProperty("eventId").GetString()!,
            e.GetProperty("ackToken").GetString()!))
        .ToList();

    var result = buffer.Acknowledge(@namespace, triggerName, requests);

    app.Logger.LogInformation(
        "[acknowledge] ns={Namespace} trigger={Trigger} acked={Acked} notFound={NotFound}",
        @namespace, triggerName, result.Acknowledged, result.Failed);

    return Results.Ok(result);
});

// ─── GET /status ───────────────────────────────────────────────────────────────
// Debug endpoint — shows buffer sizes and recent SM call log.
app.MapGet("/status", (EventBuffer buffer) =>
    Results.Ok(new
    {
        message = "MockDataPlaneServer running",
        timestamp = DateTime.UtcNow,
        smCalls = buffer.GetSmLog()
    }));

app.Logger.LogInformation("MockDataPlaneServer starting on http://localhost:5100");
app.Run("http://localhost:5100");
