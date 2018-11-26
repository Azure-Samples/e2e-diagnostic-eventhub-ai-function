#r "Microsoft.ServiceBus"
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.DataContracts;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.ServiceBus.Messaging;
using Newtonsoft.Json;
using System;

public class AI
{
    const string DefaultRoleInstance = "default";
    const string DefaultIoTHubRoleName = "IoT Hub";
    const string DefaultDeviceRoleName = "Devices";
    static TelemetryConfiguration configuration = new TelemetryConfiguration(Environment.GetEnvironmentVariable("E2E_DIAGNOSTICS_AI_INSTRUMENTATION_KEY", EnvironmentVariableTarget.Process));
    static TelemetryClient telemetry = new TelemetryClient(configuration);

    public static void SendD2CLog(string deviceId, int d2cLatency, string id, string time, bool hasError = false)
    {
        var dependencyTelemetry = new DependencyTelemetry
        {
            Id = id,
            Target = DefaultIoTHubRoleName,
            Duration = new TimeSpan(0, 0, 0, 0, d2cLatency),
            Success = !hasError,
        };

        if (!DateTimeOffset.TryParse(time, out var timestamp))
        {
            timestamp = DateTimeOffset.Now;
            dependencyTelemetry.Timestamp = timestamp;
            dependencyTelemetry.Properties["originalTimestamp"] = time;
        }
        else
        {
            dependencyTelemetry.Timestamp = timestamp;
        }

        telemetry.Context.Cloud.RoleName = DefaultDeviceRoleName;
        telemetry.Context.Cloud.RoleInstance = deviceId;

        telemetry.TrackDependency(dependencyTelemetry);
        telemetry.Flush();
    }

    public static void SendIngressLog(int ingressLatency, string id, string parentId, string time, bool hasError = false)
    {
        var requestTelemetry = new RequestTelemetry
        {
            Id = id,
            Duration = new TimeSpan(0, 0, 0, 0, ingressLatency),
            Success = !hasError,
        };

        if (!DateTimeOffset.TryParse(time, out var timestamp))
        {
            timestamp = DateTimeOffset.Now;
            requestTelemetry.Timestamp = timestamp;
            requestTelemetry.Properties["originalTimestamp"] = time;
        }
        else
        {
            requestTelemetry.Timestamp = timestamp;
        }

        telemetry.Context.Cloud.RoleName = DefaultIoTHubRoleName;
        telemetry.Context.Cloud.RoleInstance = DefaultRoleInstance;
        telemetry.Context.Operation.ParentId = parentId;
        telemetry.TrackRequest(requestTelemetry);
        telemetry.Flush();
    }

    public static void SendEgressLog(string endpointName, int egressLatency, string time, bool hasError = false)
    {
        var dependencyId = Guid.NewGuid().ToString();
        var requestId = Guid.NewGuid().ToString();

        var dependencyTelemetry = new DependencyTelemetry
        {
            Id = dependencyId,
            Duration = new TimeSpan(0, 0, 0, 0, egressLatency),
            Target = endpointName,
            Success = !hasError,
        };

        DateTimeOffset timestamp;
        if (!DateTimeOffset.TryParse(time, out timestamp))
        {
            timestamp = DateTimeOffset.Now;
            dependencyTelemetry.Timestamp = timestamp;
            dependencyTelemetry.Properties["originalTimestamp"] = time;
        }
        else
        {
            dependencyTelemetry.Timestamp = timestamp;
        }

        telemetry.Context.Cloud.RoleName = DefaultIoTHubRoleName;
        telemetry.Context.Cloud.RoleInstance = DefaultRoleInstance;
        telemetry.TrackDependency(dependencyTelemetry);
        telemetry.Flush();

        var requestTelemetry = new RequestTelemetry
        {
            Id = requestId
        };

        requestTelemetry.Timestamp = timestamp;

        telemetry.Context.Cloud.RoleName = endpointName;
        telemetry.Context.Cloud.RoleInstance = DefaultRoleInstance;
        telemetry.Context.Operation.ParentId = dependencyId;
        telemetry.TrackRequest(requestTelemetry);
        telemetry.Flush();
    }
}

class Record
{
    public string time;
    public string resourceId;
    public string operationName;
    public string durationMs;
    public string correlationId;
    public string properties;
    public string level;
}

class D2CProperties
{
    public string messageSize;
    public string deviceId;
    public string callerLocalTimeUtc;
    public string calleeLocalTimeUtc;
}

class IngressProperties
{
    public string isRoutingEnabled;
    public string parentSpanId;
}

class EgressProperties
{
    public string endpointType;
    public string endpointName;
    public string parentSpanId;
}

class EventHubMessage
{
    public Record[] records;
}

static string ParseSpanId(string correlationId)
{
    return correlationId.Split('-')[2];
}

static long DateTimeToMilliseconds(DateTime time)
{
    return (long)(time - new DateTime(1970, 1, 1)).TotalMilliseconds;
}

public static void Run(EventData myEventHubMessage, TraceWriter log)
{
    string messageBody = System.Text.Encoding.UTF8.GetString(myEventHubMessage.GetBytes());
    EventHubMessage ehm = null;
    try
    {
        ehm = JsonConvert.DeserializeObject<EventHubMessage>(messageBody);
    }
    catch (JsonSerializationException e)
    {
        log.Error($"Cannot parse Event Hub messages: {e.Message}");
    }
    catch (Exception e)
    {
        log.Error($"Unknown error when parse Event Hub messages: {e.Message}");
    }

    if (ehm == null)
    {
        return;
    }

    foreach (Record record in ehm.records)
    {
        log.Info($"Get Record: {record.operationName}");
        var hasError = record.level == "Error";
        if (record.operationName == "DiagnosticIoTHubD2C")
        {
            try
            {
                var properties = JsonConvert.DeserializeObject<D2CProperties>(record.properties);
                if (properties != null)
                {
                    var deviceId = properties.deviceId;
                    var callerLocalTimeUtc = DateTimeToMilliseconds(DateTimeOffset.Parse(properties.callerLocalTimeUtc).UtcDateTime);
                    var calleeLocalTimeUtc = DateTimeToMilliseconds(DateTimeOffset.Parse(properties.calleeLocalTimeUtc).UtcDateTime);
                    var d2cLatency = (int)(calleeLocalTimeUtc - callerLocalTimeUtc);

                    var spanId = ParseSpanId(record.correlationId);
                    AI.SendD2CLog(deviceId, d2cLatency, spanId, record.time, hasError);
                }
                else
                {
                    log.Error($"D2C log properties is null: {record.properties}");
                }
            }
            catch (JsonSerializationException e)
            {
                log.Error($"Cannot parse D2C log properties: {e.Message}");
            }
            catch (Exception e)
            {
                log.Error($"Send D2C log to AI failed: {e.Message}");
            }
        }
        else if (record.operationName == "DiagnosticIoTHubIngress")
        {
            try
            {
                var properties = JsonConvert.DeserializeObject<IngressProperties>(record.properties);
                if (properties != null)
                {
                    var spanId = ParseSpanId(record.correlationId);
                    AI.SendIngressLog(Convert.ToInt32(record.durationMs), spanId, properties.parentSpanId, record.time, hasError);
                }
                else
                {
                    log.Error($"Ingress log properties is null: {record.properties}");
                }
            }
            catch (JsonSerializationException e)
            {
                log.Error($"Cannot parse Ingress log properties: {e.Message}");
            }
            catch (Exception e)
            {
                log.Error($"Send Ingress log to AI failed: {e.Message}");
            }
        }
        else if (record.operationName == "DiagnosticIoTHubEgress")
        {
            try
            {
                var properties = JsonConvert.DeserializeObject<EgressProperties>(record.properties);
                if (properties != null)
                {
                    AI.SendEgressLog(properties.endpointName, Convert.ToInt32(record.durationMs), record.time, hasError);
                }
                else
                {
                    log.Error($"Egress log properties is null: {record.properties}");
                }

            }
            catch (JsonSerializationException e)
            {
                log.Error($"Cannot parse Egress log properties: {e.Message}");
            }
            catch (Exception e)
            {
                log.Error($"Send Egress log to AI failed: {e.Message}");
            }
        }
    }
}