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

    public static void SendD2CLog(string deviceId, int d2cLatency, string time, string correlationId, D2CProperties properties, bool hasError = false)
    {
        var dependencyTelemetry = new DependencyTelemetry
        {
            Id = correlationId,
            Target = DefaultIoTHubRoleName,
            Duration = new TimeSpan(0, 0, 0, 0, d2cLatency),
            Success = !hasError,
            Name = "D2C Latency: " + deviceId
        };

        dependencyTelemetry.Properties["calleeLocalTimeUtc"] = properties.calleeLocalTimeUtc;
        dependencyTelemetry.Properties["callerLocalTimeUtc"] = properties.callerLocalTimeUtc;
        dependencyTelemetry.Properties["deviceId"] = properties.deviceId;
        dependencyTelemetry.Properties["messageSize"] = properties.messageSize;

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
        telemetry.Context.Operation.Id = correlationId;

        telemetry.TrackDependency(dependencyTelemetry);
        telemetry.Flush();
    }

    public static void SendIngressLog(int ingressLatency, string parentId, string time, string correlationId, IngressProperties properties, bool hasError = false)
    {
        var requestTelemetry = new RequestTelemetry
        {
            Id = correlationId,
            Duration = new TimeSpan(0, 0, 0, 0, ingressLatency),
            Success = !hasError,
            Name = "Ingress Latency"
        };

        requestTelemetry.Properties["isRoutingEnabled"] = properties.isRoutingEnabled;
        requestTelemetry.Properties["parentSpanId"] = properties.parentSpanId;

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
        telemetry.Context.Operation.Id = correlationId;

        telemetry.TrackRequest(requestTelemetry);
        telemetry.Flush();
    }

    public static void SendEgressLog(string endpointName, int egressLatency, string time, string correlationId, EgressProperties properties, bool hasError = false)
    {
        var dependencyId = correlationId;
        var reqeustId = Guid.NewGuid().ToString();
        var dependencyTelemetry = new DependencyTelemetry
        {
            Id = dependencyId,
            Duration = new TimeSpan(0, 0, 0, 0, egressLatency),
            Target = endpointName,
            Success = !hasError,
            Name = "Egress Latency"
        };

        dependencyTelemetry.Properties["parentSpanId"] = properties.parentSpanId;
        dependencyTelemetry.Properties["endpointName"] = properties.endpointName;
        dependencyTelemetry.Properties["endpointType"] = properties.endpointType;

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
        telemetry.Context.Operation.Id = correlationId;

        telemetry.TrackDependency(dependencyTelemetry);
        telemetry.Flush();


        var requestTelemetry = new RequestTelemetry
        {
            Id = reqeustId
        };

        requestTelemetry.Timestamp = timestamp;

        telemetry.Context.Cloud.RoleName = endpointName;
        telemetry.Context.Cloud.RoleInstance = DefaultRoleInstance;
        telemetry.Context.Operation.ParentId = dependencyId;
        //This telemtry is used to draw the application Map, so we do not save its correlation id

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

static string ParseParentId(string correlationId, string parentSpanId)
{
    var ids = correlationId.Split('-');
    ids[2] = parentSpanId;
    return string.Join("-", ids);
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

                    AI.SendD2CLog(deviceId, d2cLatency, record.time, record.correlationId, properties, hasError);
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
                    var parentId = ParseParentId(record.correlationId, properties.parentSpanId);
                    AI.SendIngressLog(Convert.ToInt32(record.durationMs), parentId, record.time, record.correlationId, properties, hasError);
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
                    AI.SendEgressLog(properties.endpointName, Convert.ToInt32(record.durationMs), record.time, record.correlationId, properties, hasError);
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