#r "Microsoft.ServiceBus"
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.DataContracts;
using Microsoft.ServiceBus.Messaging;
using Newtonsoft.Json;
using System;

public class AI
{
    TelemetryClient telemetry = new TelemetryClient();
    const string DefaultRoleInstance = "default";
    const string DefaultIoTHubRoleName = "IoT Hub";
    const string DefaultDeviceRoleName = "Devices";

    public AI()
    {
        telemetry = new TelemetryClient();
        telemetry.InstrumentationKey = Environment.GetEnvironmentVariable("E2E_DIAGNOSTICS_AI_INSTRUMENTATION_KEY", EnvironmentVariableTarget.Process);
    }

    public void SendD2CLog(string deviceId, int d2cLatency, string id)
    {
        var dependencyTelemety = new DependencyTelemetry
        {
            Id = id,
            Target = DefaultIoTHubRoleName,
            Duration = new TimeSpan(0, 0, 0, 0, d2cLatency)
        };
        telemetry.Context.Cloud.RoleName = DefaultDeviceRoleName;
        telemetry.Context.Cloud.RoleInstance = deviceId;

        telemetry.TrackDependency(dependencyTelemety);
        telemetry.Flush();
    }

    public void SendIngressLog(int ingressLatency, string id, string parentId)
    {
        var requestTelemetry = new RequestTelemetry
        {
            Id = id,
            Duration = new TimeSpan(0, 0, 0, 0, ingressLatency)
        };
        telemetry.Context.Cloud.RoleName = DefaultIoTHubRoleName;
        telemetry.Context.Cloud.RoleInstance = DefaultRoleInstance;
        telemetry.Context.Operation.ParentId = parentId;
        telemetry.TrackRequest(requestTelemetry);
        telemetry.Flush();
    }

    public void SendEgressLog(string endpointName, int egressLatency)
    {
        var dependencyId = Guid.NewGuid().ToString();
        var requestId = Guid.NewGuid().ToString();

        var dependencyTelemety = new DependencyTelemetry
        {
            Id = dependencyId,
            Duration = new TimeSpan(0, 0, 0, 0, egressLatency),
            Target = endpointName
        };
        telemetry.Context.Cloud.RoleName = DefaultIoTHubRoleName;
        telemetry.Context.Cloud.RoleInstance = DefaultRoleInstance;
        telemetry.TrackDependency(dependencyTelemety);
        telemetry.Flush();

        var requestTelemetry = new RequestTelemetry
        {
            Id = requestId
        };
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
    EventHubMessage ehm = JsonConvert.DeserializeObject<EventHubMessage>(messageBody);
    var ai = new AI();

    foreach (Record record in ehm.records)
    {
        log.Info($"Get Record: {record.operationName}");
        if (record.operationName == "DiagnosticIoTHubD2C")
        {
            var properties = JsonConvert.DeserializeObject<D2CProperties>(record.properties);
            var deviceId = properties.deviceId;
            var callerLocalTimeUtc = DateTimeToMilliseconds(DateTimeOffset.Parse(properties.callerLocalTimeUtc).UtcDateTime);
            var calleeLocalTimeUtc = DateTimeToMilliseconds(DateTimeOffset.Parse(properties.calleeLocalTimeUtc).UtcDateTime);
            var d2cLatency = (int)(calleeLocalTimeUtc - callerLocalTimeUtc);

            var spanId = ParseSpanId(record.correlationId);
            ai.SendD2CLog(deviceId, d2cLatency, spanId);
        }
        else if (record.operationName == "DiagnosticIoTHubIngress")
        {
            var properties = JsonConvert.DeserializeObject<IngressProperties>(record.properties);
            var spanId = ParseSpanId(record.correlationId);
            ai.SendIngressLog(Convert.ToInt32(record.durationMs), spanId, properties.parentSpanId);
        }
        else if (record.operationName == "DiagnosticIoTHubEgress")
        {
            var properties = JsonConvert.DeserializeObject<EgressProperties>(record.properties);
            ai.SendEgressLog(properties.endpointName, Convert.ToInt32(record.durationMs));
        }
    } 
}