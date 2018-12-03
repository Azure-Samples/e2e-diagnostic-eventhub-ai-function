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
            Name = "D2C Latency"
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

        if (!DateTimeOffset.TryParse(time, out DateTimeOffset timestamp))
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
            Id = reqeustId,
            Name = "Endpoint Latency"
        };

        requestTelemetry.Timestamp = timestamp;

        telemetry.Context.Cloud.RoleName = endpointName;
        telemetry.Context.Cloud.RoleInstance = DefaultRoleInstance;
        telemetry.Context.Operation.ParentId = dependencyId;
        telemetry.TrackRequest(requestTelemetry);
        telemetry.Flush();
    }

    public static void SendThirdPartyD2CLog(string endpointName, string thirdPartyServiceName, int endpointToThirdParyLatency, string correlationId, string time, ThirdPartyD2CProperties properties, bool hasError = false)
    {
        var dependencyTelemetry = new DependencyTelemetry
        {
            Id = correlationId,
            Target = thirdPartyServiceName,
            Duration = new TimeSpan(0, 0, 0, 0, endpointToThirdParyLatency),
            Success = !hasError,
            Name = "Third Party Service Latency"
        };

        dependencyTelemetry.Properties["calleeLocalTimeUtc"] = properties.calleeLocalTimeUtc;
        dependencyTelemetry.Properties["callerLocalTimeUtc"] = properties.callerLocalTimeUtc;
        dependencyTelemetry.Properties["thirdPartyServiceName"] = properties.thirdPartyServiceName;

        if (!DateTimeOffset.TryParse(time, out DateTimeOffset timestamp))
        {
            timestamp = DateTimeOffset.Now;
            dependencyTelemetry.Timestamp = timestamp;
            dependencyTelemetry.Properties["originalTimestamp"] = time;
        }
        else
        {
            dependencyTelemetry.Timestamp = timestamp;
        }

        telemetry.Context.Cloud.RoleName = endpointName;
        telemetry.Context.Cloud.RoleInstance = DefaultRoleInstance;
        telemetry.TrackDependency(dependencyTelemetry);
        telemetry.Flush();
    }

    public static void SendThirdPartyIngressLog(string thirdPartyServiceName, string parentId, int processLatency, string correlationId, string time, ThirdPartyIngressProperties properties, bool hasError = false)
    {
        var requestTelemetry = new RequestTelemetry
        {
            Id = correlationId,
            Duration = new TimeSpan(0, 0, 0, 0, processLatency),
            Success = !hasError,
            Timestamp = DateTimeOffset.Parse(time),
            Name = "Third Party Service Ingress Latency"
        };

        requestTelemetry.Properties["parentSpanId"] = properties.parentSpanId;
        requestTelemetry.Properties["thirdPartyServiceName"] = properties.thirdPartyServiceName;

        if (!DateTimeOffset.TryParse(time, out DateTimeOffset timestamp))
        {
            timestamp = DateTimeOffset.Now;
            requestTelemetry.Timestamp = timestamp;
            requestTelemetry.Properties["originalTimestamp"] = time;
        }
        else
        {
            requestTelemetry.Timestamp = timestamp;
        }

        telemetry.Context.Cloud.RoleName = thirdPartyServiceName;
        telemetry.Context.Cloud.RoleInstance = DefaultRoleInstance;
        telemetry.Context.Operation.ParentId = parentId;
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

public class D2CProperties
{
    public string messageSize;
    public string deviceId;
    public string callerLocalTimeUtc;
    public string calleeLocalTimeUtc;
}

public class IngressProperties
{
    public string isRoutingEnabled;
    public string parentSpanId;
}

public class EgressProperties
{
    public string endpointType;
    public string endpointName;
    public string parentSpanId;
}

public class ThirdPartyD2CProperties
{
    public string thirdPartyServiceName;
    public string callerLocalTimeUtc;
    public string calleeLocalTimeUtc;
}

public class ThirdPartyIngressProperties
{
    public string thirdPartyServiceName;
    public string parentSpanId;
}

class EventHubMessage
{
    public Record[] records;
}

class ByteArrayToHexStringConverter
{
    static readonly uint[] lookup32 = CreateLookup32();
    static uint[] CreateLookup32()
    {
        var result = new uint[256];
        for (int i = 0; i < 256; i++)
        {
            string s = i.ToString("X2");
            result[i] = ((uint)s[0]) + ((uint)s[1] << 16);
        }
        return result;
    }
    internal static string Convert(byte[] bytes)
    {
        var result = new char[bytes.Length * 2];
        for (int i = 0; i < bytes.Length; i++)
        {
            var val = lookup32[bytes[i]];
            result[2 * i] = (char)val;
            result[2 * i + 1] = (char)(val >> 16);
        }
        return new string(result);
    }
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

static Random random = new Random();
static string[] endpointNames = new string[] { "myEndpoint1", "myEndpoint2", "myEndpoint3" };
static string[] deviceNames = new string[] { "myDevice1", "myDevice2", "myDevice3" };
static string[] serviceNames = new string[] { "thirdPartyService1", "thirdPartyService2", "thirdPartyService3" };
static object lockObj = new object();

static string GenerateTraceId(string prefix, string spanId = null)
{
    if (spanId == null)
    {
        var bytes = new byte[8];
        random.NextBytes(bytes);
        spanId = ByteArrayToHexStringConverter.Convert(bytes);
    }

    return $"00-{prefix}-{spanId}-01";
}

static void SendRandomLogs(string deviceId)
{
    var prefixBytes = new byte[16];
    random.NextBytes(prefixBytes);
    string prefix = ByteArrayToHexStringConverter.Convert(prefixBytes);

    var bytes = new byte[8];
    random.NextBytes(bytes);
    var d2CSpanId = ByteArrayToHexStringConverter.Convert(bytes);
    var d2CCorrelationId = GenerateTraceId(prefix, d2CSpanId);

    random.NextBytes(bytes);
    var ingressSpanId = ByteArrayToHexStringConverter.Convert(bytes);
    var ingressCorrelationId = GenerateTraceId(prefix, ingressSpanId);

    //d2c log
    var randD2CLatency = random.Next(1, 1000);
    var D2CLogTimeStamp = (DateTime.Now - TimeSpan.FromMilliseconds(random.Next(1000))).ToUniversalTime().ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'fff'Z'");

    var randD2CProperties = new D2CProperties
    {
        callerLocalTimeUtc = (DateTime.Now).ToUniversalTime().ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'fff'Z'"),
        calleeLocalTimeUtc = (DateTime.Now + TimeSpan.FromMilliseconds(random.Next(1000))).ToUniversalTime().ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'fff'Z'"),
        deviceId = deviceId,
        messageSize = random.Next(1000).ToString()
    };

    AI.SendD2CLog(deviceId, randD2CLatency, D2CLogTimeStamp, d2CCorrelationId, randD2CProperties, random.Next(1000) == 100);

    //ingress log
    var randIngressLatency = random.Next(1, 1000);
    var IngressLogTimeStamp = DateTime.Now.ToUniversalTime().ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'fff'Z'");

    var isRoutingEnable = !(random.Next(1000) == 100);
    var randIngressProperties = new IngressProperties
    {
        parentSpanId = d2CSpanId,
        isRoutingEnabled = isRoutingEnable.ToString()
    };

    AI.SendIngressLog(randIngressLatency, d2CCorrelationId, IngressLogTimeStamp, ingressCorrelationId, randIngressProperties, random.Next(1000) == 100);

    if (!isRoutingEnable)
    {
        // Simulate routing disabled logs
        return;
    }

    //egress
    random.NextBytes(bytes);
    var egressSpanId = ByteArrayToHexStringConverter.Convert(bytes);
    var egressCorrelationId = GenerateTraceId(prefix, egressSpanId);

    var randPointName = endpointNames[random.Next(endpointNames.Length)];
    var randEgressLatency = random.Next(1, 1000);
    var EgressLogTimeStamp = DateTime.Now.ToUniversalTime().ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'fff'Z'");

    var randEgressProperties = new EgressProperties();
    randEgressProperties.endpointName = randPointName;
    randEgressProperties.parentSpanId = ingressSpanId;
    randEgressProperties.endpointType = "Event Hub";

    AI.SendEgressLog(randPointName, randEgressLatency, EgressLogTimeStamp, egressCorrelationId, randEgressProperties, random.Next(1000) == 100);

    if (random.Next(1000) == 100)
    {
        // Simulate lost logs
        return;
    }

    //third party
    random.NextBytes(bytes);
    var thirdPartyServiceD2CSpanId = ByteArrayToHexStringConverter.Convert(bytes);
    var thirdPartyServiceD2CCorrelationId = GenerateTraceId(prefix, thirdPartyServiceD2CSpanId);

    var randIndex = random.Next(deviceNames.Length);
    var randEndpoint = endpointNames[randIndex];
    var randService = serviceNames[randIndex];

    var randThirdPartyD2CLatency = random.Next(1, 1000);
    var thirdPartyD2CTimeStamp = DateTime.Now.ToUniversalTime().ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'fff'Z'");

    var ranThirdPartyProperties = new ThirdPartyD2CProperties
    {
        callerLocalTimeUtc = (DateTime.Now).ToUniversalTime().ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'fff'Z'"),
        calleeLocalTimeUtc = (DateTime.Now + TimeSpan.FromMilliseconds(random.Next(1000))).ToUniversalTime().ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'fff'Z'"),
        thirdPartyServiceName = randService
    };

    AI.SendThirdPartyD2CLog(randEndpoint, randService, randThirdPartyD2CLatency, thirdPartyServiceD2CCorrelationId, thirdPartyD2CTimeStamp, ranThirdPartyProperties, random.Next(1000) == 100);

    random.NextBytes(bytes);
    var thirdPartyServiceIngressSpanId = ByteArrayToHexStringConverter.Convert(bytes);
    var thirdPartyServiceIngressCorrelationId = GenerateTraceId(prefix, thirdPartyServiceIngressSpanId);
    var thirdPartyServiceIngressLatency = random.Next(1, 1000);
    var thirdPartyServiceIngressTimeStamp = DateTime.Now.ToUniversalTime().ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'fff'Z'");

    var randThirdPartyIngressProperties = new ThirdPartyIngressProperties
    {
        parentSpanId = d2CSpanId,
        thirdPartyServiceName = randService
    };

    AI.SendThirdPartyIngressLog(randService, thirdPartyServiceD2CCorrelationId, thirdPartyServiceIngressLatency, thirdPartyServiceIngressCorrelationId, thirdPartyServiceIngressTimeStamp, randThirdPartyIngressProperties, random.Next(1000) == 100);
}

public static void Run(EventData myEventHubMessage, TraceWriter log)
{
    log.Info($"C# Event Hub trigger function processed a message: {myEventHubMessage}");
    if(myEventHubMessage.Properties.Keys.Contains("$.tracestate"))
    {
        log.Info($"Receive message with diagnostic header.");
        var deviceId = myEventHubMessage.SystemProperties["iothub-connection-device-id"].ToString();
        SendRandomLogs(deviceId);
    }
    else
    {
        log.Info($"Receive message without diagnostic header.");
    }
}