#r "Microsoft.ServiceBus"
using System;
using System.Net;
using System.Reflection;
using System.Text;
using System.Linq;
using Microsoft.ServiceBus.Common;
using Microsoft.ServiceBus.Messaging;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.DataContracts;
using Microsoft.ApplicationInsights.Extensibility;
using Newtonsoft.Json;

class Record {
    public string time;
    public string resourceId;
    public string operationName;
    public string durationMs;
    public string correlationId;
    public string properties;
}

class EventHubMessage {
    public Record[] records;
}

public static void Run(EventData myEventHubMessage, TraceWriter log)
{
    TelemetryClient telemetry = new TelemetryClient();
    telemetry.InstrumentationKey = System.Environment.GetEnvironmentVariable("E2E_DIAGNOSTICS_AI_INSTRUMENTATION_KEY", EnvironmentVariableTarget.Process);
    string messageBody = System.Text.Encoding.UTF8.GetString(myEventHubMessage.GetBytes());
    EventHubMessage ehm = JsonConvert.DeserializeObject<EventHubMessage>(messageBody);
    
    string[] OPERATIONS = {
        "DiagnosticIoTHubIngress",
        "DiagnosticIoTHubD2C",
        "DiagnosticIoTHubEgress",
        "DiagnosticIoTHubRouting",
        "deviceDisconnect",
        "deviceConnect"
    };
    foreach(Record record in ehm.records) {
        if(!OPERATIONS.Contains(record.operationName)) {
            continue;
        }
        var properties = new Dictionary<string, string>()
        {
            {"time", record.time},
            {"resourceId", record.resourceId},
            {"operationName", record.operationName},
            {"durationMs", record.durationMs},
            {"correlationId", record.correlationId},
            {"properties", record.properties}
        };
        telemetry.TrackEvent("E2EDiagnostics", properties);
    }
}
