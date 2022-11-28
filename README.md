An example Java/Maven project showcasing how to use use OpenTelemetry including Trace Context Propagation when producing messages for an AWS SQS Queue.
Importing the Otel spans in Dynatrace
Make sure you have within Settings > Preferences > OneAgent features the Sensor OpenTelemetry (Java) enabled
You also need to make sure that you have under Settings > Server-side service monitoring an entry that enforces Span context propagation for Span of kind Producer. Without it OneAgent will not lift a finger when it comes to injectingtraceparent and tracestate
