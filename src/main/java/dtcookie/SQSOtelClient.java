package dtcookie;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.context.propagation.TextMapSetter;
import io.opentelemetry.exporters.logging.LoggingSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

public final class SQSOtelClient extends Thread implements HttpHandler, TextMapSetter<Map<String, MessageAttributeValue>> {
	
	private static final int LISTEN_PORT = 58080;
	private static final String QUEUE_URL = System.getenv("QUEUE_URL");
	
	@Override
	public void set(Map<String, MessageAttributeValue> carrier, String key, String value) {
		carrier.put(key, MessageAttributeValue.builder().dataType("String").stringValue(value).build());
	}

	private void sendMessage(String body) {
		Tracer tracer = GlobalOpenTelemetry.getTracerProvider().tracerBuilder("dtcookie-sqs-opentel-client").build();
		Span span = tracer.spanBuilder(body).setSpanKind(SpanKind.PRODUCER).setParent(Context.current()).startSpan();
		
		try (Scope scope = span.makeCurrent()) {			
			SqsClient sqsClient = SqsClient.builder().region(Region.US_EAST_1).build();
            Map<String, MessageAttributeValue> attributes = new HashMap<>();
            GlobalOpenTelemetry.getPropagators().getTextMapPropagator().inject(Context.current(), attributes, this);
            
        	SendMessageRequest sendRequest = SendMessageRequest.builder()
        			.queueUrl(QUEUE_URL)
        			.messageAttributes(attributes)
        			.messageBody(body)	        			
        			.build();
        	
        	sqsClient.sendMessage(sendRequest);        	
		} finally {
			span.end();
		}		
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	

	@Override
	public void handle(HttpExchange exchange) throws IOException {
		try (Scope scope = Context.current().makeCurrent()) {
			byte[] response = null;
			try {
				String messageBody = UUID.randomUUID().toString();
				sendMessage(messageBody);
				response = messageBody.getBytes(StandardCharsets.UTF_8);
			} catch (Throwable t) {
				response = stackTraceToString(t).getBytes(StandardCharsets.UTF_8);
			}
		
			if (response != null) {
				try (OutputStream out = exchange.getResponseBody()) {
					exchange.sendResponseHeaders(200, response.length);
					out.write(response);
				}
			} else {
				exchange.sendResponseHeaders(300, 0);
			}			
		}
	}

	public static void main(String[] args) throws Exception {
		if ((QUEUE_URL == null) || QUEUE_URL.isEmpty()) {
			System.out.println("Environment Variable QUEUE_URL not found!");
			System.exit(0);
		}
		new SQSOtelClient().launch();
	}
	
	private final HttpServer server;
	
    public SQSOtelClient() throws IOException {
    	Runtime.getRuntime().addShutdownHook(this);
		this.server = HttpServer.create(new InetSocketAddress(LISTEN_PORT), 0);
		this.server.createContext("/send", this);
		this.server.setExecutor(null); // creates a default executor
    }
	
    private void launch() {
    	this.server.start();    	
    }
    
    @Override
    public void run() {
    	if (this.server != null) {
    		try {
    			this.server.stop(0);	
    		} catch (Throwable t) {
    			// ignore
    		}
    		
    	}
    }
    
    private static String stackTraceToString(Throwable t) {
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		t.printStackTrace(pw);
		pw.flush();
		sw.flush();
		return sw.getBuffer().toString();

    }
	

}
