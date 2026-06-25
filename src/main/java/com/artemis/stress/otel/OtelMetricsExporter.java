package com.artemis.stress.otel;

import com.artemis.stress.config.StressConfig;
import com.artemis.stress.metrics.MetricsCollector;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableDoubleMeasurement;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Exports stress-tool metrics in OpenTelemetry format via OTLP/gRPC.
 *
 * <h3>Architecture</h3>
 * <pre>
 *   StressTool ──► OtelMetricsExporter ──► OTLP/gRPC ──► OTel Collector
 *                                                              │
 *                                              ┌──────────────┼──────────────┐
 *                                           Prometheus    Grafana LGTM    Jaeger
 * </pre>
 *
 * <h3>Metrics published</h3>
 * All metrics are under the instrument scope {@code artemis.stress}.
 *
 * Producer metrics:
 * <ul>
 *   <li>{@code artemis.stress.producer.sent}         — total messages sent</li>
 *   <li>{@code artemis.stress.producer.errors}        — send errors</li>
 *   <li>{@code artemis.stress.producer.throughput}    — msg/s overall</li>
 *   <li>{@code artemis.stress.producer.bytes}         — total bytes sent (MB)</li>
 *   <li>{@code artemis.stress.producer.latency.mean}  — mean send latency (ms)</li>
 *   <li>{@code artemis.stress.producer.latency.p50}   — p50 send latency (ms)</li>
 *   <li>{@code artemis.stress.producer.latency.p90}   — p90 send latency (ms)</li>
 *   <li>{@code artemis.stress.producer.latency.p99}   — p99 send latency (ms)</li>
 *   <li>{@code artemis.stress.producer.latency.p999}  — p99.9 send latency (ms)</li>
 *   <li>{@code artemis.stress.producer.latency.max}   — max send latency (ms)</li>
 * </ul>
 *
 * Consumer metrics (only when mode is CONSUME or BOTH):
 * <ul>
 *   <li>{@code artemis.stress.consumer.received}      — total messages received</li>
 *   <li>{@code artemis.stress.consumer.errors}        — consumer errors / duplicates</li>
 *   <li>{@code artemis.stress.consumer.throughput}    — msg/s overall</li>
 *   <li>{@code artemis.stress.consumer.e2e.mean}      — mean E2E latency (ms)</li>
 *   <li>{@code artemis.stress.consumer.e2e.p50}       — p50 E2E latency (ms)</li>
 *   <li>{@code artemis.stress.consumer.e2e.p90}       — p90 E2E latency (ms)</li>
 *   <li>{@code artemis.stress.consumer.e2e.p99}       — p99 E2E latency (ms)</li>
 *   <li>{@code artemis.stress.consumer.e2e.p999}      — p99.9 E2E latency (ms)</li>
 *   <li>{@code artemis.stress.consumer.e2e.max}       — max E2E latency (ms)</li>
 * </ul>
 *
 * <h3>Attributes (labels)</h3>
 * Every metric carries:
 * <ul>
 *   <li>{@code service.name}  — configurable via {@code --otel-service-name}</li>
 *   <li>{@code broker.url}    — broker URL (passwords stripped)</li>
 *   <li>{@code destination}   — queue/topic name</li>
 *   <li>{@code mode}          — PRODUCE / CONSUME / BOTH</li>
 * </ul>
 */
public class OtelMetricsExporter implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(OtelMetricsExporter.class);

    private static final String SCOPE = "artemis.stress";

    private final MetricsCollector metrics;
    private final StressConfig     config;
    private final OpenTelemetrySdk sdk;
    private final AtomicBoolean    closed = new AtomicBoolean(false);
    private static final AttributeKey<String> SERVICE_NAME    = AttributeKey.stringKey("service.name");
    private static final AttributeKey<String> SERVICE_VERSION = AttributeKey.stringKey("service.version");
    /**
     * @param metrics          shared metrics collector
     * @param config           stress tool configuration
     * @param otlpEndpoint     OTLP/gRPC endpoint, e.g. {@code http://localhost:4317}
     * @param exportIntervalMs how often to push metrics (milliseconds)
     * @param serviceName      value of the {@code service.name} resource attribute
     */
    public OtelMetricsExporter(MetricsCollector metrics,
                               StressConfig config,
                               String otlpEndpoint,
                               long exportIntervalMs,
                               String serviceName) {
        this.metrics = metrics;
        this.config  = config;

        // ── Resource ──────────────────────────────────────────────────────────
        Resource resource = Resource.getDefault().merge(
                Resource.create(Attributes.of(
                        SERVICE_NAME,    serviceName,
                        SERVICE_VERSION, "1.2.0")));

        // ── OTLP exporter ─────────────────────────────────────────────────────
        OtlpGrpcMetricExporter otlpExporter = OtlpGrpcMetricExporter.builder()
                .setEndpoint(otlpEndpoint)
                .setTimeout(Duration.ofSeconds(10))
                .build();

        // ── Periodic reader ───────────────────────────────────────────────────
        PeriodicMetricReader reader = PeriodicMetricReader.builder(otlpExporter)
                .setInterval(Duration.ofMillis(exportIntervalMs))
                .build();

        // ── SDK ───────────────────────────────────────────────────────────────
        SdkMeterProvider meterProvider = SdkMeterProvider.builder()
                .setResource(resource)
                .registerMetricReader(reader)
                .build();

        this.sdk = OpenTelemetrySdk.builder()
                .setMeterProvider(meterProvider)
                .buildAndRegisterGlobal();

        // ── Register instruments ───────────────────────────────────────────────
        registerInstruments(sdk.getMeter(SCOPE));

        log.info("OpenTelemetry exporter started — endpoint={} interval={}ms service={}",
                otlpEndpoint, exportIntervalMs, serviceName);
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            // Force a final export before shutdown
            sdk.getSdkMeterProvider().forceFlush();
            sdk.close();
            log.info("OpenTelemetry exporter closed");
        }
    }

    // ─── Instrument registration ──────────────────────────────────────────────

    private void registerInstruments(Meter meter) {
        Attributes attrs = buildAttributes();
        boolean trackConsumer = config.getMode() != StressConfig.Mode.PRODUCE;

        // ── Producer instruments ───────────────────────────────────────────────

        meter.gaugeBuilder("artemis.stress.producer.sent")
                .setDescription("Total messages sent by producers")
                .setUnit("messages")
                .ofLongs()
                .buildWithCallback(m -> observe(m, metrics.getSendCount(), attrs));

        meter.gaugeBuilder("artemis.stress.producer.errors")
                .setDescription("Total producer send errors")
                .setUnit("errors")
                .ofLongs()
                .buildWithCallback(m -> observe(m, metrics.getSendErrors(), attrs));

        meter.gaugeBuilder("artemis.stress.producer.throughput")
                .setDescription("Overall producer throughput")
                .setUnit("msg/s")
                .buildWithCallback(m -> observe(m, metrics.overallSendRate(), attrs));

        meter.gaugeBuilder("artemis.stress.producer.bytes")
                .setDescription("Total data sent by producers")
                .setUnit("MB")
                .buildWithCallback(m -> observe(m,
                        metrics.getSendBytes() / (1024.0 * 1024.0), attrs));

        meter.gaugeBuilder("artemis.stress.producer.latency.mean")
                .setDescription("Mean producer send latency")
                .setUnit("ms")
                .buildWithCallback(m -> observe(m,
                        metrics.getSendLatMeanNs() / 1e6, attrs));

        meter.gaugeBuilder("artemis.stress.producer.latency.p50")
                .setDescription("p50 producer send latency")
                .setUnit("ms")
                .buildWithCallback(m -> observe(m,
                        metrics.getSendPercentileNs(0.50) / 1e6, attrs));

        meter.gaugeBuilder("artemis.stress.producer.latency.p90")
                .setDescription("p90 producer send latency")
                .setUnit("ms")
                .buildWithCallback(m -> observe(m,
                        metrics.getSendPercentileNs(0.90) / 1e6, attrs));

        meter.gaugeBuilder("artemis.stress.producer.latency.p99")
                .setDescription("p99 producer send latency")
                .setUnit("ms")
                .buildWithCallback(m -> observe(m,
                        metrics.getSendPercentileNs(0.99) / 1e6, attrs));

        meter.gaugeBuilder("artemis.stress.producer.latency.p999")
                .setDescription("p99.9 producer send latency")
                .setUnit("ms")
                .buildWithCallback(m -> observe(m,
                        metrics.getSendPercentileNs(0.999) / 1e6, attrs));

        meter.gaugeBuilder("artemis.stress.producer.latency.max")
                .setDescription("Max producer send latency")
                .setUnit("ms")
                .buildWithCallback(m -> observe(m,
                        metrics.getSendLatMaxNs() / 1e6, attrs));

        // ── Consumer instruments (only in CONSUME / BOTH mode) ─────────────────

        if (trackConsumer) {
            meter.gaugeBuilder("artemis.stress.consumer.received")
                    .setDescription("Total messages received by consumers")
                    .setUnit("messages")
                    .ofLongs()
                    .buildWithCallback(m -> observe(m, metrics.getE2eCount(), attrs));

            meter.gaugeBuilder("artemis.stress.consumer.errors")
                    .setDescription("Total consumer errors and duplicates")
                    .setUnit("errors")
                    .ofLongs()
                    .buildWithCallback(m -> observe(m, metrics.getE2eErrors(), attrs));

            meter.gaugeBuilder("artemis.stress.consumer.throughput")
                    .setDescription("Overall consumer throughput")
                    .setUnit("msg/s")
                    .buildWithCallback(m -> observe(m, metrics.overallE2eRate(), attrs));

            meter.gaugeBuilder("artemis.stress.consumer.e2e.mean")
                    .setDescription("Mean end-to-end latency")
                    .setUnit("ms")
                    .buildWithCallback(m -> observe(m,
                            metrics.getE2eLatMeanNs() / 1e6, attrs));

            meter.gaugeBuilder("artemis.stress.consumer.e2e.p50")
                    .setDescription("p50 end-to-end latency")
                    .setUnit("ms")
                    .buildWithCallback(m -> observe(m,
                            metrics.getE2ePercentileNs(0.50) / 1e6, attrs));

            meter.gaugeBuilder("artemis.stress.consumer.e2e.p90")
                    .setDescription("p90 end-to-end latency")
                    .setUnit("ms")
                    .buildWithCallback(m -> observe(m,
                            metrics.getE2ePercentileNs(0.90) / 1e6, attrs));

            meter.gaugeBuilder("artemis.stress.consumer.e2e.p99")
                    .setDescription("p99 end-to-end latency")
                    .setUnit("ms")
                    .buildWithCallback(m -> observe(m,
                            metrics.getE2ePercentileNs(0.99) / 1e6, attrs));

            meter.gaugeBuilder("artemis.stress.consumer.e2e.p999")
                    .setDescription("p99.9 end-to-end latency")
                    .setUnit("ms")
                    .buildWithCallback(m -> observe(m,
                            metrics.getE2ePercentileNs(0.999) / 1e6, attrs));

            meter.gaugeBuilder("artemis.stress.consumer.e2e.max")
                    .setDescription("Max end-to-end latency")
                    .setUnit("ms")
                    .buildWithCallback(m -> observe(m,
                            metrics.getE2eLatMaxNs() / 1e6, attrs));
        }
    }

    // ─── Helpers ─────────────────────────────────────────────────────────────

    private Attributes buildAttributes() {
        return Attributes.builder()
                .put("broker.url",  sanitiseBrokerUrl(config.getBrokerUrl()))
                .put("destination", config.getQueue())
                .put("mode",        config.getMode().name())
                .build();
    }

    /** Strips user= and password= from the URL before using it as a label. */
    private String sanitiseBrokerUrl(String url) {
        return url.replaceAll("[?&]password=[^&]*", "")
                  .replaceAll("[?&]user=[^&]*", "")
                  .replaceAll("[?&]$", "");
    }

    private void observe(ObservableDoubleMeasurement m, double value, Attributes attrs) {
        if (!closed.get()) m.record(value, attrs);
    }

    private void observe(ObservableLongMeasurement m, long value, Attributes attrs) {
        if (!closed.get()) m.record(value, attrs);
    }
}
