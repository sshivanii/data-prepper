/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.oteltracegroup;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Strings;
import io.micrometer.core.instrument.Counter;
import org.opensearch.client.json.JsonData;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.FieldValue;
import org.opensearch.client.opensearch._types.query_dsl.BoolQuery;
import org.opensearch.client.opensearch._types.query_dsl.Query;
import org.opensearch.client.opensearch._types.query_dsl.TermsQuery;
import org.opensearch.client.opensearch._types.query_dsl.TermsQueryField;
import org.opensearch.client.opensearch.core.SearchRequest;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.opensearch.core.search.Hit;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.trace.DefaultTraceGroupFields;
import org.opensearch.dataprepper.model.trace.Span;
import org.opensearch.dataprepper.model.trace.TraceGroupFields;
import org.opensearch.dataprepper.plugins.processor.oteltracegroup.model.TraceGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.EVENT;

@DataPrepperPlugin(name = "otel_trace_group", pluginType = Processor.class)
public class OTelTraceGroupProcessor extends AbstractProcessor<Record<Span>, Record<Span>> {

    public static final String RECORDS_IN_MISSING_TRACE_GROUP = "recordsInMissingTraceGroup";
    public static final String RECORDS_OUT_FIXED_TRACE_GROUP = "recordsOutFixedTraceGroup";
    public static final String RECORDS_OUT_MISSING_TRACE_GROUP = "recordsOutMissingTraceGroup";

    private static final Logger LOG = LoggerFactory.getLogger(OTelTraceGroupProcessor.class);

    private final OTelTraceGroupProcessorConfig otelTraceGroupProcessorConfig;

    private final OpenSearchClient openSearchClient;

    private final Counter recordsInMissingTraceGroupCounter;
    private final Counter recordsOutFixedTraceGroupCounter;
    private final Counter recordsOutMissingTraceGroupCounter;

    public OTelTraceGroupProcessor(final PluginSetting pluginSetting) {
        super(pluginSetting);
        otelTraceGroupProcessorConfig = OTelTraceGroupProcessorConfig.buildConfig(pluginSetting);

        openSearchClient = otelTraceGroupProcessorConfig.getEsConnectionConfig().createOpenSearchClient();
        recordsInMissingTraceGroupCounter = pluginMetrics.counter(RECORDS_IN_MISSING_TRACE_GROUP);
        recordsOutFixedTraceGroupCounter = pluginMetrics.counter(RECORDS_OUT_FIXED_TRACE_GROUP);
        recordsOutMissingTraceGroupCounter = pluginMetrics.counter(RECORDS_OUT_MISSING_TRACE_GROUP);
    }

    @Override
    public Collection<Record<Span>> doExecute(final Collection<Record<Span>> rawSpanRecords) {
        final List<Record<Span>> recordsOut = new LinkedList<>();
        final Set<Record<Span>> recordsMissingTraceGroupInfo = new HashSet<>();
        final Set<String> traceIdsToLookUp = new HashSet<>();
        for (Record<Span> record: rawSpanRecords) {
            final Span span = record.getData();
            final String traceGroup = span.getTraceGroup();
            final String traceId = span.getTraceId();
            if (Strings.isNullOrEmpty(traceGroup)) {
                traceIdsToLookUp.add(traceId);
                recordsMissingTraceGroupInfo.add(record);
                recordsInMissingTraceGroupCounter.increment();
            } else {
                recordsOut.add(record);
            }
        }

        final Map<String, TraceGroup> traceGroupMap = searchTraceGroupByTraceId(traceIdsToLookUp);
        for (final Record<Span> record: recordsMissingTraceGroupInfo) {
            final Span span = record.getData();
            final String traceId = span.getTraceId();
            final TraceGroup traceGroup = traceGroupMap.get(traceId);
            if (traceGroup != null) {
                try {
                    fillInTraceGroupInfo(span, traceGroup);
                    recordsOut.add(record);
                    recordsOutFixedTraceGroupCounter.increment();
                } catch (Exception e) {
                    recordsOut.add(record);
                    recordsOutMissingTraceGroupCounter.increment();
                    LOG.error(EVENT, "Failed to process the span: [{}]", record.getData(), e);
                }
            } else {
                recordsOut.add(record);
                recordsOutMissingTraceGroupCounter.increment();
                final String spanId = span.getSpanId();
                LOG.warn("Failed to find traceGroup for spanId: {} due to traceGroup missing for traceId: {}", spanId, traceId);
            }
        }

        return recordsOut;
    }

    private void fillInTraceGroupInfo(final Span span, final TraceGroup traceGroup) {
        span.setTraceGroup(traceGroup.getTraceGroup());
        span.setTraceGroupFields(traceGroup.getTraceGroupFields());
    }


    private Map<String, TraceGroup> searchTraceGroupByTraceId(final Collection<String> traceIds) {
        final Map<String, TraceGroup> traceIdToTraceGroup = new HashMap<>();

        SearchRequest searchRequest = new SearchRequest.Builder()
                .index("otel-v1-apm-span")
                .query(q -> q.bool(new BoolQuery.Builder()
                                .must(new Query.Builder()
                                        .terms(new TermsQuery.Builder()
                                                .field(OTelTraceGroupProcessorConfig.TRACE_ID_FIELD)
                                                .terms(new TermsQueryField.Builder().value(traceIds.stream().map(traceId -> FieldValue.of(traceId)).collect(Collectors.toList())).build())
                                                .build()).build(),
                                        new Query.Builder()
                                                .terms(new TermsQuery.Builder()
                                                        .field(OTelTraceGroupProcessorConfig.PARENT_SPAN_ID_FIELD)
                                                        .terms(new TermsQueryField.Builder().value(List.of(FieldValue.of(""))).build())
                                                        .build()).build())
                        .build()
                ))
                // TODO: construct docValueFields() list
                .build();


        try {
            SearchResponse<ObjectNode> searchResponse = openSearchClient.search(searchRequest, ObjectNode.class);
            final List<Hit<ObjectNode>> searchHits = searchResponse.hits().hits();
            searchHits.forEach(searchHit -> {
                final Optional<Map.Entry<String, TraceGroup>> optionalStringTraceGroupEntry = fromSearchHitObjectToMapEntry(searchHit);
                optionalStringTraceGroupEntry.ifPresent(entry -> traceIdToTraceGroup.put(entry.getKey(), entry.getValue()));
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


        return traceIdToTraceGroup;
    }


    private Optional<Map.Entry<String, TraceGroup>> fromSearchHitObjectToMapEntry(final Hit<ObjectNode> searchHit) {
        final JsonData traceIdDocField = searchHit.fields().get(OTelTraceGroupProcessorConfig.TRACE_ID_FIELD);
        final JsonData traceGroupNameDocField = searchHit.fields().get(TraceGroup.TRACE_GROUP_NAME_FIELD);
        final JsonData traceGroupEndTimeDocField = searchHit.fields().get(TraceGroup.TRACE_GROUP_END_TIME_FIELD);
        final JsonData traceGroupDurationInNanosDocField = searchHit.fields().get(TraceGroup.TRACE_GROUP_DURATION_IN_NANOS_FIELD);
        final JsonData traceGroupStatusCodeDocField = searchHit.fields().get(TraceGroup.TRACE_GROUP_STATUS_CODE_FIELD);
        if (Stream.of(traceIdDocField, traceGroupNameDocField, traceGroupEndTimeDocField, traceGroupDurationInNanosDocField,
                traceGroupStatusCodeDocField).allMatch(Objects::nonNull)) {
            final String traceId = traceIdDocField.toString();
            final String traceGroupName = traceGroupNameDocField.toString();
            final String traceGroupEndTime = normalizeDateTime(traceGroupEndTimeDocField.toString());
            final Number traceGroupDurationInNanos = traceGroupDurationInNanosDocField.to(Number.class);
            final Number traceGroupStatusCode = traceGroupStatusCodeDocField.to(Number.class);
            final TraceGroupFields traceGroupFields = DefaultTraceGroupFields.builder()
                    .withEndTime(traceGroupEndTime)
                    .withDurationInNanos(traceGroupDurationInNanos.longValue())
                    .withStatusCode(traceGroupStatusCode.intValue())
                    .build();
            final TraceGroup traceGroup = new TraceGroup.TraceGroupBuilder()
                    .setTraceGroup(traceGroupName)
                    .setTraceGroupFields(traceGroupFields)
                    .build();
            return Optional.of(new AbstractMap.SimpleEntry<>(traceId, traceGroup));
        }
        return Optional.empty();
    }

    /**
     * Restores trailing zeros for thousand, e.g. 2020-08-20T05:40:46.0895568Z -> 2020-08-20T05:40:46.089556800Z
     */
    private String normalizeDateTime(String dateTimeString) {
        return Instant.parse(dateTimeString).toString();
    }

    @Override
    public void prepareForShutdown() {

    }

    @Override
    public boolean isReadyForShutdown() {
        return true;
    }

    @Override
    public void shutdown() {
        openSearchClient.shutdown();
    }
}
