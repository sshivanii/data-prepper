/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate;

import static org.opensearch.dataprepper.test.helper.ReflectivelySetField.setField;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.apache.commons.lang3.RandomStringUtils;
import org.opensearch.dataprepper.plugins.processor.aggregate.actions.RemoveDuplicatesAggregateAction;
import org.opensearch.dataprepper.plugins.processor.aggregate.actions.PutAllAggregateAction;
import org.opensearch.dataprepper.plugins.processor.aggregate.actions.CountAggregateAction;
import org.opensearch.dataprepper.plugins.processor.aggregate.actions.CountAggregateActionConfig;
import org.opensearch.dataprepper.plugins.processor.aggregate.actions.HistogramAggregateAction;
import org.opensearch.dataprepper.plugins.processor.aggregate.actions.HistogramAggregateActionConfig;
import org.opensearch.dataprepper.plugins.processor.aggregate.actions.OutputFormat;
import static org.opensearch.dataprepper.plugins.processor.aggregate.actions.CountAggregateActionConfig.DEFAULT_COUNT_KEY;
import static org.opensearch.dataprepper.plugins.processor.aggregate.actions.CountAggregateActionConfig.DEFAULT_START_TIME_KEY;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadLocalRandom;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.collection.IsIn.in;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

/**
 * These integration tests are executing concurrent code that is inherently difficult to test, and even more difficult to recreate a failed test.
 * If any of these tests are to fail, please create a bug report as a GitHub issue with the details of the failed test.
 */
@ExtendWith(MockitoExtension.class)
public class AggregateProcessorIT {

    private static final int testValue = 1;
    private static final int NUM_EVENTS_PER_BATCH = 200;
    private static final int NUM_UNIQUE_EVENTS_PER_BATCH = 8;
    private static final int NUM_THREADS = 100;
    private static final int GROUP_DURATION_FOR_ONLY_SINGLE_CONCLUDE = 2;
    @Mock
    private AggregateProcessorConfig aggregateProcessorConfig;

    private AggregateAction aggregateAction;
    private PluginMetrics pluginMetrics;

    @Mock
    private ExpressionEvaluator expressionEvaluator;

    private Collection<Record<Event>> eventBatch;
    private ConcurrentLinkedQueue<Map<String, Object>> aggregatedResult;
    private Set<Map<String, Object>> uniqueEventMaps;

    @Mock
    private PluginFactory pluginFactory;

    @Mock
    private PluginModel actionConfiguration;

    @BeforeEach
    void setup() {
        aggregatedResult = new ConcurrentLinkedQueue<>();
        uniqueEventMaps = new HashSet<>();

        final List<String> identificationKeys = new ArrayList<>();
        identificationKeys.add("firstRandomNumber");
        identificationKeys.add("secondRandomNumber");
        identificationKeys.add("thirdRandomNumber");


        eventBatch = getBatchOfEvents(false);

        pluginMetrics = PluginMetrics.fromNames(UUID.randomUUID().toString(), UUID.randomUUID().toString());

        when(aggregateProcessorConfig.getIdentificationKeys()).thenReturn(identificationKeys);
        when(aggregateProcessorConfig.getAggregateAction()).thenReturn(actionConfiguration);
        when(actionConfiguration.getPluginName()).thenReturn(UUID.randomUUID().toString());
        when(actionConfiguration.getPluginSettings()).thenReturn(Collections.emptyMap());
    }

    private AggregateProcessor createObjectUnderTest() {
        return new AggregateProcessor(aggregateProcessorConfig, pluginMetrics, pluginFactory, expressionEvaluator);
    }

    @RepeatedTest(value = 10)
    void aggregateWithNoConcludingGroupsReturnsExpectedResult() throws InterruptedException {
        aggregateAction = new RemoveDuplicatesAggregateAction();
        when(pluginFactory.loadPlugin(eq(AggregateAction.class), any(PluginSetting.class)))
                .thenReturn(aggregateAction);
        when(aggregateProcessorConfig.getGroupDuration()).thenReturn(Duration.ofSeconds(1000));
        final AggregateProcessor objectUnderTest = createObjectUnderTest();

        final ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS);
        final CountDownLatch countDownLatch = new CountDownLatch(NUM_THREADS);

        for (int i = 0; i < NUM_THREADS; i++) {
            executorService.execute(() -> {
                final List<Record<Event>> recordsOut = (List<Record<Event>>) objectUnderTest.doExecute(eventBatch);
                for (final Record<Event> record : recordsOut) {
                    final Map<String, Object> map = record.getData().toMap();
                    aggregatedResult.add(map);
                }
                countDownLatch.countDown();
            });
        }

        boolean allThreadsFinished = countDownLatch.await(5L, TimeUnit.SECONDS);

        assertThat(allThreadsFinished, equalTo(true));
        assertThat(aggregatedResult.size(), equalTo(NUM_UNIQUE_EVENTS_PER_BATCH));

        for (final Map<String, Object> uniqueEventMap : uniqueEventMaps) {
            assertThat(aggregatedResult, hasItem(uniqueEventMap));
        }

        for (final Map<String, Object> eventMap : aggregatedResult) {
            assertThat(eventMap, in(uniqueEventMaps));
        }
    }

    @RepeatedTest(value = 2)
    void aggregateWithConcludingGroupsOnceReturnsExpectedResult() throws InterruptedException {
        aggregateAction = new RemoveDuplicatesAggregateAction();
        when(pluginFactory.loadPlugin(eq(AggregateAction.class), any(PluginSetting.class)))
                .thenReturn(aggregateAction);
        when(aggregateProcessorConfig.getGroupDuration()).thenReturn(Duration.ofSeconds(GROUP_DURATION_FOR_ONLY_SINGLE_CONCLUDE));
        final AggregateProcessor objectUnderTest = createObjectUnderTest();

        final ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS);
        final CountDownLatch countDownLatch = new CountDownLatch(NUM_THREADS);

        objectUnderTest.doExecute(eventBatch);
        Thread.sleep(GROUP_DURATION_FOR_ONLY_SINGLE_CONCLUDE * 1000);

        for (int i = 0; i < NUM_THREADS; i++) {
            executorService.execute(() -> {
                final List<Record<Event>> recordsOut = (List<Record<Event>>) objectUnderTest.doExecute(eventBatch);
                for (final Record<Event> record : recordsOut) {
                    final Map<String, Object> map = record.getData().toMap();
                    aggregatedResult.add(map);
                }
                countDownLatch.countDown();
            });
        }

        boolean allThreadsFinished = countDownLatch.await(5L, TimeUnit.SECONDS);

        assertThat(allThreadsFinished, equalTo(true));
        assertThat(aggregatedResult.size(), equalTo(NUM_UNIQUE_EVENTS_PER_BATCH));

        for (final Map<String, Object> uniqueEventMap : uniqueEventMaps) {
            assertThat(aggregatedResult, hasItem(uniqueEventMap));
        }

        for (final Map<String, Object> eventMap : aggregatedResult) {
            assertThat(eventMap, in(uniqueEventMaps));
        }
    }

    @RepeatedTest(value = 2)
    void aggregateWithPutAllActionAndCondition() throws InterruptedException {
        aggregateAction = new PutAllAggregateAction();
        when(pluginFactory.loadPlugin(eq(AggregateAction.class), any(PluginSetting.class)))
                .thenReturn(aggregateAction);
        String condition = "/firstRandomNumber < 100";
        when(aggregateProcessorConfig.getGroupDuration()).thenReturn(Duration.ofSeconds(GROUP_DURATION_FOR_ONLY_SINGLE_CONCLUDE));
        when(aggregateProcessorConfig.getWhenCondition()).thenReturn(condition);
        int count = 0;
        for (Record<Event> record: eventBatch) {
            Event event = record.getData();
            boolean value = (count % 2 == 0) ? true : false;
            when(expressionEvaluator.evaluate(condition, event)).thenReturn(value);
            if (!value) {
                uniqueEventMaps.remove(event.toMap());
            }
            count++;
        }
        final AggregateProcessor objectUnderTest = createObjectUnderTest();

        final ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS);
        final CountDownLatch countDownLatch = new CountDownLatch(NUM_THREADS);

        objectUnderTest.doExecute(eventBatch);
        Thread.sleep(GROUP_DURATION_FOR_ONLY_SINGLE_CONCLUDE * 1000);

        for (int i = 0; i < NUM_THREADS; i++) {
            executorService.execute(() -> {
                final List<Record<Event>> recordsOut = (List<Record<Event>>) objectUnderTest.doExecute(eventBatch);
                for (final Record<Event> record : recordsOut) {
                    final Map<String, Object> map = record.getData().toMap();
                    aggregatedResult.add(map);
                }
                countDownLatch.countDown();
            });
        }

        boolean allThreadsFinished = countDownLatch.await(5L, TimeUnit.SECONDS);

        assertThat(allThreadsFinished, equalTo(true));
        assertThat(aggregatedResult.size(), equalTo(NUM_UNIQUE_EVENTS_PER_BATCH/2));

        for (final Map<String, Object> uniqueEventMap : uniqueEventMaps) {
            assertThat(aggregatedResult, hasItem(uniqueEventMap));
        }
    }

    @RepeatedTest(value = 2)
    void aggregateWithCountAggregateAction() throws InterruptedException, NoSuchFieldException, IllegalAccessException {
        CountAggregateActionConfig countAggregateActionConfig = new CountAggregateActionConfig();
        setField(CountAggregateActionConfig.class, countAggregateActionConfig, "outputFormat", OutputFormat.RAW.toString());
        aggregateAction = new CountAggregateAction(countAggregateActionConfig);
        when(pluginFactory.loadPlugin(eq(AggregateAction.class), any(PluginSetting.class)))
                .thenReturn(aggregateAction);
        when(aggregateProcessorConfig.getGroupDuration()).thenReturn(Duration.ofSeconds(GROUP_DURATION_FOR_ONLY_SINGLE_CONCLUDE));
        eventBatch = getBatchOfEvents(true);

        final AggregateProcessor objectUnderTest = createObjectUnderTest();

        final ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS);
        final CountDownLatch countDownLatch = new CountDownLatch(NUM_THREADS);

        for (int i = 0; i < NUM_THREADS; i++) {
            executorService.execute(() -> {
                final List<Record<Event>> recordsOut = (List<Record<Event>>) objectUnderTest.doExecute(eventBatch);
                countDownLatch.countDown();
            });
        }
        Thread.sleep(GROUP_DURATION_FOR_ONLY_SINGLE_CONCLUDE * 1000);

        boolean allThreadsFinished = countDownLatch.await(5L, TimeUnit.SECONDS);
        assertThat(allThreadsFinished, equalTo(true));

        Collection<Record<Event>> results = objectUnderTest.doExecute(new ArrayList<Record<Event>>());
        assertThat(results.size(), equalTo(1));

        Map<String, Object> expectedEventMap = new HashMap<>(getEventMap(testValue));
        expectedEventMap.put(DEFAULT_COUNT_KEY, NUM_THREADS * NUM_EVENTS_PER_BATCH);

        final Record<Event> record = (Record<Event>)results.toArray()[0];
        expectedEventMap.forEach((k, v) -> assertThat(record.getData().toMap(), hasEntry(k,v)));
        assertThat(record.getData().toMap(), hasKey(DEFAULT_START_TIME_KEY));
    }

    @RepeatedTest(value = 2)
    void aggregateWithCountAggregateActionWithCondition() throws InterruptedException, NoSuchFieldException, IllegalAccessException {
        CountAggregateActionConfig countAggregateActionConfig = new CountAggregateActionConfig();
        setField(CountAggregateActionConfig.class, countAggregateActionConfig, "outputFormat", OutputFormat.RAW.toString());
        aggregateAction = new CountAggregateAction(countAggregateActionConfig);
        when(pluginFactory.loadPlugin(eq(AggregateAction.class), any(PluginSetting.class)))
                .thenReturn(aggregateAction);
        final String condition = "/firstRandomNumber < 100";
        when(aggregateProcessorConfig.getGroupDuration()).thenReturn(Duration.ofSeconds(GROUP_DURATION_FOR_ONLY_SINGLE_CONCLUDE));
        when(aggregateProcessorConfig.getWhenCondition()).thenReturn(condition);
        int count = 0;
        eventBatch = getBatchOfEvents(true);
        for (Record<Event> record: eventBatch) {
            Event event = record.getData();
            boolean value = (count % 2 == 0) ? true : false;
            when(expressionEvaluator.evaluate(condition, event)).thenReturn(value);
            count++;
        }

        final AggregateProcessor objectUnderTest = createObjectUnderTest();

        final ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS);
        final CountDownLatch countDownLatch = new CountDownLatch(NUM_THREADS);

        for (int i = 0; i < NUM_THREADS; i++) {
            executorService.execute(() -> {
                final List<Record<Event>> recordsOut = (List<Record<Event>>) objectUnderTest.doExecute(eventBatch);
                countDownLatch.countDown();
            });
        }
        Thread.sleep(GROUP_DURATION_FOR_ONLY_SINGLE_CONCLUDE * 1000);

        boolean allThreadsFinished = countDownLatch.await(5L, TimeUnit.SECONDS);
        assertThat(allThreadsFinished, equalTo(true));

        Collection<Record<Event>> results = objectUnderTest.doExecute(new ArrayList<Record<Event>>());
        assertThat(results.size(), equalTo(1));

        Map<String, Object> expectedEventMap = new HashMap<>(getEventMap(testValue));
        expectedEventMap.put(DEFAULT_COUNT_KEY, NUM_THREADS * NUM_EVENTS_PER_BATCH/2);

        final Record<Event> record = (Record<Event>)results.toArray()[0];
        expectedEventMap.forEach((k, v) -> assertThat(record.getData().toMap(), hasEntry(k,v)));
        assertThat(record.getData().toMap(), hasKey(DEFAULT_START_TIME_KEY));
    }

    @RepeatedTest(value = 2)
    void aggregateWithHistogramAggregateAction() throws InterruptedException, NoSuchFieldException, IllegalAccessException {

        HistogramAggregateActionConfig histogramAggregateActionConfig = new HistogramAggregateActionConfig();
        setField(HistogramAggregateActionConfig.class, histogramAggregateActionConfig, "outputFormat", OutputFormat.RAW.toString());
        final String testKey = RandomStringUtils.randomAlphabetic(5);
        setField(HistogramAggregateActionConfig.class, histogramAggregateActionConfig, "key", testKey);
        final String testKeyPrefix = RandomStringUtils.randomAlphabetic(4)+"_";
        setField(HistogramAggregateActionConfig.class, histogramAggregateActionConfig, "generatedKeyPrefix", testKeyPrefix);
        final String testUnits = RandomStringUtils.randomAlphabetic(3);
        setField(HistogramAggregateActionConfig.class, histogramAggregateActionConfig, "units", testUnits);
        setField(HistogramAggregateActionConfig.class, histogramAggregateActionConfig, "recordMinMax", true);
        List<Number> testBuckets = new ArrayList<Number>();
        final double TEST_VALUE_RANGE_MIN = 0.0;
        final double TEST_VALUE_RANGE_MAX = 40.0;
        final double TEST_VALUE_RANGE_STEP = 10.0;
        for (double d = TEST_VALUE_RANGE_MIN; d <= TEST_VALUE_RANGE_MAX; d += TEST_VALUE_RANGE_STEP) {
            testBuckets.add(d);
        }
        setField(HistogramAggregateActionConfig.class, histogramAggregateActionConfig, "buckets", testBuckets);

        aggregateAction = new HistogramAggregateAction(histogramAggregateActionConfig);
        when(pluginFactory.loadPlugin(eq(AggregateAction.class), any(PluginSetting.class)))
                .thenReturn(aggregateAction);
        when(aggregateProcessorConfig.getGroupDuration()).thenReturn(Duration.ofSeconds(GROUP_DURATION_FOR_ONLY_SINGLE_CONCLUDE));
        eventBatch = getBatchOfEvents(true);
        for (final Record<Event> record : eventBatch) {
            final double value = ThreadLocalRandom.current().nextDouble(TEST_VALUE_RANGE_MIN-TEST_VALUE_RANGE_STEP, TEST_VALUE_RANGE_MAX+TEST_VALUE_RANGE_STEP);
            Event event = record.getData();
            event.put(testKey, value);
        }

        final AggregateProcessor objectUnderTest = createObjectUnderTest();

        final ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS);
        final CountDownLatch countDownLatch = new CountDownLatch(NUM_THREADS);

        for (int i = 0; i < NUM_THREADS; i++) {
            executorService.execute(() -> {
                final List<Record<Event>> recordsOut = (List<Record<Event>>) objectUnderTest.doExecute(eventBatch);
                countDownLatch.countDown();
            });
        }
        Thread.sleep(GROUP_DURATION_FOR_ONLY_SINGLE_CONCLUDE * 1000);

        boolean allThreadsFinished = countDownLatch.await(5L, TimeUnit.SECONDS);
        assertThat(allThreadsFinished, equalTo(true));

        Collection<Record<Event>> results = objectUnderTest.doExecute(new ArrayList<Record<Event>>());
        assertThat(results.size(), equalTo(1));

        final String expectedCountKey = histogramAggregateActionConfig.getCountKey();
        Map<String, Object> expectedEventMap = new HashMap<>();
        expectedEventMap.put(expectedCountKey, NUM_THREADS * NUM_EVENTS_PER_BATCH);
        final String expectedStartTimeKey = histogramAggregateActionConfig.getStartTimeKey();
        
        final Record<Event> record = (Record<Event>)results.toArray()[0];
        expectedEventMap.forEach((k, v) -> assertThat(record.getData().toMap(), hasEntry(k, v)));
        assertThat(record.getData().toMap(), hasKey(expectedStartTimeKey));
        final String expectedBucketsKey = histogramAggregateActionConfig.getBucketsKey();
        List<Double> bucketsInResult = (ArrayList<Double>)record.getData().toMap().get(expectedBucketsKey);
        for (int i = 0; i < testBuckets.size(); i++) {
            assertThat(testBuckets.get(i).doubleValue(), equalTo(bucketsInResult.get(i)));
        }
    }

    private List<Record<Event>> getBatchOfEvents(boolean withSameValue) {
        final List<Record<Event>> events = new ArrayList<>();

        for (int i = 0; i < NUM_EVENTS_PER_BATCH; i++) {
            final Map<String, Object> eventMap = (withSameValue) ? getEventMap(testValue) : getEventMap(i % NUM_UNIQUE_EVENTS_PER_BATCH);
            final Event event = JacksonEvent.builder()
                    .withEventType("event")
                    .withData(eventMap)
                    .build();
            if (withSameValue) {
                event.put("data", UUID.randomUUID().toString());
            }

            uniqueEventMaps.add(eventMap);
            events.add(new Record<>(event));
        }
        return events;
    }

    private Map<String, Object> getEventMap(int i) {
        final Map<String, Object> eventMap = new HashMap<>();
        eventMap.put("firstRandomNumber", i);
        eventMap.put("secondRandomNumber", i);
        eventMap.put("thirdRandomNumber", i);
        return eventMap;
    }

}
