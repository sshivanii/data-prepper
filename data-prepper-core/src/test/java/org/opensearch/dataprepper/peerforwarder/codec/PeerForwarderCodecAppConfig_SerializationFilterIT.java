/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder.codec;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.opensearch.dataprepper.model.document.JacksonDocument;
import org.opensearch.dataprepper.model.event.DefaultEventMetadata;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.log.JacksonOtelLog;
import org.opensearch.dataprepper.model.metric.JacksonExponentialHistogram;
import org.opensearch.dataprepper.model.metric.JacksonGauge;
import org.opensearch.dataprepper.model.metric.JacksonHistogram;
import org.opensearch.dataprepper.model.metric.JacksonSum;
import org.opensearch.dataprepper.model.metric.JacksonSummary;
import org.opensearch.dataprepper.model.trace.DefaultTraceGroupFields;
import org.opensearch.dataprepper.model.trace.JacksonSpan;
import org.opensearch.dataprepper.peerforwarder.model.PeerForwardingEvents;
import org.reflections.Reflections;
import org.reflections.util.ConfigurationBuilder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InvalidClassException;
import java.io.ObjectInputFilter;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Modifier;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/**
 * Verifies the behavior of the {@link ObjectInputFilter} provided by {@link PeerForwarderCodecAppConfig}.
 * These tests verify a few important things:
 * 1) Some arbitrary classes which are not registered are not deserialized.
 * 2) Classes which Data Prepper knows should be registered can be deserialized.
 * 3) Classes which Data Prepper should be able to deserialized are included in test #2.
 * Note that these tests use the {@link ObjectInputStream} directly. This is because using the
 * {@link org.opensearch.dataprepper.peerforwarder.codec.JavaPeerForwarderCodec} will throw class casting exceptions,
 * but we want to be sure the exception comes from the filter, not the cast.
 */
class PeerForwarderCodecAppConfig_SerializationFilterIT {
    private ObjectInputFilter createObjectUnderTest() {
        return new PeerForwarderCodecAppConfig().objectInputFilter();
    }

    @ParameterizedTest
    @ArgumentsSource(SomeUnregisteredSerializableArgumentsProvider.class)
    void filter_will_not_deserialize_unregistered_classes_when_they_are_the_root_object(final Object createdObject) throws IOException {
        final byte[] serializedBytes = createByteArrayWithObject(createdObject);

        final ObjectInputFilter filterUnderTest = createObjectUnderTest();

        assertThat(filterUnderTest, notNullValue());

        final InvalidClassException actualException;
        try (final InputStream inputStream = new ByteArrayInputStream(serializedBytes);
             final ObjectInputStream objectInputStream = new ObjectInputStream(inputStream)) {
            objectInputStream.setObjectInputFilter(filterUnderTest);
            actualException = assertThrows(InvalidClassException.class, objectInputStream::readObject);
        }

        assertThat(actualException.getMessage(), containsString("REJECTED"));
    }

    @ParameterizedTest
    @ArgumentsSource(SomeUnregisteredSerializableArgumentsProvider.class)
    void filter_will_not_deserialize_unregistered_classes_which_are_nested_inside_objects(final Object createdObject) throws IOException {
        final List<Object> innerList = Collections.singletonList(createdObject);
        final Map<String, List<Object>> outerObject = Collections.singletonMap(UUID.randomUUID().toString(), innerList);

        final byte[] serializedBytes = createByteArrayWithObject(outerObject);

        final ObjectInputFilter filterUnderTest = createObjectUnderTest();

        assertThat(filterUnderTest, notNullValue());

        final InvalidClassException actualException;
        try (final InputStream inputStream = new ByteArrayInputStream(serializedBytes);
             final ObjectInputStream objectInputStream = new ObjectInputStream(inputStream)) {
            objectInputStream.setObjectInputFilter(filterUnderTest);
            actualException = assertThrows(InvalidClassException.class, objectInputStream::readObject);
        }

        assertThat(actualException.getMessage(), containsString("REJECTED"));
    }

    @ParameterizedTest
    @ArgumentsSource(SomeKnownSerializableArgumentsProvider.class)
    void filter_will_deserialize_known_classes(final Object createdObject) throws IOException, ClassNotFoundException {
        final byte[] serializedBytes = createByteArrayWithObject(createdObject);

        final ObjectInputFilter filterUnderTest = createObjectUnderTest();

        assertThat(filterUnderTest, notNullValue());

        final Object actualObject;
        try (final InputStream inputStream = new ByteArrayInputStream(serializedBytes);
             final ObjectInputStream objectInputStream = new ObjectInputStream(inputStream)) {
            objectInputStream.setObjectInputFilter(filterUnderTest);
            actualObject = objectInputStream.readObject();
        }

        assertThat(actualObject, not(sameInstance(createdObject)));
        assertThat(actualObject, equalTo(createdObject));
    }

    @ParameterizedTest
    @ArgumentsSource(SomeKnownSerializableArgumentsProvider.class)
    void filter_will_deserialize_known_classes_which_are_nested_inside_other_objects(final Object createdObject) throws IOException, ClassNotFoundException {

        final List<Object> innerList = Collections.singletonList(createdObject);
        final Map<String, List<Object>> outerObject = Collections.singletonMap(UUID.randomUUID().toString(), innerList);

        final byte[] serializedBytes = createByteArrayWithObject(outerObject);

        final ObjectInputFilter filterUnderTest = createObjectUnderTest();

        assertThat(filterUnderTest, notNullValue());

        final Object actualObject;
        try (final InputStream inputStream = new ByteArrayInputStream(serializedBytes);
             final ObjectInputStream objectInputStream = new ObjectInputStream(inputStream)) {
            objectInputStream.setObjectInputFilter(filterUnderTest);
            actualObject = objectInputStream.readObject();
        }

        assertThat(actualObject, not(sameInstance(outerObject)));
        assertThat(actualObject, equalTo(outerObject));
    }


    @ParameterizedTest
    @ArgumentsSource(EventBuilderArgumentsProvider.class)
    void filter_will_deserialize_known_Event_classes(final JacksonEvent.Builder jacksonEventBuilder) throws IOException, ClassNotFoundException {
        final Event event = jacksonEventBuilder
                .withEventType(UUID.randomUUID().toString())
                .withData(Collections.singletonMap(UUID.randomUUID().toString(), UUID.randomUUID().toString()))
                .build();
        final PeerForwardingEvents peerForwardingEvents = new PeerForwardingEvents(Collections.singletonList(event), UUID.randomUUID().toString(), UUID.randomUUID().toString());
        final byte[] serializedBytes = createByteArrayWithObject(peerForwardingEvents);

        final ObjectInputFilter filterUnderTest = createObjectUnderTest();

        assertThat(filterUnderTest, notNullValue());

        final Object actualObject;
        try (final InputStream inputStream = new ByteArrayInputStream(serializedBytes);
             final ObjectInputStream objectInputStream = new ObjectInputStream(inputStream)) {
            objectInputStream.setObjectInputFilter(filterUnderTest);
            actualObject = objectInputStream.readObject();
        }

        assertThat(actualObject, not(sameInstance(event)));
    }

    /**
     * This is not really a unit test. It is scanning all of the sub-types of the {@link Event} model
     * and making sure that the {@link EventBuilderArgumentsProvider} class includes them in testing.
     * If this test fails, then there is some {@link Event} class used in Data Prepper which is not
     * supported by the core peer-forwarder. This needs to be corrected.
     */
    @Test
    void all_subclasses_of_Event_are_verified_in_this_test_suite_to_be_acceptable_for_serialization() {
        final Reflections reflections = new Reflections(new ConfigurationBuilder()
                .forPackages("org.opensearch.dataprepper"));
        final Set<Class<? extends Event>> allSubTypes = reflections.getSubTypesOf(Event.class);

        final Set<Class<? extends Event>> allConcreteSubTypes = allSubTypes.stream()
                .filter(clazz -> !Modifier.isAbstract(clazz.getModifiers()))
                .filter(clazz -> !Modifier.isInterface(clazz.getModifiers()))
                .collect(Collectors.toSet());

        assertThat(allConcreteSubTypes.size(), greaterThanOrEqualTo(1));

        final Stream<? extends Arguments> stream = new EventBuilderArgumentsProvider().provideArguments(null);

        final Set<? extends Class<? extends JacksonEvent>> classesVerified = stream
                .map(a -> a.get()[0])
                .filter(b -> b instanceof JacksonEvent.Builder)
                .map(b -> (JacksonEvent.Builder) b)
                .map(b -> b.withEventType(UUID.randomUUID().toString()))
                .map(b -> b.withData(Collections.singletonMap(UUID.randomUUID().toString(), UUID.randomUUID().toString())))
                .map(b -> b.build())
                .map(e -> e.getClass())
                .collect(Collectors.toSet());

        final Sets.SetView<Class<? extends Event>> difference = Sets.difference(allConcreteSubTypes, classesVerified);
        assertThat("If this test is failing, then a Data Prepper Event model was created which is unable to be deserialized in Core Peer Forwarder. These classes as not verified: " + difference,
                difference, empty());
    }

    private byte[] createByteArrayWithObject(final Object object) throws IOException {
        try (final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
             final ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)) {
            objectOutputStream.writeObject(object);
            return byteArrayOutputStream.toByteArray();
        }
    }

    /**
     * This is a rather arbitrary selection of {@link java.io.Serializable} classes which are
     * not registered.
     */
    static class SomeUnregisteredSerializableArgumentsProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext context) {
            return Stream.of(
                    arguments(new LinkedBlockingQueue<>()),
                    arguments(new ArrayBlockingQueue<>(1)),
                    arguments(Pattern.compile("[1-9]"))
            );
        }
    }

    static class SomeKnownSerializableArgumentsProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext context) {
            return Stream.of(
                    arguments(UUID.randomUUID().toString()),
                    arguments(Collections.singletonList(UUID.randomUUID().toString())),
                    arguments(Collections.singletonMap(UUID.randomUUID().toString(), UUID.randomUUID().toString())),
                    arguments(Instant.now()),
                    arguments(Duration.ofMinutes(5)),
                    arguments(DefaultEventMetadata.builder().withEventType(UUID.randomUUID().toString()).build())
            );
        }
    }

    static class EventBuilderArgumentsProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext context) {
            return Stream.of(
                    arguments(JacksonEvent.builder()),
                    arguments(JacksonSpan.builder()
                            .withTraceId(UUID.randomUUID().toString())
                            .withTraceGroup(UUID.randomUUID().toString())
                            .withSpanId(UUID.randomUUID().toString())
                            .withName(UUID.randomUUID().toString())
                            .withKind(UUID.randomUUID().toString())
                            .withStartTime(Instant.now().toString())
                            .withEndTime(Instant.now().toString())
                            .withDurationInNanos(100L)
                            .withTraceGroupFields(DefaultTraceGroupFields.builder().build())
                    ),
                    arguments(JacksonLog.builder()),
                    arguments(JacksonOtelLog.builder()),
                    arguments(JacksonExponentialHistogram.builder()
                            .withName(UUID.randomUUID().toString())
                            .withEventKind(UUID.randomUUID().toString())
                            .withTime(Instant.now().toString())
                            .withSum(10.0)
                    ),
                    arguments(JacksonGauge.builder()
                            .withName(UUID.randomUUID().toString())
                            .withEventKind(UUID.randomUUID().toString())
                            .withTime(Instant.now().toString())
                            .withValue(10.0)
                    ),
                    arguments(JacksonHistogram.builder()
                            .withName(UUID.randomUUID().toString())
                            .withEventKind(UUID.randomUUID().toString())
                            .withTime(Instant.now().toString())
                            .withSum(10.0)
                    ),
                    arguments(JacksonSum.builder()
                            .withName(UUID.randomUUID().toString())
                            .withEventKind(UUID.randomUUID().toString())
                            .withTime(Instant.now().toString())
                            .withValue(10.0)
                            .withIsMonotonic(true)
                    ),
                    arguments(JacksonSummary.builder()
                            .withName(UUID.randomUUID().toString())
                            .withEventKind(UUID.randomUUID().toString())
                            .withTime(Instant.now().toString())
                            .withAttributes(Collections.emptyMap())
                    ),
                    arguments(JacksonDocument.builder())
            );
        }
    }
}