/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.anomalydetector;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.plugin.PluginFactory;

import java.util.Collection;
import java.util.List;

@DataPrepperPlugin(name = "anomaly_detector", pluginType = Processor.class, pluginConfigurationType = AnomalyDetectorProcessorConfig.class)
public class AnomalyDetectorProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {
    public static final String DEVIATION_KEY = "deviation_from_expected";
    public static final String GRADE_KEY = "grade";

    private final List<String> keys;
    private final AnomalyDetectorMode mode;
    private final AnomalyDetectorProcessorConfig anomalyDetectorProcessorConfig;

    @DataPrepperPluginConstructor
    public AnomalyDetectorProcessor(final AnomalyDetectorProcessorConfig anomalyDetectorProcessorConfig, final PluginMetrics pluginMetrics, final PluginFactory pluginFactory) {
        super(pluginMetrics);
        this.anomalyDetectorProcessorConfig = anomalyDetectorProcessorConfig;
        keys = anomalyDetectorProcessorConfig.getKeys();
        mode = loadAnomalyDetectorMode(pluginFactory);
        mode.initialize(keys);
    }

    private AnomalyDetectorMode loadAnomalyDetectorMode(final PluginFactory pluginFactory) {
        final PluginModel modeConfiguration = anomalyDetectorProcessorConfig.getDetectorMode();
        final PluginSetting modePluginSetting = new PluginSetting(modeConfiguration.getPluginName(), modeConfiguration.getPluginSettings());
        return pluginFactory.loadPlugin(AnomalyDetectorMode.class, modePluginSetting);
    }

    @Override
    public Collection<Record<Event>> doExecute(Collection<Record<Event>> records) {
        return mode.handleEvents(records);
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

    }
}
