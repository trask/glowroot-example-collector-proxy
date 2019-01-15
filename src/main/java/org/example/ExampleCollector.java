/*
 * Copyright 2018-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.example;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.glowroot.agent.collector.Collector;
import org.glowroot.agent.shaded.com.google.common.collect.Maps;
import org.glowroot.agent.shaded.org.glowroot.wire.api.model.AgentConfigOuterClass.AgentConfig;
import org.glowroot.agent.shaded.org.glowroot.wire.api.model.CollectorServiceOuterClass.GaugeValueMessage.GaugeValue;
import org.glowroot.agent.shaded.org.glowroot.wire.api.model.CollectorServiceOuterClass.InitMessage.Environment;
import org.glowroot.agent.shaded.org.glowroot.wire.api.model.CollectorServiceOuterClass.LogMessage.LogEvent;

import static java.util.concurrent.TimeUnit.MINUTES;

public class ExampleCollector implements org.glowroot.agent.collector.Collector {

    private static final long OUTPUT_INTERVAL_MILLIS = MINUTES.toMillis(1);

    private final Map<String, List<GaugeValue>> collectedGaugeValuesMap = Maps.newHashMap();
    private long nextOutputTime;

    private final Collector delegate;

    public ExampleCollector(Collector delegate) {
        this.delegate = delegate;
    }

    @Override
    public void init(List<File> confDirs, Environment environment, AgentConfig agentConfig,
            AgentConfigUpdater agentConfigUpdater) throws Exception {
        delegate.init(confDirs, environment, agentConfig, agentConfigUpdater);
    }

    @Override
    public void collectAggregates(AggregateReader aggregateReader) throws Exception {
        delegate.collectAggregates(aggregateReader);
    }

    // this method is called every 5 seconds with list of gauge values captured
    // note: the gauge values in one call can have slightly different capture times, but the capture
    // times will be
    // monotonically increasing
    @Override
    public void collectGaugeValues(List<GaugeValue> gaugeValues) throws Exception {
        delegate.collectGaugeValues(gaugeValues);
        synchronized (collectedGaugeValuesMap) {
            for (GaugeValue gaugeValue : gaugeValues) {
                long captureTime = gaugeValue.getCaptureTime();
                if (captureTime > nextOutputTime) {
                    write(collectedGaugeValuesMap, nextOutputTime);
                    collectedGaugeValuesMap.clear();
                    nextOutputTime = getNextOutputTime(captureTime);
                }
                List<GaugeValue> collectedGaugeValues =
                        collectedGaugeValuesMap.get(gaugeValue.getGaugeName());
                if (collectedGaugeValues == null) {
                    collectedGaugeValues = new ArrayList<GaugeValue>();
                    collectedGaugeValuesMap.put(gaugeValue.getGaugeName(), collectedGaugeValues);
                }
                collectedGaugeValues.add(gaugeValue);
            }
        }
    }

    @Override
    public void collectTrace(TraceReader traceReader) throws Exception {
        delegate.collectTrace(traceReader);
    }

    @Override
    public void log(LogEvent logEvent) throws Exception {
        delegate.log(logEvent);
    }

    private static void write(Map<String, List<GaugeValue>> collectedGaugeValuesMap,
            long outputTime) throws FileNotFoundException {
        PrintStream out = new PrintStream(new FileOutputStream(new File("gaugevalues.txt"), true));
        try {
            for (Map.Entry<String, List<GaugeValue>> entry : collectedGaugeValuesMap.entrySet()) {
                double totalWeightedValue = 0;
                long totalWeight = 0;
                for (GaugeValue gaugeValue : entry.getValue()) {
                    totalWeightedValue += gaugeValue.getValue() * gaugeValue.getWeight();
                    totalWeight += gaugeValue.getWeight();
                }
                out.format("%tF %tT\t%s\t%f%n", outputTime, outputTime, entry.getKey(),
                        totalWeightedValue / totalWeight);
            }
        } finally {
            out.close();
        }
    }

    private static long getNextOutputTime(long captureTime) {
        return (long) Math.ceil(captureTime / (double) OUTPUT_INTERVAL_MILLIS)
                * OUTPUT_INTERVAL_MILLIS;
    }
}
