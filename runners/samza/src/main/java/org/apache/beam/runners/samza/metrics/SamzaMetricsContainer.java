/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.samza.metrics;

import static org.apache.beam.runners.core.metrics.MetricsContainerStepMap.asAttemptedOnlyMetricResults;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.beam.sdk.metrics.GaugeResult;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.Metric;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.metrics.MetricsVisitor;
import org.apache.samza.metrics.Timer;


/**
 * This class holds the {@link MetricsContainer}s for BEAM metrics, and update the results to Samza
 * metrics.
 */
public class SamzaMetricsContainer {
  private static final String BEAM_METRICS_GROUP = "BeamMetrics";
  private static final String DELIMITER = "-";
  private static final AtomicInteger counter = new AtomicInteger(0);

  private final MetricsContainerStepMap metricsContainers = new MetricsContainerStepMap();
  private final MetricsRegistryMap metricsRegistry;

  public SamzaMetricsContainer(MetricsRegistryMap metricsRegistry) {
    this.metricsRegistry = metricsRegistry;
    this.metricsRegistry.metrics().put(BEAM_METRICS_GROUP, new ConcurrentHashMap<>());
  }

  public MetricsContainer getContainer(String stepName) {
    return this.metricsContainers.getContainer(stepName);
  }

  public MetricsContainerStepMap getContainers() {
    return this.metricsContainers;
  }

  private void printMetrics() {
    HashMap<String, HashMap<String, String>> maps = new HashMap<>();
    ConcurrentHashMap<String, ConcurrentHashMap<String, Metric>> metrics = metricsRegistry.metrics();
    for (Map.Entry<String, ConcurrentHashMap<String, Metric>> stringConcurrentHashMapEntry : metrics.entrySet()) {
      HashMap<String, String> map = new HashMap<>();
      maps.put(stringConcurrentHashMapEntry.getKey(), map);
      ConcurrentHashMap<String, Metric> value = stringConcurrentHashMapEntry.getValue();
      for (Map.Entry<String, Metric> stringMetricEntry : value.entrySet()) {
        Metric metric = stringMetricEntry.getValue();
        metric.visit(new MetricsVisitor() {
          @Override
          public void counter(Counter counter) {
            map.put(stringMetricEntry.getKey(), counter.toString());
          }

          @Override
          public <T> void gauge(Gauge<T> gauge) {
            map.put(stringMetricEntry.getKey(), gauge.toString());
          }

          @Override
          public void timer(Timer timer) {
            map.put(stringMetricEntry.getKey(), String.valueOf(timer.getSnapshot().getAverage()));
          }
        });
      }
    }
    System.out.println(maps);
  }

  public void updateMetrics(String stepName) {
//    System.out.println("updating metrics :" + counter.incrementAndGet());
    if (counter.incrementAndGet() % 500000 == 0) {
      printMetrics();
    }
    assert metricsRegistry != null;

    final MetricResults metricResults = asAttemptedOnlyMetricResults(metricsContainers);
    final MetricQueryResults results =
        metricResults.queryMetrics(MetricsFilter.builder().addStep(stepName).build());

    final CounterUpdater updateCounter = new CounterUpdater();
    results.getCounters().forEach(updateCounter);

    final GaugeUpdater updateGauge = new GaugeUpdater();
    results.getGauges().forEach(updateGauge);

    // TODO: add distribution metrics to Samza
  }

  private class CounterUpdater implements Consumer<MetricResult<Long>> {
    @Override
    public void accept(MetricResult<Long> metricResult) {
      final String metricName = getMetricName(metricResult);
      Counter counter = (Counter) getSamzaMetricFor(metricName);
      if (counter == null) {
        counter = metricsRegistry.newCounter(BEAM_METRICS_GROUP, metricName);
      }
      counter.dec(counter.getCount());
      counter.inc(metricResult.getAttempted());
    }
  }

  private class GaugeUpdater implements Consumer<MetricResult<GaugeResult>> {
    @Override
    public void accept(MetricResult<GaugeResult> metricResult) {
      final String metricName = getMetricName(metricResult);
      @SuppressWarnings("unchecked")
      Gauge<Long> gauge = (Gauge<Long>) getSamzaMetricFor(metricName);
      if (gauge == null) {
        gauge = metricsRegistry.newGauge(BEAM_METRICS_GROUP, metricName, 0L);
      }
      gauge.set(metricResult.getAttempted().getValue());
    }
  }

  private Metric getSamzaMetricFor(String metricName) {
    return metricsRegistry.getGroup(BEAM_METRICS_GROUP).get(metricName);
  }

  private static String getMetricName(MetricResult<?> metricResult) {
    return metricResult.getKey().toString();
  }
}
