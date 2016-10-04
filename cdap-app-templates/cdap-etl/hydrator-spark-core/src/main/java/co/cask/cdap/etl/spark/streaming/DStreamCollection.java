/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.etl.spark.streaming;

import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.etl.api.batch.SparkSink;
import co.cask.cdap.etl.api.streaming.Windower;
import co.cask.cdap.etl.spark.SparkCollection;
import co.cask.cdap.etl.spark.SparkPairCollection;
import co.cask.cdap.etl.spark.function.PluginFunctionContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;

/**
 * JavaDStream backed {@link co.cask.cdap.etl.spark.SparkCollection}
 *
 * @param <T> type of objects in the collection
 */
public class DStreamCollection<T> implements SparkCollection<T> {

  private final JavaSparkExecutionContext sec;
  private final JavaDStream<T> stream;

  public DStreamCollection(JavaSparkExecutionContext sec, JavaDStream<T> stream) {
    this.sec = sec;
    this.stream = stream;
  }

  @SuppressWarnings("unchecked")
  @Override
  public JavaDStream<T> getUnderlying() {
    return stream;
  }

  @Override
  public SparkCollection<T> cache() {
    return wrap(stream.cache());
  }

  @SuppressWarnings("unchecked")
  @Override
  public SparkCollection<T> union(SparkCollection<T> other) {
    return wrap(stream.union((JavaDStream<T>) other.getUnderlying()));
  }

  @Override
  public <U> SparkCollection<U> flatMap(FlatMapFunction<T, U> function) {
    return wrap(stream.flatMap(function));
  }

  @Override
  public <K, V> SparkPairCollection<K, V> flatMapToPair(PairFlatMapFunction<T, K, V> function) {
    return new PairDStreamCollection<>(sec, stream.flatMapToPair(function));
  }

  @Override
  public <U> SparkCollection<U> compute(PluginFunctionContext pluginFunctionContext) throws Exception {
    return wrap(stream.transform(new ComputeTransformFunction<T, U>(sec, pluginFunctionContext)));
  }

  @Override
  public void store(final String stageName, final PairFlatMapFunction<T, Object, Object> sinkFunction) {
    stream.foreachRDD(new StreamingBatchSinkFunction<>(sinkFunction, sec, stageName));
  }

  @Override
  public void store(String stageName, SparkSink<T> sink) throws Exception {
    // should never be called.
    throw new UnsupportedOperationException("Spark sink not supported in Spark Streaming.");
  }

  @Override
  public SparkCollection<T> window(final String stageName, Windower windower) {
    return wrap(stream.transform(new CountingTranformFunction<T>(stageName, sec.getMetrics(), "records.in"))
                  .window(Durations.seconds(windower.getWidth()), Durations.seconds(windower.getSlideInterval()))
                  .transform(new CountingTranformFunction<T>(stageName, sec.getMetrics(), "records.out")));
  }

  private <U> SparkCollection<U> wrap(JavaDStream<U> stream) {
    return new DStreamCollection<>(sec, stream);
  }
}
