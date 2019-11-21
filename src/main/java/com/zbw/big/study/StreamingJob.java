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

package com.zbw.big.study;

import java.util.Properties;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import com.zbw.big.study.pojo.DosKafkaMessagePojo;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

	public static void main(String[] args) throws Exception {
		final ParameterTool params = ParameterTool.fromArgs(args);
		
		// get the streaming execution environment automatically, based on different context (local JVM of flink cluster, or real flink cluster)
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
		env.getConfig().setGlobalJobParameters(params);
		
		// manual input data
//		DataStream<Tuple2<String, Integer>> retailSource;
//		retailSource = env.addSource(RetailSource.create());
//		DataStream<Long> retailSink = retailSource.keyBy(0).window(TumblingProcessingTimeWindows.of(Time.seconds(5))).aggregate(new RowAggregate());
//		retailSink.print();
		
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		// only required for Kafka 0.8
//		properties.setProperty("zookeeper.connect", "localhost:2181");
		properties.setProperty("group.id", "testgroup1");
		
		// 用SimpleStringSchema做Kafka反序列化
//		DataStream<String> retailSource = env.addSource(new FlinkKafkaConsumer011<>("test", new SimpleStringSchema(), properties));
		
		// 用Json做Kafka反序列化
//		DataStream<ObjectNode> retailSource = env.addSource(new FlinkKafkaConsumer011<>("test", new JSONKeyValueDeserializationSchema(true), properties));
		
		// 用自定义Kafka反序列化器，反序列化为Pojo对象
//		FlinkKafkaConsumer011<Pojo> kafkaConsumer = new FlinkKafkaConsumer011<Pojo>("test", new PojoDeSerializer(), properties);
//		kafkaConsumer.setStartFromLatest();
//		DataStream<Pojo> retailSource = env.addSource(kafkaConsumer);
//		DataStream<Pojo> retailSink = retailSource.keyBy("name").window(TumblingProcessingTimeWindows.of(Time.seconds(5))).sum(field);
//		retailSink.print();
		
		// 用自定义Kafka反序列化器，反序列化为Dos对象
		FlinkKafkaConsumer011<DosKafkaMessagePojo> kafkaConsumer = 
				new FlinkKafkaConsumer011<DosKafkaMessagePojo>("test", new DosKafkaMessagePojoDeSerializer(), properties);
		kafkaConsumer.setStartFromLatest();
		DataStream<DosKafkaMessagePojo> retailSource = env.addSource(kafkaConsumer);
		retailSource = retailSource.filter(new FilterFunction<DosKafkaMessagePojo>() {
		    @Override
		    public boolean filter(DosKafkaMessagePojo pojo) throws Exception {
		    	return "I".equals(pojo.getOpType());
		    }
		});
		// 每5秒，按照table名分keyed小组，进行count(*)，写RDBMS记录
		DataStream<Tuple2<String, Long>> retailSink = retailSource.keyBy("table") // keyedStream
													.window(TumblingProcessingTimeWindows.of(Time.seconds(5))) // windowAssigner+defaultWindowTrigger
													.aggregate(new RowAggregate()); //windowFunction
		retailSink.print();
		
//		DataStream<Tuple2<String, Integer>> retailSink = retailSource.keyBy(0).window(TumblingProcessingTimeWindows.of(Time.seconds(5))).sum(1);
		
//		retailSink
//	    .map(new MapFunction<Tuple2<String, Integer>, String>() {
//	        @Override
//	        public String map(Tuple2<String, Integer> tuple) {
//	            return tuple.f0 + tuple.f1;
//	        }
//	    })
//	    .addSink(new FlinkKafkaProducer011<>("localhost:9092", "test", new SimpleStringSchema()));

		// submit your jar onto flink cluster
		env.execute("Flink Streaming Java API Skeleton");
	}
	
	private static class RetailSource implements SourceFunction<Tuple2<String, Integer>> {
		private static final long serialVersionUID = 1L;
		private Integer[] retail;
		private String[] brands;

		private volatile boolean isRunning = true;

		private RetailSource() {
			retail = new Integer[3];
			brands = new String[3];
			retail[0] = 3;
			retail[1] = 2;
			retail[2] = 1;
			brands[0] = "bk";
			brands[1] = "ch";
			brands[2] = "cd";
		}

		public static RetailSource create() {
			return new RetailSource();
		}

		@Override
		public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
			while (isRunning) {
				Thread.sleep(1000);
				// 0-buick, 1-chevy, 2-caddy
				for (int brand = 0; brand < brands.length; brand++) {
					Tuple2<String, Integer> record = new Tuple2<>(brands[brand], retail[brand]);
					ctx.collect(record);
				}
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}
	}
}
