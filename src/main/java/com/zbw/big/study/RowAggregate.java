package com.zbw.big.study;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import com.zbw.big.study.pojo.DosKafkaMessagePojo;

public class RowAggregate implements AggregateFunction<DosKafkaMessagePojo, Tuple2<String, Long>, Tuple2<String, Long>> {

	@Override
	public Tuple2<String, Long> createAccumulator() {
		return new Tuple2<>("", 0L);
	}

	@Override
	public Tuple2<String, Long> add(DosKafkaMessagePojo value, Tuple2<String, Long> accumulator) {
		return new Tuple2<>(value.getTable(), accumulator.f1 + 1l);
	}

	@Override
	public Tuple2<String, Long> getResult(Tuple2<String, Long> accumulator) {
		return accumulator;
	}

	@Override
	public Tuple2<String, Long> merge(Tuple2<String, Long> a, Tuple2<String, Long> b) {
		return new Tuple2<>(a.f0, a.f1 + b.f1);
	}

}
