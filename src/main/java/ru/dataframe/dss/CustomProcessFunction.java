package ru.dataframe.dss;

import org.apache.flink.api.common.accumulators.SimpleAccumulator;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;


public class CustomProcessFunction extends KeyedProcessFunction<String, Transaction, Transaction> {
	private final Long windowDuration;
	private final MapStateDescriptor<Long, Set<Transaction>> windowStateDescriptor =
			new MapStateDescriptor<>("windowState",
					BasicTypeInfo.LONG_TYPE_INFO,
					TypeInformation.of(new TypeHint<Set<Transaction>>() {
					})
			);
	private final SimpleAccumulator<Double> aggregator;
	private transient MapState<Long, Set<Transaction>> windowState;

	public CustomProcessFunction(Time time, SimpleAccumulator<Double> aggregator) {
		this.windowDuration = time.toMilliseconds();
		this.aggregator = aggregator;
	}

	private static <K, V> void addToStateValuesSet(MapState<K, Set<V>> mapState, K key, V value)
			throws Exception {
		Set<V> valuesSet = mapState.get(key);
		if (valuesSet == null) {
			valuesSet = new HashSet<>();
		}
		valuesSet.add(value);
		mapState.put(key, valuesSet);
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		windowState = getRuntimeContext().getMapState(windowStateDescriptor);

		getRuntimeContext().getMetricGroup()
				.meter("eventsPerSecond", new MeterView(60));
	}

	@Override
	public void processElement(Transaction currentTransaction,
							   KeyedProcessFunction<String, Transaction, Transaction>.Context context,
							   Collector<Transaction> collector) throws Exception {
		long currentEventTime = currentTransaction.getEventTime();

		// Evaluate start timestamp by window duration and current event time
		Long windowStartTimestampForEvent = currentEventTime - windowDuration;

		// Calculate average
		for (Long iteratedEventTime : windowState.keys()) {
			if (isStateValueInWindow(iteratedEventTime,
					windowStartTimestampForEvent,
					currentTransaction.getEventTime()
			)) {
				for (Transaction iteratedTransaction : windowState.get(iteratedEventTime)) {
					aggregator.add(iteratedTransaction.getAmount());
				}
			}
		}

		// Set calculated average and the rule
		currentTransaction.setClientAvg(aggregator.getLocalValue());
		currentTransaction.setRule(evaluateRule(currentTransaction.getAmount(),
				aggregator.getLocalValue()
		));
		collector.collect(currentTransaction);

		// Add transaction to state
		addToStateValuesSet(windowState, currentEventTime, currentTransaction);

		// Register timer to ensure state cleanup
		long cleanupTime = (currentEventTime * 1000) / 1000;
		context.timerService()
				.registerEventTimeTimer(cleanupTime);
	}

	private boolean isStateValueInWindow(Long stateEventTime,
										 Long windowStartTimeStampForEvent,
										 long currentEventTime) {
		return stateEventTime >= windowStartTimeStampForEvent && stateEventTime <= currentEventTime;
	}

	private boolean evaluateRule(double amount, double averageAmount) {
		return amount > 3 * averageAmount;
	}

	@Override
	public void onTimer(long timestamp,
						KeyedProcessFunction<String, Transaction, Transaction>.OnTimerContext ctx,
						Collector<Transaction> out) {
		long cleanupTimeEventThreshold = timestamp - windowDuration;
		try {
			Iterator<Long> keys = windowState.keys()
					.iterator();
			while (keys.hasNext()) {
				Long stateEventTime = keys.next();
				if (stateEventTime < cleanupTimeEventThreshold) {
					keys.remove();
				}
			}
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}
}
