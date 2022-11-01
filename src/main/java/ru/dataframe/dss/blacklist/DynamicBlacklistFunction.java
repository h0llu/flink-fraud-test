package ru.dataframe.dss.blacklist;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import ru.dataframe.dss.dto.BlacklistItem;
import ru.dataframe.dss.dto.Transaction;

public class DynamicBlacklistFunction
		extends KeyedBroadcastProcessFunction<String, Transaction, BlacklistItem, Transaction> {

	private final MapStateDescriptor<String, BlacklistItem> descriptor;

	public DynamicBlacklistFunction(MapStateDescriptor<String, BlacklistItem> descriptor) {
		this.descriptor = descriptor;
	}

	@Override
	public void processBroadcastElement(BlacklistItem blacklistItem,
										KeyedBroadcastProcessFunction<String, Transaction, BlacklistItem, Transaction>.Context context,
										Collector<Transaction> collector) throws Exception {
		BroadcastState<String, BlacklistItem> state = context.getBroadcastState(descriptor);
		state.put(blacklistItem.getClientId(), blacklistItem);
	}

	@Override
	public void processElement(Transaction transaction,
							   KeyedBroadcastProcessFunction<String, Transaction, BlacklistItem, Transaction>.ReadOnlyContext readOnlyContext,
							   Collector<Transaction> collector) throws Exception {
		ReadOnlyBroadcastState<String, BlacklistItem> state =
				readOnlyContext.getBroadcastState(descriptor);
		if (isBlacklisted(transaction, state)) {
			collector.collect(transaction);
		}
	}

	private boolean isBlacklisted(Transaction transaction,
								  ReadOnlyBroadcastState<String, BlacklistItem> state)
			throws Exception {
		return state != null && state.contains(transaction.getClientId());
	}
}
