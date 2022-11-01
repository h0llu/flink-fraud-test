package ru.dataframe.dss.blacklist;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import ru.dataframe.dss.dto.BlacklistItem;
import ru.dataframe.dss.dto.Transaction;

import java.util.List;
import java.util.Objects;

public class FilterBlacklistedFunction
		extends KeyedProcessFunction<String, Transaction, Transaction>
		implements CheckpointedFunction {
	private transient final List<BlacklistItem> blacklist;
	private transient ListState<BlacklistItem> blacklistItems;

	public FilterBlacklistedFunction(List<BlacklistItem> blacklist) {
		this.blacklist = blacklist;
		System.out.println(blacklist);
	}

	@Override
	public void snapshotState(FunctionSnapshotContext functionSnapshotContext) {
	}

	@Override
	public void initializeState(FunctionInitializationContext functionInitializationContext)
			throws Exception {
		ListStateDescriptor<BlacklistItem> blacklistDescriptor = new ListStateDescriptor<>(
				"blacklist",
				TypeInformation.of(new TypeHint<BlacklistItem>() {
				})
		);
		blacklistItems = functionInitializationContext.getOperatorStateStore()
				.getListState(blacklistDescriptor);
		blacklistItems.clear();
		blacklistItems.addAll(blacklist);
	}

	@Override
	public void processElement(Transaction transaction,
							   KeyedProcessFunction<String, Transaction, Transaction>.Context context,
							   Collector<Transaction> collector) throws Exception {
		if (!isBlacklisted(transaction)) {
			collector.collect(transaction);
		}
	}

	private boolean isBlacklisted(Transaction transaction) throws Exception {
		for (BlacklistItem item : blacklistItems.get()) {
			if (Objects.equals(transaction.getClientId(), item.getClientId())) {
				return item.isBlacklisted();
			}
		}
		return false;
	}
}
