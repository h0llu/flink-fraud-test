package ru.dataframe.dss.cases;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import ru.dataframe.dss.average.AverageAccumulator;
import ru.dataframe.dss.average.AverageFraudCheckingFunction;
import ru.dataframe.dss.dto.Transaction;
import ru.dataframe.dss.serialization.TransactionDeserializationSchema;
import ru.dataframe.dss.utils.ConfigLoader;
import ru.dataframe.dss.utils.FlinkProvider;

import java.util.Properties;

public class AverageAndUnion {
	public static void main(String[] args) throws Exception {
		Properties config = ConfigLoader.load("union_config.properties");

		String source1Topic = config.getProperty("source1.topic");
		String source2Topic = config.getProperty("source2.topic");
		String sourceBootstrapServer = config.getProperty("source.bootstrap-server");
		String sinkTopic = config.getProperty("sink.topic");
		String sinkBootstrapServer = config.getProperty("sink.bootstrap-server");
		Time windowDuration = Time.seconds(Long.parseLong(config.getProperty("window-duration")));

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Transaction> dataStream1 = FlinkProvider.getDataStream(env,
				source1Topic,
				sourceBootstrapServer,
				new TransactionDeserializationSchema(),
				WatermarkStrategy.<Transaction>forMonotonousTimestamps()
						.withTimestampAssigner((transaction, timestamp) -> transaction.getEventTime()),
				"Transaction Source 1"
		);

		DataStream<Transaction> dataStream2 = FlinkProvider.getDataStream(env,
				source2Topic,
				sourceBootstrapServer,
				new TransactionDeserializationSchema(),
				WatermarkStrategy.<Transaction>forMonotonousTimestamps()
						.withTimestampAssigner((transaction, timestamp) -> transaction.getEventTime()),
				"Transaction Source 2"
		);

		DataStream<Transaction> processedDataStream1 = dataStream1.keyBy(Transaction::getClientId)
				.process(new AverageFraudCheckingFunction(windowDuration,
						new AverageAccumulator()
				));

		DataStream<Transaction> processedDataStream2 = dataStream2.keyBy(Transaction::getClientId)
				.process(new AverageFraudCheckingFunction(windowDuration,
						new AverageAccumulator()
				));

		processedDataStream1.union(processedDataStream2)
				.map(String::valueOf)
				.sinkTo(FlinkProvider.getSink(sinkTopic,
						sinkBootstrapServer,
						new SimpleStringSchema()
				));


		env.execute();
	}
}
