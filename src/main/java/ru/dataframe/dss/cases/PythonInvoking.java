package ru.dataframe.dss.cases;

import org.apache.commons.io.IOUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import ru.dataframe.dss.dto.BlacklistItem;
import ru.dataframe.dss.dto.Transaction;
import ru.dataframe.dss.python.PythonProcessFunction;
import ru.dataframe.dss.serialization.TransactionDeserializationSchema;
import ru.dataframe.dss.utils.ConfigLoader;
import ru.dataframe.dss.utils.FlinkProvider;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;

public class PythonInvoking {
	public static void main(String[] args) throws Exception {
		Properties config = ConfigLoader.load("average_config.properties");
		List<BlacklistItem> blacklist = BlacklistItem.getBlacklist("blacklist.txt");

		String sourceTopic = config.getProperty("source.topic");
		String sourceBootstrapServer = config.getProperty("source.bootstrap-server");
		String sinkTopic = config.getProperty("sink.topic");
		String sinkBootstrapServer = config.getProperty("sink.bootstrap-server");

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Transaction> dataStream = FlinkProvider.getDataStream(env,
				sourceTopic,
				sourceBootstrapServer,
				new TransactionDeserializationSchema(),
				WatermarkStrategy.<Transaction>forMonotonousTimestamps()
						.withTimestampAssigner((transaction, timestamp) -> transaction.getEventTime()),
				"Transaction Source"
		);

		dataStream.keyBy(Transaction::getClientId)
				.process(new PythonProcessFunction())
				.map(String::valueOf)
				.sinkTo(FlinkProvider.getSink(sinkTopic,
						sinkBootstrapServer,
						new SimpleStringSchema()
				));

		env.execute();
	}

	public static void testProcess() throws Exception {
		ProcessBuilder processBuilder =
				new ProcessBuilder("python3", "src/main/resources/script.py");
		processBuilder.redirectErrorStream(true);
		Process process = processBuilder.start();
		String result = IOUtils.toString(process.getInputStream(), StandardCharsets.UTF_8);
		System.out.println(result);
	}
}
