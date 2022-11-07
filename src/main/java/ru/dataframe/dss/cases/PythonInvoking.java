package ru.dataframe.dss.cases;

import jep.Interpreter;
import jep.JepException;
import jep.SharedInterpreter;
import org.apache.commons.io.IOUtils;
import ru.dataframe.dss.dto.Transaction;

import java.nio.charset.StandardCharsets;

public class PythonInvoking {
	public static void main(String[] args) throws Exception {
//		Properties config = ConfigLoader.load("average_config.properties");
//		List<BlacklistItem> blacklist = BlacklistItem.getBlacklist("blacklist.txt");
//
//		String sourceTopic = config.getProperty("source.topic");
//		String sourceBootstrapServer = config.getProperty("source.bootstrap-server");
//		String sinkTopic = config.getProperty("sink.topic");
//		String sinkBootstrapServer = config.getProperty("sink.bootstrap-server");
//
//		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//		DataStream<Transaction> dataStream = FlinkProvider.getDataStream(env,
//				sourceTopic,
//				sourceBootstrapServer,
//				new TransactionDeserializationSchema(),
//				WatermarkStrategy.<Transaction>forMonotonousTimestamps()
//						.withTimestampAssigner((transaction, timestamp) -> transaction.getEventTime()),
//				"Transaction Source"
//		);
//
//		dataStream.keyBy(Transaction::getClientId)
//				.process(new PythonProcessFunction())
//				.map(String::valueOf)
//				.sinkTo(FlinkProvider.getSink(sinkTopic,
//						sinkBootstrapServer,
//						new SimpleStringSchema()
//				));
//
//		env.execute();
		testJep();
	}

	public static void testProcess() throws Exception {
		ProcessBuilder processBuilder =
				new ProcessBuilder("python3", "src/main/resources/script.py");
		processBuilder.redirectErrorStream(true);
		Process process = processBuilder.start();
		String result = IOUtils.toString(process.getInputStream(), StandardCharsets.UTF_8);
		System.out.println(result);
	}

	public static void testJep() {
		Transaction transaction = new Transaction(1, "123", "aaa", 12345L, 20000);
		System.out.println(transaction);
		try (Interpreter interpreter = new SharedInterpreter()) {
			interpreter.set("transaction", transaction);
			interpreter.runScript("src/main/resources/script.py");
			transaction = interpreter.getValue("transaction", Transaction.class);
		} catch (JepException e) {
			e.printStackTrace();
		}
		System.out.println(transaction);
	}
}
