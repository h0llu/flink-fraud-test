package ru.dataframe.dss.python;

import org.apache.commons.io.IOUtils;

import java.nio.charset.StandardCharsets;

public class PythonProcessFunction {
	public static void testProcess() throws Exception {
		ProcessBuilder processBuilder =
				new ProcessBuilder("python3", "src/main/resources/script.py");
		processBuilder.redirectErrorStream(true);
		Process process = processBuilder.start();
		String result = IOUtils.toString(process.getInputStream(), StandardCharsets.UTF_8);
		System.out.println(result);
	}
}
