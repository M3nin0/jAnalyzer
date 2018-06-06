package analyzer;

import org.apache.spark.api.java.function.Function2;

public class LogAnalyzerUtils {
	// Função para realizar a soma de dois valores inteiros
	@SuppressWarnings("unused")
	static Function2<Long, Long, Long> SUM_REDUCER = (a, b) -> a + b;
}
