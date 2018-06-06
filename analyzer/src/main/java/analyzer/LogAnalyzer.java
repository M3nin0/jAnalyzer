package analyzer;

import java.util.Comparator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import log.ApacheAccessLog;

/**
 * Classe baseada na documentação do databricks
 * 
 * @author Felipe
 *
 */
public class LogAnalyzer {

	private JavaSparkContext sparkContext = null;

	public LogAnalyzer() {
		// Criando um contexto
		SparkConf conf = new SparkConf().setAppName("Analizador de log");
		sparkContext = new JavaSparkContext(conf);
	}

	/**
	 * Método para carregar um arquivo de texto
	 * 
	 * @param file
	 * @return
	 */
	public JavaRDD<ApacheAccessLog> readTextFile(String file) {

		JavaRDD<String> logLines = sparkContext.textFile(file);
		/*
		 * Transforma cada linha em um objeto ApacheAccessLog, a função cache é
		 * utilizada já que este objeto será muito manipulado
		 */
		return logLines.map(ApacheAccessLog::parseFromLogLine).cache();
	}

	/**
	 * Método para o cálculo de algums parâmetros estatísticos dos dados recebidos
	 */
	public void logStat(JavaRDD<ApacheAccessLog> accessLog) {
		// Extraindo os tamanhos dos conteúdos
		// Utiliza o cache para evitar que operações como estas sejam recalculadas
		JavaRDD<Long> contentSize = accessLog.map(ApacheAccessLog::getContentSize).cache();

		System.out.println(String.format("Tamanho do conteúdo Média: %s, Mínimo: %s, Máximo: %s",
				contentSize.reduce(LogAnalyzerUtils.SUM_REDUCER) / contentSize.count(),
				contentSize.min(Comparator.naturalOrder()), contentSize.max(Comparator.naturalOrder())));
	}
	/**
	 * ToDo: Adicionar contagens dos códigos
	 */
}
