package br.com.usd.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class TestMapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,IntWritable> {
	/* Classe mapper sempre é implementada com  pares de Chave-Valor:
	 * <CHAVE_ENTRADA,VALOR_ENTRADA,CHAVE_SAIDA,VALOR_SAIDA>
	 */
	
	// variável usada para incrementar o valor
	private final static IntWritable one = new IntWritable(1);
	
	// le a linha com o leitor de registros default
	public void map(LongWritable key, Text value,
			OutputCollector<Text,IntWritable> output, Reporter reporter) throws IOException {
		/*
		 * Key = contagem cumulativa dos caracteres da linha 
		 * value = linha inteira, como Text, até o caracter de final de linha
		 * output = classe que  coleta a saida implementada com os tipos da saida
		 */
		 // converte linha para string
		String tempString = value.toString();
		// converte para uma array de strings sepadados por ponto e vírgula 
		String[] SingleBookData = tempString.split("\";\"");
		// coleta a saida, posicao 3 da matriz é o ano da publicação
		output.collect(new Text(SingleBookData[3]), one);
	}

	

}
