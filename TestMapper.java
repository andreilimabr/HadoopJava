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
	/* Classe mapper sempre � implementada com  pares de Chave-Valor:
	 * <CHAVE_ENTRADA,VALOR_ENTRADA,CHAVE_SAIDA,VALOR_SAIDA>
	 */
	
	// vari�vel usada para incrementar o valor
	private final static IntWritable one = new IntWritable(1);
	
	// le a linha com o leitor de registros default
	public void map(LongWritable key, Text value,
			OutputCollector<Text,IntWritable> output, Reporter reporter) throws IOException {
		/*
		 * Key = contagem cumulativa dos caracteres da linha 
		 * value = linha inteira, como Text, at� o caracter de final de linha
		 * output = classe que  coleta a saida implementada com os tipos da saida
		 */
		 // converte linha para string
		String tempString = value.toString();
		// converte para uma array de strings sepadados por ponto e v�rgula 
		String[] SingleBookData = tempString.split("\";\"");
		// coleta a saida, posicao 3 da matriz � o ano da publica��o
		output.collect(new Text(SingleBookData[3]), one);
	}

	

}
