package br.com.usd.hadoop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class TestReducer extends MapReduceBase implements Reducer<Text,IntWritable,Text,IntWritable> {
	/* Classe reducer sempre é implementada com  pares de Chave-Valor:
	 * <CHAVE_ENTRADA,VALOR_ENTRADA,CHAVE_SAIDA,VALOR_SAIDA>
	 */
	public void reduce(Text _key, Iterator<IntWritable> values,
			OutputCollector<Text,IntWritable> output, Reporter reporter) throws IOException {
		/*
		 * _key = chave que vem do mapper
		 * values = Iterator dos valores do mapper
		 */
		// replace KeyType with the real type of your key
		Text key = _key;
		
		// queremos saber a frequencia simples dos valores
		int frequencyForYear = 0;
		
		while (values.hasNext()) {
			// replace ValueType with the real type of your value
			IntWritable value = (IntWritable) values.next();
			// soma a frequencia
			frequencyForYear += value.get();

			// process value
			
		}
		// coleta a saida, fazendo a redução
		output.collect(key, new IntWritable(frequencyForYear));
	}

}
