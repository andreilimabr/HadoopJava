package br.com.usd.hadoop;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;

public class BookXDriver {
	public static void main(String[] args) {
		//cliente do job do mapreduce
		JobClient client = new JobClient();
		// configurações do job
		JobConf conf = new JobConf(BookXDriver.class);
		// nome do job
		conf.setJobName("BookCroossing1.0");
		// Tipo de dados da saída da chave e valor
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		// seta as classes de map e reduce
		conf.setMapperClass(TestMapper.class);
		conf.setReducerClass(TestReducer.class);
		//formatos de tipos de dados de entrada e saida
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		// especifica os DIRETORIOS (não arquivos) de entrada e saida
		FileInputFormat.setInputPaths(conf,new Path(args[0]));
		FileOutputFormat.setOutputPath(	conf,new Path(args[1]));
		
		// seta as configurações no cliente
		client.setConf(conf);
		// realiza o job
		try {
			// rodando o job com as configurações que fizemos
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
