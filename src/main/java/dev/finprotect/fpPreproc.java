package dev.finprotect;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Collections;
import java.util.Properties;
import java.util.Arrays;
import java.util.UUID;
import java.util.List;
import java.util.stream.*;

import com.fasterxml.jackson.databind.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class fpPreproc
{

	static class TRecord
	{
		public int tid;
		public String type;
		public float amount;
		public String nameOrig;
		public float oldBalanceOrig;
		public float newBalanceOrig;
		public String nameDest;
		public float oldBalanceDest;
		public float newBalanceDest;
		//public int isFraud;
		//public int isFlaggedFraud;
		
		public TRecord()
		{
		}
		
		public TRecord(int i, String t, float a, String no, float obo, float nbo, String nd, float obd, float nbd)
		{
			tid = i;
			type = t;
			amount = a;
			nameOrig = no;
			oldBalanceOrig = obo;
			newBalanceOrig = nbo;
			nameDest = nd;
			oldBalanceDest = obd;
			newBalanceDest = nbd;
			//isFraud = ifr;
			//isFlaggedFraud = iffr;
		}
	}
	
	static class TRecordExt
	{
		public int tid;
		public String type;
		public float amount;
		public String nameOrig;
		public float oldBalanceOrig;
		public float newBalanceOrig;
		public String nameDest;
		public float oldBalanceDest;
		public float newBalanceDest;
		//public int isFraud;
		//public int isFlaggedFraud;
		public int recurrence;
		public int destBlacklisted;
		
		public TRecordExt()
		{
		}
		
		public TRecordExt(TRecord tr, int recur, int dbl)
		{
			tid = tr.tid;
			type = tr.type;
			amount = tr.amount;
			nameOrig = tr.nameOrig;
			oldBalanceOrig = tr.oldBalanceOrig;
			newBalanceOrig = tr.newBalanceOrig;
			nameDest = tr.nameDest;
			oldBalanceDest = tr.oldBalanceDest;
			newBalanceDest = tr.newBalanceDest;
			//isFraud = tr.ifr;
			//isFlaggedFraud = tr.iffr;
			recurrence = recur;
			destBlacklisted = dbl;
		}
	}
	
	
	public static void main(String[] args) throws Exception
	{
		Properties config = new Properties();
		config.put("client.id","fpPreprocIn");
		config.put("bootstrap.servers","localhost:9092");
		config.put("group.id",UUID.randomUUID().toString());
		config.put("enable.auto.commit", "true");
      		config.put("auto.commit.interval.ms", "1000");
      		config.put("auto.offset.reset","earliest");
      		config.put("session.timeout.ms", "30000");
		config.put("log.dirs","/home/fprotect/finprotect/kafka-logs");
		config.put("acks","all");
		config.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
      		config.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
		
		Consumer<String,String> cons = new KafkaConsumer<String,String>(config);
		cons.subscribe(Arrays.asList("fp_trdata_raw"));
		
		
		Properties prodConfig = new Properties();
		prodConfig.put("bootstrap.servers","localhost:9092");
		prodConfig.put("client.id","fpPreprocOut");
		prodConfig.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
      		prodConfig.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
      		
      		Producer<String,String> prod = new KafkaProducer<String,String>(prodConfig);
		
		
		while(true)
		{
			//System.out.println("sadasd");
			ConsumerRecords<String,String> crecs = cons.poll(1);
			for(ConsumerRecord<String,String> crec : crecs)
			{
				ObjectMapper mapper = new ObjectMapper();
				TRecord tr = mapper.readValue(crec.value(),TRecord.class);
				TRecordExt tre = new TRecordExt();
				
				try
				{
					tre = genExtRecord(tr);
				}
				catch(Exception e)
				{
					System.out.println(e);
				}
				
				String value = mapper.writeValueAsString(tre);
				
				String key = String.valueOf(System.currentTimeMillis());
				ProducerRecord<String,String> prec = new ProducerRecord<String,String>("fp_trdata",key,value);
				prod.send(prec);
				prod.flush();
			}
		}
	}
	
	static TRecordExt genExtRecord(TRecord tr) throws Exception
	{
		//Class.forName("com.mysql.cj.jdbc.Driver");
		
		Connection conn = DriverManager.getConnection("jdbc:mysql://localhost/fprotect?user=root&password=root");
		
		PreparedStatement query = conn.prepareStatement("select count(*) from trhistory_unlabeled where nameOrig = ? and nameDest = ?");
		query.setString(1,tr.nameOrig);
		query.setString(2,tr.nameDest);		
		ResultSet res = query.executeQuery();
		res.first();
		int recurrence = res.getInt(res.getMetaData().getColumnName(1));
		
		query = conn.prepareStatement("select * from blacklist where accountNumber = ?");
		query.setString(1,tr.nameDest);
		res = query.executeQuery();
		res.first();
		int destBlacklisted = res.isBeforeFirst() ? 1 : 0;

		return new TRecordExt(tr,recurrence,destBlacklisted);
	}
}