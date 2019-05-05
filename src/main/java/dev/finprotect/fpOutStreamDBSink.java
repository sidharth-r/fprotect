package dev.finprotect;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.Consumer;
import java.util.Properties;
import java.util.Arrays;
import java.util.UUID;

import com.fasterxml.jackson.databind.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

public class fpOutStreamDBSink
{

	static class TResult
	{
		public int tid;
		public int isFraud;
		
		public TResult()
		{
		}
		
		public TResult(int i, int ifr)
		{
			tid = i;
			isFraud = ifr;
		}
	}	
	
	public static void main(String[] args) throws Exception
	{
            String topic = args[0];
            String table = args[1];
            
            Properties config = new Properties();
            config.put("client.id","fpStreamSink");
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

            Consumer<String,String> cons = new KafkaConsumer<>(config);
            cons.subscribe(Arrays.asList(topic));	

            Connection conn = DriverManager.getConnection("jdbc:mysql://localhost/fprotect?user=root&password=root");
            Statement stmt = conn.createStatement();
            stmt.executeUpdate("delete from "+table);
            PreparedStatement query = conn.prepareStatement("insert into "+table+" values(?,?)");	

            while(true)
            {
                ConsumerRecords<String,String> crecs = cons.poll(1);
                for(ConsumerRecord<String,String> crec : crecs)
                {
                    ObjectMapper mapper = new ObjectMapper();
                    TResult tr = mapper.readValue(crec.value(),TResult.class);

                    try
                    {					
                        query.setInt(1,tr.tid);
                        query.setInt(2,tr.isFraud);		
                        query.executeUpdate();
                    }
                    catch(SQLException e)
                    {
                        System.out.println(e);
                    }
                }
            }
	}
}