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
import java.io.File;
import java.io.FileInputStream;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class fpPreproc
{
    static Connection conn;

    static class TRecord
    {
            public int tid;
            public int step;
            public String type;
            public float amount;
            public String nameOrig;
            public float oldBalanceOrig;
            public float newBalanceOrig;
            public String nameDest;
            public float oldBalanceDest;
            public float newBalanceDest;
            //public int isFraud;

            public TRecord()
            {
            }

            public TRecord(int i, int s, String t, float a, String no, float obo, float nbo, String nd, float obd, float nbd)
            {
                    tid = i;
                    step = s;
                    type = t;
                    amount = a;
                    nameOrig = no;
                    oldBalanceOrig = obo;
                    newBalanceOrig = nbo;
                    nameDest = nd;
                    oldBalanceDest = obd;
                    newBalanceDest = nbd;
                    //isFraud = ifr;
            }
    }

    static class TRecordExt
    {
            public int tid;
            public int step;
            public String type;
            public float amount;
            public String nameOrig;
            public float oldBalanceOrig;
            public float newBalanceOrig;
            public String nameDest;
            public float oldBalanceDest;
            public float newBalanceDest;
            //public int isFraud;
            public int recurrence;
            public int destBlacklisted;
            public int errorBalanceOrig;
            public int errorBalanceDest;

            public TRecordExt()
            {
            }

            public TRecordExt(TRecord tr, int recur, int dbl)
            {
                    tid = tr.tid;
                    step = tr.step;
                    type = tr.type;
                    amount = tr.amount;
                    nameOrig = tr.nameOrig;
                    oldBalanceOrig = tr.oldBalanceOrig;
                    newBalanceOrig = tr.newBalanceOrig;
                    nameDest = tr.nameDest;
                    oldBalanceDest = tr.oldBalanceDest;
                    newBalanceDest = tr.newBalanceDest;
                    recurrence = recur;
                    destBlacklisted = dbl;
                    errorBalanceOrig = (int)(newBalanceOrig + amount - oldBalanceOrig);
                    errorBalanceDest = (int)(oldBalanceDest + amount - newBalanceDest);
                    //isFraud = tr.isFraud;
            }
    }


    public static void main(String[] args) throws Exception
    {
        Properties props = new Properties();
        try{
            props.load(new FileInputStream(new File("FProtect.properties")));  
        }
        catch(Exception e)
        {
            System.out.println("Failed to load properties file. Exiting...");
            return;
        }
        
        String sqlConnStr = new StringBuilder()
                .append(props.getProperty("mysql.db.url"))
                .append("?")
                .append("user=")
                .append(props.getProperty("mysql.db.user"))
                .append("&password=")
                .append(props.getProperty("mysql.db.pass"))
                .toString();
        conn = DriverManager.getConnection(sqlConnStr);

        String kafkaBootstrapServer = props.getProperty("kafka.bootstrap.server");
        String kafkaLogDir = props.getProperty("kafka.log.dir");

        Properties config = new Properties();
        config.put("client.id","fpPreprocIn");
        config.put("bootstrap.servers",kafkaBootstrapServer);
        config.put("group.id",UUID.randomUUID().toString());
        config.put("enable.auto.commit", "true");
        config.put("auto.commit.interval.ms", "1000");
        config.put("auto.offset.reset","earliest");
        config.put("session.timeout.ms", "30000");
        config.put("log.dirs",kafkaLogDir);
        config.put("acks","all");
        config.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        config.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");

        Consumer<String,String> cons = new KafkaConsumer<String,String>(config);
        cons.subscribe(Arrays.asList("fp_trdata_raw"));


        Properties prodConfig = new Properties();
        prodConfig.put("bootstrap.servers",kafkaBootstrapServer);
        prodConfig.put("client.id","fpPreprocOut");
        prodConfig.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        prodConfig.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        Producer<String,String> prod = new KafkaProducer<String,String>(prodConfig);


        while(true)
        {
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