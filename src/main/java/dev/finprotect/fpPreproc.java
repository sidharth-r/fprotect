package dev.finprotect;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.List;
import java.util.ArrayList;
import java.util.Collection;

import scala.Tuple2;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.Durations;


import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.functions.*;
import org.apache.spark.sql.streaming.*;
import org.apache.spark.sql.kafka010.*;
import org.apache.spark.sql.types.*;

//import com.mysql.cj.jdbc.Driver;


public final class fpPreproc
{
	private static final Pattern delim = Pattern.compile(",");
	public static void main(String args[]) throws Exception
	{
		SparkSession spark = SparkSession
		      .builder()
		      .master("local[*]")
		      .appName("fpMain")
		      .config("spark.jars","/home/fprotect/finprotect/fprotect/target/fprotect-0.1.jar")
		      .getOrCreate();
		
		StructType trSchema = new StructType().add("tid","integer").add("type","string").add("amount","float").add("nameOrig","string").add("oldBalanceOrig","float").add("newBalanceOrig","float").add("nameDest","string").add("oldBalanceDest","float").add("newBalanceDest","float").add("isFraud","integer").add("isFlaggedFraud","integer").add("recur","integer");
		Dataset<Row> df = spark
				.readStream()
				.format("kafka")
				.option("kafka.bootstrap.servers","localhost:9092")
				.option("subscribe","fp_trdata_raw")
				.option("startingOffsets","earliest")	//testing only
				.option("failOnDataLoss","false")
				.load();
		df = df.select(functions.from_json(df.col("value").cast("string"),trSchema));
		df = df.select("jsontostructs(CAST(value AS STRING)).*");
    		
    		df.printSchema();
    		
    		Dataset<Row> dfh = spark.read()
    			.format("jdbc")
    			.option("url","jdbc:mysql://localhost:3306/fprotect")
    			.option("driver","com.mysql.cj.jdbc.Driver")
    			.option("dbtable","trhistory")
    			.option("user","root")
    			.option("password","root")
    			.load();
    			
    		dfh.printSchema();
    			
    		dfh = dfh.select(dfh.col("nameOrig").as("nameOrigH"),dfh.col("nameDest").as("nameDestH"),dfh.col("tid").as("htid"));
    		dfh.show();
    		
    		dfh.createOrReplaceTempView("trHistory");
    		df.createOrReplaceTempView("trCurr");
    		
    		//Dataset<Row> c = spark.sql("SELECT trCurr.tid,count(*) FROM trHistory,trCurr GROUP BY WHERE trCurr.nameOrig = trHistory.nameOrig AND trCurr.nameDest = trHistory.nameDest");
		//c = c.select("*");
    		
    		StructType trSchemaExt = new StructType().add("tid","integer").add("recur","integer");
    		
    		Dataset<Row> dfj = df.join(dfh,df.col("nameOrig").equalTo(dfh.col("nameOrigH")).and(df.col("nameDest").equalTo(dfh.col("nameDestH"))),"left_outer");
    		//dfj = dfj.orderBy(dfj.col("tid"));
    		
    		//JavaRDD<tRecordExt> rdd = df.map(rec -> dfh.filter(dfh.col("nameOrig").equalTo(rec.getString(3))).filter(dfh.col("nameDest").equalTo(rec.getString(6))).count());
    		
    		/*RowFactory rf = new RowFactory();
    		/*Dataset<Row> dfe = df.map(new MapFunction<Row,Row>()
    					{
    						@Override
    						public Row call(Row rec)
    						{
    							long c = dfh.filter(dfh.col("nameOrig").equalTo(rec.getString(3))).filter(dfh.col("nameDest").equalTo(rec.getString(6))).count();
    							return rf.create(rec.getInt(0),c);
    						}
    					}, RowEncoder.apply(trSchemaExt));*/   		
    		
    		
    		
    		StreamingQuery dsw = dfj.writeStream()
    				.outputMode("append")
    				.format("console")
    				.start();
    		dsw.awaitTermination();
    		
    		
    		df = df.select(df.col("tid").cast("string").as("key"), functions.struct("*").cast("string").as("value"));
    		
    		/*StreamingQuery dsw = df.writeStream()
				.format("kafka")
				.option("kafka.bootstrap.servers","localhost:9092")
				.option("topic","fp_trdata")
				.option("checkpointLocation","/home/fprotect/finprotect/fprotect/checkpoints")
				.start();
		dsw.awaitTermination();*/
		
	}
	
	static class tRecord
	{
		int tid, isFraud;
		String type, nameOrig, nameDest;
		float amount, oldBalanceOrig, newBalanceOrig, oldBalanceDest, newBalanceDest;
		
		tRecord(int i, String t, float amt, String no, float obo, float nbo, String nd, float obd, float nbd, int ifr)
		{
			tid = i;
			type = t;
			amount = amt;
			nameOrig = no;
			oldBalanceOrig = obo;
			newBalanceOrig = nbo;
			nameDest = nd;
			oldBalanceDest = obd;
			newBalanceDest = nbd;
			isFraud = ifr;
		}
	}
	
	static class tRecordExt
	{
		int tid;
		int recur;
		tRecordExt(int i, int r)
		{
			tid = i;
			recur = r;
		}
	}
}


