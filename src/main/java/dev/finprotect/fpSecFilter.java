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

import org.apache.spark.util.*;

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
import org.apache.spark.sql.functions.*;
import org.apache.spark.sql.streaming.*;
import org.apache.spark.sql.kafka010.*;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.Encoders;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.OneHotEncoderEstimator;
import org.apache.spark.ml.feature.OneHotEncoderModel;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;



public final class fpSecFilter
{
	private static final Pattern delim = Pattern.compile(",");
	public static void main(String args[]) throws Exception
	{
		SparkSession spark = SparkSession
		      .builder()
		      .master("local[*]")
		      .appName("fpSecFilter")
		      .config("spark.jars","/home/fprotect/finprotect/fprotect/target/fprotect-0.1.jar")
		      .getOrCreate();
		
		StructType trSchema = new StructType().add("tid","integer")
						.add("step","integer")
						.add("type","string")
						.add("amount","float")
						.add("nameOrig","string")
						.add("oldBalanceOrig","float")
						.add("newBalanceOrig","float")
						.add("nameDest","string")
						.add("oldBalanceDest","float")
						.add("newBalanceDest","float")
						.add("recurrence","integer")
						.add("destBlacklisted","integer")
						.add("errorBalanceOrig","integer")
						.add("errorBalanceDest","integer");
		Dataset<Row> df = spark
				.readStream()
				.format("kafka")
				.option("kafka.bootstrap.servers","localhost:9092")
				.option("subscribe","fp_trdata")	//fp_trdata_clean_prim for actual
				.option("startingOffsets","earliest")	//testing only
				.option("failOnDataLoss","false")
				.load();
		df = df.select(functions.from_json(df.col("value").cast("string"),trSchema));
		df = df.select("jsontostructs(CAST(value AS STRING)).*");
    		
    		Dataset<Row> dfh = spark.read()
    			.format("jdbc")
    			.option("url","jdbc:mysql://localhost:3306/fprotect")
    			.option("driver","com.mysql.cj.jdbc.Driver")
    			.option("dbtable","trhistory")
    			.option("user","root")
    			.option("password","root")
    			.load();
    		
    		StringIndexer indexer = new StringIndexer()
    						.setInputCol("type")
    						.setOutputCol("typeIndex");
    		OneHotEncoderEstimator encoder = new OneHotEncoderEstimator()
    						.setInputCols(new String[]{"typeIndex"})
    						.setOutputCols(new String[]{"typeVec"});
    		VectorAssembler assembler = new VectorAssembler()
    						.setInputCols(new String[]{"typeVec","amount","oldBalanceOrig","newBalanceOrig","oldBalanceDest","newBalanceDest","recurrence", "errorBalanceOrig", "errorBalanceDest"})
    						.setOutputCol("features");
    		//KMeans km = new KMeans().setK(2).setSeed(1L);
    		//KMeansModel kmodel = km.fit(dfh);
    		//Dataset<Row> pred = kmodel.transform(dfh);	
    		
    		//df = df.withColumn("isFraud",functions.lit(0));
    		
    		RandomForestClassifier rf = new RandomForestClassifier()
    						.setLabelCol("isFraud")
    						.setFeaturesCol("features");			
    		
    		Pipeline pipeline = new Pipeline().setStages(new PipelineStage[]{indexer,encoder,assembler,rf});
    		System.out.println("Training...");
    		PipelineModel model = pipeline.fit(dfh);
    		
    		System.out.println("Predicting...");
    		Dataset<Row> preds = model.transform(df);
    		
    		
    		preds = preds.select(preds.col("tid"),preds.col("prediction").as("isFraud").cast("int"));
    		//preds = preds.select(preds.col("tid"),preds.col("isFraud"));
    		
    		preds.printSchema();
    		
    		//preds = preds.filter(preds.col("isFraud").equalTo(1));
    		/*
    		StreamingQuery dswc = preds.writeStream()
    				.outputMode("append")
    				.format("console")
    				.start();
    		dswc.awaitTermination();*/
    		
    		preds = preds.select(preds.col("tid").cast("string").as("key"), functions.to_json(functions.struct("*")).cast("string").as("value"));
    		
    		System.out.println("Writing to output stream...");		
    		StreamingQuery dsw = preds.writeStream()
				.format("kafka")
				.option("kafka.bootstrap.servers","localhost:9092")
				.option("topic","fp_results")
				.option("checkpointLocation","/home/fprotect/finprotect/fprotect/checkpoints")
				.start();
		dsw.awaitTermination();	
    		
    		/********************** Performance evaluation **********************/
    		/*
    		preds = preds.select(preds.col("tid"),preds.col("prediction").as("pred"));
    		
    		Dataset<Row> dfexp = spark.read()
    			.format("jdbc")
    			.option("url","jdbc:mysql://localhost:3306/fpdemo")
    			.option("driver","com.mysql.cj.jdbc.Driver")
    			.option("dbtable","detection_expected")
    			.option("user","root")
    			.option("password","root")
    			.load();
    		
    		
    		preds = preds.join(dfexp,"tid");
    		preds.printSchema();
    		
    		//long fp = preds.filter(preds.col("pred").equalTo(preds.col("isFraud"))).count();
    		//StructType schema = new StructType().add("tid","integer").add("pred","integer").add("isFraud","integer");
    		LongAccumulator t = spark.sparkContext().longAccumulator();
   		LongAccumulator total = spark.sparkContext().longAccumulator();    		
    		
    		Dataset<Float> acc = preds.map(new MapFunction<Row,Float>()
    					{
    						@Override
    						public Float call(Row rec)
    						{
    							if(rec.getInt(1) == rec.getInt(2))
    								t.add(1);
    							total.add(1);
    							//if(total % 100 == 0)
    								//System.out.println("Accuracy : "+(t/total));
    							Float acc = (float)(t.value())/total.value();
    							return acc;
    						}
    					}, Encoders.FLOAT());
    		
    		preds = preds.groupBy(preds.col("pred")).count();
    		
    		StreamingQuery dsw = preds.writeStream()
    				.outputMode("complete")
    				.format("console")
    				.start();
    		dsw.awaitTermination();
    		*/
    		/********************************************************************/
    		
    		//Dataset<Row> df_det = df.select("*");
    		//Dataset<Row> df_undet = df.except(df_det);
    		
    		//df_det = df.select(df.col("tid").cast("string").as("key"), functions.struct("*").cast("string").as("value"));
    		//df_det = acc.select(acc.col("value").cast("string").as("key"), acc.col("value").cast("string").as("value"));
    		
    		/*StreamingQuery dsw = df_det.writeStream()
				.format("kafka")
				.option("kafka.bootstrap.servers","localhost:9092")
				.option("topic","fp_trdata_det_sec")
				.option("checkpointLocation","/home/fprotect/finprotect/fprotect/checkpoints")
				.start();
		dsw.awaitTermination();*/
		
		//df_undet = df.select(df.col("tid").cast("string").as("key"), functions.struct("*").cast("string").as("value"));
    		
    		/*StreamingQuery dsw = df_undet.writeStream()
				.format("kafka")
				.option("kafka.bootstrap.servers","localhost:9092")
				.option("topic","fp_trdata_clean_sec")
				.option("checkpointLocation","/home/fprotect/finprotect/fprotect/checkpoints")
				.start();
		dsw.awaitTermination();*/
		
	}
}