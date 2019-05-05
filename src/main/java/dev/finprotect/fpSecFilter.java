package dev.finprotect;

import java.util.regex.Pattern;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.*;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.OneHotEncoderEstimator;




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
				.option("subscribe","fp_trdata")
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
    		
    		RandomForestClassifier rf = new RandomForestClassifier()
    						.setLabelCol("isFraud")
    						.setFeaturesCol("features");			
    		
    		Pipeline pipeline = new Pipeline().setStages(new PipelineStage[]{indexer,encoder,assembler,rf});
    		System.out.println("Training...");
    		PipelineModel model = pipeline.fit(dfh);
    		
    		System.out.println("Predicting...");
    		Dataset<Row> preds = model.transform(df);    		
    		
    		preds = preds.select(preds.col("tid"),preds.col("prediction").as("isFraud").cast("int"));
    		Dataset<Row> df_undet = df.filter(df.col("tid").notEqual(preds.col("tid")));
    		preds = preds.select(preds.col("tid").cast("string").as("key"), functions.to_json(functions.struct("*")).cast("string").as("value"));
    		
                System.out.println("Writing detections to topic fp_det_prim");
                System.out.println("Writing non-detection to topic fp_clean_prim");		
    		StreamingQuery dsw = preds.writeStream()
				.format("kafka")
				.option("kafka.bootstrap.servers","localhost:9092")
				.option("topic","fp_det_sec")
				.option("checkpointLocation","/home/fprotect/finprotect/fprotect/checkpoints")
				.start();
		
		df_undet = df_undet.select(df_undet.col("tid").cast("string").as("key"), functions.struct("*").cast("string").as("value"));
    		
    		StreamingQuery dswu = df_undet.writeStream()
				.format("kafka")
				.option("kafka.bootstrap.servers","localhost:9092")
				.option("topic","fp_clean_sec")
				.option("checkpointLocation","/home/fprotect/finprotect/fprotect/checkpoints")
				.start();
                
                dsw.awaitTermination();	
		dswu.awaitTermination();
	}
}