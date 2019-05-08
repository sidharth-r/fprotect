package dev.finprotect;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;
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
import org.apache.spark.ml.classification.GBTClassifier;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.OneHotEncoderEstimator;




public final class fpSecFilter
{
	private static final Pattern delim = Pattern.compile(",");
        
        public enum Model{RANDOM_FOREST,GBT,LOGISTIC_REGRESSION};
        
	public static void main(String args[]) throws Exception
	{
            Model mdl = Model.valueOf(args[0]);
            
            Properties props = new Properties();
            try{
                props.load(new FileInputStream(new File("FProtect.properties")));  
            }
            catch(IOException e)
            {
                System.out.println("Failed to load properties file. Exiting...");
                return;
            }
            
            String sparkAppJar = props.getProperty("fprotect.classpath");
            String sparkCheckpointDir = props.getProperty("spark.checkpoints.dir.sec");
            String sparkMaster = props.getProperty("spark.master");
            String kafkaBootstrapServer = props.getProperty("kafka.bootstrap.server");
            String sqlDbUrl = props.getProperty("mysql.db.url");
            String sqlDbUser = props.getProperty("mysql.db.user");
            String sqlDbPass = props.getProperty("mysql.db.pass");
            
            SparkSession spark = SparkSession
                  .builder()
                  .master(sparkMaster)
                  .appName("fpSecFilter")
                  .config("spark.jars",sparkAppJar)
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
                            .option("kafka.bootstrap.servers",kafkaBootstrapServer)
                            .option("subscribe","fp_trdata")
                            .option("startingOffsets","earliest")	//testing only
                            .option("failOnDataLoss","false")
                            .load();
            df = df.select(functions.from_json(df.col("value").cast("string"),trSchema));
            df = df.select("jsontostructs(CAST(value AS STRING)).*");

            Dataset<Row> dfh = spark.read()
                    .format("jdbc")
                    .option("url",sqlDbUrl)
                    .option("driver","com.mysql.cj.jdbc.Driver")
                    .option("dbtable","trhistory")
                    .option("user",sqlDbUser)
                    .option("password",sqlDbPass)
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
            
            ArrayList<PipelineStage> stageList = new ArrayList<>();
            stageList.add(indexer);
            stageList.add(encoder);
            stageList.add(assembler);
            
            switch(mdl){
                case GBT:
                    GBTClassifier gbt = new GBTClassifier()
                            .setLabelCol("isFraud")
                            .setFeaturesCol("features")
                            .setMaxIter(5);
                    stageList.add(gbt);
                    break;
                case LOGISTIC_REGRESSION:
                    LogisticRegression lr = new LogisticRegression()
                            .setFeaturesCol("features")
                            .setLabelCol("isFraud")
                            .setElasticNetParam(0.8)
                            .setRegParam(0.3)
                            .setMaxIter(5);
                    stageList.add(lr);
                    break;
                default:
                case RANDOM_FOREST:
                    RandomForestClassifier rf = new RandomForestClassifier()
                                            .setLabelCol("isFraud")
                                            .setFeaturesCol("features");
                    stageList.add(rf);
                    break;
            }
            
            PipelineStage[] stages = new PipelineStage[stageList.size()];
            stageList.toArray(stages);

            Pipeline pipeline = new Pipeline().setStages(stages);
            System.out.println("Training...");
            PipelineModel model = pipeline.fit(dfh);

            System.out.println("Predicting...");
            Dataset<Row> preds = model.transform(df);    		

            preds = preds.select(preds.col("tid"),preds.col("prediction").as("isFraud").cast("int"));
            Dataset<Row> df_undet = df.filter(df.col("tid").notEqual(preds.col("tid")));
            preds = preds.select(preds.col("tid").cast("string").as("key"), functions.to_json(functions.struct("*")).cast("string").as("value"));

            System.out.println("Writing detections to topic fp_det_sec");
            //System.out.println("Writing non-detection to topic fp_clean_sec");	
            
            /*
            StreamingQuery dsw = preds.writeStream()
                    .format("console")
                    .outputMode("append")
                    .start();*/
            
            StreamingQuery dsw = preds.writeStream()
                            .format("kafka")
                            .option("kafka.bootstrap.servers",kafkaBootstrapServer)
                            .option("topic","fp_det_sec")
                            .option("checkpointLocation",sparkCheckpointDir)
                            .start();

            /*df_undet = df_undet.select(df_undet.col("tid").cast("string").as("key"), functions.struct("*").cast("string").as("value"));

            StreamingQuery dswu = df_undet.writeStream()
                            .format("kafka")
                            .option("kafka.bootstrap.servers",kafkaBootstrapServer)
                            .option("topic","fp_clean_sec")
                            .option("checkpointLocation",sparkCheckpointDir)
                            .start();*/

            dsw.awaitTermination();	
            //dswu.awaitTermination();
	}
}