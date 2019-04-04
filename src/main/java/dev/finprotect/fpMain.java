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

import java.io.File;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;


import scala.Tuple2;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.apache.spark.api.java.*;
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


public final class fpMain
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
		
		StructType trSchema = new StructType().add("tid","integer").add("type","string").add("amount","float").add("nameOrig","string").add("oldBalanceOrig","float").add("newBalanceOrig","float").add("nameDest","string").add("oldBalanceDest","float").add("newBalanceDest","float").add("isFraud","integer").add("isFlaggedFraud","integer");
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
		 	
    	
    		/*StreamingQuery dsw = df.writeStream()
    				.format("csv")
    				.option("path","/home/fprotect/finprotect/fprotect/out")
    				.option("checkpointLocation","/home/fprotect/finprotect/fprotect/checkpoints")
    				.outputMode("append")
    				.start();
    		dsw.awaitTermination();*/
    		
    		df.printSchema();
    		
    		Ruleset ruleset = new Ruleset();
    		
    		//Dataset<Row> df_det = df.select("*").where("isFraud = 1");
    		Dataset<Row> df_det = df.select("*");
    		
    		for(int i = 0; i < ruleset.ruleCount; i++)
    		{
    			Rule rule = ruleset.rules.get(i);
    			switch(rule.operator)
    			{
    				case "lessThan":
    					df_det = df_det.select("*").filter(df_det.col(rule.attribute).lt(rule.value));
    					break;
    				case "lessThanOrEqualTo":
    					df_det = df_det.select("*").filter(df_det.col(rule.attribute).leq(rule.value));
    					break;
    				case "greaterThan":
    					df_det = df_det.select("*").filter(df_det.col(rule.attribute).gt(rule.value));
    					break;
    				case "greaterThanOrEqualTo":
    					df_det = df_det.select("*").filter(df_det.col(rule.attribute).geq(rule.value));
    					break;
    				case "equalTo":
    					df_det = df_det.select("*").filter(df_det.col(rule.attribute).equalTo(rule.value));
    					break;
    				case "notEqualTo":
    					df_det = df_det.select("*").filter(df_det.col(rule.attribute).notEqual(rule.value));
    					break;
    				default:
    			}
    		}    		
    		
    		
    		StreamingQuery dsw = df_det.writeStream()
    				.outputMode("append")
    				.format("console")
    				.start();
    		dsw.awaitTermination();
    		
    		df_det = df.select(df.col("tid").cast("string").as("key"), functions.struct("*").cast("string").as("value"));
    		
    		/*StreamingQuery dsw = df_det.writeStream()
				.format("kafka")
				.option("kafka.bootstrap.servers","localhost:9092")
				.option("topic","fp_trdata_det")
				.option("checkpointLocation","/home/fprotect/finprotect/fprotect/checkpoints")
				.start();
		dsw.awaitTermination();*/
		
	}
	
	static class Rule
	{
		int rid;
		String attribute;
		String operator;
		Object value;
		String valueType;
		Object value2;
		String value2Type;
		
		Rule(int r, String attr, String op, String val, String valType, String val2, String val2Type)
		{
			rid = r;
			attribute = attr;
			operator = op;
			valueType = valType;
			switch(valType)
			{
				case "int":
					value = Integer.parseInt(val);
					if(value2 == "")
						value2 = 0;
					else
						value2 = Integer.parseInt(val2);
					break;
				case "float":
					value = Float.parseFloat(val);
					if(value2 == "")
						value2 = 0;
					else
						value2 = Float.parseFloat(val2);
				case "string":
				default:
					value = val;
					value2 = val2;
			}
			value2Type = val2Type;
		}
	}
	
	static class Ruleset
	{
		public int ruleCount;
		public ArrayList<Rule> rules;
		
		Ruleset()
		{
			ruleCount = 0;
			rules = new ArrayList<Rule>();
			try
			{
				loadRuleset();
			}
			catch(Exception e)
			{
				System.out.println(e);
			}
		}
		
		void loadRuleset() throws Exception
		{
			File ruleFile = new File("/home/fprotect/finprotect/fprotect/ruleset.xml");
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			Document doc = db.parse(ruleFile);
			
			doc.getDocumentElement().normalize();
			NodeList nodes = doc.getElementsByTagName("rule");
			for(int i = 0; i < nodes.getLength(); i++)
			{
				Element e = (Element)nodes.item(i);
				int id = Integer.parseInt(getTag(e,"ruleId"));
				String attr = getTag(e,"attribute");
				String op = getTag(e,"operator");
				String val = getTag(e,"value");
				String valType = getTag(e,"valueType");
				String val2 = getTag(e,"value2");
				String val2Type = getTag(e,"value2Type");
				Rule rule = new Rule(id,attr,op,val,valType,val2,val2Type);

				rules.add(rule);
				ruleCount++;
			}
		}
		
		String getTag(Element element, String tag)
		{
			String v;
			try
			{
				v = element.getElementsByTagName(tag).item(0).getChildNodes().item(0).getNodeValue();
			}
			catch(Exception e)
			{
				v = "";
			}
			return v;
		}
	}
}


