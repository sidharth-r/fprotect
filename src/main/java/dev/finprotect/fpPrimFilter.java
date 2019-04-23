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


public final class fpPrimFilter
{
	private static final Pattern delim = Pattern.compile(",");
	public static void main(String args[]) throws Exception
	{
		SparkSession spark = SparkSession
		      .builder()
		      .master("local[*]")
		      .appName("fpPrimFilter")
		      .config("spark.jars","/home/fprotect/finprotect/fprotect/target/fprotect-0.1.jar")
		      .getOrCreate();
		
		StructType trSchema = new StructType().add("tid","integer")
						.add("type","string")
						.add("amount","float")
						.add("nameOrig","string")
						.add("oldBalanceOrig","float")
						.add("newBalanceOrig","float")
						.add("nameDest","string")
						.add("oldBalanceDest","float")
						.add("newBalanceDest","float")
						.add("recurrence","integer")
						.add("destBlacklisted","integer");
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
    		
    		Ruleset ruleset = new Ruleset();
    		
    		Dataset<Row> df_det = df.select("*");
    		Dataset<Row> dft = df.select("*");
    		
    		/*
    		for(int i = 0; i < ruleset.ruleCount; i++)
    		{    			
    			Rule rule = ruleset.rules.get(i);
    			if(rule.ruleType.equals("basic"))
    			{
	    			switch(rule.operator)
	    			{
	    				case "lessThan":
	    					dft = df.select("*").filter(df.col(rule.attribute).lt(rule.value));
	    					break;
	    				case "lessThanOrEqualTo":
	    					dft = df.select("*").filter(df.col(rule.attribute).leq(rule.value));
	    					break;
	    				case "greaterThan":
	    					dft = df.select("*").filter(df.col(rule.attribute).gt(rule.value));
	    					break;
	    				case "greaterThanOrEqualTo":
	    					dft = df.select("*").filter(df.col(rule.attribute).geq(rule.value));
	    					break;
	    				case "equalTo":
	    					dft = df.select("*").filter(df.col(rule.attribute).equalTo(rule.value));
	    					break;
	    				case "notEqualTo":
	    					dft = df.select("*").filter(df.col(rule.attribute).notEqual(rule.value));
	    					break;
	    				default:
	    			}
	    			
	    			if(i == 0)
	    				df_det = dft;
	    			else
		    			df_det.union(dft);
		    	}
		    	else if(rule.ruleType.equals("compound"))
		    	{
		    		
		    	}
    		}  */
    		
    		df.createOrReplaceTempView("trdata");
    		System.out.println(ruleset.genQuery("trdata"));
    		df_det = spark.sql(ruleset.genQuery("trdata"));    		
    		
    		StreamingQuery dsw = df_det.writeStream()
    				.outputMode("append")
    				.format("console")
    				.start();
    		dsw.awaitTermination();
    		
    		Dataset<Row> df_undet = df.except(df_det);
    		
    		df_det = df.select(df.col("tid").cast("string").as("key"), functions.struct("*").cast("string").as("value"));
    		
    		/*StreamingQuery dsw = df_det.writeStream()
				.format("kafka")
				.option("kafka.bootstrap.servers","localhost:9092")
				.option("topic","fp_trdata_det_prim")
				.option("checkpointLocation","/home/fprotect/finprotect/fprotect/checkpoints")
				.start();
		dsw.awaitTermination();*/
		
		df_undet = df.select(df.col("tid").cast("string").as("key"), functions.struct("*").cast("string").as("value"));
    		
    		/*StreamingQuery dsw = df_undet.writeStream()
				.format("kafka")
				.option("kafka.bootstrap.servers","localhost:9092")
				.option("topic","fp_trdata_clean_prim")
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
		
		String ruleType;
		String connector;
		Rule subRule1, subRule2;
		
		Rule()
		{
		}
		
		Rule(int r, String attr, String op, String val, String valType, String val2, String val2Type)
		{
			rid = r;
			ruleType = "basic";
			attribute = attr;
			operator = op;
			valueType = valType;
			switch(valType)
			{
				case "int":
					value = Integer.parseInt(val);
					if(val2.equals(""))
						value2 = 0;
					else
						value2 = Integer.parseInt(val2);
					break;
				case "float":
					value = Float.parseFloat(val);
					if(val2.equals(""))
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
		
		Rule(int r, Rule r1, Rule r2, String conn)
		{
			rid = r;
			ruleType = "compound";
			subRule1 = r1;
			subRule2 = r2;
			connector = conn;
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
			//NodeList nodes = doc.getElementsByTagName("rule");
			NodeList nodes = doc.getFirstChild().getChildNodes();
			for(int i = 0; i < nodes.getLength(); i++)
			{
				Node node = nodes.item(i);
				if(!node.getNodeName().equals("rule"))
					continue;
				Rule rule = getRule(node);
				System.out.println("got rule with rule id "+ rule.rid);
				rules.add(rule);
				ruleCount++;
			}
		}
		
		Rule getRule(Node node)
		{
			Element e = (Element)node;
			Rule rule = new Rule();
			String rtype = getTag(e,"ruleType");			
			if(rtype.equals("basic"))
			{
				int id = Integer.parseInt(getTag(e,"ruleId"));
				String attr = getTag(e,"attribute");
				String op = getTag(e,"operator");
				String val = getTag(e,"value");
				String valType = getTag(e,"valueType");
				String val2 = getTag(e,"value2");
				String val2Type = getTag(e,"value2Type");
				rule = new Rule(id,attr,op,val,valType,val2,val2Type);				
			}
			else if(rtype.equals("compound"))
			{
				rule = getCompoundRule(node);
				
			}
			else
			{
				System.out.println("type error");
			}			
			return rule;
		}
		
		Rule getCompoundRule(Node node)
		{
			Element e = (Element)node;
			int id = 0;
			String conn = "";
			id = Integer.parseInt(getTag(e,"ruleId"));
			conn = getTag(e,"connector");
			NodeList nodes = e.getElementsByTagName("rule");
			Rule r1 = getRule(nodes.item(0));
			Rule r2 = getRule(nodes.item(1));
			
			return new Rule(id,r1,r2,conn);
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
		
		String genQuery(String view)
		{
			String sql = "select * from " + view + " where ";
			for(int i = 0; i < ruleCount; i++)
			{
				sql = sql + ruleStringBasic(rules.get(i));
				if(i < ruleCount - 1)
					sql = sql + " OR ";
			}
			return sql;		
		}
		
		String ruleStringBasic(Rule rule)
		{
			String str = "";
			if(rule.ruleType.equals("basic"))
			{
				str = rule.attribute + " " + convOperator(rule.operator) + " " + convValue(rule.value,rule.valueType);				 
			}
			else if(rule.ruleType.equals("compound"))
			{
				str = "(" + ruleStringBasic(rule.subRule1) + " " + rule.connector + " " + ruleStringBasic(rule.subRule2) + ")"; 
			}
			return str;
		}
		
		String convOperator(String op)
		{
			switch(op)
    			{
    				case "lessThan":
    					return "<";
    				case "lessThanOrEqualTo":
    					return "<=";
    				case "greaterThan":
    					return ">";
    				case "greaterThanOrEqualTo":
    					return ">=";
    				case "equalTo":
    					return "=";
    				case "notEqualTo":
    					return "<>";
    				default:
    			}
    			return "=";    			
		}
		
		String convValue(Object value, String valueType)
		{
			if(valueType.equals("string"))
				return "'"+value+"'";
			else
				return String.valueOf(value);
		}
	}
}