package dev.finprotect;

import java.util.regex.Pattern;
import java.util.ArrayList;
import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.*;
import org.apache.spark.sql.types.*;


public final class fpPrimFilter
{
	private static final Pattern delim = Pattern.compile(",");
	public static void main(String args[]) throws Exception
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
            
            String sparkAppJar = props.getProperty("fprotect.classpath");
            String sparkCheckpointDir = props.getProperty("spark.checkpoints.dir.prim");
            String sparkMaster = props.getProperty("spark.master");
            String kafkaBootstrapServer = props.getProperty("kafka.bootstrap.server");

            SparkSession spark = SparkSession
                  .builder()
                  .master(sparkMaster)
                  .appName("fpPrimFilter")
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

            Ruleset ruleset = new Ruleset(props.getProperty("fprotect.ruleset"));
            System.out.println("Loaded ruleset with "+ruleset.ruleCount+" rules.");
            System.out.println("Starting filter");
            System.out.println("Writing detections to topic fp_det_prim");
            //System.out.println("Writing non-detection to topic fp_clean_prim");

            Dataset<Row> df_det;

            df.createOrReplaceTempView("trdata");
            //System.out.println(ruleset.genQuery("trdata"));
            df_det = spark.sql(ruleset.genQuery("trdata"));    		

            /*StreamingQuery dswc = df_det.writeStream()
                            .outputMode("append")
                            .format("console")
                            .start();
            dswc.awaitTermination();*/

            //Dataset<Row> df_undet = df.except(df_det);

            df_det = df_det.select(df_det.col("tid")).withColumn("isFraud", functions.lit(1));
            df_det = df_det.select(df_det.col("tid").cast("string").as("key"),functions.to_json(functions.struct("*")).cast("string").as("value"));

            StreamingQuery dsw = df_det.writeStream()
                            .format("kafka")
                            .option("kafka.bootstrap.servers",kafkaBootstrapServer)
                            .option("topic","fp_det_prim")
                            .option("checkpointLocation",sparkCheckpointDir)
                            .start();

            /*df_undet = df_undet.select(df_undet.col("tid").cast("string").as("key"), functions.struct("*").cast("string").as("value"));

            StreamingQuery dswu = df_undet.writeStream()
                            .format("kafka")
                            .option("kafka.bootstrap.servers",kafkaBootstrapServer)
                            .option("topic","fp_clean_prim")
                            .option("checkpointLocation",sparkCheckpointDir)
                            .start();*/

            dsw.awaitTermination();
            //dswu.awaitTermination();
		
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
		
		Ruleset(String rulesetPath)
		{
			ruleCount = 0;
			rules = new ArrayList<Rule>();
			try
			{
				loadRuleset(rulesetPath);
			}
			catch(Exception e)
			{
				System.out.println(e);
			}
		}
		
		void loadRuleset(String rulesetPath) throws Exception
		{
			File ruleFile = new File(rulesetPath);
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
				System.out.println("Rule load failed: type error");
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