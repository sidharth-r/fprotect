package dev.finprotect;

import java.io.*;
import java.util.regex.Pattern;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class fpPerfEval
{
	static Connection conn;	
	
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

            ResultSet res;
            Statement stmt = conn.createStatement();
            stmt.executeUpdate("delete from fpdemo.perfeval");

            stmt.executeUpdate("insert into fpdemo.perfeval(tid,expected,actual) select fprotect.det_sec.tid,fpdemo.detection_expected.isFraud,fprotect.det_sec.isFraud from fprotect.det_sec join fpdemo.detection_expected on fprotect.det_sec.tid = fpdemo.detection_expected.tid");

            int tp, tn, fp, fn, t, f, p, n, total;
            float acc, prc, rec;

            res = stmt.executeQuery("select count(*) from fpdemo.perfeval");
            res.first();
            total = res.getInt(res.getMetaData().getColumnName(1));

            res = stmt.executeQuery("select count(*) from fpdemo.perfeval where expected = 1 and actual = 1");
            res.first();
            tp = res.getInt(res.getMetaData().getColumnName(1));

            res = stmt.executeQuery("select count(*) from fpdemo.perfeval where expected = 0 and actual = 0");
            res.first();
            tn = res.getInt(res.getMetaData().getColumnName(1));

            res = stmt.executeQuery("select count(*) from fpdemo.perfeval where expected = 0 and actual = 1");
            res.first();
            fp = res.getInt(res.getMetaData().getColumnName(1));

            res = stmt.executeQuery("select count(*) from fpdemo.perfeval where expected = 1 and actual = 0");
            res.first();
            fn = res.getInt(res.getMetaData().getColumnName(1));

            conn.close();

            t = tp + tn;
            f = fp + fn;
            p = tp + fn;
            n = tn + fp;

            acc = (float)t/(t+f);
            prc = (float)tp/(tp+fp);
            rec = (float)tp/p;

            System.out.println("Processed "+total+" records.");
            System.out.println("TP: "+tp);
            System.out.println("TN: "+tn);
            System.out.println("FP: "+fp);
            System.out.println("FN: "+fn);
            System.out.println("Accuracy: "+acc);	
            System.out.println("Precision: "+prc);
            System.out.println("Recall: "+rec);	
	}
}