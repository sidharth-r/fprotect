package dev.finprotect;

import java.io.*;
import java.util.regex.Pattern;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class fpPerfEval
{
	static Connection conn;	
	
	public static void main(String[] args) throws Exception
	{
		conn = DriverManager.getConnection("jdbc:mysql://localhost/fpdemo?user=root&password=root");
		
		ResultSet res;
		Statement stmt = conn.createStatement();
		stmt.executeUpdate("delete from perfeval");
		
		stmt.executeUpdate("insert into perfeval(tid,expected,actual) select fprotect.results.tid,detection_expected.isFraud,fprotect.results.isFraud from fprotect.results join detection_expected on fprotect.results.tid = detection_expected.tid");
		
		int tp, tn, fp, fn, t, f, p, n;
		float acc, prc, rec;
		
		res = stmt.executeQuery("select count(*) from perfeval where expected = 1 and actual = 1");
		res.first();
		tp = res.getInt(res.getMetaData().getColumnName(1));
		
		res = stmt.executeQuery("select count(*) from perfeval where expected = 0 and actual = 0");
		res.first();
		tn = res.getInt(res.getMetaData().getColumnName(1));
		
		res = stmt.executeQuery("select count(*) from perfeval where expected = 0 and actual = 1");
		res.first();
		fp = res.getInt(res.getMetaData().getColumnName(1));
		
		res = stmt.executeQuery("select count(*) from perfeval where expected = 1 and actual = 0");
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
		
		System.out.println("TP: "+tp);
		System.out.println("TN: "+tn);
		System.out.println("FP: "+fp);
		System.out.println("FN: "+fn);
		System.out.println("Accuracy: "+acc);	
		System.out.println("Precision: "+prc);
		System.out.println("Recall: "+rec);	
	}
}