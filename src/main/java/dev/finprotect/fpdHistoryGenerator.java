package dev.finprotect;

import java.io.*;
import java.util.Collections;
import java.util.Properties;
import java.util.Arrays;
import java.util.UUID;
import java.util.List;
import java.util.stream.*;

import com.fasterxml.jackson.databind.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class fpdHistoryGenerator
{
	static Connection conn;
	
	static class TRecord
	{
		public int tid;
		public String type;
		public float amount;
		public String nameOrig;
		public float oldBalanceOrig;
		public float newBalanceOrig;
		public String nameDest;
		public float oldBalanceDest;
		public float newBalanceDest;
		public int isFraud;
		//public int isFlaggedFraud;
		
		public TRecord()
		{
		}
		
		public TRecord(int i, String t, float a, String no, float obo, float nbo, String nd, float obd, float nbd, int ifr)
		{
			tid = i;
			type = t;
			amount = a;
			nameOrig = no;
			oldBalanceOrig = obo;
			newBalanceOrig = nbo;
			nameDest = nd;
			oldBalanceDest = obd;
			newBalanceDest = nbd;
			isFraud = ifr;
			//isFlaggedFraud = iffr;
		}
	}
	
	static class TRecordExt
	{
		public int tid;
		public String type;
		public float amount;
		public String nameOrig;
		public float oldBalanceOrig;
		public float newBalanceOrig;
		public String nameDest;
		public float oldBalanceDest;
		public float newBalanceDest;
		public int isFraud;
		//public int isFlaggedFraud;
		public int recurrence;
		public int destBlacklisted;
		
		public TRecordExt()
		{
		}
		
		public TRecordExt(TRecord tr, int recur, int dbl)
		{
			tid = tr.tid;
			type = tr.type;
			amount = tr.amount;
			nameOrig = tr.nameOrig;
			oldBalanceOrig = tr.oldBalanceOrig;
			newBalanceOrig = tr.newBalanceOrig;
			nameDest = tr.nameDest;
			oldBalanceDest = tr.oldBalanceDest;
			newBalanceDest = tr.newBalanceDest;
			isFraud = tr.isFraud;
			//isFlaggedFraud = tr.iffr;
			recurrence = recur;
			destBlacklisted = dbl;
		}
	}
	
	
	public static void main(String[] args) throws Exception
	{
		BufferedReader in = new BufferedReader(new FileReader(args[1]));
		int i = 1;
		
		boolean nolabel = false;
		if(args.length > 2)
		{
			if(args[2].equals("nolabel"))
				nolabel = true;
		}
		
		ObjectMapper mapper = new ObjectMapper();
		conn = DriverManager.getConnection("jdbc:mysql://localhost/fprotect?user=root&password=root");
		
		Statement stmt = conn.createStatement();
		if(nolabel)
			stmt.executeUpdate("delete from trhistory_unlabeled");
		else
			stmt.executeUpdate("delete from trhistory");
		PreparedStatement ps;
		if(nolabel)
			ps = conn.prepareStatement("insert into trhistory_unlabeled values(?,?,?,?,?,?,?,?,?,?,?)");
		else
			ps = conn.prepareStatement("insert into trhistory values(?,?,?,?,?,?,?,?,?,?,?,?)");
		
		String rec = in.readLine();
		while(rec != null)
		{
			TRecord tr = mapper.readValue(rec,TRecord.class);			
			TRecordExt tre = genExtRecord(tr);
			
			ps.setInt(1,tre.tid);
			ps.setString(2,tre.type);
			ps.setFloat(3,tre.amount);
			ps.setString(4,tre.nameOrig);
			ps.setFloat(5,tre.oldBalanceOrig);
			ps.setFloat(6,tre.newBalanceOrig);
			ps.setString(7,tre.nameDest);
			ps.setFloat(8,tre.oldBalanceDest);
			ps.setFloat(9,tre.newBalanceDest);
			ps.setInt(10,tre.recurrence);
			ps.setInt(11,tre.destBlacklisted);
			if(!nolabel)
				ps.setInt(12,tre.isFraud);
			
			ps.executeUpdate();
			
			System.out.println("Wrote "+i+" record(s)");
			i++;
			rec = in.readLine();
		}
	}
	
	static TRecordExt genExtRecord(TRecord tr) throws Exception
	{
		PreparedStatement query = conn.prepareStatement("select count(*) from trhistory where nameOrig = ? and nameDest = ?");
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