package dev.finprotect;

import java.io.*;
import java.util.regex.Pattern;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class fpdLabelGen
{
	static Connection conn;	
	
	public static void main(String[] args) throws Exception
	{
		BufferedReader in = new BufferedReader(new FileReader(args[1]));
		int i = 1;
		
		int limit = -1;
		if(args.length > 2)
		{
			limit = Integer.parseInt(args[2]);
		}

		conn = DriverManager.getConnection("jdbc:mysql://localhost/fpdemo?user=root&password=root");
		
		Statement stmt = conn.createStatement();
		stmt.executeUpdate("delete from detection_expected");
		PreparedStatement ps;
		ps = conn.prepareStatement("insert into detection_expected values(?,?)");
		
		String rec = in.readLine();
		while(rec != null && (i <= limit || limit == -1))
		{	
			int ifr = Integer.parseInt(rec.split(",")[9]);			
				
			ps.setInt(1,i);
			ps.setInt(2,ifr);
			
			ps.executeUpdate();
			
			System.out.println("Wrote "+i+" record(s)");
			i++;
			rec = in.readLine();
		}
		
		conn.close();
	}
}