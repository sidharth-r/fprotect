package dev.finprotect;

import java.io.*;
import java.util.regex.*;
import java.util.List;
import java.util.stream.*;
import com.fasterxml.jackson.databind.*;


public class fpdDataFormatter
{
	
	static class tr
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
		
		public tr(int i, String t, float a, String no, float obo, float nbo, String nd, float obd, float nbd, int ifr)
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
	
	static class trc
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
		//public int isFraud;
		//public int isFlaggedFraud;
		
		public trc(int i, String t, float a, String no, float obo, float nbo, String nd, float obd, float nbd)
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
			//isFraud = ifr;
			//isFlaggedFraud = iffr;
		}
	}

	public static void main(String args[]) throws Exception
	{
		if(args.length != 4 && args.length != 5)
		{
			for(int i = 0; i < args.length; i++)
				System.out.println(args[i]);
			System.out.println("USAGE: java datagen input-file output-file starting-tid OPTIONAL:exclude(excludes label column)");
			return;
		}
		Pattern pattern = Pattern.compile(",");
		BufferedReader in = new BufferedReader(new FileReader(args[1]));
		int tid = Integer.parseInt(args[3]) - 1;
		int[] id = {tid};
		
		if(args.length == 5)
		{
			if(args[4].equals("exclude"))
			{
				List<trc> trs = in.lines().map(line -> {
					String[] s = pattern.split(line);
					id[0]++;
					return new trc(id[0],s[1],Float.parseFloat(s[2]),s[3],Float.parseFloat(s[4]),Float.parseFloat(s[5]),s[6],Float.parseFloat(s[7]),Float.parseFloat(s[8]));
				}).collect(Collectors.toList());
				ObjectMapper jsonMapper = new ObjectMapper();
				//jsonMapper.enable(SerializationFeature.INDENT_OUTPUT);
				
				FileOutputStream fout = new FileOutputStream(args[2]);
				for(trc t:trs)
				{
					String s = jsonMapper.writeValueAsString(t) + "\n";
					fout.write(s.getBytes());
				}
		
				fout.close();
			}
		}
		else
		{
			List<tr> trs = in.lines().map(line -> {
					String[] s = pattern.split(line);
					id[0]++;
					return new tr(id[0],s[1],Float.parseFloat(s[2]),s[3],Float.parseFloat(s[4]),Float.parseFloat(s[5]),s[6],Float.parseFloat(s[7]),Float.parseFloat(s[8]),Integer.parseInt(s[9]));
				}).collect(Collectors.toList());
			ObjectMapper jsonMapper = new ObjectMapper();
			//jsonMapper.enable(SerializationFeature.INDENT_OUTPUT);
			
			FileOutputStream fout = new FileOutputStream(args[2]);
			for(tr t:trs)
			{
				String s = jsonMapper.writeValueAsString(t) + "\n";
				fout.write(s.getBytes());
			}
	
			fout.close();
		}
	}
}