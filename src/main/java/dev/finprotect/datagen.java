package dev.finprotect;

import java.io.*;
import java.util.regex.*;
import java.util.List;
import java.util.stream.*;
import com.fasterxml.jackson.databind.*;


public class datagen
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
		public int isFlaggedFraud;
		
		public tr(int i, String t, float a, String no, float obo, float nbo, String nd, float obd, float nbd, int ifr, int iffr)
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
			isFlaggedFraud = iffr;
		}
	}

	public static void main(String args[]) throws Exception
	{
		Pattern pattern = Pattern.compile(",");
		BufferedReader in = new BufferedReader(new FileReader("/home/fprotect/finprotect/data/ps_500_headless.csv"));
		int[] id = {-1};
		List<tr> trs = in.lines().map(line -> {
			String[] s = pattern.split(line);
			id[0]++;
			return new tr(id[0],s[1],Float.parseFloat(s[2]),s[3],Float.parseFloat(s[4]),Float.parseFloat(s[5]),s[6],Float.parseFloat(s[7]),Float.parseFloat(s[8]),Integer.parseInt(s[9]),Integer.parseInt(s[10]));
			}).collect(Collectors.toList());
		ObjectMapper jsonMapper = new ObjectMapper();
		//jsonMapper.enable(SerializationFeature.INDENT_OUTPUT);
		
		FileOutputStream fout = new FileOutputStream("/home/fprotect/finprotect/data/ps_500_hl.json");
		//jsonMapper.writeValue(fout,trs);
		//for(listIterator<tr> iter = trs.listIterator(); iter.hasNext();)
		for(tr t:trs)
		{
			String s = jsonMapper.writeValueAsString(t) + "\n";
			fout.write(s.getBytes());
		}

		fout.close();
	}
}