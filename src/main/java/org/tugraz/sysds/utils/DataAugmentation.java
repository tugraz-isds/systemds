package org.tugraz.sysds.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.tugraz.sysds.common.Types.ValueType;
import org.tugraz.sysds.runtime.matrix.data.FrameBlock;

public class DataAugmentation {
	
	// Refactoring, separate methods for each kind of error
	// Enum error types
	public static FrameBlock dataCorruption(FrameBlock input, double pTypo, double pMiss, double pOut, double pSwap) {
		FrameBlock res = new FrameBlock(input);
		ValueType[] schema = res.getSchema();
		List<Integer> numerics = new ArrayList<>();
		List<Integer> strings = new ArrayList<>();

		
		for(int i=0;i<schema.length;i++) {
			if(schema[i].isNumeric()) { 
				numerics.add(i);
			}else if(schema[i].equals(ValueType.STRING)) {
				strings.add(i);
			}
		}
		
		String[] labels = new String[res.getNumRows()];
		Random rand = new Random();
		for(int r=0;r<res.getNumRows();r++) {
			if(!strings.isEmpty() && rand.nextDouble()<=pTypo) {
				int c = strings.get(rand.nextInt(strings.size()));
				String s = (String) res.get(r, c);
				int i = rand.nextInt(s.length());
				if(i!=s.length()-1 && rand.nextDouble()<=0.5) s = swap(s, i, i+1);
				else if(i!=0) s = swap(s, i-1, i);
				else {labels[r] = "none"; continue;}
				res.set(r, c, s);
				labels[r] = "typo";
				continue;
			}
			// Drop multiple values on row, column
			if(rand.nextDouble()<=pMiss) {
				int c = rand.nextInt(schema.length);
				res.set(r, c, null);
				labels[r] = "missing";
				continue;
			}
			// Standard deviation
			if(!numerics.isEmpty() && rand.nextDouble()<=pOut) {
				int c = numerics.get(rand.nextInt(numerics.size()));
				// separate ValueTypes
				if(schema[c].equals(ValueType.FP32) || schema[c].equals(ValueType.FP64)) {
					Double n = (Double) res.get(r, c);
					n = n*100;
					res.set(r, c, n);
				}else if(schema[c].equals(ValueType.INT32) || schema[c].equals(ValueType.INT64)) {
					Integer n = (Integer) res.get(r, c);
					n = n*100;
					res.set(r, c, n);
				}
				
				labels[r] = "outlier";
				continue;
			}
			
//			if(res.getNumColumns()>1 && rand.nextDouble()<=pSwap) {
//				int c = rand.nextInt(schema.length);
//				Object tmp = res.get(r, c);
//				if(c!=res.getNumColumns()-1 && rand.nextDouble()<=0.5) {
//					res.set(r, c, res.get(r, c+1));
//					res.set(r, c, tmp);
//				}else {
//					res.set(r, c, res.get(r, c-1));
//					res.set(r, c, tmp);
//				}
//				
//				labels[r] = "swap";
//			}
			
			labels[r] = "none";
			
		}
		
		res.appendColumn(labels);
		
		return res;
	}
	
	private static String swap(String str, int i, int j) 
    { 
        if (j == str.length() - 1) 
            return str.substring(0, i) + str.charAt(j) 
                + str.substring(i + 1, j) + str.charAt(i); 
  
        return str.substring(0, i) + str.charAt(j) 
            + str.substring(i + 1, j) + str.charAt(i) 
            + str.substring(j + 1, str.length()); 
    } 
}
