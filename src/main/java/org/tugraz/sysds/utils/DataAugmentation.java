package org.tugraz.sysds.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.tugraz.sysds.common.Types.ValueType;
import org.tugraz.sysds.runtime.matrix.data.FrameBlock;

public class DataAugmentation {
	
	// Refactoring, separate methods for each kind of error
	
	/**
	 * This function returns a new frame block with error introduced in the data:
	 * Typos in string values, null values, outliers in numeric data and swapped elements 
	 * 
	 * @param input Original frame block
	 * @param pTypo Probability of introducing a typo in a row
	 * @param pMiss Probability of introducing missing values in a row
	 * @param pOut Probability of introducing outliers in a row
	 * @param pSwap Probability swapping two elements in a row
	 * @return A new frameblock with corrupted elements
	 * 
	 */
	public static FrameBlock dataCorruption(FrameBlock input, double pTypo, double pMiss, double pOut, double pSwap) {
		List<Integer> numerics = new ArrayList<>();
		List<Integer> strings = new ArrayList<>();

		FrameBlock res = preprocessing(input, numerics, strings);
		
		res = typos(res, pTypo);
		
		res = miss(res, pMiss);
		
		res = outlier(res, pOut, 3);
		
		return res;
	}
	
	public static FrameBlock preprocessing(FrameBlock frame, List<Integer> numerics, List<Integer> strings) {
		FrameBlock res = new FrameBlock(frame);
		for(int i=0;i<res.getNumColumns();i++) {
			if(res.getSchema()[i].isNumeric()) { 
				numerics.add(i);
			}else if(res.getSchema()[i].equals(ValueType.STRING)) {
				strings.add(i);
			}
		}
		
		String[] labels = new String[res.getNumRows()];
		Arrays.fill(labels, "");
		res.appendColumn(labels);
		
		return res;
	}
	
	public static FrameBlock typos(FrameBlock frame, double pTypo) {
		List<Integer> strings = new ArrayList<Integer>();
		for(int i=0;i<frame.getNumColumns();i++) {
			if(frame.getSchema()[i].equals(ValueType.STRING)) { 
				strings.add(i);
			}
		}
		if(strings.isEmpty()) return frame;
		
		Random rand = new Random();
		for(int r=0;r<frame.getNumRows();r++) {
			int c = strings.get(rand.nextInt(strings.size()));
			String s = (String) frame.get(r, c);
			if(s.length()!=1 && rand.nextDouble()<=pTypo) {
				int i = rand.nextInt(s.length());
				if(i!=s.length()-1 && rand.nextDouble()<=0.5) s = swap(s, i, i+1);
				else s = swap(s, i-1, i);
				frame.set(r, c, s);
				
				String label = (String) frame.get(r, frame.getNumColumns()-1);
				if(label.equals("")) frame.set(r, frame.getNumColumns()-1, "typo");
				else frame.set(r, frame.getNumColumns()-1, label + ",typo");
			}
		}
		return frame;
	}
	
	public static FrameBlock miss(FrameBlock frame, double pMiss) {
		Random rand = new Random();
		for(int r=0;r<frame.getNumRows();r++) {
			if(rand.nextDouble()<=pMiss) {
				int c = rand.nextInt(frame.getSchema().length);
				frame.set(r, c, null);
				
				String label = (String) frame.get(r, frame.getNumColumns()-1);
				if(label.equals("")) frame.set(r, frame.getNumColumns()-1, "missing");
				else frame.set(r, frame.getNumColumns()-1, label + ",missing");
			}
		}
		return frame;
	}
	
	public static FrameBlock outlier(FrameBlock frame, double pOut, int times) {
		List<Integer> numerics = new ArrayList<Integer>();
		for(int i=0;i<frame.getNumColumns();i++) {
			if(frame.getSchema()[i].isNumeric()) { 
				numerics.add(i);
			}
		}
		
		Map<Integer, Double> stds = new HashMap<Integer, Double>();
		Random rand = new Random();
		for(int r=0;r<frame.getNumRows();r++) {
			if(rand.nextDouble()>pOut) continue;
			int c = numerics.get(rand.nextInt(numerics.size()));
			if(!stds.containsKey(c)) {
				switch(frame.getSchema()[c]) {
				case INT32:{
					List<Integer> l = new ArrayList<Integer>();
					for(int i=0;i<frame.getNumRows();i++) {
						l.add((Integer) frame.get(i, c));
					}
					
					Double sum = 0.;
					
					for(int i=0; i<l.size();i++) {
						sum += l.get(i);
					}
					Double mean = sum/l.size();
					sum = 0.;
					for(int i=0; i<l.size();i++) {
						Double diff = l.get(i)-mean;
						sum += diff*diff;
					}
					
					stds.put(c, Math.sqrt(sum/l.size()));
				}
					break;
				case INT64:{
					List<Long> l = new ArrayList<Long>();
					for(int i=0;i<frame.getNumRows();i++) {
						l.add((Long) frame.get(i, c));
					}
					
					Double sum = 0.;
					
					for(int i=0; i<l.size();i++) {
						sum += l.get(i);
					}
					Double mean = sum/l.size();
					sum = 0.;
					for(int i=0; i<l.size();i++) {
						Double diff = l.get(i)-mean;
						sum += diff*diff;
					}
					
					stds.put(c, Math.sqrt(sum/l.size()));
				}
					break;
				case FP32:{
					List<Float> l = new ArrayList<Float>();
					for(int i=0;i<frame.getNumRows();i++) {
						l.add((Float) frame.get(i, c));
					}
					
					Double sum = 0.;
					
					for(int i=0; i<l.size();i++) {
						sum += l.get(i);
					}
					Double mean = sum/l.size();
					sum = 0.;
					for(int i=0; i<l.size();i++) {
						Double diff = l.get(i)-mean;
						sum += diff*diff;
					}
					
					stds.put(c, Math.sqrt(sum/l.size()));
				}
					break;
				case FP64:{
					List<Double> l = new ArrayList<Double>();
					for(int i=0;i<frame.getNumRows();i++) {
						l.add((Double) frame.get(i, c));
					}
					
					Double sum = 0.;
					
					for(int i=0; i<l.size();i++) {
						sum += l.get(i);
					}
					Double mean = sum/l.size();
					sum = 0.;
					for(int i=0; i<l.size();i++) {
						Double diff = l.get(i)-mean;
						sum += diff*diff;
					}
					
					stds.put(c, Math.sqrt(sum/l.size()));
				}
					break;
				default:
					break;
				}				
			}
			
			Double std = stds.get(c);
			if(frame.getSchema()[c].equals(ValueType.INT32)) {
				Integer val = (Integer) frame.get(r, c);
				val += (int) Math.round(times*std);
				frame.set(r, c, val);
			} else if(frame.getSchema()[c].equals(ValueType.INT64)) {
				Long val = (Long) frame.get(r, c);
				val += Math.round(times*std);
				frame.set(r, c, val);
			} else if(frame.getSchema()[c].equals(ValueType.FP32)) {
				Float val = (Float) frame.get(r, c);
				val += (float) (times*std);
				frame.set(r, c, val);
			} else if(frame.getSchema()[c].equals(ValueType.FP64)) {
				Double val = (Double) frame.get(r, c);
				val += times*std;
				frame.set(r, c, val);
			}
			
			String label = (String) frame.get(r, frame.getNumColumns()-1);
			if(label.equals("")) frame.set(r, frame.getNumColumns()-1, "outlier");
			else frame.set(r, frame.getNumColumns()-1, label + ",outlier");
		}
		
		return frame;
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
