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
		
	/**
	 * This function returns a new frame block with error introduced in the data:
	 * Typos in string values, null values, outliers in numeric data and swapped elements.
	 * 
	 * @param input Original frame block
	 * @param pTypo Probability of introducing a typo in a row
	 * @param pMiss Probability of introducing missing values in a row
	 * @param pDrop Probability of dropping a value inside a row
	 * @param pOut Probability of introducing outliers in a row
	 * @param pSwap Probability swapping two elements in a row
	 * @return A new frameblock with corrupted elements
	 * 
	 */
	public static FrameBlock dataCorruption(FrameBlock input, double pTypo, double pMiss, double pDrop, double pOut, double pSwap) {
		List<Integer> numerics = new ArrayList<Integer>();
		List<Integer> strings = new ArrayList<Integer>();
		List<Integer> swappable = new ArrayList<Integer>();

		FrameBlock res = preprocessing(input, numerics, strings, swappable);
		
		res = typos(res, strings, pTypo);
		
		res = miss(res, pMiss, pDrop);
		
		res = outlier(res, numerics, pOut, 0.5, 3);
		
		return res;
	}
	
	/**
	 * This function returns a new frame block with a labels column added, and build the lists
	 * with column index of the different types of data.
	 * 
	 * @param frame Original frame block
	 * @param numerics Empty list to return the numeric positions
	 * @param strings Empty list to return the string positions
	 * @param swappable Empty list to return the swappable positions
	 * @return A new frameblock with a labels column
	 * 
	 */
	public static FrameBlock preprocessing(FrameBlock frame, List<Integer> numerics, List<Integer> strings, List<Integer> swappable) {
		FrameBlock res = new FrameBlock(frame);
		for(int i=0;i<res.getNumColumns();i++) {
			if(res.getSchema()[i].isNumeric()) { 
				numerics.add(i);
			}else if(res.getSchema()[i].equals(ValueType.STRING)) {
				strings.add(i);
			}
			if(i!=res.getNumColumns()-1 && res.getSchema()[i].equals(res.getSchema()[i+1])) {
				swappable.add(i);
			}
		}
		
		
		String[] labels = new String[res.getNumRows()];
		Arrays.fill(labels, "");
		res.appendColumn(labels);
		
		res.getColumnNames()[res.getNumColumns()-1] = "errorLabels";
		
		
		return res;
	}
	
	/**
	 * This function modifies the given, preprocessed frame block to add typos to the string values,
	 * marking them with the label typos.
	 * 
	 * @param frame Original frame block
	 * @param strings List with the columns of string type, generated during preprocessing
	 * @param pTypo Probability of adding a typo to a row
	 * @return A new frameblock with typos
	 * 
	 */
	public static FrameBlock typos(FrameBlock frame, List<Integer> strings, double pTypo) {
		
		if(!frame.getColumnName(frame.getNumColumns()-1).equals("errorLabels")) {
			throw new IllegalArgumentException("The FrameBlock passed has not been preprocessed.");
		}
		
		if(strings.isEmpty()) return frame;
		
		Random rand = new Random();
		for(int r=0;r<frame.getNumRows();r++) {
			int c = strings.get(rand.nextInt(strings.size()));
			String s = (String) frame.get(r, c);
			if(s.length()!=1 && rand.nextDouble()<=pTypo) {
				int i = rand.nextInt(s.length());
				if(i==s.length()-1) s = swapchr(s, i-1, i);
				else if(i==0) s = swapchr(s, i, i+1);
				else if(rand.nextDouble()<=0.5) s = swapchr(s, i, i+1);
				else s = swapchr(s, i-1, i);
				frame.set(r, c, s);
				
				String label = (String) frame.get(r, frame.getNumColumns()-1);
				if(label.equals("")) frame.set(r, frame.getNumColumns()-1, "typo");
				else frame.set(r, frame.getNumColumns()-1, label + ",typo");
			}
		}
		return frame;
	}
	
	
	/**
	 * This function modifies the given, preprocessed frame block to add missing values to some of the rows,
	 * marking them with the label missing.
	 * 
	 * @param frame Original frame block
	 * @param pMiss Probability of adding missing values to a row
	 * @param pTypo Probability of dropping a value from a row previously selected by pTypo
	 * @return A new frameblock with missing values
	 * 
	 */
	public static FrameBlock miss(FrameBlock frame, double pMiss, double pDrop) {
		if(!frame.getColumnName(frame.getNumColumns()-1).equals("errorLabels")) {
			throw new IllegalArgumentException("The FrameBlock passed has not been preprocessed.");
		}
		Random rand = new Random();
		for(int r=0;r<frame.getNumRows();r++) {
			if(rand.nextDouble()<=pMiss) {
				int dropped = 0;
				for(int c=0;c<frame.getNumColumns()-1;c++) {
					if((frame.get(r, c)!=null && !frame.get(r, c).equals(0)) && rand.nextDouble()<=pDrop) {
						frame.set(r, c, null);
						dropped++;
					}
				}
					
				if(dropped>0) {
					String label = (String) frame.get(r, frame.getNumColumns()-1);
					if(label.equals("")) frame.set(r, frame.getNumColumns()-1, "missing");
					else frame.set(r, frame.getNumColumns()-1, label + ",missing");	
				}
			}
		}
		return frame;
	}
	
	/**
	 * This function modifies the given, preprocessed frame block to add outliers to some
	 * of the numeric data of the frame, adding or  several times the standard deviation,
	 * and marking them with the label outlier.
	 * 
	 * @param frame Original frame block
	 * @param numerics List with the columns of numeric type, generated during preprocessing
	 * @param pOut Probability of introducing an outlier in a row
	 * @param pPos Probability of using positive deviation
	 * @param times Times the standard deviation is added
	 * @return A new frameblock with outliers
	 * 
	 */
	public static FrameBlock outlier(FrameBlock frame, List<Integer> numerics, double pOut, double pPos, int times) {
		
		if(!frame.getColumnName(frame.getNumColumns()-1).equals("errorLabels")) {
			throw new IllegalArgumentException("The FrameBlock passed has not been preprocessed.");
		}
		
		if(numerics.isEmpty()) return frame;
		
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
				if(rand.nextDouble()<=pPos) val += (int) Math.round(times*std);
				else val -= (int) Math.round(times*std);
				frame.set(r, c, val);
			} else if(frame.getSchema()[c].equals(ValueType.INT64)) {
				Long val = (Long) frame.get(r, c);
				if(rand.nextDouble()<=pPos) val += Math.round(times*std);
				else val -= Math.round(times*std);
				frame.set(r, c, val);
			} else if(frame.getSchema()[c].equals(ValueType.FP32)) {
				Float val = (Float) frame.get(r, c);
				if(rand.nextDouble()<=pPos) val += (float) (times*std);
				else val -= (float) (times*std);
				frame.set(r, c, val);
			} else if(frame.getSchema()[c].equals(ValueType.FP64)) {
				Double val = (Double) frame.get(r, c);
				if(rand.nextDouble()<=pPos) val += times*std;
				else val -= times*std;
				frame.set(r, c, val);
			}
			
			String label = (String) frame.get(r, frame.getNumColumns()-1);
			if(label.equals("")) frame.set(r, frame.getNumColumns()-1, "outlier");
			else frame.set(r, frame.getNumColumns()-1, label + ",outlier");
		}
		
		return frame;
	}
	/**
	 * This function modifies the given, preprocessed frame block to add swapped fields of the same ValueType
	 * that are consecutive, marking them with the label swap.
	 * 
	 * @param frame Original frame block
	 * @param swappable List with the columns that are swappable, generated during preprocessing
	 * @param pSwap Probability of swapping two fields in a row
	 * @return A new frameblock with swapped elements
	 * 
	 */
	public static FrameBlock swap(FrameBlock frame, List<Integer> swappable, double pSwap) {
		if(!frame.getColumnName(frame.getNumColumns()-1).equals("errorLabels")) {
			throw new IllegalArgumentException("The FrameBlock passed has not been preprocessed.");
		}
		
		Random rand = new Random();
		for(int r=0;r<frame.getNumRows();r++) {
			if(rand.nextDouble()<=pSwap) {
				int i = swappable.get(rand.nextInt(swappable.size()));
				Object tmp = frame.get(r, i);
				frame.set(r, i, frame.get(r, i+1));
				frame.set(r, i+1, tmp);
				
				String label = (String) frame.get(r, frame.getNumColumns()-1);
				if(label.equals("")) frame.set(r, frame.getNumColumns()-1, "swap");
				else frame.set(r, frame.getNumColumns()-1, label + ",swap");
			}
		}
		
		return frame;
	}
	
	private static String swapchr(String str, int i, int j) 
    { 
        if (j == str.length() - 1) 
            return str.substring(0, i) + str.charAt(j) 
                + str.substring(i + 1, j) + str.charAt(i); 
  
        return str.substring(0, i) + str.charAt(j) 
            + str.substring(i + 1, j) + str.charAt(i) 
            + str.substring(j + 1, str.length()); 
    }
}
