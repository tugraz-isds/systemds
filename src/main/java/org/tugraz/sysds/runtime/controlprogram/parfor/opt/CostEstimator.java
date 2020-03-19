/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.tugraz.sysds.runtime.controlprogram.parfor.opt;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.tugraz.sysds.lops.LopProperties.ExecType;
import org.tugraz.sysds.runtime.controlprogram.parfor.opt.OptNode.ParamType;

/**
 * Base class for all potential cost estimators
 * 
 * TODO account for shared read-only matrices when computing aggregated stats
 * 
 */
public abstract class CostEstimator 
{
	protected static final Log LOG = LogFactory.getLog(CostEstimator.class.getName());

	//default parameters
	public static final double DEFAULT_EST_PARALLELISM = 1.0; //default degree of parallelism: serial
	public static final long   FACTOR_NUM_ITERATIONS   = 10; //default problem size
	public static final double DEFAULT_TIME_ESTIMATE   = 5;  //default execution time: 5ms
	public static final double DEFAULT_MEM_ESTIMATE_CP = 1024; //default memory consumption: 1KB 
	public static final double DEFAULT_MEM_ESTIMATE_SP = 20*1024*1024; //default memory consumption: 20MB 

	public enum TestMeasure {
		EXEC_TIME, MEMORY_USAGE
	}
	
	public enum DataFormat {
		DENSE, SPARSE
	}
	
	public enum ExcludeType {
		NONE, SHARED_READ, RESULT_LIX
	}
	
	protected boolean _inclCondPart = false;
	protected Collection<String> _exclVars = null;
	protected ExcludeType _exclType = ExcludeType.NONE;
	
	/**
	 * Main leaf node estimation method - to be overwritten by specific cost estimators
	 * 
	 * @param measure ?
	 * @param node internal representation of a plan alternative for program blocks and instructions
	 * @return estimate?
	 */
	public abstract double getLeafNodeEstimate( TestMeasure measure, OptNode node );

	/**
	 * Main leaf node estimation method - to be overwritten by specific cost estimators
	 * 
	 * @param measure ?
	 * @param node internal representation of a plan alternative for program blocks and instructions
	 * @param et forced execution type for leaf node 
	 * @return estimate?
	 */
	public abstract double getLeafNodeEstimate( TestMeasure measure, OptNode node, ExecType et );
	
	
	/////////
	//methods invariant to concrete estimator
	///
	
	/**
	 * Main estimation method.
	 * 
	 * @param measure ?
	 * @param node internal representation of a plan alternative for program blocks and instructions
	 * @return estimate?
	 */
	public double getEstimate( TestMeasure measure, OptNode node ) {
		return getEstimate(measure, node, null);
	}
	
	public double getEstimate( TestMeasure measure, OptNode node, boolean inclCondPart ) {
		_inclCondPart = inclCondPart; //temporary
		double val = getEstimate(measure, node, null);
		_inclCondPart = false;
		return val;
	}
	
	public double getEstimate(TestMeasure measure, OptNode node, boolean inclCondPart, Collection<String> vars, ExcludeType extype) {
		_inclCondPart = inclCondPart; //temporary
		_exclVars = vars;
		_exclType = extype;
		double val = getEstimate(measure, node, null);
		_inclCondPart = false; 
		_exclVars = null;
		_exclType = ExcludeType.NONE;
		return val;
	}
	
	/**
	 * Main estimation method.
	 * 
	 * @param measure estimate type (time or memory)
	 * @param node plan opt tree node
	 * @param et execution type
	 * @return estimate
	 */
	public double getEstimate( TestMeasure measure, OptNode node, ExecType et ) {
		double val = -1;
		
		if( node.isLeaf() )
		{
			if( _inclCondPart && node.getParam(ParamType.DATA_PARTITION_COND_MEM) != null )
				val = Double.parseDouble(node.getParam(ParamType.DATA_PARTITION_COND_MEM));
			else if( et != null )
				val = getLeafNodeEstimate(measure, node, et); //forced type
			else 
				val = getLeafNodeEstimate(measure, node); //default	
		}
		else
		{
			//aggreagtion methods for different program block types and measure types
			String tmp = null;
			double N = -1;
			switch ( measure )
			{
				case EXEC_TIME:
					switch( node.getNodeType() )
					{
						case GENERIC:
						case FUNCCALL:	
							val = getSumEstimate(measure, node.getChilds(), et); 
							break;
						case IF:
							if( node.getChilds().size()==2 )
								val = getWeightedEstimate(measure, node.getChilds(), et);
							else
								val = getMaxEstimate(measure, node.getChilds(), et); 
							break;
						case WHILE:
							val = FACTOR_NUM_ITERATIONS * getSumEstimate(measure, node.getChilds(), et); 
							break;
						case FOR:
							tmp = node.getParam(ParamType.NUM_ITERATIONS);
							N = (tmp!=null) ? (double)Long.parseLong(tmp) : FACTOR_NUM_ITERATIONS; 
							val = N * getSumEstimate(measure, node.getChilds(), et);
							break; 
						case PARFOR:
							tmp = node.getParam(ParamType.NUM_ITERATIONS);
							N = (tmp!=null) ? (double)Long.parseLong(tmp) : FACTOR_NUM_ITERATIONS; 
							val = N * getSumEstimate(measure, node.getChilds(), et) 
									/ Math.max(node.getK(), 1); 
							break;	
						default:
							//do nothing
					}
					break;
					
				case MEMORY_USAGE:
					switch( node.getNodeType() )
					{
						case GENERIC:
						case FUNCCALL:
						case IF:
						case WHILE:
						case FOR:
							val = getMaxEstimate(measure, node.getChilds(), et); 
							break;
						case PARFOR:
							if( node.getExecType() == OptNode.ExecType.SPARK )
								val = getMaxEstimate(measure, node.getChilds(), et); //executed in different JVMs
							else if ( node.getExecType() == OptNode.ExecType.CP || node.getExecType() == null )
								val = getMaxEstimate(measure, node.getChilds(), et) 
									* Math.max(node.getK(), 1); //everything executed within 1 JVM
							break;
						default:
							//do nothing
					}
					break;
			}
		}
		
		return val;
	}

	protected double getDefaultEstimate(TestMeasure measure) {
		switch( measure ) {
			case EXEC_TIME:    return DEFAULT_TIME_ESTIMATE;
			case MEMORY_USAGE: return DEFAULT_MEM_ESTIMATE_CP;
		}
		return -1;
	}

	protected double getMaxEstimate( TestMeasure measure, ArrayList<OptNode> nodes, ExecType et ) {
		return nodes.stream().mapToDouble(n -> getEstimate(measure, n, et))
			.max().orElse(Double.NEGATIVE_INFINITY);
	}

	protected double getSumEstimate( TestMeasure measure, ArrayList<OptNode> nodes, ExecType et ) {
		return nodes.stream().mapToDouble(n -> getEstimate(measure, n, et)).sum();
	}

	protected double getWeightedEstimate( TestMeasure measure, ArrayList<OptNode> nodes, ExecType et ) {
		return nodes.stream().mapToDouble(n -> getEstimate(measure, n, et)).sum() / nodes.size(); //weighting
	}
}
