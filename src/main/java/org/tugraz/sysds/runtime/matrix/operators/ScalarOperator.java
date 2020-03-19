/*
 * Modifications Copyright 2020 Graz University of Technology
 *
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

package org.tugraz.sysds.runtime.matrix.operators;

import org.tugraz.sysds.runtime.functionobjects.And;
import org.tugraz.sysds.runtime.functionobjects.BitwShiftL;
import org.tugraz.sysds.runtime.functionobjects.BitwShiftR;
import org.tugraz.sysds.runtime.functionobjects.Builtin;
import org.tugraz.sysds.runtime.functionobjects.Equals;
import org.tugraz.sysds.runtime.functionobjects.Minus;
import org.tugraz.sysds.runtime.functionobjects.MinusNz;
import org.tugraz.sysds.runtime.functionobjects.Multiply;
import org.tugraz.sysds.runtime.functionobjects.Multiply2;
import org.tugraz.sysds.runtime.functionobjects.NotEquals;
import org.tugraz.sysds.runtime.functionobjects.Power2;
import org.tugraz.sysds.runtime.functionobjects.ValueFunction;
import org.tugraz.sysds.runtime.functionobjects.Builtin.BuiltinCode;


/**
 * Base class for all scalar operators.
 * 
 */
public abstract class ScalarOperator extends Operator 
{
	private static final long serialVersionUID = 4547253761093455869L;

	public final ValueFunction fn;
	protected final double _constant;
	
	public ScalarOperator(ValueFunction p, double cst) {
		this(p, cst, false);
	}
	
	protected ScalarOperator(ValueFunction p, double cst, boolean altSparseSafe) {
		super( isSparseSafeStatic(p) || altSparseSafe
				|| (p instanceof NotEquals && cst==0)
				|| (p instanceof Equals && cst!=0)
				|| (p instanceof Minus && cst==0)
				|| (p instanceof Builtin && ((Builtin)p).getBuiltinCode()==BuiltinCode.MAX && cst<=0)
				|| (p instanceof Builtin && ((Builtin)p).getBuiltinCode()==BuiltinCode.MIN && cst>=0));
		fn = p;
		_constant = cst;
	}
	
	public double getConstant() {
		return _constant;
	}
	
	public abstract ScalarOperator setConstant(double cst);
	
	/**
	 * Apply the scalar operator over a given input value.
	 * 
	 * @param in input value
	 * @return result
	 */
	public abstract double executeScalar(double in);
	
	/**
	 * Indicates if the function is statically sparse safe, i.e., it is always
	 * sparse safe independent of the given constant.
	 * @param fn ValueFunction object
	 * @return true if function statically sparse safe
	 */
	protected static boolean isSparseSafeStatic(ValueFunction fn) {
		return ( fn instanceof Multiply || fn instanceof Multiply2 
			|| fn instanceof Power2 || fn instanceof And || fn instanceof MinusNz
			|| fn instanceof Builtin && ((Builtin)fn).getBuiltinCode()==BuiltinCode.LOG_NZ)
			|| fn instanceof BitwShiftL || fn instanceof BitwShiftR;
	}
}
