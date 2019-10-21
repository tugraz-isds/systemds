/*
 * Modifications Copyright 2019 Graz University of Technology
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

package org.tugraz.sysds.parser;

import org.antlr.v4.runtime.ParserRuleContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.wink.json4j.JSONArray;
import org.apache.wink.json4j.JSONObject;
import org.tugraz.sysds.api.DMLScript;
import org.tugraz.sysds.common.Types.DataType;
import org.tugraz.sysds.common.Types.ValueType;
import org.tugraz.sysds.conf.CompilerConfig.ConfigType;
import org.tugraz.sysds.conf.ConfigurationManager;
import org.tugraz.sysds.hops.DataGenOp;
import org.tugraz.sysds.parser.LanguageException.LanguageErrorCodes;
import org.tugraz.sysds.parser.dml.CustomErrorListener;
import org.tugraz.sysds.runtime.DMLRuntimeException;
import org.tugraz.sysds.runtime.controlprogram.parfor.stat.InfrastructureAnalyzer;
import org.tugraz.sysds.runtime.io.FileFormatPropertiesMM;
import org.tugraz.sysds.runtime.io.IOUtilFunctions;
import org.tugraz.sysds.runtime.util.HDFSTool;
import org.tugraz.sysds.runtime.util.UtilFunctions;
import org.tugraz.sysds.utils.JSONHelper;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map.Entry;


public class DataExpression extends DataIdentifier 
{
	public static final String RAND_DIMS = "dims";

	public static final String RAND_ROWS = "rows";
	public static final String RAND_COLS = "cols";
	public static final String RAND_MIN = "min";
	public static final String RAND_MAX = "max";
	public static final String RAND_SPARSITY = "sparsity";
	public static final String RAND_SEED = "seed";
	public static final String RAND_PDF = "pdf";
	public static final String RAND_LAMBDA = "lambda";
	
	public static final String RAND_PDF_UNIFORM = "uniform";
	
	public static final String RAND_BY_ROW = "byrow";
	public static final String RAND_DIMNAMES = "dimnames";
	public static final String RAND_DATA = "data";
	
	public static final String IO_FILENAME = "iofilename";
	public static final String READROWPARAM = "rows";
	public static final String READCOLPARAM = "cols";
	public static final String READNNZPARAM = "nnz";
	
	public static final String FORMAT_TYPE = "format";
	public static final String FORMAT_TYPE_VALUE_TEXT = "text";
	public static final String FORMAT_TYPE_VALUE_BINARY = "binary";
	public static final String FORMAT_TYPE_VALUE_CSV = "csv";
	public static final String FORMAT_TYPE_VALUE_MATRIXMARKET = "mm";
	public static final String FORMAT_TYPE_VALUE_LIBSVM = "libsvm";
	
	public static final String ROWBLOCKCOUNTPARAM = "rows_in_block";
	public static final String COLUMNBLOCKCOUNTPARAM = "cols_in_block";
	public static final String DATATYPEPARAM = "data_type";
	public static final String VALUETYPEPARAM = "value_type";
	public static final String DESCRIPTIONPARAM = "description";
	public static final String AUTHORPARAM = "author";
	public static final String SCHEMAPARAM = "schema";
	public static final String CREATEDPARAM = "created";

	// Parameter names relevant to reading/writing delimited/csv files
	public static final String DELIM_DELIMITER = "sep";
	public static final String DELIM_HAS_HEADER_ROW = "header";
	public static final String DELIM_FILL = "fill";
	public static final String DELIM_FILL_VALUE = "default";
	//public static final String DELIM_RECODE = "recode";
	public static final String DELIM_NA_STRINGS = "na.strings";
	public static final String DELIM_NA_STRING_SEP = "\u00b7";
	
	public static final String DELIM_SPARSE = "sparse";  // applicable only for write
	
	public static final String[] RAND_VALID_PARAM_NAMES = 
		{RAND_ROWS, RAND_COLS, RAND_DIMS, RAND_MIN, RAND_MAX, RAND_SPARSITY, RAND_SEED, RAND_PDF, RAND_LAMBDA};
	
	public static final String[] RESHAPE_VALID_PARAM_NAMES =
		{  RAND_BY_ROW, RAND_DIMNAMES, RAND_DATA, RAND_ROWS, RAND_COLS, RAND_DIMS};

	// Valid parameter names in a metadata file
	public static final String[] READ_VALID_MTD_PARAM_NAMES =
		{ IO_FILENAME, READROWPARAM, READCOLPARAM, READNNZPARAM, FORMAT_TYPE,
			ROWBLOCKCOUNTPARAM, COLUMNBLOCKCOUNTPARAM, DATATYPEPARAM, VALUETYPEPARAM, SCHEMAPARAM, DESCRIPTIONPARAM,
			AUTHORPARAM, CREATEDPARAM,
			// Parameters related to delimited/csv files.
			DELIM_FILL_VALUE, DELIM_DELIMITER, DELIM_FILL, DELIM_HAS_HEADER_ROW, DELIM_NA_STRINGS
		};

	public static final String[] READ_VALID_PARAM_NAMES = 
	{	IO_FILENAME, READROWPARAM, READCOLPARAM, FORMAT_TYPE, DATATYPEPARAM, VALUETYPEPARAM, SCHEMAPARAM,
		ROWBLOCKCOUNTPARAM, COLUMNBLOCKCOUNTPARAM, READNNZPARAM, 
			// Parameters related to delimited/csv files.
			DELIM_FILL_VALUE, DELIM_DELIMITER, DELIM_FILL, DELIM_HAS_HEADER_ROW, DELIM_NA_STRINGS
	};
	
	/* Default Values for delimited (CSV/LIBSVM) files */
	public static final String  DEFAULT_DELIM_DELIMITER = ",";
	public static final boolean DEFAULT_DELIM_HAS_HEADER_ROW = false;
	public static final boolean DEFAULT_DELIM_FILL = true;
	public static final double  DEFAULT_DELIM_FILL_VALUE = 0.0;
	public static final boolean DEFAULT_DELIM_SPARSE = false;
	
	private DataOp _opcode;
	private HashMap<String, Expression> _varParams;
	private boolean _strInit = false; //string initialize
	private boolean _checkMetadata = true; // local skip meta data reads

	public DataExpression(){
		//do nothing
	}

	
	public void setCheckMetadata(boolean checkMetadata) {
		_checkMetadata = checkMetadata;
	}

	public static DataExpression getDataExpression(ParserRuleContext ctx, String functionName,
			ArrayList<ParameterExpression> passedParamExprs, String filename, CustomErrorListener errorListener) {
		ParseInfo pi = ParseInfo.ctxAndFilenameToParseInfo(ctx, filename);
		return getDataExpression(functionName, passedParamExprs, pi, errorListener);
	}

	public static DataExpression getDataExpression(String functionName, ArrayList<ParameterExpression> passedParamExprs,
			ParseInfo parseInfo, CustomErrorListener errorListener) {
		if (functionName == null || passedParamExprs == null)
			return null;
		
		// check if the function name is built-in function
		//	 (assign built-in function op if function is built-in)
		Expression.DataOp dop = null;
		DataExpression dataExpr = null;
		if (functionName.equals("read") || functionName.equals("readMM") || functionName.equals("read.csv")) {
			dop = Expression.DataOp.READ;
			dataExpr = new DataExpression(dop, new HashMap<String, Expression>(), parseInfo);

			if (functionName.equals("readMM"))
				dataExpr.addVarParam(DataExpression.FORMAT_TYPE,
						new StringIdentifier(DataExpression.FORMAT_TYPE_VALUE_MATRIXMARKET, parseInfo));

			if (functionName.equals("read.csv"))
				dataExpr.addVarParam(DataExpression.FORMAT_TYPE,
						new StringIdentifier(DataExpression.FORMAT_TYPE_VALUE_CSV, parseInfo));

			if (functionName.equals("read.libsvm"))
				dataExpr.addVarParam(DataExpression.FORMAT_TYPE,
						new StringIdentifier(DataExpression.FORMAT_TYPE_VALUE_LIBSVM, parseInfo));

			// validate the filename is the first parameter
			if (passedParamExprs.size() < 1){
				errorListener.validationError(parseInfo, "read method must have at least filename parameter");
				return null;
			}
			
			ParameterExpression pexpr = (passedParamExprs.size() == 0) ? null : passedParamExprs.get(0);
			
			if ( (pexpr != null) &&  (!(pexpr.getName() == null) || (pexpr.getName() != null && pexpr.getName().equalsIgnoreCase(DataExpression.IO_FILENAME)))){
				errorListener.validationError(parseInfo, "first parameter to read statement must be filename");
				return null;
			} else if( pexpr != null ){
				dataExpr.addVarParam(DataExpression.IO_FILENAME, pexpr.getExpr());
			}
			
			// validate all parameters are added only once and valid name
			for (int i = 1; i < passedParamExprs.size(); i++){
				String currName = passedParamExprs.get(i).getName();
				Expression currExpr = passedParamExprs.get(i).getExpr();
				
				if (dataExpr.getVarParam(currName) != null){
					errorListener.validationError(parseInfo, "attempted to add IOStatement parameter " + currName + " more than once");
					return null;
				}
				// verify parameter names for read function
				boolean isValidName = false;
				for (String paramName : READ_VALID_PARAM_NAMES){
					if (paramName.equals(currName))
						isValidName = true;
				}
				if (!isValidName){
					errorListener.validationError(parseInfo, "attempted to add invalid read statement parameter " + currName);
					return null;
				}	
				dataExpr.addVarParam(currName, currExpr);
			}				
		}
		else if (functionName.equalsIgnoreCase("rand")){
			
			dop = Expression.DataOp.RAND;
			dataExpr = new DataExpression(dop, new HashMap<String, Expression>(), parseInfo);
			
			for (ParameterExpression currExpr : passedParamExprs){
				String pname = currExpr.getName();
				Expression pexpr = currExpr.getExpr();
				if (pname == null){
					errorListener.validationError(parseInfo, "for rand statement, all arguments must be named parameters");
					return null;
				}
				dataExpr.addRandExprParam(pname, pexpr); 
			}
			dataExpr.setRandDefault();
		}
		
		else if (functionName.equals("matrix")){
			dop = Expression.DataOp.MATRIX;
			dataExpr = new DataExpression(dop, new HashMap<String, Expression>(), parseInfo);
		
			int namedParamCount = 0, unnamedParamCount = 0;
			for (ParameterExpression currExpr : passedParamExprs) {
				if (currExpr.getName() == null)
					unnamedParamCount++;
				else
					namedParamCount++;
			}

			// check whether named or unnamed parameters are used
			if (passedParamExprs.size() < 3){
				errorListener.validationError(parseInfo, "for matrix statement, must specify at least 3 arguments: data, rows, cols");
				return null;
			}
			
			if (unnamedParamCount > 1){
				
				if (namedParamCount > 0) {
					errorListener.validationError(parseInfo, "for matrix statement, cannot mix named and unnamed parameters");
					return null;
				}
				
				if (unnamedParamCount < 3) {
					errorListener.validationError(parseInfo, "for matrix statement, must specify at least 3 arguments: data, rows, cols");
					return null;
				}
				

				// assume: data, rows, cols, [byRow], [dimNames]
				dataExpr.addMatrixExprParam(DataExpression.RAND_DATA,passedParamExprs.get(0).getExpr());
				dataExpr.addMatrixExprParam(DataExpression.RAND_ROWS,passedParamExprs.get(1).getExpr());
				dataExpr.addMatrixExprParam(DataExpression.RAND_COLS,passedParamExprs.get(2).getExpr());
				
				if (unnamedParamCount >= 4)
					dataExpr.addMatrixExprParam(DataExpression.RAND_BY_ROW,passedParamExprs.get(3).getExpr());
				
				if (unnamedParamCount == 5)
					dataExpr.addMatrixExprParam(DataExpression.RAND_DIMNAMES,passedParamExprs.get(4).getExpr());
				
				if (unnamedParamCount > 5) {
					errorListener.validationError(parseInfo, "for matrix statement, at most 5 arguments supported: data, rows, cols, byrow, dimname");
					return null;
				}
				
			} else {
				// handle first parameter, which is data and may be unnamed
				ParameterExpression firstParam = passedParamExprs.get(0);
				if (firstParam.getName() != null && !firstParam.getName().equals(DataExpression.RAND_DATA)){
					errorListener.validationError(parseInfo, "matrix method must have data parameter as first parameter or unnamed parameter");
					return null;
				} else {
					dataExpr.addMatrixExprParam(DataExpression.RAND_DATA, passedParamExprs.get(0).getExpr());
				}
				
				for (int i=1; i<passedParamExprs.size(); i++){
					if (passedParamExprs.get(i).getName() == null){
						errorListener.validationError(parseInfo, "for matrix statement, cannot mix named and unnamed parameters, only data parameter can be unnammed");
						return null;
					} else {
						dataExpr.addMatrixExprParam(passedParamExprs.get(i).getName(), passedParamExprs.get(i).getExpr());
					}
				}
			}
			dataExpr.setMatrixDefault();
		} else if (functionName.equals("tensor")){
			dop = Expression.DataOp.TENSOR;
			dataExpr = new DataExpression(dop, new HashMap<String, Expression>(), parseInfo);

			int namedParamCount = 0, unnamedParamCount = 0;
			for (ParameterExpression currExpr : passedParamExprs) {
				if (currExpr.getName() == null)
					unnamedParamCount++;
				else
					namedParamCount++;
			}

			// check whether named or unnamed parameters are used
			if (passedParamExprs.size() < 2){
				errorListener.validationError(parseInfo, "for tensor statement, must specify at least 2 arguments: data, dims[]");
				return null;
			}

			if (unnamedParamCount > 1){
				if (namedParamCount > 0) {
					errorListener.validationError(parseInfo, "for tensor statement, cannot mix named and unnamed parameters");
					return null;
				}

				// assume: data, dims[], [byRow], [dimNames]
				dataExpr.addTensorExprParam(DataExpression.RAND_DATA,passedParamExprs.get(0).getExpr());
				dataExpr.addTensorExprParam(DataExpression.RAND_DIMS,passedParamExprs.get(1).getExpr());

				if (unnamedParamCount >= 3)
					// TODO use byRow parameter
					dataExpr.addTensorExprParam(DataExpression.RAND_BY_ROW,passedParamExprs.get(2).getExpr());

				if (unnamedParamCount == 4)
					dataExpr.addTensorExprParam(DataExpression.RAND_DIMNAMES,passedParamExprs.get(3).getExpr());

				if (unnamedParamCount > 4) {
					errorListener.validationError(parseInfo, "for tensor statement, at most 4 arguments supported: data, dims, byrow, dimname");
					return null;
				}

			} else {
				// handle first parameter, which is data and may be unnamed
				ParameterExpression firstParam = passedParamExprs.get(0);
				if (firstParam.getName() != null && !firstParam.getName().equals(DataExpression.RAND_DATA)){
					errorListener.validationError(parseInfo, "tensor method must have data parameter as first parameter or unnamed parameter");
					return null;
				} else {
					dataExpr.addTensorExprParam(DataExpression.RAND_DATA, passedParamExprs.get(0).getExpr());
				}

				for (int i=1; i<passedParamExprs.size(); i++){
					if (passedParamExprs.get(i).getName() == null){
						errorListener.validationError(parseInfo, "for tensor statement, cannot mix named and unnamed parameters, only data parameter can be unnammed");
						return null;
					} else {
						dataExpr.addTensorExprParam(passedParamExprs.get(i).getName(), passedParamExprs.get(i).getExpr());
					}
				}
			}
			dataExpr.setTensorDefault();
		}

		if (dataExpr != null) {
			dataExpr.setParseInfo(parseInfo);
		}
		return dataExpr;
	
	} // end method getBuiltinFunctionExpression
	
	public void addRandExprParam(String paramName, Expression paramValue) 
	{
		if (DMLScript.VALIDATOR_IGNORE_ISSUES && (paramValue == null)) {
			return;
		}
		// check name is valid
		boolean found = false;
		if (paramName != null ){
			for (String name : RAND_VALID_PARAM_NAMES){
				if (name.equals(paramName)) {
					found = true;
					break;
				}			
			}
		}
		if (!found){
			raiseValidateError("unexpected parameter \"" + paramName +
					"\". Legal parameters for Rand statement are " 
					+ "(capitalization-sensitive): " 	+ RAND_ROWS 	
					+ ", " + RAND_COLS		+ ", " + RAND_MIN + ", " + RAND_MAX  	
					+ ", " + RAND_SPARSITY + ", " + RAND_SEED     + ", " + RAND_PDF + ", " + RAND_LAMBDA);
		}
		if (getVarParam(paramName) != null){
			raiseValidateError("attempted to add Rand statement parameter " + paramValue + " more than once");
		}
		// Process the case where user provides double values to rows or cols
		if (paramName.equals(RAND_ROWS) && paramValue instanceof DoubleIdentifier) {
			paramValue = new IntIdentifier((long) ((DoubleIdentifier) paramValue).getValue(), this);
		} else if (paramName.equals(RAND_COLS) && paramValue instanceof DoubleIdentifier) {
			paramValue = new IntIdentifier((long) ((DoubleIdentifier) paramValue).getValue(), this);
		}
		// add the parameter to expression list
		paramValue.setParseInfo(this);
		addVarParam(paramName,paramValue);
		
	}
	
	public void addMatrixExprParam(String paramName, Expression paramValue) 
	{
		// check name is valid
		boolean found = false;
		if (paramName != null ){
			found = Arrays.stream(RESHAPE_VALID_PARAM_NAMES).anyMatch((name) -> name.equals(paramName));
		}
		
		if (!found){
			raiseValidateError("unexpected parameter \"" + paramName +
					"\". Legal parameters for  matrix statement are " 
					+ "(capitalization-sensitive): " + RAND_DATA + ", " + RAND_ROWS
					+ ", " + RAND_COLS + ", " + RAND_BY_ROW);
		}
		if (getVarParam(paramName) != null) {
			raiseValidateError("attempted to add matrix statement parameter " + paramValue + " more than once");
		}
		// Process the case where user provides double values to rows or cols
		if (paramName.equals(RAND_ROWS) && paramValue instanceof DoubleIdentifier) {
			paramValue = new IntIdentifier((long) ((DoubleIdentifier) paramValue).getValue(), this);
		} else if (paramName.equals(RAND_COLS) && paramValue instanceof DoubleIdentifier) {
			paramValue = new IntIdentifier((long) ((DoubleIdentifier) paramValue).getValue(), this);
		}

		// add the parameter to expression list
		paramValue.setParseInfo(this);
		addVarParam(paramName,paramValue);
	}

	public void addTensorExprParam(String paramName, Expression paramValue)
	{
		// check name is valid
		boolean found = false;
		if (paramName != null ){
			found = Arrays.stream(RESHAPE_VALID_PARAM_NAMES).anyMatch((name) -> name.equals(paramName));
		}

		if (!found){
			raiseValidateError("unexpected parameter \"" + paramName + "\". Legal parameters for tensor statement are "
					+ "(capitalization-sensitive): " + RAND_DATA + ", " + RAND_DIMS +
					", " + RAND_BY_ROW + ", " + RAND_DIMNAMES);
		}
		if (getVarParam(paramName) != null) {
			raiseValidateError("attempted to add tensor statement parameter " + paramValue + " more than once");
		}
		// Process the case where user provides double values to rows or cols
		// TODO convert double Matrix to long Matrix
		/*if (paramName.equals(RAND_DIMS) && paramValue instanceof DoubleIdentifier) {
			paramValue = new IntIdentifier((long) ((DoubleIdentifier) paramValue).getValue(), this);
		} else if (paramName.equals(RAND_COLS) && paramValue instanceof DoubleIdentifier) {
			paramValue = new IntIdentifier((long) ((DoubleIdentifier) paramValue).getValue(), this);
		}*/

		// add the parameter to expression list
		paramValue.setParseInfo(this);
		addVarParam(paramName,paramValue);
	}

	public DataExpression(DataOp op, HashMap<String, Expression> varParams, ParseInfo parseInfo) {
		_opcode = op;
		_varParams = varParams;
		setParseInfo(parseInfo);
	}

	public DataExpression(ParserRuleContext ctx, DataOp op, HashMap<String,Expression> varParams, 
			String filename) {
		_opcode = op;
		_varParams = varParams;
		setCtxValuesAndFilename(ctx, filename);
	}

	@Override
	public Expression rewriteExpression(String prefix) {
		HashMap<String,Expression> newVarParams = new HashMap<>();
		for( Entry<String, Expression> e : _varParams.entrySet() ){
			String key = e.getKey();
			Expression newExpr = e.getValue().rewriteExpression(prefix);
			newVarParams.put(key, newExpr);
		}
		DataExpression retVal = new DataExpression(_opcode, newVarParams, this);
		retVal._strInit = this._strInit;
		
		return retVal;
	}

	/**
	 * By default we use rowwise matrix reshape according to our internal dense/sparse matrix representations.
	 * ByRow specifies both input and output orientation. Note that this is different from R, where inputs are 
	 * always read by-column and the default for byRow is by-column as well.
	 */
	public void setMatrixDefault(){
		if (getVarParam(RAND_BY_ROW) == null)
			addVarParam(RAND_BY_ROW, new BooleanIdentifier(true, this));
	}

	public void setTensorDefault(){
		if (getVarParam(RAND_BY_ROW) == null)
			addVarParam(RAND_BY_ROW, new BooleanIdentifier(true, this));
	}

	public void setRandDefault() {
		if (getVarParam(RAND_DIMS) == null) {
			if( getVarParam(RAND_ROWS) == null ) {
				IntIdentifier id = new IntIdentifier(1L, this);
				addVarParam(RAND_ROWS, id);
			}
			if( getVarParam(RAND_COLS) == null ) {
				IntIdentifier id = new IntIdentifier(1L, this);
				addVarParam(RAND_COLS, id);
			}
		}
		if (getVarParam(RAND_MIN) == null) {
			DoubleIdentifier id = new DoubleIdentifier(0.0, this);
			addVarParam(RAND_MIN, id);
		}
		if (getVarParam(RAND_MAX) == null) {
			DoubleIdentifier id = new DoubleIdentifier(1.0, this);
			addVarParam(RAND_MAX, id);
		}
		if (getVarParam(RAND_SPARSITY) == null) {
			DoubleIdentifier id = new DoubleIdentifier(1.0, this);
			addVarParam(RAND_SPARSITY, id);
		}
		if (getVarParam(RAND_SEED) == null) {
			IntIdentifier id = new IntIdentifier(DataGenOp.UNSPECIFIED_SEED, this);
			addVarParam(RAND_SEED, id);
		}
		if (getVarParam(RAND_PDF) == null) {
			StringIdentifier id = new StringIdentifier(RAND_PDF_UNIFORM, this);
			addVarParam(RAND_PDF, id);
		}
		if (getVarParam(RAND_LAMBDA) == null) {
			DoubleIdentifier id = new DoubleIdentifier(1.0, this);
			addVarParam(RAND_LAMBDA, id);
		}
	}

	public void setOpCode(DataOp op) {
		_opcode = op;
	}
	
	public DataOp getOpCode() {
		return _opcode;
	}
	
	public HashMap<String,Expression> getVarParams() {
		return _varParams;
	}
	
	public void setVarParams(HashMap<String, Expression> varParams) {
		_varParams = varParams;
	}
	
	public Expression getVarParam(String name) {
		return _varParams.get(name);
	}

	public void addVarParam(String name, Expression value){
		if (DMLScript.VALIDATOR_IGNORE_ISSUES && (value == null)) {
			return;
		}
		_varParams.put(name, value);
		
		// if required, initialize values
		setFilename(value.getFilename());
		if (getBeginLine() == 0) setBeginLine(value.getBeginLine());
		if (getBeginColumn() == 0) setBeginColumn(value.getBeginColumn());
		if (getEndLine() == 0) setEndLine(value.getEndLine());
		if (getEndColumn() == 0) setEndColumn(value.getEndColumn());
		if (getText() == null) setText(value.getText());
	}
	
	public void removeVarParam(String name) {
		_varParams.remove(name);
	}
	
	public void removeVarParam(String... names) {
		for( String name : names )
			removeVarParam(name);
	}
	
	private String getInputFileName(HashMap<String, ConstIdentifier> currConstVars, boolean conditional) {
		String filename = null;
		
		Expression fileNameExpr = getVarParam(IO_FILENAME);
		if (fileNameExpr instanceof ConstIdentifier){
			return fileNameExpr.toString();
		}
		else if (fileNameExpr instanceof BinaryExpression) {
			BinaryExpression expr = (BinaryExpression)fileNameExpr;
			Expression.BinaryOp op = expr.getOpCode();
			switch (op){
			case PLUS:
				filename = "";
				filename = fileNameCat(expr, currConstVars, filename, conditional);
				// Since we have computed the value of filename, we update
				// varParams with a const string value
				StringIdentifier fileString = new StringIdentifier(filename, this);
				removeVarParam(IO_FILENAME);
				addVarParam(IO_FILENAME, fileString);
				break;
			default:
				raiseValidateError("for read method, parameter " + IO_FILENAME + " can only be const string concatenations. ", conditional);
			}
		}
		else {
			raiseValidateError("for read method, parameter " + IO_FILENAME + " can only be a const string or const string concatenations. ", conditional);
		}
		
		return filename;
	}
	
	public static String getMTDFileName(String inputFileName) {
		return inputFileName + ".mtd";
	}
	
	/**
	 * Validate parse tree : Process Data Expression in an assignment
	 * statement
	 */
	@Override
	public void validateExpression(HashMap<String, DataIdentifier> ids, HashMap<String, ConstIdentifier> currConstVars, boolean conditional)
	{
		// validate all input parameters
		for ( Entry<String,Expression> e : getVarParams().entrySet() ) {
			String s = e.getKey();
			Expression inputParamExpr = e.getValue();
			
			if (inputParamExpr instanceof FunctionCallIdentifier) {
				raiseValidateError("UDF function call not supported as parameter to built-in function call", false,LanguageErrorCodes.INVALID_PARAMETERS);
			}
			
			inputParamExpr.validateExpression(ids, currConstVars, conditional);
			if ( getVarParam(s).getOutput().getDataType() != DataType.SCALAR && !s.equals(RAND_DATA) && !s.equals(RAND_DIMS)) {
				raiseValidateError("Non-scalar data types are not supported for data expression.", conditional,LanguageErrorCodes.INVALID_PARAMETERS);
			}
		}
	
		//general data expression constant propagation
		performConstantPropagationRand( currConstVars );
		performConstantPropagationReadWrite( currConstVars );
		
		// check if data parameter of matrix is scalar or matrix -- if scalar, use Rand instead
		Expression dataParam1 = getVarParam(RAND_DATA);
		if (dataParam1 == null && (getOpCode().equals(DataOp.MATRIX) || getOpCode().equals(DataOp.TENSOR))){
			raiseValidateError("for matrix or tensor, must defined data parameter", conditional, LanguageErrorCodes.INVALID_PARAMETERS);
		}
		// We need to remember the operation if we replace the OpCode by rand so we have the correct output
		if (dataParam1 != null && dataParam1.getOutput().getDataType() == DataType.SCALAR &&
				(_opcode == DataOp.MATRIX || _opcode == DataOp.TENSOR)/*&& dataParam instanceof ConstIdentifier*/ ){
			//MB: note we should not check for const identifiers here, because otherwise all matrix constructors with
			//variable input are routed to a reshape operation (but it works only on matrices and hence, crashes)
			
			// replace DataOp MATRIX with RAND -- Rand handles matrix generation for Scalar values
			// replace data parameter with min / max within Rand case below
			this.setOpCode(DataOp.RAND);
		}
		
		
		// IMPORTANT: for each operation, one must handle unnamed parameters
		
		switch (this.getOpCode()) {
		
		case READ:
			if (getVarParam(DATATYPEPARAM) != null && !(getVarParam(DATATYPEPARAM) instanceof StringIdentifier)){
				raiseValidateError("for read statement, parameter " + DATATYPEPARAM + " can only be a string. " +
						"Valid values are: " + Statement.MATRIX_DATA_TYPE +", " + Statement.SCALAR_DATA_TYPE, conditional);
			}
			
			String dataTypeString = (getVarParam(DATATYPEPARAM) == null) ? null : getVarParam(DATATYPEPARAM).toString();
			
			// disallow certain parameters while reading a scalar
			if (dataTypeString != null && dataTypeString.equalsIgnoreCase(Statement.SCALAR_DATA_TYPE)){
				if ( getVarParam(READROWPARAM) != null
						|| getVarParam(READCOLPARAM) != null
						|| getVarParam(ROWBLOCKCOUNTPARAM) != null
						|| getVarParam(COLUMNBLOCKCOUNTPARAM) != null
						|| getVarParam(FORMAT_TYPE) != null
						|| getVarParam(DELIM_DELIMITER) != null	
						|| getVarParam(DELIM_HAS_HEADER_ROW) != null
						|| getVarParam(DELIM_FILL) != null
						|| getVarParam(DELIM_FILL_VALUE) != null
						|| getVarParam(DELIM_NA_STRINGS) != null
						)
				{
					raiseValidateError("Invalid parameters in read statement of a scalar: " +
						toString() + ". Only " + VALUETYPEPARAM + " is allowed.", conditional, LanguageErrorCodes.INVALID_PARAMETERS);
				}
			}
			
			JSONObject configObject = null;

			// Process expressions in input filename
			String inputFileName = getInputFileName(currConstVars, conditional);
			
			// Obtain and validate metadata filename
			String mtdFileName = getMTDFileName(inputFileName);

			// track whether should attempt to read MTD file or not
			boolean shouldReadMTD = _checkMetadata
				&& (!ConfigurationManager.getCompilerConfigFlag(ConfigType.IGNORE_READ_WRITE_METADATA)
					|| HDFSTool.existsFileOnHDFS(mtdFileName)); // existing mtd file

			// Check for file existence (before metadata parsing for meaningful error messages)
			if( shouldReadMTD //skip check for jmlc/mlcontext
				&& !HDFSTool.existsFileOnHDFS(inputFileName)) 
			{
				String fsext = InfrastructureAnalyzer.isLocalMode() ? "FS (local mode)" : "HDFS";
				raiseValidateError("Read input file does not exist on "+fsext+": " + 
					inputFileName, conditional);
			}

			// track whether format type has been inferred 
			boolean inferredFormatType = false;
			
			// get format type string
			String formatTypeString = (getVarParam(FORMAT_TYPE) == null) ? null : getVarParam(FORMAT_TYPE).toString();
			
			// check if file is matrix market format
			if (formatTypeString == null && shouldReadMTD){
				if ( checkHasMatrixMarketFormat(inputFileName, mtdFileName, conditional) ) {
					formatTypeString = FORMAT_TYPE_VALUE_MATRIXMARKET;
					addVarParam(FORMAT_TYPE, new StringIdentifier(FORMAT_TYPE_VALUE_MATRIXMARKET, this));
					inferredFormatType = true;
					shouldReadMTD = false;
				}
			}
			
			// check if file is delimited format
			if (formatTypeString == null && shouldReadMTD ) {
				formatTypeString = checkHasDelimitedFormat(inputFileName, conditional);
				if (formatTypeString != null) {
					addVarParam(FORMAT_TYPE, new StringIdentifier(formatTypeString, this));
					inferredFormatType = true;
				}
			}
			
			if (formatTypeString != null && formatTypeString.equalsIgnoreCase(FORMAT_TYPE_VALUE_MATRIXMARKET)){
				/*
				 *  handle MATRIXMARKET_FORMAT_TYPE format
				 *
				 * 1) only allow IO_FILENAME as ONLY valid parameter
				 * 
				 * 2) open the file
				 *  A) verify header line (1st line) equals 
				 *  B) read and discard comment lines
				 *  C) get size information from sizing info line --- M N L
				 */
				
				// should NOT attempt to read MTD file for MatrixMarket format
				shouldReadMTD = false;
				
				// get metadata from MatrixMarket format file
				String[] headerLines = null;
				try {
					headerLines = IOUtilFunctions.readMatrixMarketHeader(inputFileName);
				}
				catch(DMLRuntimeException ex) {
					raiseValidateError(ex.getMessage(), conditional);
				}
				
				if (headerLines != null && headerLines.length >= 2){
					// process 1st line of MatrixMarket format to check for support types
					
					String firstLine = headerLines[0].trim();
					FileFormatPropertiesMM props = FileFormatPropertiesMM.parse(firstLine);
					
					// process 2nd line of MatrixMarket format -- must have size information
				
					String secondLine = headerLines[1];
					String[] sizeInfo = secondLine.trim().split("\\s+");
					if (sizeInfo.length != 3){
						raiseValidateError("Unsupported size line in MatrixMarket file: " +
							headerLines[1] + ". Only supported format in MatrixMarket file has size line: <NUM ROWS> <NUM COLS> <NUM NON-ZEROS>, where each value is an integer.", conditional);
					}
				
					long rowsCount = Long.parseLong(sizeInfo[0]);
					if (rowsCount < 0)
						raiseValidateError("MM file: invalid number of rows: "+rowsCount);
					else if( getVarParam(READROWPARAM) != null ) {
						long rowsCount2 = Long.parseLong(getVarParam(READROWPARAM).toString());
						if( rowsCount2 != rowsCount )
							raiseValidateError("MM file: invalid specified number of rows: "+rowsCount2+" vs "+rowsCount);
					}
					addVarParam(READROWPARAM, new IntIdentifier(rowsCount, this));
					

					long colsCount = Long.parseLong(sizeInfo[1]);
					if (colsCount < 0)
						raiseValidateError("MM file: invalid number of columns: "+colsCount);
					else if( getVarParam(READCOLPARAM) != null ) {
						long colsCount2 = Long.parseLong(getVarParam(READCOLPARAM).toString());
						if( colsCount2 != colsCount )
							raiseValidateError("MM file: invalid specified number of columns: "+colsCount2+" vs "+colsCount);
					}
					addVarParam(READCOLPARAM, new IntIdentifier(colsCount, this));
					
					long nnzCount = Long.parseLong(sizeInfo[2]) * (props.isSymmetric() ? 2 : 1);
					if (nnzCount < 0)
						raiseValidateError("MM file: invalid number of non-zeros: "+nnzCount);
					else if( getVarParam(READNNZPARAM) != null ) {
						long nnzCount2 = Long.parseLong(getVarParam(READNNZPARAM).toString());
						if( nnzCount2 != nnzCount )
							raiseValidateError("MM file: invalid specified number of non-zeros: "+nnzCount2+" vs "+nnzCount);
					}
					addVarParam(READNNZPARAM, new IntIdentifier(nnzCount, this));
				}
			}
			
			if (shouldReadMTD){
				configObject = readMetadataFile(mtdFileName, conditional);
				// if the MTD file exists, check the values specified in read statement match values in metadata MTD file
				if (configObject != null){
					parseMetaDataFileParameters(mtdFileName, configObject, conditional);
					inferredFormatType = true;
				}
				else {
					LOG.warn("Metadata file: " + new Path(mtdFileName) + " not provided");
				}
			}
			
			boolean isCSV = false;
			isCSV = (formatTypeString != null && formatTypeString.equalsIgnoreCase(FORMAT_TYPE_VALUE_CSV));
			if (isCSV){
				 // Handle delimited file format
				 // 
				 // 1) only allow IO_FILENAME, _HEADER_ROW, FORMAT_DELIMITER, READROWPARAM, READCOLPARAM   
				 //  
				 // 2) open the file
				 //
			
				// there should be no MTD file for delimited file format
				shouldReadMTD = true;
				
				// only allow IO_FILENAME, HAS_HEADER_ROW, FORMAT_DELIMITER, READROWPARAM, READCOLPARAM   
				//		as ONLY valid parameters
				if( !inferredFormatType ){
					for (String key : _varParams.keySet()){
						if (!  (key.equals(IO_FILENAME) || key.equals(FORMAT_TYPE) 
								|| key.equals(DELIM_HAS_HEADER_ROW) || key.equals(DELIM_DELIMITER) 
								|| key.equals(DELIM_FILL) || key.equals(DELIM_FILL_VALUE)
								|| key.equals(READROWPARAM) || key.equals(READCOLPARAM)
								|| key.equals(READNNZPARAM) || key.equals(DATATYPEPARAM) || key.equals(VALUETYPEPARAM)
								|| key.equals(SCHEMAPARAM)) )
						{	
							String msg = "Only parameters allowed are: " + IO_FILENAME     + "," 
									   + SCHEMAPARAM + "," 
									   + DELIM_HAS_HEADER_ROW   + "," 
									   + DELIM_DELIMITER 	+ ","
									   + DELIM_FILL 		+ ","
									   + DELIM_FILL_VALUE 	+ ","
									   + READROWPARAM     + "," 
									   + READCOLPARAM;
							
							raiseValidateError("Invalid parameter " + key + " in read statement: " +
									toString() + ". " + msg, conditional, LanguageErrorCodes.INVALID_PARAMETERS);
						}
					}
				}
				
				// DEFAULT for "sep" : ","
				if (getVarParam(DELIM_DELIMITER) == null) {
					addVarParam(DELIM_DELIMITER, new StringIdentifier(DEFAULT_DELIM_DELIMITER, this));
				} else {
					if ( (getVarParam(DELIM_DELIMITER) instanceof ConstIdentifier)
						&& (! (getVarParam(DELIM_DELIMITER) instanceof StringIdentifier)))
					{
						raiseValidateError("For delimited file '" + getVarParam(DELIM_DELIMITER) 
								+  "' must be a string value ", conditional);
					}
				} 
				
				// DEFAULT for "default": 0
				if (getVarParam(DELIM_FILL_VALUE) == null) {
					addVarParam(DELIM_FILL_VALUE, new DoubleIdentifier(DEFAULT_DELIM_FILL_VALUE, this));
				} else {
					if ( (getVarParam(DELIM_FILL_VALUE) instanceof ConstIdentifier)
							&& (! (getVarParam(DELIM_FILL_VALUE) instanceof IntIdentifier ||  getVarParam(DELIM_FILL_VALUE) instanceof DoubleIdentifier)))
					{
						raiseValidateError("For delimited file '" + getVarParam(DELIM_FILL_VALUE)  +  "' must be a numeric value ", conditional);
					}
				} 
				
				// DEFAULT for "header": boolean false
				if (getVarParam(DELIM_HAS_HEADER_ROW) == null) {
					addVarParam(DELIM_HAS_HEADER_ROW, new BooleanIdentifier(DEFAULT_DELIM_HAS_HEADER_ROW, this));
				} else {
					if ((getVarParam(DELIM_HAS_HEADER_ROW) instanceof ConstIdentifier)
						&& (! (getVarParam(DELIM_HAS_HEADER_ROW) instanceof BooleanIdentifier)))
					{
						raiseValidateError("For delimited file '" + getVarParam(DELIM_HAS_HEADER_ROW) + "' must be a boolean value ", conditional);
					}
				}
				
				// DEFAULT for "fill": boolean false
				if (getVarParam(DELIM_FILL) == null){
					addVarParam(DELIM_FILL, new BooleanIdentifier(DEFAULT_DELIM_FILL, this));
				}
				else {
					
					if ((getVarParam(DELIM_FILL) instanceof ConstIdentifier)
							&& (! (getVarParam(DELIM_FILL) instanceof BooleanIdentifier)))
					{
						raiseValidateError("For delimited file '" + getVarParam(DELIM_FILL) + "' must be a boolean value ", conditional);
					}
				}		
			} 

			boolean islibsvm = false;
			islibsvm = (formatTypeString != null && formatTypeString.equalsIgnoreCase(FORMAT_TYPE_VALUE_LIBSVM));
			if (islibsvm){
				 // Handle libsvm file format
				shouldReadMTD = true;
				
				// only allow IO_FILENAME, READROWPARAM, READCOLPARAM   
				// as valid parameters
				if( !inferredFormatType ){
					for (String key : _varParams.keySet()){
						if (!  (key.equals(IO_FILENAME) || key.equals(FORMAT_TYPE) 
								|| key.equals(READROWPARAM) || key.equals(READCOLPARAM)
								|| key.equals(READNNZPARAM) || key.equals(DATATYPEPARAM) 
								|| key.equals(VALUETYPEPARAM) ))
						{	
							String msg = "Only parameters allowed are: " + IO_FILENAME     + "," 
									   + READROWPARAM     + "," 
									   + READCOLPARAM;
							
							raiseValidateError("Invalid parameter " + key + " in read statement: " +
									toString() + ". " + msg, conditional, LanguageErrorCodes.INVALID_PARAMETERS);
						}
					}
				}
			}
			
	        dataTypeString = (getVarParam(DATATYPEPARAM) == null) ? null : getVarParam(DATATYPEPARAM).toString();
			
			if ( dataTypeString == null || dataTypeString.equalsIgnoreCase(Statement.MATRIX_DATA_TYPE) 
					|| dataTypeString.equalsIgnoreCase(Statement.FRAME_DATA_TYPE)) {
				
				boolean isMatrix = false;
				if ( dataTypeString == null || dataTypeString.equalsIgnoreCase(Statement.MATRIX_DATA_TYPE))
						isMatrix = true;
				
				// set data type
		        getOutput().setDataType(isMatrix ? DataType.MATRIX : DataType.FRAME);
		        
		        // set number non-zeros
		        Expression ennz = this.getVarParam("nnz");
		        long nnz = -1;
		        if( ennz != null )
		        {
			        nnz = Long.valueOf(ennz.toString());
			        getOutput().setNnz(nnz);
		        }
		        
		        // Following dimension checks must be done when data type = MATRIX_DATA_TYPE 
				// initialize size of target data identifier to UNKNOWN
				getOutput().setDimensions(-1, -1);
				
				if ( !isCSV && ConfigurationManager.getCompilerConfig()
						.getBool(ConfigType.REJECT_READ_WRITE_UNKNOWNS) //skip check for csv format / jmlc api
					&& (getVarParam(READROWPARAM) == null || getVarParam(READCOLPARAM) == null) ) {
						raiseValidateError("Missing or incomplete dimension information in read statement: "
								+ mtdFileName, conditional, LanguageErrorCodes.INVALID_PARAMETERS);
				}
				
				if (getVarParam(READROWPARAM) instanceof ConstIdentifier 
					&& getVarParam(READCOLPARAM) instanceof ConstIdentifier)
				{
					// these are strings that are long values
					Long dim1 = (getVarParam(READROWPARAM) == null) ? null : Long.valueOf( getVarParam(READROWPARAM).toString());
					Long dim2 = (getVarParam(READCOLPARAM) == null) ? null : Long.valueOf( getVarParam(READCOLPARAM).toString());
					if ( !isCSV && (dim1 < 0 || dim2 < 0) && ConfigurationManager
							.getCompilerConfig().getBool(ConfigType.REJECT_READ_WRITE_UNKNOWNS) ) {
						raiseValidateError("Invalid dimension information in read statement", conditional, LanguageErrorCodes.INVALID_PARAMETERS);
					}
					
					// set dim1 and dim2 values 
					if (dim1 != null && dim2 != null){
						getOutput().setDimensions(dim1, dim2);
					} else if (!isCSV && ((dim1 != null) || (dim2 != null))) {
						raiseValidateError("Partial dimension information in read statement", conditional, LanguageErrorCodes.INVALID_PARAMETERS);
					}
				}
				
				// initialize block dimensions to UNKNOWN 
				getOutput().setBlocksize(-1);
				
				// find "format": 1=text, 2=binary
				int format = 1; // default is "text"
				String fmt =  (getVarParam(FORMAT_TYPE) == null ? null : getVarParam(FORMAT_TYPE).toString());
				
				if (fmt == null || fmt.equalsIgnoreCase("text")){
					getOutput().setFormatType(FormatType.TEXT);
					format = 1;
				} else if ( fmt.equalsIgnoreCase("binary") ) {
					getOutput().setFormatType(FormatType.BINARY);
					format = 2;
				} else if ( fmt.equalsIgnoreCase(FORMAT_TYPE_VALUE_CSV)) 
				{
					getOutput().setFormatType(FormatType.CSV);
					format = 1;
				}
				else if ( fmt.equalsIgnoreCase(FORMAT_TYPE_VALUE_MATRIXMARKET) )
				{
					getOutput().setFormatType(FormatType.MM);
					format = 1;
				} 
				else if ( fmt.equalsIgnoreCase(FORMAT_TYPE_VALUE_LIBSVM)) 
				{
					getOutput().setFormatType(FormatType.LIBSVM);
					format = 1;
				} else {
					raiseValidateError("Invalid format '" + fmt+ "' in statement: " + this.toString(), conditional);
				}
				
				if (getVarParam(ROWBLOCKCOUNTPARAM) instanceof ConstIdentifier && getVarParam(COLUMNBLOCKCOUNTPARAM) instanceof ConstIdentifier)  {
					Integer rowBlockCount = (getVarParam(ROWBLOCKCOUNTPARAM) == null) ?
						null : Integer.valueOf(getVarParam(ROWBLOCKCOUNTPARAM).toString());
					getOutput().setBlocksize(rowBlockCount != null ? rowBlockCount : -1);
				}
				
				// block dimensions must be -1x-1 when format="text"
				// NOTE MB: disabled validate of default blocksize for inputs w/ format="binary"
				// because we automatically introduce reblocks if blocksizes don't match
				if ( (format == 1 || !isMatrix)  && getOutput().getBlocksize() != -1 ){
					raiseValidateError("Invalid block dimensions (" + getOutput().getBlocksize() + ") when format=" + getVarParam(FORMAT_TYPE) + " in \"" + this.toString() + "\".", conditional);
				}
			
			}
			else if ( dataTypeString.equalsIgnoreCase(Statement.SCALAR_DATA_TYPE)) {
				getOutput().setDataType(DataType.SCALAR);
				getOutput().setNnz(-1L);
			}
			
			else{		
				raiseValidateError("Unknown Data Type " + dataTypeString + ". Valid  values: " + Statement.SCALAR_DATA_TYPE +", " + Statement.MATRIX_DATA_TYPE, conditional, LanguageErrorCodes.INVALID_PARAMETERS);
			}
			
			// handle value type parameter
			if (getVarParam(VALUETYPEPARAM) != null && !(getVarParam(VALUETYPEPARAM) instanceof StringIdentifier)){
				raiseValidateError("for read method, parameter " + VALUETYPEPARAM + " can only be a string. " +
						"Valid values are: " + Statement.DOUBLE_VALUE_TYPE +", " + Statement.INT_VALUE_TYPE + ", " + Statement.BOOLEAN_VALUE_TYPE + ", " + Statement.STRING_VALUE_TYPE, conditional);
			}
			// Identify the value type (used only for read method)
			String valueTypeString = getVarParam(VALUETYPEPARAM) == null ? null :  getVarParam(VALUETYPEPARAM).toString();
			if (valueTypeString != null) {
				if (valueTypeString.equalsIgnoreCase(Statement.DOUBLE_VALUE_TYPE)) {
					getOutput().setValueType(ValueType.FP64);
				} else if (valueTypeString.equalsIgnoreCase(Statement.STRING_VALUE_TYPE)) {
					getOutput().setValueType(ValueType.STRING);
				} else if (valueTypeString.equalsIgnoreCase(Statement.INT_VALUE_TYPE)) {
					getOutput().setValueType(ValueType.INT64);
				} else if (valueTypeString.equalsIgnoreCase(Statement.BOOLEAN_VALUE_TYPE)) {
					getOutput().setValueType(ValueType.BOOLEAN);
				} else {
					raiseValidateError("Unknown Value Type " + valueTypeString
							+ ". Valid values are: " + Statement.DOUBLE_VALUE_TYPE +", " + Statement.INT_VALUE_TYPE + ", " + Statement.BOOLEAN_VALUE_TYPE + ", " + Statement.STRING_VALUE_TYPE, conditional);
				}
			} else {
				getOutput().setValueType(ValueType.FP64);
			}

			break; 
			
		case WRITE:
			
			// for delimited format, if no delimiter specified THEN set default ","
			if (getVarParam(FORMAT_TYPE) == null || getVarParam(FORMAT_TYPE).toString().equalsIgnoreCase(FORMAT_TYPE_VALUE_CSV)){
				if (getVarParam(DELIM_DELIMITER) == null) {
					addVarParam(DELIM_DELIMITER, new StringIdentifier(DEFAULT_DELIM_DELIMITER, this));
				}
				if (getVarParam(DELIM_HAS_HEADER_ROW) == null) {
					addVarParam(DELIM_HAS_HEADER_ROW, new BooleanIdentifier(DEFAULT_DELIM_HAS_HEADER_ROW, this));
				}
				if (getVarParam(DELIM_SPARSE) == null) {
					addVarParam(DELIM_SPARSE, new BooleanIdentifier(DEFAULT_DELIM_SPARSE, this));
				}
			}

			if (getVarParam(FORMAT_TYPE) == null || getVarParam(FORMAT_TYPE).toString().equalsIgnoreCase(FORMAT_TYPE_VALUE_LIBSVM)){
				if (getVarParam(DELIM_SPARSE) == null) {
					addVarParam(DELIM_SPARSE, new BooleanIdentifier(DEFAULT_DELIM_SPARSE, this));
				}
			}
			
			/* NOTE MB: disabled filename concatenation because we now support dynamic rewrite
			if (getVarParam(IO_FILENAME) instanceof BinaryExpression){
				BinaryExpression expr = (BinaryExpression)getVarParam(IO_FILENAME);
								
				if (expr.getKind()== Expression.Kind.BinaryOp){
					Expression.BinaryOp op = expr.getOpCode();
					switch (op){
						case PLUS:
							mtdFileName = "";
							mtdFileName = fileNameCat(expr, currConstVars, mtdFileName);
							// Since we have computed the value of filename, we update
							// varParams with a const string value
							StringIdentifier fileString = new StringIdentifier(mtdFileName, 
									this.getFilename(), this.getBeginLine(), this.getBeginColumn(), 
									this.getEndLine(), this.getEndColumn());
							removeVarParam(IO_FILENAME);
							addVarParam(IO_FILENAME, fileString);
												
							break;
						default:
							raiseValidateError("for OutputStatement, parameter " + IO_FILENAME 
									+ " can only be a const string or const string concatenations. ", 
									conditional);
					}
				}
			}*/
			
			//validate read filename
			if (getVarParam(FORMAT_TYPE) == null || getVarParam(FORMAT_TYPE).toString().equalsIgnoreCase("text"))
				getOutput().setBlocksize(-1);
			else if (getVarParam(FORMAT_TYPE).toString().equalsIgnoreCase("binary"))
				getOutput().setBlocksize(ConfigurationManager.getBlocksize());
			else if (getVarParam(FORMAT_TYPE).toString().equalsIgnoreCase(FORMAT_TYPE_VALUE_MATRIXMARKET) || 
					(getVarParam(FORMAT_TYPE).toString().equalsIgnoreCase(FORMAT_TYPE_VALUE_CSV)) ||
					 getVarParam(FORMAT_TYPE).toString().equalsIgnoreCase(FORMAT_TYPE_VALUE_LIBSVM))
				getOutput().setBlocksize(-1);
			
			else{
				raiseValidateError("Invalid format " + getVarParam(FORMAT_TYPE) +  " in statement: " + this.toString(), conditional);
			}
			break;

			case RAND:
			
			Expression dataParam = getVarParam(RAND_DATA);
			
			if( dataParam != null )
			{
				// handle input variable (matrix/scalar)
				if( dataParam instanceof DataIdentifier )
				{
					addVarParam(RAND_MIN, dataParam);
					addVarParam(RAND_MAX, dataParam);
				}
				// handle integer constant
				else if (dataParam instanceof IntIdentifier) {
					addVarParam(RAND_MIN, dataParam);
					addVarParam(RAND_MAX, dataParam);
				}
				// handle double constant
				else if (dataParam instanceof DoubleIdentifier) {
					double roundedValue = ((DoubleIdentifier)dataParam).getValue();
					Expression minExpr = new DoubleIdentifier(roundedValue, this);
					addVarParam(RAND_MIN, minExpr);
					addVarParam(RAND_MAX, minExpr);
				}
				// handle string constant (string init)
				else if (dataParam instanceof StringIdentifier) {
					String data = ((StringIdentifier)dataParam).getValue();
					Expression minExpr = new StringIdentifier(data, this);
					addVarParam(RAND_MIN, minExpr);
					addVarParam(RAND_MAX, minExpr);
					_strInit = true;
				}
				else {
					// handle general expression
					dataParam.validateExpression(ids, currConstVars, conditional);
					addVarParam(RAND_MIN, dataParam);
					addVarParam(RAND_MAX, dataParam);
				}
				
				removeVarParam(RAND_DATA);
				removeVarParam(RAND_BY_ROW);
				this.setRandDefault();
			}
			
			//check valid parameters
			validateParams(conditional, RAND_VALID_PARAM_NAMES, "Legal parameters for Rand statement are "
					+ "(capitalization-sensitive): " 	+ RAND_ROWS + ", " + RAND_COLS + ", " + RAND_DIMS + ", "
					+ RAND_MIN + ", " + RAND_MAX + ", " + RAND_SPARSITY + ", " + RAND_SEED     + ", "
					+ RAND_PDF  + ", " + RAND_LAMBDA);

			//parameters w/ support for variable inputs
			if (getVarParam(RAND_ROWS) instanceof StringIdentifier || getVarParam(RAND_ROWS) instanceof BooleanIdentifier){
				raiseValidateError("for Rand statement " + RAND_ROWS + " has incorrect value type", conditional);
			}
			
			if (getVarParam(RAND_COLS) instanceof StringIdentifier || getVarParam(RAND_COLS) instanceof BooleanIdentifier){
				raiseValidateError("for Rand statement " + RAND_COLS + " has incorrect value type", conditional);
			}

			if (getVarParam(RAND_DIMS) instanceof IntIdentifier || getVarParam(RAND_DIMS) instanceof DoubleIdentifier
					|| getVarParam(RAND_DIMS) instanceof BooleanIdentifier){
				raiseValidateError("for Rand statement " + RAND_DIMS + " has incorrect value type", conditional);
			}

			if (getVarParam(RAND_SEED) instanceof StringIdentifier || getVarParam(RAND_SEED) instanceof BooleanIdentifier) {
				raiseValidateError("for Rand statement " + RAND_SEED + " has incorrect value type", conditional);
			}

			boolean isTensorOperation = getVarParam(RAND_DIMS) != null;

			if ((getVarParam(RAND_MAX) instanceof StringIdentifier && !_strInit) ||
					(getVarParam(RAND_MAX) instanceof BooleanIdentifier && !isTensorOperation)) {
				raiseValidateError("for Rand statement " + RAND_MAX + " has incorrect value type", conditional);
			}

			if ((getVarParam(RAND_MIN) instanceof StringIdentifier && !_strInit) ||
					getVarParam(RAND_MIN) instanceof BooleanIdentifier && !isTensorOperation)
				raiseValidateError("for Rand statement " + RAND_MIN + " has incorrect value type", conditional);

			// Since sparsity can be arbitrary expression (SYSTEMML-515), no validation check for DoubleIdentifier/IntIdentifier required.
			
			if (!(getVarParam(RAND_PDF) instanceof StringIdentifier)) {
				raiseValidateError("for Rand statement " + RAND_PDF + " has incorrect value type", conditional);
			}
	
			Expression lambda = getVarParam(RAND_LAMBDA);
			if (!( (lambda instanceof DataIdentifier
					|| lambda instanceof ConstIdentifier)
				&& (lambda.getOutput().getValueType() == ValueType.FP64
					|| lambda.getOutput().getValueType() == ValueType.INT64) )) {
				raiseValidateError("for Rand statement " + RAND_LAMBDA + " has incorrect data type", conditional);
			}
				
			long rowsLong = -1L, colsLong = -1L;
			
			Expression rowsExpr = getVarParam(RAND_ROWS);
			Expression colsExpr = getVarParam(RAND_COLS);
			if (!isTensorOperation) {
				///////////////////////////////////////////////////////////////////
				// HANDLE ROWS
				///////////////////////////////////////////////////////////////////
				if( rowsExpr instanceof IntIdentifier ) {
					if( ((IntIdentifier) rowsExpr).getValue() < 0 ) {
						raiseValidateError("In rand statement, can only assign rows a long " +
								"(integer) value >= 0 -- attempted to assign value: " + ((IntIdentifier) rowsExpr).getValue(), conditional);
					}
					rowsLong = ((IntIdentifier) rowsExpr).getValue();
				}
				else if( rowsExpr instanceof DoubleIdentifier ) {
					if( ((DoubleIdentifier) rowsExpr).getValue() < 0 ) {
						raiseValidateError("In rand statement, can only assign rows a long " +
								"(integer) value >= 0 -- attempted to assign value: " + rowsExpr.toString(), conditional);
					}
					rowsLong = UtilFunctions.toLong(Math.floor(((DoubleIdentifier) rowsExpr).getValue()));
				}
				else if( rowsExpr instanceof DataIdentifier && !(rowsExpr instanceof IndexedIdentifier) ) {
					
					// check if the DataIdentifier variable is a ConstIdentifier
					String identifierName = ((DataIdentifier) rowsExpr).getName();
					if( currConstVars.containsKey(identifierName) ) {
						
						// handle int constant
						ConstIdentifier constValue = currConstVars.get(identifierName);
						if( constValue instanceof IntIdentifier ) {
							// check rows is >= 1 --- throw exception
							if( ((IntIdentifier) constValue).getValue() < 0 ) {
								raiseValidateError("In rand statement, can only assign rows a long " +
										"(integer) value >= 0 -- attempted to assign value: " + constValue.toString(), conditional);
							}
							// update row expr with new IntIdentifier
							long roundedValue = ((IntIdentifier) constValue).getValue();
							rowsExpr = new IntIdentifier(roundedValue, this);
							addVarParam(RAND_ROWS, rowsExpr);
							rowsLong = roundedValue;
						}
						// handle double constant
						else if( constValue instanceof DoubleIdentifier ) {
							if( ((DoubleIdentifier) constValue).getValue() < 0 ) {
								raiseValidateError("In rand statement, can only assign rows a long " +
										"(double) value >= 0 -- attempted to assign value: " + constValue.toString(), conditional);
							}
							// update row expr with new IntIdentifier (rounded down)
							long roundedValue = Double.valueOf(Math.floor(((DoubleIdentifier) constValue).getValue())).longValue();
							rowsExpr = new IntIdentifier(roundedValue, this);
							addVarParam(RAND_ROWS, rowsExpr);
							rowsLong = roundedValue;
						}
						else {
							// exception -- rows must be integer or double constant
							raiseValidateError("In rand statement, can only assign rows a long " +
									"(integer) value >= 0 -- attempted to assign value: " + constValue.toString(), conditional);
						}
					}
					else {
						// handle general expression
						rowsExpr.validateExpression(ids, currConstVars, conditional);
					}
				}
				else {
					// handle general expression
					rowsExpr.validateExpression(ids, currConstVars, conditional);
				}
				
				///////////////////////////////////////////////////////////////////
				// HANDLE COLUMNS
				///////////////////////////////////////////////////////////////////
				if( colsExpr instanceof IntIdentifier ) {
					if( ((IntIdentifier) colsExpr).getValue() < 0 ) {
						raiseValidateError("In rand statement, can only assign cols a long " +
								"(integer) value >= 0 -- attempted to assign value: " + colsExpr.toString(), conditional);
					}
					colsLong = ((IntIdentifier) colsExpr).getValue();
				}
				else if( colsExpr instanceof DoubleIdentifier ) {
					if( ((DoubleIdentifier) colsExpr).getValue() < 0 ) {
						raiseValidateError("In rand statement, can only assign cols a long " +
								"(integer) value >= 0 -- attempted to assign value: " + colsExpr.toString(), conditional);
					}
					colsLong = Double.valueOf((Math.floor(((DoubleIdentifier) colsExpr).getValue()))).longValue();
				}
				else if( colsExpr instanceof DataIdentifier && !(colsExpr instanceof IndexedIdentifier) ) {
					
					// check if the DataIdentifier variable is a ConstIdentifier
					String identifierName = ((DataIdentifier) colsExpr).getName();
					if( currConstVars.containsKey(identifierName) ) {
						
						// handle int constant
						ConstIdentifier constValue = currConstVars.get(identifierName);
						if( constValue instanceof IntIdentifier ) {
							if( ((IntIdentifier) constValue).getValue() < 0 ) {
								raiseValidateError("In rand statement, can only assign cols a long " +
										"(integer) value >= 0 -- attempted to assign value: " + constValue.toString(), conditional);
							}
							// update col expr with new IntIdentifier
							long roundedValue = ((IntIdentifier) constValue).getValue();
							colsExpr = new IntIdentifier(roundedValue, this);
							addVarParam(RAND_COLS, colsExpr);
							colsLong = roundedValue;
						}
						// handle double constant
						else if( constValue instanceof DoubleIdentifier ) {
							if( ((DoubleIdentifier) constValue).getValue() < 0 ) {
								raiseValidateError("In rand statement, can only assign cols a long " +
										"(double) value >= 0 -- attempted to assign value: " + constValue.toString(), conditional);
							}
							// update col expr with new IntIdentifier (rounded down)
							long roundedValue = Double.valueOf(Math.floor(((DoubleIdentifier) constValue).getValue())).longValue();
							colsExpr = new IntIdentifier(roundedValue, this);
							addVarParam(RAND_COLS, colsExpr);
							colsLong = roundedValue;
						}
						else {
							// exception -- rows must be integer or double constant
							raiseValidateError("In rand statement, can only assign cols a long " +
									"(integer) value >= 0 -- attempted to assign value: " + constValue.toString(), conditional);
						}
					}
					else {
						// handle general expression
						colsExpr.validateExpression(ids, currConstVars, conditional);
					}
				}
				else {
					// handle general expression
					colsExpr.validateExpression(ids, currConstVars, conditional);
				}
			}
			///////////////////////////////////////////////////////////////////
			// HANDLE MIN
			///////////////////////////////////////////////////////////////////
			Expression minExpr = getVarParam(RAND_MIN);
			
			// perform constant propogation
			if (minExpr instanceof DataIdentifier && !(minExpr instanceof IndexedIdentifier)) {
				
				// check if the DataIdentifier variable is a ConstIdentifier
				String identifierName = ((DataIdentifier)minExpr).getName();
				if (currConstVars.containsKey(identifierName)){
					
					// handle int constant
					ConstIdentifier constValue = currConstVars.get(identifierName);
					if (constValue instanceof IntIdentifier){
						
						// update min expr with new IntIdentifier
						long roundedValue = ((IntIdentifier)constValue).getValue();
						minExpr = new DoubleIdentifier(roundedValue, this);
						addVarParam(RAND_MIN, minExpr);
					}
					// handle double constant
					else if (constValue instanceof DoubleIdentifier){
		
						// update col expr with new IntIdentifier (rounded down)
						double roundedValue = ((DoubleIdentifier)constValue).getValue();
						minExpr = new DoubleIdentifier(roundedValue, this);
						addVarParam(RAND_MIN, minExpr);
						
					}
					else {
						// exception -- rows must be integer or double constant
						raiseValidateError("In rand statement, can only assign min a numerical " +
								"value -- attempted to assign: " + constValue.toString(), conditional);
					}
				}
				else {
					// handle general expression
					minExpr.validateExpression(ids, currConstVars, conditional);
				}
			}
			else {
				// handle general expression
				minExpr.validateExpression(ids, currConstVars, conditional);
			}
			
			
			///////////////////////////////////////////////////////////////////
			// HANDLE MAX
			///////////////////////////////////////////////////////////////////
			Expression maxExpr = getVarParam(RAND_MAX);
			
			// perform constant propogation
			if (maxExpr instanceof DataIdentifier && !(maxExpr instanceof IndexedIdentifier)) {
				
				// check if the DataIdentifier variable is a ConstIdentifier
				String identifierName = ((DataIdentifier)maxExpr).getName();
				if (currConstVars.containsKey(identifierName)) {
					// handle int constant
					ConstIdentifier constValue = currConstVars.get(identifierName);
					if (constValue instanceof IntIdentifier) {
						// update min expr with new IntIdentifier
						long roundedValue = ((IntIdentifier)constValue).getValue();
						maxExpr = new DoubleIdentifier(roundedValue, this);
						addVarParam(RAND_MAX, maxExpr);
					}
					// handle double constant
					else if (constValue instanceof DoubleIdentifier) {
						// update col expr with new IntIdentifier (rounded down)
						double roundedValue = ((DoubleIdentifier)constValue).getValue();
						maxExpr = new DoubleIdentifier(roundedValue, this);
						addVarParam(RAND_MAX, maxExpr);
					}
					else {
						// exception -- rows must be integer or double constant
						raiseValidateError("In rand statement, can only assign max a numerical " +
								"value -- attempted to assign: " + constValue.toString(), conditional);
					}
				}
				else {
					// handle general expression
					maxExpr.validateExpression(ids, currConstVars, conditional);
				}
			}
			else {
				// handle general expression
				maxExpr.validateExpression(ids, currConstVars, conditional);
			}
		
			getOutput().setFormatType(FormatType.BINARY);
			if (isTensorOperation) {
				getOutput().setDataType(DataType.TENSOR);
				getOutput().setValueType(getVarParam(RAND_MIN).getOutput().getValueType());
				// TODO set correct dimensions
				getOutput().setDimensions(-1, -1);
			} else {
				getOutput().setDataType(DataType.MATRIX);
				getOutput().setValueType(ValueType.FP64);
				getOutput().setDimensions(rowsLong, colsLong);
			}
			
			if (getOutput() instanceof IndexedIdentifier){
				// process the "target" being indexed
				DataIdentifier targetAsSeen = ids.get(((DataIdentifier)getOutput()).getName());
				if (targetAsSeen == null){
					raiseValidateError("cannot assign value to indexed identifier " + ((DataIdentifier)getOutput()).getName() + " without first initializing " + ((DataIdentifier)getOutput()).getName(), conditional);
				}
				//_output.setProperties(targetAsSeen);
				((IndexedIdentifier) getOutput()).setOriginalDimensions(targetAsSeen.getDim1(), targetAsSeen.getDim2());
				//((IndexedIdentifier) getOutput()).setOriginalDimensions(getOutput().getDim1(), getOutput().getDim2());
			}
			if (getOutput() instanceof IndexedIdentifier){
				LOG.warn(this.printWarningLocation() + "Output for Rand Statement may have incorrect size information");
			}
			
			break;
			
		case MATRIX: 
			
			//handle default and input arguments
			setMatrixDefault();
			validateParams(conditional, RESHAPE_VALID_PARAM_NAMES,
					"Legal parameters for matrix statement are (case-sensitive): "
						+ RAND_DATA + ", " + RAND_ROWS	+ ", " + RAND_COLS + ", " + RAND_BY_ROW);

			//validate correct value types
			if (getVarParam(RAND_DATA) != null && (getVarParam(RAND_DATA) instanceof BooleanIdentifier)){
				raiseValidateError("for matrix statement " + RAND_DATA + " has incorrect value type", conditional);
			}
			if (getVarParam(RAND_ROWS) != null && (getVarParam(RAND_ROWS) instanceof StringIdentifier || getVarParam(RAND_ROWS) instanceof BooleanIdentifier)){
				raiseValidateError("for matrix statement " + RAND_ROWS + " has incorrect value type", conditional);
			}				
			if (getVarParam(RAND_COLS) != null && (getVarParam(RAND_COLS) instanceof StringIdentifier || getVarParam(RAND_COLS) instanceof BooleanIdentifier)){
				raiseValidateError("for matrix statement " + RAND_COLS + " has incorrect value type", conditional);
			}				
			if ( !(getVarParam(RAND_BY_ROW) instanceof BooleanIdentifier)) {
				raiseValidateError("for matrix statement " + RAND_BY_ROW + " has incorrect value type", conditional);
			}
			
			//validate general data expression
			getVarParam(RAND_DATA).validateExpression(ids, currConstVars, conditional);
			
			rowsLong = -1L; 
			colsLong = -1L;

			///////////////////////////////////////////////////////////////////
			// HANDLE ROWS
			///////////////////////////////////////////////////////////////////
			rowsExpr = getVarParam(RAND_ROWS);
			if (rowsExpr != null){
				if (rowsExpr instanceof IntIdentifier) {
					if  (((IntIdentifier)rowsExpr).getValue() >= 1 ) {
						rowsLong = ((IntIdentifier)rowsExpr).getValue();
					}
					else {
						raiseValidateError("In matrix statement, can only assign rows a long " +
								"(integer) value >= 1 -- attempted to assign value: " + ((IntIdentifier)rowsExpr).getValue(), conditional);
					}
				}
				else if (rowsExpr instanceof DoubleIdentifier) {
					if  (((DoubleIdentifier)rowsExpr).getValue() >= 1 ) {
						rowsLong = Double.valueOf((Math.floor(((DoubleIdentifier)rowsExpr).getValue()))).longValue();
					}
					else {
						raiseValidateError("In matrix statement, can only assign rows a long " +
								"(integer) value >= 1 -- attempted to assign value: " + rowsExpr.toString(), conditional);
					}		
				}
				else if (rowsExpr instanceof DataIdentifier && !(rowsExpr instanceof IndexedIdentifier)) {
					
					// check if the DataIdentifier variable is a ConstIdentifier
					String identifierName = ((DataIdentifier)rowsExpr).getName();
					if (currConstVars.containsKey(identifierName)){
						
						// handle int constant
						ConstIdentifier constValue = currConstVars.get(identifierName);
						if (constValue instanceof IntIdentifier){
							
							// check rows is >= 1 --- throw exception
							if (((IntIdentifier)constValue).getValue() < 1){
								raiseValidateError("In matrix statement, can only assign rows a long " +
										"(integer) value >= 1 -- attempted to assign value: " + constValue.toString(), conditional);
							}
							// update row expr with new IntIdentifier 
							long roundedValue = ((IntIdentifier)constValue).getValue();
							rowsExpr = new IntIdentifier(roundedValue, this);
							addVarParam(RAND_ROWS, rowsExpr);
							rowsLong = roundedValue; 
						}
						// handle double constant 
						else if (constValue instanceof DoubleIdentifier){
							
							if (((DoubleIdentifier)constValue).getValue() < 1.0){
								raiseValidateError("In matrix statement, can only assign rows a long " +
										"(integer) value >= 1 -- attempted to assign value: " + constValue.toString(), conditional);
							}
							// update row expr with new IntIdentifier (rounded down)
							long roundedValue = Double.valueOf(Math.floor(((DoubleIdentifier)constValue).getValue())).longValue();
							rowsExpr = new IntIdentifier(roundedValue, this);
							addVarParam(RAND_ROWS, rowsExpr);
							rowsLong = roundedValue; 
							
						}
						else {
							// exception -- rows must be integer or double constant
							raiseValidateError("In matrix statement, can only assign rows a long " +
									"(integer) value >= 1 -- attempted to assign value: " + constValue.toString(), conditional);
						}
					}
					else {
						// handle general expression
						rowsExpr.validateExpression(ids, currConstVars, conditional);
					}
				}	
				else {
					// handle general expression
					rowsExpr.validateExpression(ids, currConstVars, conditional);
				}
			}
	
			///////////////////////////////////////////////////////////////////
			// HANDLE COLUMNS
			///////////////////////////////////////////////////////////////////
			
			colsExpr = getVarParam(RAND_COLS);
			if (colsExpr != null){
				if (colsExpr instanceof IntIdentifier) {
					if  (((IntIdentifier)colsExpr).getValue() >= 1 ) {
						colsLong = ((IntIdentifier)colsExpr).getValue();
					}
					else {
						raiseValidateError("In matrix statement, can only assign cols a long " +
								"(integer) value >= 1 -- attempted to assign value: " + colsExpr.toString(), conditional);
					}
				}
				else if (colsExpr instanceof DoubleIdentifier) {
					if  (((DoubleIdentifier)colsExpr).getValue() >= 1 ) {
						colsLong = Double.valueOf((Math.floor(((DoubleIdentifier)colsExpr).getValue()))).longValue();
					}
					else {
						raiseValidateError("In matrix statement, can only assign rows a long " +
								"(integer) value >= 1 -- attempted to assign value: " + colsExpr.toString(), conditional);
					}		
				}
				else if (colsExpr instanceof DataIdentifier && !(colsExpr instanceof IndexedIdentifier)) {
					
					// check if the DataIdentifier variable is a ConstIdentifier
					String identifierName = ((DataIdentifier)colsExpr).getName();
					if (currConstVars.containsKey(identifierName)){
						
						// handle int constant
						ConstIdentifier constValue = currConstVars.get(identifierName);
						if (constValue instanceof IntIdentifier){
							
							// check cols is >= 1 --- throw exception
							if (((IntIdentifier)constValue).getValue() < 1){
								raiseValidateError("In matrix statement, can only assign cols a long " +
										"(integer) value >= 1 -- attempted to assign value: " 
										+ constValue.toString(), conditional);
							}
							// update col expr with new IntIdentifier 
							long roundedValue = ((IntIdentifier)constValue).getValue();
							colsExpr = new IntIdentifier(roundedValue, this);
							addVarParam(RAND_COLS, colsExpr);
							colsLong = roundedValue; 
						}
						// handle double constant 
						else if (constValue instanceof DoubleIdentifier){
							
							if (((DoubleIdentifier)constValue).getValue() < 1){
								raiseValidateError("In matrix statement, can only assign cols a long " +
										"(integer) value >= 1 -- attempted to assign value: " 
										+ constValue.toString(), conditional);
							}
							// update col expr with new IntIdentifier (rounded down)
							long roundedValue = Double.valueOf(Math.floor(((DoubleIdentifier)constValue).getValue())).longValue();
							colsExpr = new IntIdentifier(roundedValue, this);
							addVarParam(RAND_COLS, colsExpr);
							colsLong = roundedValue; 
							
						}
						else {
							// exception -- rows must be integer or double constant
							raiseValidateError("In matrix statement, can only assign cols a long " +
									"(integer) value >= 1 -- attempted to assign value: " + constValue.toString(), conditional);
						}
					}
					else {
						// handle general expression
						colsExpr.validateExpression(ids, currConstVars, conditional);
					}
						
				}	
				else {
					// handle general expression
					colsExpr.validateExpression(ids, currConstVars, conditional);
				}
			}	
			getOutput().setFormatType(FormatType.BINARY);
			getOutput().setDataType(DataType.MATRIX);
			getOutput().setValueType(ValueType.FP64);
			getOutput().setDimensions(rowsLong, colsLong);
				
			if (getOutput() instanceof IndexedIdentifier){
				((IndexedIdentifier) getOutput()).setOriginalDimensions(getOutput().getDim1(), getOutput().getDim2());
			}
			//getOutput().computeDataType();

			if (getOutput() instanceof IndexedIdentifier){
				LOG.warn(this.printWarningLocation() + "Output for matrix Statement may have incorrect size information");
			}
			
			break;

		case TENSOR:
			//handle default and input arguments
			setTensorDefault();
			validateParams(conditional, RESHAPE_VALID_PARAM_NAMES,
					"Legal parameters for tensor statement are (case-sensitive): "
						+ RAND_DATA + ", " + RAND_DIMS	+ ", " + RAND_BY_ROW);

			//validate correct value types
			/*if (getVarParam(RAND_DATA) != null && (getVarParam(RAND_DATA) instanceof BooleanIdentifier)){
				raiseValidateError("for tensor statement " + RAND_DATA + " has incorrect value type", conditional);
			}*/
			if (getVarParam(RAND_DIMS) != null && (getVarParam(RAND_DIMS) instanceof BooleanIdentifier)){
				raiseValidateError("for tensor statement " + RAND_DIMS + " has incorrect value type", conditional);
			}
			if ( !(getVarParam(RAND_BY_ROW) instanceof BooleanIdentifier)) {
				raiseValidateError("for tensor statement " + RAND_BY_ROW + " has incorrect value type", conditional);
			}

			//validate general data expression
			getVarParam(RAND_DATA).validateExpression(ids, currConstVars, conditional);
			getVarParam(RAND_DIMS).validateExpression(ids, currConstVars, conditional);

			getOutput().setFormatType(FormatType.BINARY);
			getOutput().setDataType(DataType.TENSOR);
			getOutput().setValueType(getVarParam(RAND_DATA).getOutput().getValueType());
			// TODO get size
			getOutput().setDimensions(-1, -1);

			if (getOutput() instanceof IndexedIdentifier){
				((IndexedIdentifier) getOutput()).setOriginalDimensions(getOutput().getDim1(), getOutput().getDim2());
			}
			//getOutput().computeDataType();

			if (getOutput() instanceof IndexedIdentifier){
				LOG.warn(this.printWarningLocation() + "Output for tensor Statement may have incorrect size information");
			}

			break;
	
		default:
			raiseValidateError("Unsupported Data expression"+ this.getOpCode(), false, LanguageErrorCodes.INVALID_PARAMETERS); //always unconditional
		}
	}

	private void validateParams(boolean conditional, String[] validParamNames, String legalMessage) {
		for( String key : _varParams.keySet() )
		{
			boolean found = false;
			for (String name : validParamNames) {
				found |= name.equals(key);
			}
			if( !found ) {
				raiseValidateError("unexpected parameter \"" + key + "\". "
						+ legalMessage, conditional);
			}
		}
	}

	private void performConstantPropagationRand( HashMap<String, ConstIdentifier> currConstVars )
	{
		//here, we propagate constants for all rand parameters that are required during validate.
		String[] paramNamesForEval = new String[]{RAND_DATA, RAND_SPARSITY, RAND_MIN, RAND_MAX};
		
		//replace data identifiers with const identifiers
		performConstantPropagation(currConstVars, paramNamesForEval);
	}

	private void performConstantPropagationReadWrite( HashMap<String, ConstIdentifier> currConstVars )
	{
		//here, we propagate constants for all read/write parameters that are required during validate.
		String[] paramNamesForEval = new String[]{FORMAT_TYPE, IO_FILENAME, READROWPARAM, READCOLPARAM, READNNZPARAM};
		
		//replace data identifiers with const identifiers
		performConstantPropagation(currConstVars, paramNamesForEval);
	}

	private void performConstantPropagation( HashMap<String, ConstIdentifier> currConstVars, String[] paramNames )
	{
		for( String paramName : paramNames )
		{
			Expression paramExp = getVarParam(paramName);
			if (   paramExp != null && paramExp instanceof DataIdentifier && !(paramExp instanceof IndexedIdentifier) 
				&& currConstVars.containsKey(((DataIdentifier) paramExp).getName()))
			{
				addVarParam(paramName, currConstVars.get(((DataIdentifier)paramExp).getName()));
			}
		}
	}
	
	
	private String fileNameCat(BinaryExpression expr, HashMap<String, ConstIdentifier> currConstVars, String filename, boolean conditional)
	{
		// Processing the left node first
		if (expr.getLeft() instanceof BinaryExpression 
			&& ((BinaryExpression)expr.getLeft()).getOpCode() == BinaryOp.PLUS){
			filename = fileNameCat((BinaryExpression)expr.getLeft(), currConstVars, filename, conditional)+ filename;
		}
		else if (expr.getLeft() instanceof ConstIdentifier){
			filename = ((ConstIdentifier)expr.getLeft()).toString()+ filename;
		}
		else if (expr.getLeft() instanceof DataIdentifier 
			&& ((DataIdentifier)expr.getLeft()).getDataType() == DataType.SCALAR){ 
			String name = ((DataIdentifier)expr.getLeft()).getName();
			filename = ((StringIdentifier)currConstVars.get(name)).getValue() + filename;
		}
		else {
			raiseValidateError("Parameter " + IO_FILENAME + " only supports a const string or const string concatenations.", conditional);
		}
		// Now process the right node
		if (expr.getRight() instanceof BinaryExpression 
			&& ((BinaryExpression)expr.getRight()).getOpCode() == BinaryOp.PLUS){
			filename = filename + fileNameCat((BinaryExpression)expr.getRight(), currConstVars, filename, conditional);
		}
		// DRB: CHANGE
		else if (expr.getRight() instanceof ConstIdentifier){
			filename = filename + ((ConstIdentifier)expr.getRight()).toString();
		}
		else if (expr.getRight() instanceof DataIdentifier 
			&& ((DataIdentifier)expr.getRight()).getDataType() == DataType.SCALAR
			&& ((DataIdentifier)expr.getRight()).getValueType() == ValueType.STRING){
			String name = ((DataIdentifier)expr.getRight()).getName();
			filename =  filename + ((StringIdentifier)currConstVars.get(name)).getValue();
		}
		else {
			raiseValidateError("Parameter " + IO_FILENAME + " only supports a const string or const string concatenations.", conditional);
		}
		return filename;
			
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(_opcode.toString());
		sb.append("(");

		boolean first = true;
		for(Entry<String,Expression> e : _varParams.entrySet()) {
			String key = e.getKey();
			Expression expr = e.getValue();
			if (!first) {
				sb.append(", ");
			} else {
				first = false;
			}
			sb.append(key);
			sb.append("=");
			if (expr instanceof StringIdentifier) {
				sb.append("\"");
				sb.append(expr);
				sb.append("\"");
			} else {
				sb.append(expr);
			}
		}
		sb.append(")");
		return sb.toString();
	}

	@Override
	public VariableSet variablesRead() {
		VariableSet result = new VariableSet();
		for( Expression expr : _varParams.values() ) {
			result.addVariables ( expr.variablesRead() );
		}
		return result;
	}

	@Override
	public VariableSet variablesUpdated() {
		VariableSet result = new VariableSet();
		for( Expression expr : _varParams.values() ) {
			result.addVariables ( expr.variablesUpdated() );
		}
		result.addVariable(((DataIdentifier)this.getOutput()).getName(), (DataIdentifier)this.getOutput());
		return result;
	}
	
	@SuppressWarnings("unchecked")
	private void parseMetaDataFileParameters(String mtdFileName, JSONObject configObject, boolean conditional) 
	{
    	for( Object obj : configObject.entrySet() ){
			Entry<Object,Object> e = (Entry<Object, Object>) obj;
    		Object key = e.getKey();
    		Object val = e.getValue();
			
    		boolean isValidName = false;
    		for (String paramName : READ_VALID_MTD_PARAM_NAMES){
				if (paramName.equals(key))
					isValidName = true;
			}
    		
			if (!isValidName){ //wrong parameters always rejected
				raiseValidateError("MTD file " + mtdFileName + " contains invalid parameter name: " + key, false);
			}
			
			// if the read method parameter is a constant, then verify value matches MTD metadata file
			if (getVarParam(key.toString()) != null && (getVarParam(key.toString()) instanceof ConstIdentifier)
					&& !getVarParam(key.toString()).toString().equalsIgnoreCase(val.toString())) {
				raiseValidateError(
						"Parameter '" + key.toString()
								+ "' has conflicting values in metadata and read statement. MTD file value: '"
								+ val.toString() + "'. Read statement value: '" + getVarParam(key.toString()) + "'.",
						conditional);
			} else {
				// if the read method does not specify parameter value, then add MTD metadata file value to parameter list
				if (getVarParam(key.toString()) == null){
					if (( !key.toString().equalsIgnoreCase(DESCRIPTIONPARAM) ) &&
							( !key.toString().equalsIgnoreCase(AUTHORPARAM) ) &&
							( !key.toString().equalsIgnoreCase(CREATEDPARAM) ) )
					{
						StringIdentifier strId = new StringIdentifier(val.toString(), this);
						
						if ( key.toString().equalsIgnoreCase(DELIM_HAS_HEADER_ROW) 
								|| key.toString().equalsIgnoreCase(DELIM_FILL)
								|| key.toString().equalsIgnoreCase(DELIM_SPARSE)
								) {
							// parse these parameters as boolean values
							BooleanIdentifier boolId = null; 
							if (strId.toString().equalsIgnoreCase("true")) {
								boolId = new BooleanIdentifier(true, this);
							} else if (strId.toString().equalsIgnoreCase("false")) {
								boolId = new BooleanIdentifier(false, this);
							} else {
								raiseValidateError("Invalid value provided for '" + DELIM_HAS_HEADER_ROW + "' in metadata file '" + mtdFileName + "'. "
										+ "Must be either TRUE or FALSE.", conditional);
							}
							removeVarParam(key.toString());
							addVarParam(key.toString(), boolId);
						}
						else if ( key.toString().equalsIgnoreCase(DELIM_FILL_VALUE)) {
							// parse these parameters as numeric values
							DoubleIdentifier doubleId = new DoubleIdentifier(Double.parseDouble(strId.toString()),
									this);
							removeVarParam(key.toString());
							addVarParam(key.toString(), doubleId);
						}
						else if (key.toString().equalsIgnoreCase(DELIM_NA_STRINGS)) {
							String naStrings = null;
							if ( val instanceof String) {
								naStrings = val.toString();
							}
							else {
								StringBuilder sb = new StringBuilder();
								JSONArray valarr = (JSONArray)val;
								for(int naid=0; naid < valarr.size(); naid++ ) {
									sb.append( (String) valarr.get(naid) );
									if ( naid < valarr.size()-1)
										sb.append( DELIM_NA_STRING_SEP );
								}
								naStrings = sb.toString();
							}
							StringIdentifier sid = new StringIdentifier(naStrings, this);
							removeVarParam(key.toString());
							addVarParam(key.toString(), sid);
						}
						else {
							// by default, treat a parameter as a string
							addVarParam(key.toString(), strId);
						}
					}
				}
			}
    	}
	}
	
	public JSONObject readMetadataFile(String filename, boolean conditional) 
	{
		JSONObject retVal = null;
		boolean exists = HDFSTool.existsFileOnHDFS(filename);
		boolean isDir = HDFSTool.isDirectory(filename);
		
		// CASE: filename is a directory -- process as a directory
		if( exists && isDir ) 
		{
			retVal = new JSONObject();
			for(FileStatus stat : HDFSTool.getDirectoryListing(filename)) {
				Path childPath = stat.getPath(); // gives directory name
				if( !childPath.getName().startsWith("part") )
					continue;
				try (BufferedReader br = new BufferedReader(new InputStreamReader(
					IOUtilFunctions.getFileSystem(childPath).open(childPath))))
				{
					JSONObject childObj = JSONHelper.parse(br);
					for( Object obj : childObj.entrySet() ){
						@SuppressWarnings("unchecked")
						Entry<Object,Object> e = (Entry<Object, Object>) obj;
						Object key = e.getKey();
						Object val = e.getValue();
						retVal.put(key, val);
					}
				}
				catch(Exception e){
					raiseValidateError("for MTD file in directory, error parting part of MTD file with path " + childPath.toString() + ": " + e.getMessage(), conditional);
				}
			} 
		}
		// CASE: filename points to a file
		else if (exists) {
			Path path = new Path(filename);
			try (BufferedReader br = new BufferedReader(new InputStreamReader(
				IOUtilFunctions.getFileSystem(path).open(path))))
			{
				retVal = new JSONObject(br);
			} 
			catch (Exception e){
				raiseValidateError("error parsing MTD file with path " + filename + ": " + e.getMessage(), conditional);
			}
		}
		
		return retVal;
	}
	
	public boolean checkHasMatrixMarketFormat(String inputFileName, String mtdFileName, boolean conditional) 
	{
		// Check the MTD file exists. if there is an MTD file, return false.
		JSONObject mtdObject = readMetadataFile(mtdFileName, conditional);
		if (mtdObject != null)
			return false;
		
		if( HDFSTool.existsFileOnHDFS(inputFileName) 
			&& !HDFSTool.isDirectory(inputFileName)  )
		{
			Path path = new Path(inputFileName);
			try( BufferedReader in = new BufferedReader(new InputStreamReader(
				IOUtilFunctions.getFileSystem(path).open(path))))
			{
				String headerLine = new String("");
				if (in.ready())
					headerLine = in.readLine();
				return (headerLine !=null && headerLine.startsWith("%%"));
			}
			catch(Exception ex) {
				throw new LanguageException("Failed to read matrix market header.", ex);
			}
		}
		return false;
	}
	
	public String checkHasDelimitedFormat(String filename, boolean conditional) {
		// if the MTD file exists, check the format is not binary 
		JSONObject mtdObject = readMetadataFile(filename + ".mtd", conditional);
		if (mtdObject != null) {
			String formatTypeString = (String)JSONHelper.get(mtdObject,FORMAT_TYPE);
			if ((formatTypeString != null ) && 
				(formatTypeString.equalsIgnoreCase(FORMAT_TYPE_VALUE_CSV)
				|| formatTypeString.equalsIgnoreCase(FORMAT_TYPE_VALUE_LIBSVM)))
				return formatTypeString;
			else
				return null;
		}
		return null;
		// The file format must be specified either in .mtd file or in read() statement
		// Therefore, one need not actually read the data to infer the format.
	}
	
	public boolean isCSVReadWithUnknownSize()
	{
		Expression format = getVarParam(FORMAT_TYPE);
		if( _opcode == DataOp.READ && format!=null && format.toString().equalsIgnoreCase(FORMAT_TYPE_VALUE_CSV) ) {
			Expression rows = getVarParam(READROWPARAM);
			Expression cols = getVarParam(READCOLPARAM);
			return (rows==null || Long.parseLong(rows.toString())<0)
				||(cols==null || Long.parseLong(cols.toString())<0);
		}
		
		return false;
	}
	
	public boolean isRead()
	{
		return (_opcode == DataOp.READ);
	}
	
} // end class
