/*
 * Copyright 2019 Graz University of Technology
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.tugraz.sysds.runtime.instructions.cp;

import org.tugraz.sysds.common.Types.ValueType;
import org.tugraz.sysds.runtime.DMLRuntimeException;
import org.tugraz.sysds.runtime.controlprogram.context.ExecutionContext;
import org.tugraz.sysds.runtime.data.TensorBlock;
import org.tugraz.sysds.runtime.instructions.InstructionUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class SqlCPInstruction extends CPInstruction {
	private CPOperand _conn, _user, _pass, _query, _output;
	
	public SqlCPInstruction(CPOperand conn, CPOperand user, CPOperand pass, CPOperand query, CPOperand out,
			String opcode, String instr) {
		super(CPType.Sql, opcode, instr);
		_conn = conn;
		_user = user;
		_pass = pass;
		_query = query;
		_output = out;
	}
	
	public static SqlCPInstruction parseInstruction(String str) {
		String[] parts = InstructionUtils.getInstructionPartsWithValueType(str);
		String opcode = parts[0];
		
		if( parts.length != 6 )
			throw new DMLRuntimeException("Invalid number of operands in sql instruction: " + str);
		
		CPOperand conn, user, pass, query, out;
		conn = new CPOperand(parts[1]);
		user = new CPOperand(parts[2]);
		pass = new CPOperand(parts[3]);
		query = new CPOperand(parts[4]);
		out = new CPOperand(parts[5]);
		return new SqlCPInstruction(conn, user, pass, query, out, opcode, str);
	}
	
	@Override
	public void processInstruction(ExecutionContext ec) {
		String conn = ec.getScalarInput(_conn).getStringValue();
		String user = ec.getScalarInput(_user).getStringValue();
		String pass = ec.getScalarInput(_pass).getStringValue();
		String query = ec.getScalarInput(_query).getStringValue();
		
		try (Connection connection = user.isEmpty() ? DriverManager.getConnection(conn) :
				DriverManager.getConnection(conn, user, pass)) {
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery("SELECT COUNT(*) FROM (" + query + ") AS sub");
			resultSet.next();
			int rows = resultSet.getInt(1);
			resultSet = statement.executeQuery(query);
			ResultSetMetaData meta = resultSet.getMetaData();
			int cols = meta.getColumnCount();
			ValueType[] schema = getSchemaFromMetaData(meta);
			
			int[] dims = {rows, cols};
			TensorBlock outBlock = new TensorBlock(schema, dims);
			for (int row = 0; resultSet.next(); row++)
				for (int i = 0; i < cols; i++)
					setCell(outBlock, resultSet, schema[i], new int[]{row, i});
			ec.setTensorOutput(_output.getName(), outBlock);
			ec.getDataCharacteristics(_output.getName()).setDim(0, rows).setDim(1, cols);
		}
		catch (SQLException e) {
			throw new DMLRuntimeException("SQL Error: " + e.getMessage());
		}
	}
	
	private void setCell(TensorBlock outBlock, ResultSet resultSet, ValueType valueType, int[] ix) throws SQLException {
		int sqlCol = ix[1] + 1;
		switch (valueType) {
			case FP64: outBlock.set(ix, resultSet.getDouble(sqlCol)); break;
			case FP32: outBlock.set(ix, resultSet.getFloat(sqlCol)); break;
			case INT64: outBlock.set(ix, resultSet.getLong(sqlCol)); break;
			case INT32: outBlock.set(ix, resultSet.getInt(sqlCol)); break;
			case BOOLEAN: outBlock.set(ix, resultSet.getBoolean(sqlCol)); break;
			case STRING: outBlock.set(ix, resultSet.getString(sqlCol)); break;
		}
	}
	
	private ValueType[] getSchemaFromMetaData(ResultSetMetaData meta) throws SQLException {
		ValueType[] schema = new ValueType[meta.getColumnCount()];
		for (int i = 0; i < meta.getColumnCount(); i++) {
			int type = meta.getColumnType(i + 1);
			if( type == java.sql.Types.DOUBLE || type == java.sql.Types.FLOAT )
				schema[i] = ValueType.FP64;
			else if( type == java.sql.Types.REAL )
				schema[i] = ValueType.FP32;
			else if( type == java.sql.Types.BIGINT )
				schema[i] = ValueType.INT64;
			else if( type == java.sql.Types.SMALLINT || type == java.sql.Types.TINYINT ||
					type == java.sql.Types.INTEGER )
				schema[i] = ValueType.INT32;
			else if( type == java.sql.Types.BIT )
				schema[i] = ValueType.BOOLEAN;
			else
				schema[i] = ValueType.STRING;
		}
		return schema;
	}
}
