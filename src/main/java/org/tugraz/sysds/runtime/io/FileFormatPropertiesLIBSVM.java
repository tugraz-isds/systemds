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

package org.tugraz.sysds.runtime.io;

import java.io.Serializable;

import org.tugraz.sysds.parser.DataExpression;

public class FileFormatPropertiesLIBSVM extends FileFormatProperties implements Serializable
{
	private static final long serialVersionUID = -2870393360885401604L;
	
	private boolean header;
	private String naStrings;
	private boolean sparse;
	
	public FileFormatPropertiesLIBSVM() {
		// get the default values for LIBSVM properties from the language layer
		this.sparse = DataExpression.DEFAULT_DELIM_SPARSE;
		this.naStrings = null;
	}
	
	public FileFormatPropertiesLIBSVM(String naStrings) {
		this.naStrings = naStrings;
	}

	public FileFormatPropertiesLIBSVM(boolean sparse) {
		this.sparse = sparse;
	}
	
	public boolean hasHeader() {
		return header;
	}

	public String getNAStrings() { 
		return naStrings;
	}
	
	public boolean isSparse() {
		return sparse;
	}

	public void setSparse(boolean sparse) {
		this.sparse = sparse;
	}
}
