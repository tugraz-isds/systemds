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
 */

package org.tugraz.sysds.runtime.instructions.fed;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.tugraz.sysds.common.Types;
import org.tugraz.sysds.conf.DMLConfig;
import org.tugraz.sysds.runtime.DMLRuntimeException;
import org.tugraz.sysds.runtime.controlprogram.caching.MatrixObject;
import org.tugraz.sysds.runtime.controlprogram.context.ExecutionContext;
import org.tugraz.sysds.runtime.controlprogram.federated.FederatedData;
import org.tugraz.sysds.runtime.controlprogram.federated.FederatedRange;
import org.tugraz.sysds.runtime.controlprogram.federated.FederatedResponse;
import org.tugraz.sysds.runtime.instructions.InstructionUtils;
import org.tugraz.sysds.runtime.instructions.cp.CPOperand;
import org.tugraz.sysds.runtime.instructions.cp.Data;
import org.tugraz.sysds.runtime.instructions.cp.ListObject;
import org.tugraz.sysds.runtime.instructions.cp.ScalarObject;
import org.tugraz.sysds.runtime.instructions.cp.StringObject;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Future;

public class InitFEDInstruction extends FEDInstruction {
	private CPOperand _addresses, _ranges, _output;

	public InitFEDInstruction(CPOperand addresses, CPOperand ranges, CPOperand out, String opcode, String instr) {
		super(FEDType.Init, opcode, instr);
		_addresses = addresses;
		_ranges = ranges;
		_output = out;
	}

	public static InitFEDInstruction parseInstruction(String str) {
		String[] parts = InstructionUtils.getInstructionPartsWithValueType(str);
		// We need 4 parts: Opcode, Addresses (list of Strings with
		// url/ip:port/filepath), ranges and the output Operand
		if (parts.length != 4)
			throw new DMLRuntimeException("Invalid number of operands in federated instruction: " + str);
		String opcode = parts[0];

		CPOperand addresses, ranges, out;
		addresses = new CPOperand(parts[1]);
		ranges = new CPOperand(parts[2]);
		out = new CPOperand(parts[3]);
		return new InitFEDInstruction(addresses, ranges, out, opcode, str);
	}

	@Override
	public void processInstruction(ExecutionContext ec) {
		ListObject addresses = ec.getListObject(_addresses.getName());
		ListObject ranges = ec.getListObject(_ranges.getName());
		List<Pair<FederatedRange, FederatedData>> feds = new ArrayList<>();

		if (addresses.getLength() * 2 != ranges.getLength())
			throw new DMLRuntimeException("Federated read needs twice the amount of addresses as ranges "
				+ "(begin and end): addresses=" + addresses.getLength() + " ranges=" + ranges.getLength());

		long[] usedDims = new long[] { 0, 0 };
		for (int i = 0; i < addresses.getLength(); i++) {
			Data addressData = addresses.getData().get(i);
			if (addressData instanceof StringObject) {

				// We split address into url/ip, the port and file path of file to read
				String[] parsedValues = parseURL(((StringObject) addressData).getStringValue());
				String host = parsedValues[0];
				int port = Integer.parseInt(parsedValues[1]);
				String filePath = parsedValues[2];
				// get beginning and end of data ranges
				List<Data> rangesData = ranges.getData();
				Data beginData = rangesData.get(i * 2);
				Data endData = rangesData.get(i * 2 + 1);
				if (beginData.getDataType() != Types.DataType.LIST || endData.getDataType() != Types.DataType.LIST)
					throw new DMLRuntimeException(
						"Federated read ranges (lower, upper) have to be lists of dimensions");
				List<Data> beginDimsData = ((ListObject) beginData).getData();
				List<Data> endDimsData = ((ListObject) endData).getData();

				// fill begin and end dims
				long[] beginDims = new long[beginDimsData.size()];
				long[] endDims = new long[beginDims.length];
				for (int d = 0; d < beginDims.length; d++) {
					beginDims[d] = ((ScalarObject) beginDimsData.get(d)).getLongValue();
					endDims[d] = ((ScalarObject) endDimsData.get(d)).getLongValue();
				}
				usedDims[0] = Math.max(usedDims[0], endDims[0]);
				usedDims[1] = Math.max(usedDims[1], endDims[1]);
				try {
					FederatedData federatedData = new FederatedData(
						new InetSocketAddress(InetAddress.getByName(host), port), filePath);
					feds.add(new ImmutablePair<>(new FederatedRange(beginDims, endDims), federatedData));
				}
				catch (UnknownHostException e) {
					throw new DMLRuntimeException("federated host was unknown: " + host);
				}

			}
			else {
				throw new DMLRuntimeException("federated instruction only takes strings as addresses");
			}
		}
		MatrixObject output = ec.getMatrixObject(_output);
		output.getDataCharacteristics().setRows(usedDims[0]).setCols(usedDims[1]);
		federate(output, feds);
	}

	public static String[] parseURL(String input) {
		try {
			// Artificially making it http protocol. 
			// This is to avoid malformed address error in the URL passing.
			// TODO: Construct new protocol name for Federated communication
			URL address = new URL("http://" + input);
			String host = address.getHost();
			if (host.length() == 0)
				throw new IllegalArgumentException("Missing Host name for federated address");
			// The current system does not support ipv6, only ipv4.
			// TODO: Support IPV6 address for Federated communication
			String ipRegex = "^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$";
			if (host.matches("^\\d+\\.\\d+\\.\\d+\\.\\d+$") && !host.matches(ipRegex))
				throw new IllegalArgumentException("Input Host address looks like an IP address but is outside range");
			String port = Integer.toString(address.getPort());
			if (port.equals("-1"))
				port = DMLConfig.DEFAULT_FEDERATED_PORT;
			String filePath = address.getPath();
			if (filePath.length() <= 1)
				throw new IllegalArgumentException("Missing File path for federated address");
			// Remove the first character making the path Dynamic from the location of the worker.
			// This is in contrast to before where it was static paths
			filePath = filePath.substring(1);
			// To make static file paths use double "//" EG:
			// example.dom//staticFile.txt
			// example.dom/dynamicFile.txt 
			if (address.getQuery() != null)
				throw new IllegalArgumentException("Query is not supported");

			if (address.getRef() != null)
				throw new IllegalArgumentException("Reference is not supported");
			
			return new String[] { host, port, filePath };
		}
		catch (MalformedURLException e) {
			throw new IllegalArgumentException("federated address `" + input
				+ "` does not fit required URL pattern of \"host:port/directory\"", e);
		}
	}

	public void federate(MatrixObject output, List<Pair<FederatedRange, FederatedData>> workers) {
		Map<FederatedRange, FederatedData> fedMapping = new TreeMap<>();
		for (Pair<FederatedRange, FederatedData> t : workers) {
			// TODO support all value types
			fedMapping.put(t.getLeft(), t.getRight());
		}
		List<Pair<FederatedData, Future<FederatedResponse>>> idResponses = new ArrayList<>();
		for (Map.Entry<FederatedRange, FederatedData> entry : fedMapping.entrySet()) {
			FederatedRange range = entry.getKey();
			FederatedData value = entry.getValue();
			if (!value.isInitialized()) {
				long[] beginDims = range.getBeginDims();
				long[] endDims = range.getEndDims();
				long[] dims = output.getDataCharacteristics().getDims();
				for (int i = 0; i < dims.length; i++) {
					dims[i] = endDims[i] - beginDims[i];
				}
				idResponses.add(new ImmutablePair<>(value, value.initFederatedData()));
			}
		}
		try {
			for (Pair<FederatedData, Future<FederatedResponse>> idResponse : idResponses) {
				FederatedResponse response = idResponse.getRight().get();
				if (response.isSuccessful())
					idResponse.getLeft().setVarID((Long) response.getData());
				else
					throw new DMLRuntimeException(response.getErrorMessage());
			}
		}
		catch (Exception e) {
			throw new DMLRuntimeException("Federation initialization failed", e);
		}
		output.getDataCharacteristics().setNonZeros(output.getNumColumns() * output.getNumRows());
		output.setFedMapping(fedMapping);
	}
}
