/*
 * Modifications Copyright 2020 Graz University of Technology
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

package org.tugraz.sysds.runtime.compress.cocode;

import java.util.HashMap;
import java.util.List;

import org.tugraz.sysds.runtime.compress.cocode.PlanningCoCoder.GroupableColInfo;

public abstract class ColumnGroupPartitioner {
	/**
	 * Partitions a list of columns into a list of partitions that contains subsets of columns. Note that this call must
	 * compute a complete and disjoint partitioning.
	 * 
	 * @param groupCols     list of columns
	 * @param groupColsInfo list of column infos
	 * @return list of partitions (where each partition is a list of columns)
	 */
	public abstract List<int[]> partitionColumns(List<Integer> groupCols,
		HashMap<Integer, GroupableColInfo> groupColsInfo);
}
