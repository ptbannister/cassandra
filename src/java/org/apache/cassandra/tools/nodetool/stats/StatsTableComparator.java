/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.tools.nodetool.stats;

import java.util.Comparator;

/**
 * Comparator to sort StatsTables by a named statistic.
 */
public class StatsTableComparator implements Comparator {

	// TODO: docstring
	private String sortKey;

	// TODO: docstring
	private boolean ascending;

	public StatsTableComparator(String sortKey) {
		this.sortKey = sortKey;
		this.ascending = false;
	}
	
	public StatsTableComparator(String sortKey, boolean ascending) {
		this.sortKey = sortKey;
		this.ascending = ascending;
	}

	/**
	 * Compare StatsTable instances based on this instance's sortKey.
	 */
	public int compare(Object x, Object y) {
		if (!(x instanceof StatsTable && y instanceof StatsTable))  {
			throw new ClassCastException(String.format("StatsTableComparator cannot compare %s and %s", x.getClass().toString(), y.getClass().toString()));
		}
		if (x == null || y == null) {
			throw new NullPointerException("StatsTableComparator cannot compare null objects");
		}
		StatsTable stx = (StatsTable) x;
		StatsTable sty = (StatsTable) y;
		int sign = ascending ? -1 : 1;
		if (sortKey.equals("memtableDataSize")) {
			return sign * Long.valueOf(stx.memtableDataSize).compareTo(Long.valueOf(sty.memtableDataSize));
		}
		else if (sortKey.equals("reads")) {
			return sign * Long.valueOf(stx.localReadCount).compareTo(Long.valueOf(sty.localReadCount));
		}
		else if (sortKey.equals("readLatency")) {
			return sign * Double.valueOf(stx.localReadLatencyMs).compareTo(Double.valueOf(sty.localReadLatencyMs));
		}
		else if (sortKey.equals("spaceUsedTotal")) {
			return sign * Long.valueOf(stx.spaceUsedTotal).compareTo(Long.valueOf(sty.spaceUsedTotal));
		}
		else if (sortKey.equals("writes")) {
			return sign * Long.valueOf(stx.localWriteCount).compareTo(Long.valueOf(sty.localWriteCount));
		}
		else if (sortKey.equals("writeLatency")) {
			return sign * Double.valueOf(stx.localWriteLatencyMs).compareTo(Double.valueOf(sty.localWriteLatencyMs));
		}
		else {
			// if a valid sortKey wasn't specified, sort alphabetically by keyspace, then by table
			return sign * (stx.keyspaceName + "." + stx.tableName).compareTo(sty.keyspaceName + "." + sty.tableName);
		}
	}
}
