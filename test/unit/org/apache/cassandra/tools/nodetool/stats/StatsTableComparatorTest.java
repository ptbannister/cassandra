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

import static org.junit.Assert.assertEquals;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class StatsTableComparatorTest extends StatsTableTestBase {

	/**
	 * Builds a string of the format "table1 > table2 > ... > tablen-1 > tablen"
	 * to show the order of StatsTables in a sorted list.
	 * @returns String a string showing the relative position in the list of its StatsTables
	 */
	private String buildSortOrderString(List<StatsTable> sorted) {
		if (sorted == null)
			return null;
		if (sorted.size() == 0)
			return "";
		String names = sorted.get(0).tableName;
		for (int i = 1; i < sorted.size(); i++)
			names += " > " + sorted.get(i).tableName;
		return names;	
	}

	@Test
	public void testCompare() throws Exception {
		// reads: 6 > 5 > 4 > 3 > 2 > 1
		StatsTableComparator readsComparator = new StatsTableComparator("reads");
		Collections.sort(testTables, readsComparator);
		assertEquals("StatsTableComparator failed to sort by reads", "table6 > table5 > table4 > table3 > table2 > table1", buildSortOrderString(testTables));
		// writes: 1 > 2 > 3 > 4 > 5 > 6
		StatsTableComparator writesComparator = new StatsTableComparator("writes");
		Collections.sort(testTables, writesComparator);
		assertEquals("StatsTableComparator failed to sort by writes", "table1 > table2 > table3 > table4 > table5 > table6", buildSortOrderString(testTables));
		// read latency: 3 > 2 > 1 > 6 > 5 > 4
		StatsTableComparator readLatencyComparator = new StatsTableComparator("readLatency");
		Collections.sort(testTables, readLatencyComparator);
		assertEquals("StatsTableComparator failed to sort by readLatency", "table3 > table2 > table1 > table6 > table5 > table4", buildSortOrderString(testTables));
		// write latency: 4 > 5 > 6 > 1 > 2 > 3
		Collections.sort(testTables, new StatsTableComparator("writeLatency"));
		assertEquals("StatsTableComparator failed to sort by writeLatency", "table4 > table5 > table6 > table1 > table2 > table3", buildSortOrderString(testTables));
		// space used total: 1 > 2 > 3 > 4 > 5 > 6
		Collections.sort(testTables, new StatsTableComparator("spaceUsedTotal"));
		assertEquals("StatsTableComparator failed to sort by spaceUsedTotal", "table1 > table2 > table3 > table4 > table5 > table6", buildSortOrderString(testTables));
		// memtable data size: 6 > 5 > 4 > 3 > 2 > 1
		Collections.sort(testTables, new StatsTableComparator("memtableDataSize"));
		assertEquals("StatsTableComparator failed to sort by memtableDataSize", "table6 > table5 > table4 > table3 > table2 > table1", buildSortOrderString(testTables));
	}

}
