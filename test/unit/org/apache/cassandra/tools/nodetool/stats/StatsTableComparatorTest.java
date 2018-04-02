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

public class StatsTableComparatorTest extends TableStatsTestBase {

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

	private void runCompareTest(List<StatsTable> vector, String sortKey, String expectedOrder, boolean humanReadable)
	{
		Collections.sort(vector, new StatsTableComparator(sortKey, humanReadable));
		String failureMessage = String.format("StatsTableComparator failed to sort by %s", sortKey);
        assertEquals(failureMessage, expectedOrder, buildSortOrderString(vector));
	}

    @Test
    public void testCompareDoubles() throws Exception
    {
		// average live cells: 1 > 6 > 2 > 5 > 3 > 4
		runCompareTest(testTables,
			"average_live_cells_per_slice_last_five_minutes",
			"table1 > table6 > table2 > table5 > table3 > table4",
			false);
		// average tombstones: 6 > 1 > 5 > 2 > 3 > 4
		runCompareTest(testTables,
			"average_tombstones_per_slice_last_five_minutes",
			"table6 > table1 > table5 > table2 > table3 > table4",
			false);
        // read latency: 3 > 2 > 1 > 6 > 4 > 5
		runCompareTest(testTables,
			"read_latency",
			"table3 > table2 > table1 > table6 > table4 > table5",
			false);
        // write latency: 4 > 5 > 6 > 1 > 2 > 3
		runCompareTest(testTables,
			"write_latency",
			"table4 > table5 > table6 > table1 > table2 > table3",
			false);
		// percent repaired
		runCompareTest(testTables,
			"percent_repaired",
			"table1 > table2 > table3 > table5 > table4 > table6",
			false);
    }

    @Test
    public void testCompareLongs() throws Exception
    {
        // reads: 6 > 5 > 4 > 3 > 2 > 1
		runCompareTest(testTables,
			"reads",
			"table6 > table5 > table4 > table3 > table2 > table1",
			false);
        // writes: 1 > 2 > 3 > 4 > 5 > 6
		runCompareTest(testTables,
			"writes",
			"table1 > table2 > table3 > table4 > table5 > table6",
			false);
		// compacted partition maximum bytes: 1 > 3 > 5 > 2 > 4 = 6 
		runCompareTest(testTables,
			"compacted_partition_maximum_bytes",
			"table1 > table3 > table5 > table2 > table4 > table6",
			false);
		// compacted partition mean bytes: 1 > 3 > 2 = 4 = 5 > 6
		runCompareTest(testTables,
			"compacted_partition_mean_bytes",
			"table1 > table3 > table2 > table4 > table5 > table6",
			false);
		// compacted partition minimum bytes: 6 > 4 > 2 > 5 > 1 = 3
		runCompareTest(testTables,
			"compacted_partition_minimum_bytes",
			"table6 > table4 > table2 > table5 > table1 > table3",
			false);
		// maximum live cells last five minutes: 1 > 2 = 3 > 4 = 5 > 6
		runCompareTest(testTables,
			"maximum_live_cells_per_slice_last_five_minutes",
			"table1 > table2 > table3 > table4 > table5 > table6",
			false);
		// maximum tombstones last five minutes: 6 > 5 > 3 = 4 > 2 > 1
		runCompareTest(testTables,
			"maximum_tombstones_per_slice_last_five_minutes",
			"table6 > table5 > table3 > table4 > table2 > table1",
			false);
    }

    @Test
    public void testCompareHumanReadable() throws Exception
    {
        // human readable space used total: 6 > 5 > 4 > 3 > 2 > 1
		runCompareTest(humanReadableTables,
			"space_used_total",
			"table6 > table5 > table4 > table3 > table2 > table1",
			true);
        // human readable memtable data size: 1 > 3 > 5 > 2 > 4 > 6
		runCompareTest(humanReadableTables,
			"memtable_data_size",
			"table1 > table3 > table5 > table2 > table4 > table6",
			true);
    }

    @Test
    public void testCompareObjects() throws Exception
    {
        // bloom filter false positives: 2 > 4 > 6 > 1 > 3 > 5
		runCompareTest(testTables,
			"bloom_filter_false_positives",
			"table2 > table4 > table6 > table1 > table3 > table5",
			false);
        // bloom filter false positive ratio: 5 > 3 > 1 > 6 > 4 > 2
		runCompareTest(testTables,
			"bloom_filter_false_ratio",
			"table5 > table3 > table1 > table6 > table4 > table2",
			false);
		// memtable cell count: 3 > 5 > 6 > 1 > 2 > 4
		runCompareTest(testTables,
			"memtable_cell_count",
			"table3 > table5 > table6 > table1 > table2 > table4",
			false);
		// memtable switch count: 4 > 2 > 3 > 6 > 5 > 1
		runCompareTest(testTables,
			"memtable_switch_count",
			"table4 > table2 > table3 > table6 > table5 > table1",
			false);
		// number of partitions estimate: 1 > 2 > 3 > 4 > 5 > 6
		runCompareTest(testTables,
			"number_of_partitions_estimate",
			"table1 > table2 > table3 > table4 > table5 > table6",
			false);
		// pending flushes: 2 > 1 > 4 > 3 > 6 > 5
		runCompareTest(testTables,
			"pending_flushes",
			"table2 > table1 > table4 > table3 > table6 > table5",
			false);
		// sstable compression ratio: 5 > 4 > 1 = 2 = 6 > 3
		runCompareTest(testTables,
			"sstable_compression_ratio",
			"table5 > table4 > table1 > table2 > table6 > table3",
			false);
        // sstable count: 1 > 3 > 5 > 2 > 4 > 6
		runCompareTest(testTables,
			"sstable_count",
			"table1 > table3 > table5 > table2 > table4 > table6",
			false);
    }

    @Test
    public void testCompareOffHeap() throws Exception
    {
        // offheap memory total: 4 > 2 > 6 > 1 = 3 = 5 
		runCompareTest(testTables,
			"off_heap_memory_used_total",
			"table4 > table2 > table6 > table1 > table3 > table5",
			false);
        // bloom filter offheap: 4 > 6 > 2 > 1 > 3 > 5
		runCompareTest(testTables,
			"bloom_filter_off_heap_memory_used",
			"table4 > table6 > table2 > table1 > table3 > table5",
			false);
		// compression metadata offheap: 2 > 4 > 6 > 1 = 3 = 5
		runCompareTest(testTables,
			"compression_metadata_off_heap_memory_used",
			"table2 > table4 > table6 > table1 > table3 > table5",
			false);
		// index summary offheap: 6 > 4 > 2 > 1 = 3 = 5
		runCompareTest(testTables,
			"index_summary_off_heap_memory_used",
			"table6 > table4 > table2 > table1 > table3 > table5",
			false);
        // memtable offheap: 2 > 6 > 4 > 1 = 3 = 5
		runCompareTest(testTables,
			"memtable_off_heap_memory_used",
			"table2 > table6 > table4 > table1 > table3 > table5",
			false);
    }

    @Test
    public void testCompareStrings() throws Exception
    {
		// bloom filter space used: 2 > 4 > 6 > 1 > 3 > 5
		runCompareTest(testTables,
			"bloom_filter_space_used",
			"table2 > table4 > table6 > table1 > table3 > table5",
			false);
		// dropped mutations: 6 > 3 > 4 > 2 > 1 = 5
		runCompareTest(testTables,
			"dropped_mutations",
			"table6 > table3 > table4 > table2 > table1 > table5",
			false);
		// space used by snapshots: 5 > 1 > 2 > 4 > 3 = 6
		runCompareTest(testTables,
			"space_used_by_snapshots_total",
			"table5 > table1 > table2 > table4 > table3 > table6",
			false);
		// space used live: 6 > 5 > 4 > 2 > 1 = 3
		runCompareTest(testTables,
			"space_used_live",
			"table6 > table5 > table4 > table2 > table1 > table3",
			false);
        // space used total: 1 > 2 > 3 > 4 > 5 > 6
		runCompareTest(testTables,
			"space_used_total",
			"table1 > table2 > table3 > table4 > table5 > table6",
			false);
        // memtable data size: 6 > 5 > 4 > 3 > 2 > 1
		runCompareTest(testTables,
			"memtable_data_size",
			"table6 > table5 > table4 > table3 > table2 > table1",
			false);
    }

}
