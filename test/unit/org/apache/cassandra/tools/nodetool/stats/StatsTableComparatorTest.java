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

    @Test
    public void testCompareDoubles() throws Exception
    {
		// average live cells: 1 > 6 > 2 > 5 > 3 > 4
		Collections.sort(testTables,
			new StatsTableComparator("average_live_cells_per_slice_last_five_minutes", false));
        assertEquals("StatsTableComparator failed to sort by average_live_cells_per_slice_last_five_minutes",
			"table1 > table6 > table2 > table5 > table3 > table4",
			buildSortOrderString(testTables));
		// average tombstones: 6 > 1 > 5 > 2 > 3 > 4
		Collections.sort(testTables,
			new StatsTableComparator("average_tombstones_per_slice_last_five_minutes", false));
        assertEquals("StatsTableComparator failed to sort by average_tombstones_per_slice_last_five_minutes",
			"table6 > table1 > table5 > table2 > table3 > table4",
			buildSortOrderString(testTables));
        // read latency: 3 > 2 > 1 > 6 > 4 > 5
		Collections.sort(testTables, new StatsTableComparator("read_latency", false));
        assertEquals("StatsTableComparator failed to sort by read_latency",
			"table3 > table2 > table1 > table6 > table4 > table5",
			buildSortOrderString(testTables));
        // write latency: 4 > 5 > 6 > 1 > 2 > 3
        Collections.sort(testTables, new StatsTableComparator("write_latency", false));
        assertEquals("StatsTableComparator failed to sort by write_latency",
			"table4 > table5 > table6 > table1 > table2 > table3",
			buildSortOrderString(testTables));
		// percent repaired
        Collections.sort(testTables, new StatsTableComparator("percent_repaired", false));
        assertEquals("StatsTableComparator failed to sort by percent_repaired",
			"table1 > table2 > table3 > table5 > table4 > table6",
			buildSortOrderString(testTables));
    }

    @Test
    public void testCompareLongs() throws Exception
    {
        // reads: 6 > 5 > 4 > 3 > 2 > 1
		Collections.sort(testTables, new StatsTableComparator("reads", false));
        assertEquals("StatsTableComparator failed to sort by reads",
			"table6 > table5 > table4 > table3 > table2 > table1",
			buildSortOrderString(testTables));
        // writes: 1 > 2 > 3 > 4 > 5 > 6
		Collections.sort(testTables, new StatsTableComparator("writes", false));
        assertEquals("StatsTableComparator failed to sort by writes",
			"table1 > table2 > table3 > table4 > table5 > table6",
			buildSortOrderString(testTables));
		// compacted partition maximum bytes: 1 > 3 > 5 > 2 > 4 = 6 
		Collections.sort(testTables, new StatsTableComparator("compacted_partition_maximum_bytes", false));
        assertEquals("StatsTableComparator failed to sort by compacted_partition_maximum_bytes",
			"table1 > table3 > table5 > table2 > table4 > table6",
			buildSortOrderString(testTables));
		// compacted partition mean bytes: 1 > 3 > 2 = 4 = 5 > 6
		Collections.sort(testTables, new StatsTableComparator("compacted_partition_mean_bytes", false));
        assertEquals("StatsTableComparator failed to sort by compacted_partition_mean_bytes",
			"table1 > table3 > table2 > table4 > table5 > table6",
			buildSortOrderString(testTables));
		// compacted partition minimum bytes: 6 > 4 > 2 > 5 > 1 = 3
		Collections.sort(testTables, new StatsTableComparator("compacted_partition_minimum_bytes", false));
        assertEquals("StatsTableComparator failed to sort by compacted_partition_minimum_bytes",
			"table6 > table4 > table2 > table5 > table1 > table3",
			buildSortOrderString(testTables));
		// maximum live cells last five minutes: 1 > 2 = 3 > 4 = 5 > 6
		Collections.sort(testTables, new StatsTableComparator("maximum_live_cells_per_slice_last_five_minutes", false));
        assertEquals("StatsTableComparator failed to sort by maximum_live_cells_per_slice_last_five_minutes",
			"table1 > table2 > table3 > table4 > table5 > table6",
			buildSortOrderString(testTables));
		// maximum tombstones last five minutes: 6 > 5 > 3 = 4 > 2 > 1
		Collections.sort(testTables, new StatsTableComparator("maximum_tombstones_per_slice_last_five_minutes", false));
        assertEquals("StatsTableComparator failed to sort by maximum_tombstones_per_slice_last_five_minutes",
			"table6 > table5 > table3 > table4 > table2 > table1",
			buildSortOrderString(testTables));
    }

    @Test
    public void testCompareHumanReadable() throws Exception
    {
        // human readable space used total: 6 > 5 > 4 > 3 > 2 > 1
		Collections.sort(humanReadableTables, new StatsTableComparator("space_used_total", true));
        assertEquals("StatsTableComparator failed to sort by human readable space used total",
			"table6 > table5 > table4 > table3 > table2 > table1",
			buildSortOrderString(humanReadableTables));
        // human readable memtable data size: 1 > 3 > 5 > 2 > 4 > 6
		Collections.sort(humanReadableTables, new StatsTableComparator("memtable_data_size", true));
        assertEquals("StatsTableComparator failed to sort by human readable memtable data size",
			"table1 > table3 > table5 > table2 > table4 > table6",
			buildSortOrderString(humanReadableTables));
    }

    @Test
    public void testCompareObjects() throws Exception
    {
        // bloom filter false positives: 2 > 4 > 6 > 1 > 3 > 5
		Collections.sort(testTables, new StatsTableComparator("bloom_filter_false_positives", false));
        assertEquals("StatsTableComparator failed to sort by bloom_filter_false_positives",
			"table2 > table4 > table6 > table1 > table3 > table5",
			buildSortOrderString(testTables));
        // bloom filter false positive ratio: 5 > 3 > 1 > 6 > 4 > 2
		Collections.sort(testTables, new StatsTableComparator("bloom_filter_false_ratio", false));
        assertEquals("StatsTableComparator failed to sort by bloom_filter_false_ratio",
			"table5 > table3 > table1 > table6 > table4 > table2",
			buildSortOrderString(testTables));
		// memtable cell count
		// memtable switch count
		// number of partitions estimate
		// pending flushes
		// sstable compression ratio
        // sstable count: 1 > 3 > 5 > 2 > 4 > 6
		Collections.sort(testTables, new StatsTableComparator("sstable_count", false));
        assertEquals("StatsTableComparator failed to sort by sstable_count",
			"table1 > table3 > table5 > table2 > table4 > table6",
			buildSortOrderString(testTables));
    }

    @Test
    public void testCompareOffHeap() throws Exception
    {
        // offheap memory total: 4 > 2 > 6 > 1 = 3 = 5 
        Collections.sort(testTables, new StatsTableComparator("off_heap_memory_used_total", false));
        assertEquals("StatsTableComparator failed to sort by off_heap_memory_used_total",
			"table4 > table2 > table6 > table1 > table3 > table5",
			buildSortOrderString(testTables));
        // bloom filter offheap: 4 > 6 > 2 > 1 > 3 > 5
        Collections.sort(testTables, new StatsTableComparator("bloom_filter_off_heap_memory_used", false));
        assertEquals("StatsTableComparator failed to sort by bloom_filter_off_heap_memory_used",
			"table4 > table6 > table2 > table1 > table3 > table5",
			buildSortOrderString(testTables));
		// compression metadata offheap
		// index summary offheap
        // memtable offheap: 2 > 6 > 4 > 1 = 3 = 5
        Collections.sort(testTables, new StatsTableComparator("memtable_off_heap_memory_used", false));
        assertEquals("StatsTableComparator failed to sort by memtable_off_heap_memory_used",
			"table2 > table6 > table4 > table1 > table3 > table5",
			buildSortOrderString(testTables));
    }

    @Test
    public void testCompareStrings() throws Exception
    {
		// bloom filter space used
		// dropped mutations
		// space used by snapshots
		// space used live
        // space used total: 1 > 2 > 3 > 4 > 5 > 6
        Collections.sort(testTables, new StatsTableComparator("space_used_total", false));
        assertEquals("StatsTableComparator failed to sort by space_used_total",
			"table1 > table2 > table3 > table4 > table5 > table6",
			buildSortOrderString(testTables));
        // memtable data size: 6 > 5 > 4 > 3 > 2 > 1
        Collections.sort(testTables, new StatsTableComparator("memtable_data_size", false));
        assertEquals("StatsTableComparator failed to sort by memtable_data_size",
			"table6 > table5 > table4 > table3 > table2 > table1",
			buildSortOrderString(testTables));
    }

}
