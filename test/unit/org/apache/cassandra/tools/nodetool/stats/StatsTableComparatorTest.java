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

import org.apache.cassandra.tools.nodetool.stats.StatsKeyspace;
import org.apache.cassandra.tools.nodetool.stats.StatsTable;
import org.apache.cassandra.tools.nodetool.stats.StatsTableComparator;

import static org.junit.Assert.assertEquals;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class StatsTableComparatorTest {

	/**
	 * A test vector of StatsKeyspace and StatsTable objects
	 */
	private static List<StatsKeyspace> testKeyspaces;

	/**
	 * A test vector of StatsTable objects
	 */
	private static List<StatsTable> testTables;

	/**
	 * @returns StatsKeyspace an instance of StatsKeyspace preset with values for use in a test vector
	 */
	private static StatsKeyspace createStatsKeyspaceTemplate(String keyspaceName) {
		return new StatsKeyspace(null, keyspaceName);
	}

	/**
	 * @returns StatsTable an instance of StatsTable preset with values for use in a test vector
	 */
	private static StatsTable createStatsTableTemplate(String keyspaceName, String tableName) {
		StatsTable template = new StatsTable();
		template.keyspaceName = new String(keyspaceName);
		template.tableName = new String(tableName);
		template.isIndex = false;
		template.sstableCount = 0L;
		template.spaceUsedLive = "0";
		template.spaceUsedTotal = "0";
		template.spaceUsedBySnapshotsTotal = "0";
		template.percentRepaired = 100.0D;
		template.bytesRepaired = 0L;
		template.bytesUnrepaired = 0L;
		template.bytesPendingRepair = 0L;
		template.sstableCompressionRatio = -1.0D;
		template.numberOfPartitionsEstimate = 0L;
		template.memtableCellCount = 0L;
		template.memtableDataSize = "0";
		template.memtableSwitchCount = 0L;
		template.localReadCount =0L;
		template.localReadLatencyMs = Double.NaN;
		template.localWriteCount = 0L;
		template.localWriteLatencyMs = 0D;
		template.pendingFlushes = 0L;
		template.bloomFilterFalsePositives = 0L;
		template.bloomFilterFalseRatio = 0D;
		template.bloomFilterSpaceUsed = "0";
		template.indexSummaryOffHeapMemoryUsed = "0";
		template.compressionMetadataOffHeapMemoryUsed = "0";
		template.compactedPartitionMinimumBytes = 0L;
		template.compactedPartitionMaximumBytes = 0L;
		template.compactedPartitionMeanBytes = 0L;
		template.bytesRepaired = 0L;
		template.bytesUnrepaired = 0L;
		template.bytesPendingRepair = 0L;
		template.averageLiveCellsPerSliceLastFiveMinutes = Double.NaN;
		template.maximumLiveCellsPerSliceLastFiveMinutes = 0L;
		template.averageTombstonesPerSliceLastFiveMinutes = Double.NaN;
		template.maximumTombstonesPerSliceLastFiveMinutes = 0L;
		template.droppedMutations = "0";
		return template;
	}

	@BeforeClass
	public static void createTestVector() {
		// create test tables from templates
		StatsTable table1 = createStatsTableTemplate("keyspace1", "table1");
		StatsTable table2 = createStatsTableTemplate("keyspace1", "table2");
		StatsTable table3 = createStatsTableTemplate("keyspace1", "table3");
		StatsTable table4 = createStatsTableTemplate("keyspace2", "table4");
		StatsTable table5 = createStatsTableTemplate("keyspace2", "table5");
		StatsTable table6 = createStatsTableTemplate("keyspace3", "table6");
		// reads: 1 > 2 > 3 > 4 > 5 > 6
		table1.localReadCount = 5L;
		table2.localReadCount = 4L;
		table3.localReadCount = 3L;
		table4.localReadCount = 2L;
		table5.localReadCount = 1L;
		table6.localReadCount = 0L;
		// writes: 6 > 5 > 4 > 3 > 2 > 1
		table1.localReadCount = 0L;
		table2.localReadCount = 1L;
		table3.localReadCount = 2L;
		table4.localReadCount = 3L;
		table5.localReadCount = 4L;
		table6.localReadCount = 5L;
		// read latency: 3 > 2 > 1 > 6 > 5 > 4
		table1.localReadLatencyMs = 2D;
		table2.localReadLatencyMs = 3D;
		table3.localReadLatencyMs = 4D;
		table4.localReadLatencyMs = Double.NaN;
		table5.localReadLatencyMs = 0D;
		table6.localReadLatencyMs = 1D;
		// write latency: 4 > 5 > 6 > 1 > 2 > 3
		table1.localWriteLatencyMs = Double.NaN;
		table2.localWriteLatencyMs = 0D;
		table3.localWriteLatencyMs = 0.05D;
		table4.localWriteLatencyMs = 2D;
		table5.localWriteLatencyMs = 1D;
		table6.localWriteLatencyMs = 0.5D;
		// space used total: 1 > 2 > 3 > 4 > 5 > 6
		table1.spaceUsedTotal = "9001";
		table2.spaceUsedTotal = "1024";
		table3.spaceUsedTotal = "512";
		table4.spaceUsedTotal = "256";
		table5.spaceUsedTotal = "64";
		table6.spaceUsedTotal = "0";
		// memtable data size: 6 > 5 > 4 > 3 > 2 > 1
		table1.spaceUsedTotal = "0";
		table2.spaceUsedTotal = "900";
		table3.spaceUsedTotal = "1999";
		table4.spaceUsedTotal = "3000";
		table5.spaceUsedTotal = "20000";
		table6.spaceUsedTotal = "1000000";
		// create test keyspaces from templates
		testKeyspaces = new ArrayList<StatsKeyspace>();
		StatsKeyspace keyspace1 = createStatsKeyspaceTemplate("keyspace1");
		StatsKeyspace keyspace2 = createStatsKeyspaceTemplate("keyspace2");
		StatsKeyspace keyspace3 = createStatsKeyspaceTemplate("keyspace3");
		// populate StatsKeyspace tables lists
		keyspace1.tables.add(table1);
		keyspace1.tables.add(table2);
		keyspace1.tables.add(table3);
		keyspace2.tables.add(table4);
		keyspace2.tables.add(table5);
		keyspace3.tables.add(table6);
		// populate testKeyspaces test vector
		testKeyspaces.add(keyspace1);
		testKeyspaces.add(keyspace2);
		testKeyspaces.add(keyspace3);
		// compute keyspace statistics from relevant table metrics
		for (int i = 0; i < testKeyspaces.size(); i++) {
			StatsKeyspace ks = testKeyspaces.get(i);
			for (StatsTable st : ks.tables) {
				ks.readCount += st.localReadCount;
				ks.writeCount += st.localWriteCount;
				// TODO: totalReadTime
				// TODO: totalWriteTime
				ks.pendingFlushes += (long) st.pendingFlushes;
			}
			testKeyspaces.set(i, ks);
		}
		// populate testTables test vector
		testTables.add(table1);
		testTables.add(table2);
		testTables.add(table3);
		testTables.add(table4);
		testTables.add(table5);
		testTables.add(table6);
	}

	@Test
	public void testCompare() throws Exception {
		// reads: 1 > 2 > 3 > 4 > 5 > 6
		StatsTableComparator readsComparator = new StatsTableComparator("reads");
		Collections.sort(testTables, readsComparator);
		assertEquals(testTables.get(0).tableName, "table1", "StatsTableComparator failed to sort by reads");
		assertEquals(testTables.get(1).tableName, "table2", "StatsTableComparator failed to sort by reads");
		assertEquals(testTables.get(2).tableName, "table3", "StatsTableComparator failed to sort by reads");
		assertEquals(testTables.get(3).tableName, "table4", "StatsTableComparator failed to sort by reads");
		assertEquals(testTables.get(4).tableName, "table5", "StatsTableComparator failed to sort by reads");
		assertEquals(testTables.get(5).tableName, "table6", "StatsTableComparator failed to sort by reads");
		// writes: 6 > 5 > 4 > 3 > 2 > 1
		StatsTableComparator writesComparator = new StatsTableComparator("writes");
		Collections.sort(testTables, writesComparator);
		assertEquals(testTables.get(0).tableName, "table6", "StatsTableComparator failed to sort by writes");
		assertEquals(testTables.get(1).tableName, "table5", "StatsTableComparator failed to sort by writes");
		assertEquals(testTables.get(2).tableName, "table4", "StatsTableComparator failed to sort by writes");
		assertEquals(testTables.get(3).tableName, "table3", "StatsTableComparator failed to sort by writes");
		assertEquals(testTables.get(4).tableName, "table2", "StatsTableComparator failed to sort by writes");
		assertEquals(testTables.get(5).tableName, "table1", "StatsTableComparator failed to sort by writes");
		// read latency: 3 > 2 > 1 > 6 > 5 > 4
		StatsTableComparator readLatencyComparator = new StatsTableComparator("readLatency");
		Collections.sort(testTables, readLatencyComparator);
		assertEquals(testTables.get(0).tableName, "table3", "StatsTableComparator failed to sort by readLatency");
		assertEquals(testTables.get(1).tableName, "table2", "StatsTableComparator failed to sort by readLatency");
		assertEquals(testTables.get(2).tableName, "table1", "StatsTableComparator failed to sort by readLatency");
		assertEquals(testTables.get(3).tableName, "table6", "StatsTableComparator failed to sort by readLatency");
		assertEquals(testTables.get(4).tableName, "table5", "StatsTableComparator failed to sort by readLatency");
		assertEquals(testTables.get(5).tableName, "table4", "StatsTableComparator failed to sort by readLatency");
		// write latency: 4 > 5 > 6 > 1 > 2 > 3
		StatsTableComparator writeLatencyComparator = new StatsTableComparator("writeLatency");
		Collections.sort(testTables, writeLatencyComparator);
		assertEquals(testTables.get(0).tableName, "table3", "StatsTableComparator failed to sort by writeLatency");
		assertEquals(testTables.get(1).tableName, "table2", "StatsTableComparator failed to sort by writeLatency");
		assertEquals(testTables.get(2).tableName, "table1", "StatsTableComparator failed to sort by writeLatency");
		assertEquals(testTables.get(3).tableName, "table6", "StatsTableComparator failed to sort by writeLatency");
		assertEquals(testTables.get(4).tableName, "table5", "StatsTableComparator failed to sort by writeLatency");
		assertEquals(testTables.get(5).tableName, "table4", "StatsTableComparator failed to sort by writeLatency");
		// space used total: 1 > 2 > 3 > 4 > 5 > 6
		StatsTableComparator spaceUsedTotalComparator = new StatsTableComparator("spaceUsedTotal");
		Collections.sort(testTables, spaceUsedTotalComparator);
		assertEquals(testTables.get(0).tableName, "table1", "StatsTableComparator failed to sort by spaceUsedTotal");
		assertEquals(testTables.get(1).tableName, "table2", "StatsTableComparator failed to sort by spaceUsedTotal");
		assertEquals(testTables.get(2).tableName, "table3", "StatsTableComparator failed to sort by spaceUsedTotal");
		assertEquals(testTables.get(3).tableName, "table4", "StatsTableComparator failed to sort by spaceUsedTotal");
		assertEquals(testTables.get(4).tableName, "table5", "StatsTableComparator failed to sort by spaceUsedTotal");
		assertEquals(testTables.get(5).tableName, "table6", "StatsTableComparator failed to sort by spaceUsedTotal");
		// memtable data size: 6 > 5 > 4 > 3 > 2 > 1
		StatsTableComparator memtableDataSizeComparator = new StatsTableComparator("memtableDataSize");
		Collections.sort(testTables, memtableDataSizeComparator);
		assertEquals(testTables.get(0).tableName, "table6", "StatsTableComparator failed to sort by memtableDataSize");
		assertEquals(testTables.get(1).tableName, "table5", "StatsTableComparator failed to sort by memtableDataSize");
		assertEquals(testTables.get(2).tableName, "table4", "StatsTableComparator failed to sort by memtableDataSize");
		assertEquals(testTables.get(3).tableName, "table3", "StatsTableComparator failed to sort by memtableDataSize");
		assertEquals(testTables.get(4).tableName, "table2", "StatsTableComparator failed to sort by memtableDataSize");
		assertEquals(testTables.get(5).tableName, "table1", "StatsTableComparator failed to sort by memtableDataSize");
	}

}
