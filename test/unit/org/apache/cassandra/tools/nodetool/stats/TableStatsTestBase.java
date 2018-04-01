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
import java.util.List;

/**
 * Create a test vector for unit testing of TableStats features.
 */
public class TableStatsTestBase
{

	/**
	 * A test vector of StatsKeyspace and StatsTable objects loaded with human readable stats.
	 */
	protected static List<StatsKeyspace> humanReadableKeyspaces;

	/**
	 * A test vector of StatsTable objects loaded with human readable statistics.
	 */
	protected static List<StatsTable> humanReadableTables;

	/**
	 * A test vector of StatsKeyspace and StatsTable objects.
	 */
	protected static List<StatsKeyspace> testKeyspaces;

	/**
	 * A test vector of StatsTable objects.
	 */
	protected static List<StatsTable> testTables;

	/**
	 * @returns StatsKeyspace an instance of StatsKeyspace preset with values for use in a test vector
	 */
	private static StatsKeyspace createStatsKeyspaceTemplate(String keyspaceName)
	{
		return new StatsKeyspace(null, keyspaceName);
	}

	/**
	 * @returns StatsTable an instance of StatsTable preset with values for use in a test vector
	 */
	private static StatsTable createStatsTableTemplate(String keyspaceName, String tableName)
	{
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
	public static void createTestVector()
	{
		// create test tables from templates
		StatsTable table1 = createStatsTableTemplate("keyspace1", "table1");
		StatsTable table2 = createStatsTableTemplate("keyspace1", "table2");
		StatsTable table3 = createStatsTableTemplate("keyspace1", "table3");
		StatsTable table4 = createStatsTableTemplate("keyspace2", "table4");
		StatsTable table5 = createStatsTableTemplate("keyspace2", "table5");
		StatsTable table6 = createStatsTableTemplate("keyspace3", "table6");
		// reads: 6 > 5 > 4 > 3 > 2 > 1
		table1.localReadCount = 0L;
		table2.localReadCount = 1L;
		table3.localReadCount = 2L;
		table4.localReadCount = 3L;
		table5.localReadCount = 4L;
		table6.localReadCount = 5L;
		// writes: 1 > 2 > 3 > 4 > 5 > 6
		table1.localWriteCount = 5L;
		table2.localWriteCount = 4L;
		table3.localWriteCount = 3L;
		table4.localWriteCount = 2L;
		table5.localWriteCount = 1L;
		table6.localWriteCount = 0L;
		// read latency: 3 > 2 > 1 > 6 > 5 > 4
		table1.localReadLatencyMs = 2D;
		table2.localReadLatencyMs = 3D;
		table3.localReadLatencyMs = 4D;
		table4.localReadLatencyMs = Double.NaN;
		table5.localReadLatencyMs = 0D;
		table6.localReadLatencyMs = 1D;
		// write latency: 4 > 5 > 6 > 1 > 2 > 3
		table1.localWriteLatencyMs = 0.05D;
		table2.localWriteLatencyMs = 0D;
		table3.localWriteLatencyMs = Double.NaN;
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
		table1.memtableDataSize = "0";
		table2.memtableDataSize = "900";
		table3.memtableDataSize = "1999";
		table4.memtableDataSize = "3000";
		table5.memtableDataSize = "20000";
		table6.memtableDataSize = "1000000";
		// sstable count: 1 > 3 > 5 > 2 > 4 > 6
		table1.sstableCount = (Object) 60000;
		table2.sstableCount = (Object) 3000;
		table3.sstableCount = (Object) 50000;
		table4.sstableCount = (Object) 2000;
		table5.sstableCount = (Object) 40000;
		table6.sstableCount = (Object) 1000;
		// bloom filter false positives: 2 > 4 > 6 > 1 > 3 > 5
		table1.bloomFilterFalsePositives = (Object) 30L;
		table2.bloomFilterFalsePositives = (Object) 600L;
		table3.bloomFilterFalsePositives = (Object) 20L;
		table4.bloomFilterFalsePositives = (Object) 500L;
		table5.bloomFilterFalsePositives = (Object) 10L;
		table6.bloomFilterFalsePositives = (Object) 400L;
		// bloom filter false positive ratio: 5 > 3 > 1 > 6 > 4 > 2
		table1.bloomFilterFalseRatio = (Object) 0.40D;
		table2.bloomFilterFalseRatio = (Object) 0.01D;
		table3.bloomFilterFalseRatio = (Object) 0.50D;
		table4.bloomFilterFalseRatio = (Object) 0.02D;
		table5.bloomFilterFalseRatio = (Object) 0.60D;
		table6.bloomFilterFalseRatio = (Object) 0.03D;
		// set even numbered tables to have some offheap usage
		table2.offHeapUsed = true;
		table4.offHeapUsed = true;
		table6.offHeapUsed = true;
		table2.memtableOffHeapUsed = true;
		table4.memtableOffHeapUsed = true;
		table6.memtableOffHeapUsed = true;
		table2.bloomFilterOffHeapUsed = true;
		table4.bloomFilterOffHeapUsed = true;
		table6.bloomFilterOffHeapUsed = true;
		// offheap memory total: 4 > 2 > 6 > 1 = 3 = 5
		table2.offHeapMemoryUsedTotal = "314159363";
		table4.offHeapMemoryUsedTotal = "441213814";
		table6.offHeapMemoryUsedTotal = "162470806";
		// bloom filter offheap: 4 > 6 > 2 > 1 = 3 = 5
		table2.bloomFilterOffHeapMemoryUsed = "98";
		table4.bloomFilterOffHeapMemoryUsed = "299792458";
		table6.bloomFilterOffHeapMemoryUsed = "667408";
		// memtable offheap: 2 > 6 > 4 > 1 = 3 = 5
		table2.memtableOffHeapMemoryUsed = "314159265";
		table4.memtableOffHeapMemoryUsed = "141421356";
		table6.memtableOffHeapMemoryUsed = "161803398";
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
		for (int i = 0; i < testKeyspaces.size(); i++)
		{
			StatsKeyspace ks = testKeyspaces.get(i);
			for (StatsTable st : ks.tables)
			{
				ks.readCount += st.localReadCount;
				ks.writeCount += st.localWriteCount;
				ks.pendingFlushes += (long) st.pendingFlushes;
			}
			testKeyspaces.set(i, ks);
		}
		// populate testTables test vector
		testTables = new ArrayList<StatsTable>();
		testTables.add(table1);
		testTables.add(table2);
		testTables.add(table3);
		testTables.add(table4);
		testTables.add(table5);
		testTables.add(table6);
		//
		// create test vector for human readable case
		StatsTable humanReadableTable1 = createStatsTableTemplate("keyspace1", "table1");
		StatsTable humanReadableTable2 = createStatsTableTemplate("keyspace1", "table2");
		StatsTable humanReadableTable3 = createStatsTableTemplate("keyspace1", "table3");
		StatsTable humanReadableTable4 = createStatsTableTemplate("keyspace2", "table4");
		StatsTable humanReadableTable5 = createStatsTableTemplate("keyspace2", "table5");
		StatsTable humanReadableTable6 = createStatsTableTemplate("keyspace3", "table6");
		// human readable space used total: 6 > 5 > 4 > 3 > 2 > 1
		humanReadableTable1.spaceUsedTotal = "999 bytes";
		humanReadableTable2.spaceUsedTotal = "5 KiB";
		humanReadableTable3.spaceUsedTotal = "40 KiB";
		humanReadableTable4.spaceUsedTotal = "3 MiB";
		humanReadableTable5.spaceUsedTotal = "2 GiB";
		humanReadableTable6.spaceUsedTotal = "1 TiB";
		// human readable memtable data size: 1 > 3 > 5 > 2 > 4 > 6
		humanReadableTable1.memtableDataSize = "1.21 TiB";
		humanReadableTable2.memtableDataSize = "42 KiB";
		humanReadableTable3.memtableDataSize = "2.71 GiB";
		humanReadableTable4.memtableDataSize = "999 bytes";
		humanReadableTable5.memtableDataSize = "3.14 MiB";
		humanReadableTable6.memtableDataSize = "0 bytes";
		// create human readable keyspaces from template
		humanReadableKeyspaces = new ArrayList<StatsKeyspace>();
		StatsKeyspace humanReadableKeyspace1 = createStatsKeyspaceTemplate("keyspace1");
		StatsKeyspace humanReadableKeyspace2 = createStatsKeyspaceTemplate("keyspace2");
		StatsKeyspace humanReadableKeyspace3 = createStatsKeyspaceTemplate("keyspace3");
		// populate human readable StatsKeyspace tables lists
		humanReadableKeyspace1.tables.add(humanReadableTable1);
		humanReadableKeyspace1.tables.add(humanReadableTable2);
		humanReadableKeyspace1.tables.add(humanReadableTable3);
		humanReadableKeyspace2.tables.add(humanReadableTable4);
		humanReadableKeyspace2.tables.add(humanReadableTable5);
		humanReadableKeyspace3.tables.add(humanReadableTable6);
		// populate human readable keyspaces test vector
		humanReadableKeyspaces.add(humanReadableKeyspace1);
		humanReadableKeyspaces.add(humanReadableKeyspace2);
		humanReadableKeyspaces.add(humanReadableKeyspace3);
		// compute human readable keyspace statistics from relevant table metrics
		for (int i = 0; i < humanReadableKeyspaces.size(); i++)
		{
			StatsKeyspace ks = humanReadableKeyspaces.get(i);
			for (StatsTable st : ks.tables)
			{
				ks.readCount += st.localReadCount;
				ks.writeCount += st.localWriteCount;
				ks.pendingFlushes += (long) st.pendingFlushes;
			}
			humanReadableKeyspaces.set(i, ks);
		}
		// populate human readable tables test vector
		humanReadableTables = new ArrayList<StatsTable>();
		humanReadableTables.add(humanReadableTable1);
		humanReadableTables.add(humanReadableTable2);
		humanReadableTables.add(humanReadableTable3);
		humanReadableTables.add(humanReadableTable4);
		humanReadableTables.add(humanReadableTable5);
		humanReadableTables.add(humanReadableTable6);
	}
}
