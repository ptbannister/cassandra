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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class StatsTable
{
    public String keyspaceName;
    public String tableName;
    public boolean isIndex;
    public boolean isLeveledSstable = false;
    public Object sstableCount;
    public String spaceUsedLive;
    public String spaceUsedTotal;
    public String spaceUsedBySnapshotsTotal;
    public boolean offHeapUsed = false;
    public String offHeapMemoryUsedTotal;
    public Object sstableCompressionRatio;
    public Object numberOfPartitionsEstimate;
    public Object memtableCellCount;
    public String memtableDataSize;
    public boolean memtableOffHeapUsed = false;
    public String memtableOffHeapMemoryUsed;
    public Object memtableSwitchCount;
    public long localReadCount;
    public double localReadLatencyMs;
    public long localWriteCount;
    public double localWriteLatencyMs;
    public Object pendingFlushes;
    public Object bloomFilterFalsePositives;
    public Object bloomFilterFalseRatio;
    public String bloomFilterSpaceUsed;
    public boolean bloomFilterOffHeapUsed = false;
    public String bloomFilterOffHeapMemoryUsed;
    public boolean indexSummaryOffHeapUsed = false;
    public String indexSummaryOffHeapMemoryUsed;
    public boolean compressionMetadataOffHeapUsed = false;
    public String compressionMetadataOffHeapMemoryUsed;
    public long compactedPartitionMinimumBytes;
    public long compactedPartitionMaximumBytes;
    public long compactedPartitionMeanBytes;
    public double percentRepaired;
    public long bytesRepaired;
    public long bytesUnrepaired;
    public long bytesPendingRepair;
    public double averageLiveCellsPerSliceLastFiveMinutes;
    public long maximumLiveCellsPerSliceLastFiveMinutes;
    public double averageTombstonesPerSliceLastFiveMinutes;
    public long maximumTombstonesPerSliceLastFiveMinutes;
    public String droppedMutations;
    public List<String> sstablesInEachLevel = new ArrayList<>();
}

/**
 * Comparator to sort StatsTables by a named statistic.
 */
protected class StatsTableComparator implements Comparator {

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
	public int compare(StatsTable x, StatsTable y) {
		int sign = ascending ? -1 : 1;
		if (sortKey.equals("memtableDataSize")) {
			return sign * Long.parseString(x.memtableDataSize).compareTo(Long.parseString(y.memtableDataSize));
		}
		else if (sortKey.equals("reads")) {
			return sign * Long(x.localReadCount).compareTo(Long(y.localReadCount));
		}
		else if (sortKey.equals("readLatency")) {
			return sign * Double(x.localReadLatencyMs).compareTo(Double(y.localReadLatencyMs));
		}
		else if (sortKey.equals("spaceUsedTotal")) {
			return sign * Long.parseString(x.spaceUsedTotal).compareTo(Long.parseString(y.spaceUsedTotal));
		}
		else if (sortKey.equals("writes")) {
			return sign * Long(x.localWriteCount).compareTo(Long(y.localWriteCount));
		}
		else if (sortKey.equals("writeLatency")) {
			return sign * Double(x.localWriteLatencyMs).compareTo(Double(y.localWriteLatencyMs));
		}
		else {
			// if a valid sortKey wasn't specified, sort alphabetically by keyspace, then by table
			return sign * (x.keyspaceName + "." + x.tableName).compareTo(y.keyspaceName + "." + y.tableName);
		}
	}
}
