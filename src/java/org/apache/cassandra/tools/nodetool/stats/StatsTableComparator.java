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
public class StatsTableComparator implements Comparator
{

	// TODO: docstring
	private String sortKey;

	// TODO: docstring
	private boolean ascending;

	// TODO: docstring
	public static final String[] supportedSortKeys = { "average_live", "average_tombstones", "bloom_filter_false_positives", "bloom_filter_false_ratio", "bloom_filter_space_used", "bytes_pending_repair", "bytes_repaired", "bytes_unrepaired", "compacted_maximum", "compacted_mean", "compacted_minimum", "maximum_live", "maximum_tombstones", "memtable_data_size", "partitions", "pending_flushes", "percent_repaired", "read_latency", "reads", "space_used_live", "space_used_total", "sstable_count", "write_latency", "writes" };

	public StatsTableComparator(String sortKey)
	{
		this.sortKey = sortKey;
		this.ascending = false;
	}
	
	public StatsTableComparator(String sortKey, boolean ascending)
	{
		this.sortKey = sortKey;
		this.ascending = ascending;
	}

	/**
	 * Compare StatsTable instances based on this instance's sortKey.
	 */
	public int compare(Object x, Object y)
	{
		if (!(x instanceof StatsTable && y instanceof StatsTable))
		{
			throw new ClassCastException(String.format("StatsTableComparator cannot compare %s and %s", x.getClass().toString(), y.getClass().toString()));
		}
		if (x == null || y == null)
		{
			throw new NullPointerException("StatsTableComparator cannot compare null objects");
		}
		StatsTable stx = (StatsTable) x;
		StatsTable sty = (StatsTable) y;
		int sign = ascending ? 1 : -1;
		if (sortKey.equals("average_live"))
		{
			return sign * Double.valueOf(stx.averageLiveCellsPerSliceLastFiveMinutes)
				.compareTo(Double.valueOf(sty.averageLiveCellsPerSliceLastFiveMinutes));
		}
		else if (sortKey.equals("average_tombstones"))
		{
			return sign * Double.valueOf(stx.averageTombstonesPerSliceLastFiveMinutes)
				.compareTo(Double.valueOf(sty.averageTombstonesPerSliceLastFiveMinutes));
		}
		else if (sortKey.equals("bloom_filter_false_positives"))
		{
			return sign * ((Long) stx.bloomFilterFalsePositives)
				.compareTo((Long) sty.bloomFilterFalsePositives);
		}
		else if (sortKey.equals("bloom_filter_false_ratio"))
		{
			return sign * ((Double) stx.bloomFilterFalseRatio)
				.compareTo((Double) sty.bloomFilterFalseRatio);
		}
		else if (sortKey.equals("bloom_filter_space_used"))
		{
			return sign * Long.valueOf(stx.bloomFilterSpaceUsed)
				.compareTo(Long.valueOf(sty.bloomFilterSpaceUsed));
		}
		else if (sortKey.equals("bytes_pending_repair"))
		{
			return sign * Long.valueOf(stx.bytesPendingRepair)
				.compareTo(Long.valueOf(sty.bytesPendingRepair));
		}
		else if (sortKey.equals("bytes_repaired"))
		{
			return sign * Long.valueOf(stx.bytesRepaired)
				.compareTo(Long.valueOf(sty.bytesRepaired));
		}
		else if (sortKey.equals("bytes_unrepaired"))
		{
			return sign * Long.valueOf(stx.bytesUnrepaired)
				.compareTo(Long.valueOf(sty.bytesUnrepaired));
		}
		else if (sortKey.equals("compacted_maximum"))
		{
			return sign * Long.valueOf(stx.compactedPartitionMaximumBytes)
				.compareTo(Long.valueOf(sty.compactedPartitionMaximumBytes));
		}
		else if (sortKey.equals("compacted_mean"))
		{
			return sign * Long.valueOf(stx.compactedPartitionMeanBytes)
				.compareTo(Long.valueOf(sty.compactedPartitionMeanBytes));
		}
		else if (sortKey.equals("compacted_minimum"))
		{
			return sign * Long.valueOf(stx.compactedPartitionMinimumBytes)
				.compareTo(Long.valueOf(sty.compactedPartitionMinimumBytes));
		}
		else if (sortKey.equals("maximum_live"))
		{
			return sign * Long.valueOf(stx.maximumLiveCellsPerSliceLastFiveMinutes)
				.compareTo(Long.valueOf(sty.maximumLiveCellsPerSliceLastFiveMinutes));
		}
		else if (sortKey.equals("maximum_tombstones"))
		{
			return sign * Long.valueOf(stx.maximumTombstonesPerSliceLastFiveMinutes)
				.compareTo(Long.valueOf(sty.maximumTombstonesPerSliceLastFiveMinutes));
		}
		else if (sortKey.equals("memtable_data_size"))
		{
			return sign * Long.valueOf(stx.memtableDataSize)
				.compareTo(Long.valueOf(sty.memtableDataSize));
		}
		else if (sortKey.equals("partitions"))
		{
			return sign * ((Long) stx.numberOfPartitionsEstimate)
				.compareTo((Long) sty.numberOfPartitionsEstimate);
		}
		else if (sortKey.equals("pending_flushes"))
		{
			return sign * ((Long) stx.pendingFlushes)
				.compareTo((Long) sty.pendingFlushes);
		}
		else if (sortKey.equals("percent_repaired"))
		{
			return sign * Double.valueOf(stx.percentRepaired)
				.compareTo(Double.valueOf(sty.percentRepaired));
		}
		else if (sortKey.equals("read_latency"))
		{
			// Double.NaN means read latency of zero, rather than its usual meaning of infinity
			if (Double.isNaN(stx.localReadLatencyMs) && !Double.isNaN(sty.localReadLatencyMs))
				return sign * -1;
			else if (!Double.isNaN(stx.localReadLatencyMs) && Double.isNaN(sty.localReadLatencyMs))
				return sign * 1;
			else if (Double.isNaN(stx.localReadLatencyMs) && Double.isNaN(sty.localReadLatencyMs))
				return 0;
			else
				return sign * Double.valueOf(stx.localReadLatencyMs)
					.compareTo(Double.valueOf(sty.localReadLatencyMs));
		}
		else if (sortKey.equals("reads"))
		{
			return sign * Long.valueOf(stx.localReadCount)
				.compareTo(Long.valueOf(sty.localReadCount));
		}
		else if (sortKey.equals("space_used_live"))
		{
			return sign * Long.valueOf(stx.spaceUsedLive)
				.compareTo(Long.valueOf(sty.spaceUsedLive));
		}
		else if (sortKey.equals("space_used_total"))
		{
			return sign * Long.valueOf(stx.spaceUsedTotal)
				.compareTo(Long.valueOf(sty.spaceUsedTotal));
		}
		else if (sortKey.equals("sstable_count"))
		{
			return sign * ((Integer) stx.sstableCount)
				.compareTo((Integer) sty.sstableCount);
		}
		else if (sortKey.equals("write_latency"))
		{
			// Double.NaN means write latency of zero, rather than its usual meaning of infinity
			if (Double.isNaN(stx.localWriteLatencyMs)
			    && !Double.isNaN(sty.localWriteLatencyMs))
				return sign * -1;
			else if (!Double.isNaN(stx.localWriteLatencyMs)
				 && Double.isNaN(sty.localWriteLatencyMs))
				return sign * 1;
			else if (Double.isNaN(stx.localWriteLatencyMs)
				 && Double.isNaN(sty.localWriteLatencyMs))
				return 0;
			else
				return sign * Double.valueOf(stx.localWriteLatencyMs)
					.compareTo(Double.valueOf(sty.localWriteLatencyMs));
		}
		else if (sortKey.equals("writes"))
		{
			return sign * Long.valueOf(stx.localWriteCount)
				.compareTo(Long.valueOf(sty.localWriteCount));
		}
		else
		{
			throw new IllegalStateException(String.format("Unsupported sort key: %s", sortKey));
		}
	}
}
