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

import org.apache.cassandra.io.util.FileUtils;

/**
 * Comparator to sort StatsTables by a named statistic.
 */
public class StatsTableComparator implements Comparator
{

    /**
     * Name of the stat that will be used as the sort key.
     */
    private final String sortKey;

    /**
     * Whether data size stats are printed human readable.
     */
    private final boolean humanReadable;

    /**
     * Whether sorting should be done ascending.
     */
    private final boolean ascending;

    /**
     * Names of supported sort keys as they should be specified on the command line.
     */
    public static final String[] supportedSortKeys = { "average_live", "average_tombstones", "bloom_filter_false_positives", "bloom_filter_false_ratio", "bloom_filter_offheap", "bloom_filter_space_used", "bytes_pending_repair", "bytes_repaired", "bytes_unrepaired", "compacted_maximum", "compacted_mean", "compacted_minimum", "compression_metadata_offheap", "dropped_mutations", "index_summary_offheap", "maximum_live", "maximum_tombstones", "memtable_data_size", "memtable_offheap", "memtable_switch_count", "offheap_total", "partitions", "pending_flushes", "percent_repaired", "read_latency", "reads", "space_used_live", "space_used_snapshots", "space_used_total", "sstable_count", "sstable_compression_ratio", "write_latency", "writes" };

    public StatsTableComparator(String sortKey, boolean humanReadable)
    {
        this(sortKey, humanReadable, false);
    }
    
    public StatsTableComparator(String sortKey, boolean humanReadable, boolean ascending)
    {
        this.sortKey = sortKey;
        this.humanReadable = humanReadable;
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
        else if (sortKey.equals("bloom_filter_offheap"))
        {
            if (stx.bloomFilterOffHeapUsed && !sty.bloomFilterOffHeapUsed)
                return sign * -1;
            else if (!stx.bloomFilterOffHeapUsed && sty.bloomFilterOffHeapUsed)
                return sign * 1;
            else if (!stx.bloomFilterOffHeapUsed && !sty.bloomFilterOffHeapUsed)
                return 0;
            long bloomFilterOffHeapX = humanReadable ?
                FileUtils.parseFileSize(stx.bloomFilterOffHeapMemoryUsed)
                : Long.valueOf(stx.bloomFilterOffHeapMemoryUsed);
            long bloomFilterOffHeapY = humanReadable ?
                FileUtils.parseFileSize(sty.bloomFilterOffHeapMemoryUsed)
                : Long.valueOf(sty.bloomFilterOffHeapMemoryUsed);
            return sign * Long.compare(bloomFilterOffHeapX, bloomFilterOffHeapY);
        }
        else if (sortKey.equals("bloom_filter_space_used"))
        {
            long bloomFilterSpaceUsedX = humanReadable ?
                FileUtils.parseFileSize(stx.bloomFilterSpaceUsed)
                : Long.valueOf(stx.bloomFilterSpaceUsed);
            long bloomFilterSpaceUsedY = humanReadable ?
                FileUtils.parseFileSize(sty.bloomFilterSpaceUsed)
                : Long.valueOf(sty.bloomFilterSpaceUsed);
            return sign * Long.compare(bloomFilterSpaceUsedX, bloomFilterSpaceUsedY);
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
        else if (sortKey.equals("compression_metadata_offheap"))
        {
            if (stx.compressionMetadataOffHeapUsed && !sty.compressionMetadataOffHeapUsed)
                return sign * -1;
            else if (!stx.compressionMetadataOffHeapUsed && sty.compressionMetadataOffHeapUsed)
                return sign * 1;
            else if (!stx.compressionMetadataOffHeapUsed && !sty.compressionMetadataOffHeapUsed)
                return 0;
            long compressionMetadataOffHeapX = humanReadable ?
                FileUtils.parseFileSize(stx.compressionMetadataOffHeapMemoryUsed)
                : Long.valueOf(stx.compressionMetadataOffHeapMemoryUsed);
            long compressionMetadataOffHeapY = humanReadable ?
                FileUtils.parseFileSize(sty.compressionMetadataOffHeapMemoryUsed)
                : Long.valueOf(sty.compressionMetadataOffHeapMemoryUsed);
            return sign * Long.compare(compressionMetadataOffHeapX, compressionMetadataOffHeapY);
        }
        else if (sortKey.equals("dropped_mutations"))
        {
            long droppedMutationsX = humanReadable ?
                FileUtils.parseFileSize(stx.droppedMutations)
                : Long.valueOf(stx.droppedMutations);
            long droppedMutationsY = humanReadable ?
                FileUtils.parseFileSize(sty.droppedMutations)
                : Long.valueOf(sty.droppedMutations);
            return sign * Long.compare(droppedMutationsX, droppedMutationsY);
        }
        else if (sortKey.equals("index_summary_offheap"))
        {
            if (stx.indexSummaryOffHeapUsed && !sty.indexSummaryOffHeapUsed)
                return sign * -1;
            else if (!stx.indexSummaryOffHeapUsed && sty.indexSummaryOffHeapUsed)
                return sign * 1;
            else if (!stx.indexSummaryOffHeapUsed && !sty.indexSummaryOffHeapUsed)
                return 0;
            long indexSummaryOffHeapX = humanReadable ?
                FileUtils.parseFileSize(stx.indexSummaryOffHeapMemoryUsed)
                : Long.valueOf(stx.indexSummaryOffHeapMemoryUsed);
            long indexSummaryOffHeapY = humanReadable ?
                FileUtils.parseFileSize(sty.indexSummaryOffHeapMemoryUsed)
                : Long.valueOf(sty.indexSummaryOffHeapMemoryUsed);
            return sign * Long.compare(indexSummaryOffHeapX, indexSummaryOffHeapY);
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
        else if (sortKey.equals("memtable_cell_count"))
        {
            return sign * ((Long) stx.memtableCellCount)
                .compareTo((Long) sty.memtableCellCount); 
        }
        else if (sortKey.equals("memtable_data_size"))
        {
            long memtableDataSizeX = humanReadable ?
                FileUtils.parseFileSize(stx.memtableDataSize)
                : Long.valueOf(stx.memtableDataSize);
            long memtableDataSizeY = humanReadable ?
                FileUtils.parseFileSize(sty.memtableDataSize)
                : Long.valueOf(sty.memtableDataSize);
            return sign * Long.compare(memtableDataSizeX, memtableDataSizeY);
        }
        else if (sortKey.equals("memtable_offheap"))
        {
            if (stx.memtableOffHeapUsed && !sty.memtableOffHeapUsed)
                return sign * -1;
            else if (!stx.memtableOffHeapUsed && sty.memtableOffHeapUsed)
                return sign * 1;
            else if (!stx.memtableOffHeapUsed && !sty.memtableOffHeapUsed)
                return 0;
            long memtableOffHeapX = humanReadable ?
                FileUtils.parseFileSize(stx.memtableOffHeapMemoryUsed)
                : Long.valueOf(stx.memtableOffHeapMemoryUsed);
            long memtableOffHeapY = humanReadable ?
                FileUtils.parseFileSize(sty.memtableOffHeapMemoryUsed)
                : Long.valueOf(sty.memtableOffHeapMemoryUsed);
            return sign * Long.compare(memtableOffHeapX, memtableOffHeapY);
        }
        else if (sortKey.equals("memtable_switch_count"))
        {
            return sign * ((Long) stx.memtableSwitchCount)
                .compareTo((Long) sty.memtableSwitchCount); 
        }
        else if (sortKey.equals("offheap_total"))
        {
            if (stx.offHeapUsed && !sty.offHeapUsed)
                return sign * -1;
            else if (!stx.offHeapUsed && sty.offHeapUsed)
                return sign * 1;
            else if (!stx.offHeapUsed && !sty.offHeapUsed)
                return 0;
            long offHeapTotalX = humanReadable ?
                FileUtils.parseFileSize(stx.offHeapMemoryUsedTotal)
                : Long.valueOf(stx.offHeapMemoryUsedTotal);
            long offHeapTotalY = humanReadable ?
                FileUtils.parseFileSize(sty.offHeapMemoryUsedTotal)
                : Long.valueOf(sty.offHeapMemoryUsedTotal);
            return sign * Long.compare(offHeapTotalX, offHeapTotalY);
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
            long spaceUsedLiveX = humanReadable ?
                FileUtils.parseFileSize(stx.spaceUsedLive)
                : Long.valueOf(stx.spaceUsedLive);
            long spaceUsedLiveY = humanReadable ?
                FileUtils.parseFileSize(sty.spaceUsedLive)
                : Long.valueOf(sty.spaceUsedLive);
            return sign * Long.compare(spaceUsedLiveX, spaceUsedLiveY);
        }
        else if (sortKey.equals("space_used_snapshots"))
        {
            long spaceUsedBySnapshotsX = humanReadable ?
                FileUtils.parseFileSize(stx.spaceUsedBySnapshotsTotal)
                : Long.valueOf(stx.spaceUsedBySnapshotsTotal);
            long spaceUsedBySnapshotsY = humanReadable ?
                FileUtils.parseFileSize(sty.spaceUsedBySnapshotsTotal)
                : Long.valueOf(sty.spaceUsedBySnapshotsTotal);
            return sign * Long.compare(spaceUsedBySnapshotsX, spaceUsedBySnapshotsY);
        }
        else if (sortKey.equals("space_used_total"))
        {
            long spaceUsedTotalX = humanReadable ?
                FileUtils.parseFileSize(stx.spaceUsedTotal)
                : Long.valueOf(stx.spaceUsedTotal);
            long spaceUsedTotalY = humanReadable ?
                FileUtils.parseFileSize(sty.spaceUsedTotal)
                : Long.valueOf(sty.spaceUsedTotal);
            return sign * Long.compare(spaceUsedTotalX, spaceUsedTotalY);
        }
        else if (sortKey.equals("sstable_count"))
        {
            return sign * ((Integer) stx.sstableCount)
                .compareTo((Integer) sty.sstableCount);
        }
        else if (sortKey.equals("sstable_compression_ratio"))
        {
            return sign * ((Double) stx.sstableCompressionRatio)
                .compareTo((Double) sty.sstableCompressionRatio);
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
