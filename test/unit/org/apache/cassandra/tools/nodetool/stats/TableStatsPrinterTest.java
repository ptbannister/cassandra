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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.tools.NodeProbe;

public class TableStatsPrinterTest extends TableStatsTestBase {

	public static final String expectedDefaultPrinterTable1Output =
		"\t\tTable: %s\n" +
		"\t\tSSTable count: 0\n" +
		"\t\tSpace used (live): 0\n" +
		"\t\tSpace used (total): 9001\n" +
		"\t\tSpace used by snapshots (total): 0\n" +
		"\t\tSSTable Compression Ratio: -1.0\n" +
		"\t\tNumber of partitions (estimate): 0\n" +
		"\t\tMemtable cell count: 0\n" +
		"\t\tMemtable data size: 0\n" +
		"\t\tMemtable switch count: 0\n" +
		"\t\tLocal read count: 0\n" +
		"\t\tLocal read latency: 2.000 ms\n" +
		"\t\tLocal write count: 5\n" +
		"\t\tLocal write latency: 0.050 ms\n" +
		"\t\tPending flushes: 0\n" +
		"\t\tPercent repaired: 100.0\n" +
		"\t\tBloom filter false positives: 0\n" +
		"\t\tBloom filter false ratio: 0.00000\n" +
		"\t\tBloom filter space used: 0\n" +
		"\t\tCompacted partition minimum bytes: 0\n" +
		"\t\tCompacted partition maximum bytes: 0\n" +
		"\t\tCompacted partition mean bytes: 0\n" +
		"\t\tAverage live cells per slice (last five minutes): NaN\n" +
		"\t\tMaximum live cells per slice (last five minutes): 0\n" +
		"\t\tAverage tombstones per slice (last five minutes): NaN\n" +
		"\t\tMaximum tombstones per slice (last five minutes): 0\n" +
		"\t\tDropped Mutations: 0\n" +
		"\n";

	public static final String expectedDefaultPrinterTable2Output =
		"\t\tTable: %s\n" +
		"\t\tSSTable count: 0\n" +
		"\t\tSpace used (live): 0\n" +
		"\t\tSpace used (total): 1024\n" +
		"\t\tSpace used by snapshots (total): 0\n" +
		"\t\tSSTable Compression Ratio: -1.0\n" +
		"\t\tNumber of partitions (estimate): 0\n" +
		"\t\tMemtable cell count: 0\n" +
		"\t\tMemtable data size: 900\n" +
		"\t\tMemtable switch count: 0\n" +
		"\t\tLocal read count: 1\n" +
		"\t\tLocal read latency: 3.000 ms\n" +
		"\t\tLocal write count: 4\n" +
		"\t\tLocal write latency: 0.000 ms\n" +
		"\t\tPending flushes: 0\n" +
		"\t\tPercent repaired: 100.0\n" +
		"\t\tBloom filter false positives: 0\n" +
		"\t\tBloom filter false ratio: 0.00000\n" +
		"\t\tBloom filter space used: 0\n" +
		"\t\tCompacted partition minimum bytes: 0\n" +
		"\t\tCompacted partition maximum bytes: 0\n" +
		"\t\tCompacted partition mean bytes: 0\n" +
		"\t\tAverage live cells per slice (last five minutes): NaN\n" +
		"\t\tMaximum live cells per slice (last five minutes): 0\n" +
		"\t\tAverage tombstones per slice (last five minutes): NaN\n" +
		"\t\tMaximum tombstones per slice (last five minutes): 0\n" +
		"\t\tDropped Mutations: 0\n" +
		"\n";

	public static final String expectedDefaultPrinterTable3Output =
		"\t\tTable: %s\n" +
		"\t\tSSTable count: 0\n" +
		"\t\tSpace used (live): 0\n" +
		"\t\tSpace used (total): 512\n" +
		"\t\tSpace used by snapshots (total): 0\n" +
		"\t\tSSTable Compression Ratio: -1.0\n" +
		"\t\tNumber of partitions (estimate): 0\n" +
		"\t\tMemtable cell count: 0\n" +
		"\t\tMemtable data size: 1999\n" +
		"\t\tMemtable switch count: 0\n" +
		"\t\tLocal read count: 2\n" +
		"\t\tLocal read latency: 4.000 ms\n" +
		"\t\tLocal write count: 3\n" +
		"\t\tLocal write latency: NaN ms\n" +
		"\t\tPending flushes: 0\n" +
		"\t\tPercent repaired: 100.0\n" +
		"\t\tBloom filter false positives: 0\n" +
		"\t\tBloom filter false ratio: 0.00000\n" +
		"\t\tBloom filter space used: 0\n" +
		"\t\tCompacted partition minimum bytes: 0\n" +
		"\t\tCompacted partition maximum bytes: 0\n" +
		"\t\tCompacted partition mean bytes: 0\n" +
		"\t\tAverage live cells per slice (last five minutes): NaN\n" +
		"\t\tMaximum live cells per slice (last five minutes): 0\n" +
		"\t\tAverage tombstones per slice (last five minutes): NaN\n" +
		"\t\tMaximum tombstones per slice (last five minutes): 0\n" +
		"\t\tDropped Mutations: 0\n" +
		"\n";

	public static final String expectedDefaultPrinterTable4Output =
		"\t\tTable: %s\n" +
		"\t\tSSTable count: 0\n" +
		"\t\tSpace used (live): 0\n" +
		"\t\tSpace used (total): 256\n" +
		"\t\tSpace used by snapshots (total): 0\n" +
		"\t\tSSTable Compression Ratio: -1.0\n" +
		"\t\tNumber of partitions (estimate): 0\n" +
		"\t\tMemtable cell count: 0\n" +
		"\t\tMemtable data size: 3000\n" +
		"\t\tMemtable switch count: 0\n" +
		"\t\tLocal read count: 3\n" +
		"\t\tLocal read latency: NaN ms\n" +
		"\t\tLocal write count: 2\n" +
		"\t\tLocal write latency: 2.000 ms\n" +
		"\t\tPending flushes: 0\n" +
		"\t\tPercent repaired: 100.0\n" +
		"\t\tBloom filter false positives: 0\n" +
		"\t\tBloom filter false ratio: 0.00000\n" +
		"\t\tBloom filter space used: 0\n" +
		"\t\tCompacted partition minimum bytes: 0\n" +
		"\t\tCompacted partition maximum bytes: 0\n" +
		"\t\tCompacted partition mean bytes: 0\n" +
		"\t\tAverage live cells per slice (last five minutes): NaN\n" +
		"\t\tMaximum live cells per slice (last five minutes): 0\n" +
		"\t\tAverage tombstones per slice (last five minutes): NaN\n" +
		"\t\tMaximum tombstones per slice (last five minutes): 0\n" +
		"\t\tDropped Mutations: 0\n" +
		"\n";

	public static final String expectedDefaultPrinterTable5Output =
		"\t\tTable: %s\n" +
		"\t\tSSTable count: 0\n" +
		"\t\tSpace used (live): 0\n" +
		"\t\tSpace used (total): 64\n" +
		"\t\tSpace used by snapshots (total): 0\n" +
		"\t\tSSTable Compression Ratio: -1.0\n" +
		"\t\tNumber of partitions (estimate): 0\n" +
		"\t\tMemtable cell count: 0\n" +
		"\t\tMemtable data size: 20000\n" +
		"\t\tMemtable switch count: 0\n" +
		"\t\tLocal read count: 4\n" +
		"\t\tLocal read latency: 0.000 ms\n" +
		"\t\tLocal write count: 1\n" +
		"\t\tLocal write latency: 1.000 ms\n" +
		"\t\tPending flushes: 0\n" +
		"\t\tPercent repaired: 100.0\n" +
		"\t\tBloom filter false positives: 0\n" +
		"\t\tBloom filter false ratio: 0.00000\n" +
		"\t\tBloom filter space used: 0\n" +
		"\t\tCompacted partition minimum bytes: 0\n" +
		"\t\tCompacted partition maximum bytes: 0\n" +
		"\t\tCompacted partition mean bytes: 0\n" +
		"\t\tAverage live cells per slice (last five minutes): NaN\n" +
		"\t\tMaximum live cells per slice (last five minutes): 0\n" +
		"\t\tAverage tombstones per slice (last five minutes): NaN\n" +
		"\t\tMaximum tombstones per slice (last five minutes): 0\n" +
		"\t\tDropped Mutations: 0\n" +
		"\n";

	public static final String expectedDefaultPrinterTable6Output =
		"\t\tTable: %s\n" +
		"\t\tSSTable count: 0\n" +
		"\t\tSpace used (live): 0\n" +
		"\t\tSpace used (total): 0\n" +
		"\t\tSpace used by snapshots (total): 0\n" +
		"\t\tSSTable Compression Ratio: -1.0\n" +
		"\t\tNumber of partitions (estimate): 0\n" +
		"\t\tMemtable cell count: 0\n" +
		"\t\tMemtable data size: 1000000\n" +
		"\t\tMemtable switch count: 0\n" +
		"\t\tLocal read count: 5\n" +
		"\t\tLocal read latency: 1.000 ms\n" +
		"\t\tLocal write count: 0\n" +
		"\t\tLocal write latency: 0.500 ms\n" +
		"\t\tPending flushes: 0\n" +
		"\t\tPercent repaired: 100.0\n" +
		"\t\tBloom filter false positives: 0\n" +
		"\t\tBloom filter false ratio: 0.00000\n" +
		"\t\tBloom filter space used: 0\n" +
		"\t\tCompacted partition minimum bytes: 0\n" +
		"\t\tCompacted partition maximum bytes: 0\n" +
		"\t\tCompacted partition mean bytes: 0\n" +
		"\t\tAverage live cells per slice (last five minutes): NaN\n" +
		"\t\tMaximum live cells per slice (last five minutes): 0\n" +
		"\t\tAverage tombstones per slice (last five minutes): NaN\n" +
		"\t\tMaximum tombstones per slice (last five minutes): 0\n" +
		"\t\tDropped Mutations: 0\n" +
		"\n";

	/**
	 * Expected output of TableStatsPrinter DefaultPrinter for this dataset.
	 * Total number of tables is zero because it's non-trivial to simulate that metric
	 * without leaking test implementation into the TableStatsHolder implementation.
	 */
	public static final String expectedDefaultPrinterOutput =
		"Total number of tables: 0\n" +
		"----------------\n" +
		"Keyspace : keyspace1\n" +
		"\tRead Count: 3\n" +
		"\tRead Latency: 0.0 ms\n" +
		"\tWrite Count: 12\n" +
		"\tWrite Latency: 0.0 ms\n" +
		"\tPending Flushes: 0\n" +
		String.format(expectedDefaultPrinterTable1Output, "table1") +
		String.format(expectedDefaultPrinterTable2Output, "table2") +
		String.format(expectedDefaultPrinterTable3Output, "table3") +
		"----------------\n" +
		"Keyspace : keyspace2\n" +
		"\tRead Count: 7\n" +
		"\tRead Latency: 0.0 ms\n" +
		"\tWrite Count: 3\n" +
		"\tWrite Latency: 0.0 ms\n" +
		"\tPending Flushes: 0\n" +
		String.format(expectedDefaultPrinterTable4Output, "table4") +
		String.format(expectedDefaultPrinterTable5Output, "table5") +
		"----------------\n" +
		"Keyspace : keyspace3\n" +
		"\tRead Count: 5\n" +
		"\tRead Latency: 0.0 ms\n" +
		"\tWrite Count: 0\n" +
		"\tWrite Latency: NaN ms\n" +
		"\tPending Flushes: 0\n" +
		String.format(expectedDefaultPrinterTable6Output, "table6") +
		"----------------\n";

	@Test
	public void testDefaultPrinter() throws Exception {
		StatsHolder holder = new TestTableStatsHolder(testKeyspaces, "", 0);
		StatsPrinter printer = TableStatsPrinter.from("", false);
		ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
		printer.print(holder, new PrintStream(byteStream));
		assertEquals("StatsTablePrinter.DefaultPrinter does not print test vector as expected", expectedDefaultPrinterOutput, byteStream.toString());
	}

	@Test
	public void testSortedDefaultPrinter() throws Exception {
		assertEquals("StatsTablePrinter.SortedDefaultPrinter does not print test vector as expected", 1, 1);
	}

	/**
	 * A toy version of TableStatsHolder to hold a test vector instead of gathering stats from a live cluster.
	 */
	private static class TestTableStatsHolder extends TableStatsHolder {

            public TestTableStatsHolder(List<StatsKeyspace> testKeyspaces, String sortKey, int top) {
                super(null, false, false, new ArrayList<>(), sortKey, top);
                this.keyspaces.clear();
                this.keyspaces.addAll(testKeyspaces);
            }

            @Override
            protected boolean isTestTableStatsHolder() {
                return true;
            }
	}

}
