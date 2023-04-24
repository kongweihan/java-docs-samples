/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package bigtable;

import avro.shaded.com.google.common.collect.ImmutableList;
import com.google.bigtable.v2.Mutation.SetCell;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.bigtable.v2.Mutation;
import com.google.cloud.bigtable.beam.CloudBigtableIO;
import com.google.cloud.bigtable.beam.CloudBigtableTableConfiguration;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableWriteResult;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
A new data-generating pipeline used to benchmark Dataflow and Bigtable integration.
 */
public class DataGenerator {

  static final long ONE_KB = 1024;
  static final long ONE_MB = 1024 * ONE_KB;
  static final long ONE_GB = 1024 * ONE_MB;
  static final long ONE_TB = 1024 * ONE_GB;

  static final long BYTES_PER_SPLIT = 1024 * 1024 * 1024;

  static long numRows;
  static String rowkeyFormat;

  static final String COLUMN_FAMILY = "cf";
  static final SecureRandom random = new SecureRandom();

  public static void main(String[] args) throws IOException, GeneralSecurityException {
    BigtablePipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(BigtablePipelineOptions.class);

    Preconditions.checkNotNull(options.getBigtableInstanceId());
    CheckPowerOfTen(options.getBigtableRowsPerGroup());

    numRows = options.getBigtableRows();

    // Compute rowkey format:
    // Rowkey will be from 0 to 1, padded by leading zeros.
    // E.g. if 1000 rows, rowkeys will be 000 to 999.
    int digits = String.valueOf(numRows - 1).length();
    rowkeyFormat = "%0" + digits + "d";
    System.out.println("rowkey format=" + rowkeyFormat);

    recreateTable(options);

    generateDataWithGroupBy(options);
  }

  static void CheckPowerOfTen(long input) {
    while (input >= 10 && input % 10 == 0) {
      input /= 10;
    }
    if (input != 1) {
      throw new IllegalArgumentException("bigtableRowsPerGroup must be power of 10.");
    }
  }

  static void recreateTable(BigtablePipelineOptions options) throws IOException {
    BigtableTableAdminSettings.Builder adminSettingsBuilder = BigtableTableAdminSettings.newBuilder()
        .setProjectId(options.getProject())
        .setInstanceId(options.getBigtableInstanceId());

    // adminSettingsBuilder.stubSettings()
    //     .setEndpoint("test-bigtableadmin.sandbox.googleapis.com:443");

    BigtableTableAdminClient adminClient = BigtableTableAdminClient.create(
        adminSettingsBuilder.build());

    // Re-create the table.
    System.out.println("Re-creating table: " + options.getBigtableTableId());
    // Table table = adminClient.getTable(options.getBigtableTableId());

    try {
      adminClient.deleteTable(options.getBigtableTableId());
    } catch (Exception e) {
      // Keep going if it's NOT_FOUND
      if (!e.toString().contains("NOT_FOUND")) {
        System.out.println("Delete table fail:" + e);
        throw e;
      }
    }


    CreateTableRequest createTableRequest = CreateTableRequest.of(options.getBigtableTableId())
        .addFamily(COLUMN_FAMILY);

    // Pre-split the table for optimal write throughput
    // List<ByteString> splits = computeSplits(options.getBigtableRows(),
    //     options.getBigtableBytesPerCol() * options.getBigtableColsPerRow());
    // System.out.println("with splits:");
    // for (ByteString split : splits) {
    //   createTableRequest.addSplit(split);
    //   System.out.println("\t" + split.toString());
    // }

    adminClient.createTable(createTableRequest);
    // System.out.println("Created table with " + splits.size() + " splits");

    adminClient.close();
  }

  //
  static List<ByteString> computeSplits(Long rows, long bytesPerRow) {
    long rowsPerSplit = BYTES_PER_SPLIT / bytesPerRow;
    List<ByteString> list = new ArrayList<>();
    while (rows > 0) {
      list.add(ByteString.copyFromUtf8(String.format(rowkeyFormat, rows)));
      rows -= rowsPerSplit;
    }
    return list;
  }

  static void generateDataWithGroupBy(BigtablePipelineOptions options)
      throws IOException {

    System.out.println("Generating " + options.getBigtableRows() + " rows, each "
        + options.getBigtableColsPerRow() + " columns, " + options.getBigtableBytesPerCol()
        + " bytes per column, " + options.getBigtableColsPerRow() * options.getBigtableBytesPerCol()
        + " bytes per row.");

    String generateLabel = String
        .format("Generate %d rows for table %s", numRows, options.getBigtableTableId());
    String mutationLabel = String
        .format("Create mutations that write %d columns of total %d bytes to each row",
            options.getBigtableColsPerRow(), options.getBigtableColsPerRow()
                * options.getBigtableBytesPerCol());

    Pipeline p = Pipeline.create(options);

    PCollection<Long> numbers = p.apply(generateLabel, GenerateSequence.from(0).to(numRows));
    PCollection<KV<Long, Long>> kv = numbers.apply("Create key for numbers",
        ParDo.of(new CreateKey(options.getBigtableRowsPerGroup())));
    PCollection<KV<Long, Iterable<Long>>> grouped = kv.apply(GroupByKey.create());
    PCollection<Long> expanded = grouped.apply("Expand and sort group",
        ParDo.of(new ExpandGroup()));

    if (options.getBigtableGenerateHbaseMutations()) {
      System.out.println("Using CloudBigtableIO");
      PCollection<org.apache.hadoop.hbase.client.Mutation> mutations = expanded.apply(mutationLabel,
          ParDo.of(new CreateHbaseMutationFn(options.getBigtableColsPerRow(),
              options.getBigtableBytesPerCol(), rowkeyFormat)));

      mutations.apply(
          String.format("Write data to table %s via CloudBigtableIO", options.getBigtableTableId()),
          CloudBigtableIO.writeToTable(new CloudBigtableTableConfiguration.Builder()
              .withProjectId(options.getProject())
              .withInstanceId(options.getBigtableInstanceId())
              .withTableId(options.getBigtableTableId())
              .withConfiguration(BigtableOptionsFactory.BIGTABLE_ENABLE_BULK_MUTATION_FLOW_CONTROL, "true")
              // .withConfiguration(BigtableOptionsFactory.BIGTABLE_BULK_MAX_ROW_KEY_COUNT, "12")
              // .withConfiguration("google.bigtable.enable.bulk.mutation.flow.control", "true")
              // .withConfiguration(BigtableOptionsFactory.BIGTABLE_CPU_BASED_THROTTLING_ENABLED, "true")
              // .withConfiguration(BigtableOptionsFactory.BIGTABLE_CPU_BASED_THROTTLING_TARGET_PERCENT, "70")
              // .withConfiguration(BigtableOptionsFactory.BIGTABLE_HOST_KEY,
              //     "test-bigtable.sandbox.googleapis.com")
              // .withConfiguration(BigtableOptionsFactory.BIGTABLE_ADMIN_HOST_KEY,
              //     "test-bigtableadmin.sandbox.googleapis.com")
              // .withConfiguration(BigtableOptionsFactory.BIGTABLE_PORT_KEY, "443")
              // .withConfiguration(BigtableOptionsFactory.BIGTABLE_BULK_MAX_REQUEST_SIZE_BYTES, "1048576")
              // .withConfiguration(BigtableOptionsFactory.BIGTABLE_MUTATE_RPC_ATTEMPT_TIMEOUT_MS_KEY, "610000")
              // .withConfiguration(BigtableOptionsFactory.BIGTABLE_MUTATE_RPC_TIMEOUT_MS_KEY, "700000")
              .build()));
    } else {
      System.out.println("Using BigtableIO");

      PCollection<KV<ByteString, Iterable<Mutation>>>
          mutations = expanded.apply(mutationLabel,
          ParDo.of(new CreateMutationFn(options.getBigtableColsPerRow(),
              options.getBigtableBytesPerCol(), rowkeyFormat)));

      mutations.apply(
          String.format("Write data to table %s via BigtableIO", options.getBigtableTableId()),
          BigtableIO.write()
              .withProjectId(options.getProject())
              .withInstanceId(options.getBigtableInstanceId())
              .withTableId(options.getBigtableTableId())
              // .withMaxOutstandingElements(9999999)
              // .withMaxOutstandingBytes(99999999)
              // .withMaxOutstandingElements(999999)
              .withMaxElementsPerBatch(90)
              .withFlowControl(true)
              // .withOperationTimeout(Duration.standardMinutes(20))
              // .withFlowControl(true)
              .withAttemptTimeout(Duration.standardSeconds(60)));

    }

    p.run();
  }

  static class CreateKey extends DoFn<Long, KV<Long, Long>> {

    final long rowsPerGroup;
    final SecureRandom random = new SecureRandom();

    public CreateKey(long rowsPerGroup) {
      this.rowsPerGroup = rowsPerGroup;
    }

    @ProcessElement
    public void processElement(@Element Long number, OutputReceiver<KV<Long, Long>> out) {
      out.output(KV.of(number / rowsPerGroup, number));
      // out.output(KV.of((long)random.nextInt(1500), number));
    }
  }

  static class ExpandGroup extends DoFn<KV<Long, Iterable<Long>>, Long> {

    @ProcessElement
    public void processElement(@Element KV<Long, Iterable<Long>> number, OutputReceiver<Long> out) {
      Iterable<Long> iterable = number.getValue();
      List<Long> list = new ArrayList<>();
      for (Long num : iterable) {
        list.add(num);
      }
      Collections.sort(list);
      for (Long num : list) {
        out.output(num);
      }
    }
  }

  static class CreateMutationFn extends DoFn<Long, KV<ByteString, Iterable<Mutation>>> {

    final Logger logger = LoggerFactory.getLogger(CreateMutationFn.class);

    private long workerThreadId;

    final int colsPerRow;
    final int bytesPerCol;
    final String format;

    private String splitFirstElement;
    private String splitLastElement;
    private int splitElementCount;
    private long splitStartTime;
    private int splitNumber;
    private int bundleElementCount;
    private int bundleNumber;

    // We can't initiate workerThreadId here because all Fn objects are constructed in runner once
    // and copied and sent to workers.
    public CreateMutationFn(int colsPerRow, int bytesPerCol, String rowkeyFormat) {
      this.colsPerRow = colsPerRow;
      this.bytesPerCol = bytesPerCol;
      this.format = rowkeyFormat;
    }

    @ProcessElement
    public void processElement(@Element Long number,
        OutputReceiver<KV<ByteString, Iterable<Mutation>>> out) {
      long timestamp = 0L;  // fixed timestamp for all rows
      if (splitFirstElement == null) {
        splitNumber++;
        splitStartTime = System.currentTimeMillis();
        splitElementCount = 0;
        splitFirstElement = number.toString();
        logger.warn(String.format(
            "dg-debug id=%5d start-split bundleNumber=%d splitNumber=%d firstElement=%s",
            workerThreadId, bundleNumber, splitNumber, splitFirstElement));

      } else {
        if (splitLastElement != null && number != Integer.parseInt(splitLastElement) + 1) {
          logger.warn(String.format(
              "dg-debug id=%5d finish-split bundleNumber=%d splitNumber=%d splitElementCount=%d firstElement=%s lastElement=%s spentSeconds=%f",
              workerThreadId, bundleNumber, splitNumber, splitElementCount, splitFirstElement,
              splitLastElement, (System.currentTimeMillis() - splitStartTime) / 1000.0));
          splitNumber++;
          splitStartTime = System.currentTimeMillis();
          splitElementCount = 0;
          splitFirstElement = number.toString();
          logger.warn(String.format(
              "dg-debug id=%5d start-split bundleNumber=%d splitNumber=%d firstElement=%s",
              workerThreadId, bundleNumber, splitNumber, splitFirstElement));
        }
      }
      splitLastElement = number.toString();
      splitElementCount++;
      bundleElementCount++;

      String rowKey = String.format(format, number);
      // Reverse the rowkey so that it's evenly writing to different TS and not rolling hotspotting
      // rowKey = new StringBuilder(rowKey).reverse().toString();

      // Generate random bytes
      List<Mutation> mutations = new ArrayList<>();
      for (int c = 0; c < colsPerRow; c++) {
        byte[] randomData = new byte[(int) bytesPerCol];
        random.nextBytes(randomData);

        SetCell setCell = SetCell.newBuilder().setFamilyName(COLUMN_FAMILY)
            .setColumnQualifier(ByteString.copyFromUtf8(String.valueOf(c)))
            .setTimestampMicros(timestamp).setValue(ByteString.copyFrom(randomData)).build();
        Mutation mutation = Mutation.newBuilder().setSetCell(setCell).build();
        mutations.add(mutation);
      }

      out.output(KV.of(ByteString.copyFromUtf8(rowKey), mutations));
    }

    @StartBundle
    public void startBundle(StartBundleContext context) throws IOException {
      // Initialize the thread id the first time the thread actually runs in the worker
      if (workerThreadId == 0) {
        workerThreadId = random.nextInt(99999);  // This should not have conflict usually
        logger.warn(String.format("dg-debug id=%5d new-thread", splitFirstElement));
      }

      bundleNumber++;
      logger.warn(String.format("dg-debug id=%5d start-bundle bundleNumber=%d", workerThreadId,
          bundleNumber));
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext context) throws IOException {
      logger.warn(String.format(
          "dg-debug id=%5d finish-split bundleNumber=%d splitNumber=%d splitElementCount=%d firstElement=%s lastElement=%s spentSeconds=%f",
          workerThreadId, bundleNumber, splitNumber, splitElementCount, splitFirstElement,
          splitLastElement, (System.currentTimeMillis() - splitStartTime) / 1000.0));
      logger.warn(String.format(
          "dg-debug id=%5d finish-bundle bundleNumber=%d splitCount=%d bundleElementCount=%d",
          workerThreadId, bundleNumber, splitNumber, bundleElementCount));

      splitNumber = 0;
      splitElementCount = 0;
      splitFirstElement = null;
      splitLastElement = null;
      bundleElementCount = 0;
    }
  }

  static class CreateHbaseMutationFn extends DoFn<Long, org.apache.hadoop.hbase.client.Mutation> {

    final Logger logger = LoggerFactory.getLogger(CreateHbaseMutationFn.class);

    private long workerThreadId;

    final int colsPerRow;
    final int bytesPerCol;
    final String format;

    private String splitFirstElement;
    private String splitLastElement;
    private int splitElementCount;
    private long splitStartTime;
    private int splitNumber;
    private int bundleElementCount;
    private int bundleNumber;

    // We can't initiate workerThreadId here because all Fn objects are constructed in runner once
    // and copied and sent to workers.
    public CreateHbaseMutationFn(int colsPerRow, int bytesPerCol, String rowkeyFormat) {
      this.colsPerRow = colsPerRow;
      this.bytesPerCol = bytesPerCol;
      this.format = rowkeyFormat;
    }

    @ProcessElement
    public void processElement(@Element Long number,
        OutputReceiver<org.apache.hadoop.hbase.client.Mutation> out) {
      long timestamp = 0L;  // fixed timestamp for all rows
      if (splitFirstElement == null) {
        splitNumber++;
        splitStartTime = System.currentTimeMillis();
        splitElementCount = 0;
        splitFirstElement = number.toString();
        logger.warn(String.format(
            "dg-debug id=%5d start-split bundleNumber=%d splitNumber=%d firstElement=%s",
            workerThreadId, bundleNumber, splitNumber, splitFirstElement));

      } else {
        if (splitLastElement != null && number != Integer.parseInt(splitLastElement) + 1) {
          logger.warn(String.format(
              "dg-debug id=%5d finish-split bundleNumber=%d splitNumber=%d splitElementCount=%d firstElement=%s lastElement=%s spentSeconds=%f",
              workerThreadId, bundleNumber, splitNumber, splitElementCount, splitFirstElement,
              splitLastElement, (System.currentTimeMillis() - splitStartTime) / 1000.0));
          splitNumber++;
          splitStartTime = System.currentTimeMillis();
          splitElementCount = 0;
          splitFirstElement = number.toString();
          logger.warn(String.format(
              "dg-debug id=%5d start-split bundleNumber=%d splitNumber=%d firstElement=%s",
              workerThreadId, bundleNumber, splitNumber, splitFirstElement));
        }
      }
      splitLastElement = number.toString();
      splitElementCount++;
      bundleElementCount++;

      String rowKey = String.format(format, number);
      // Reverse the rowkey so that it's evenly writing to different TS and not rolling hotspotting
      // rowKey = new StringBuilder(rowKey).reverse().toString();

      Put row = new Put(Bytes.toBytes(rowKey));

      // Generate random bytes
      for (int c = 0; c < colsPerRow; c++) {
        byte[] randomData = new byte[(int) bytesPerCol];
        random.nextBytes(randomData);

        row.addColumn(
            Bytes.toBytes(COLUMN_FAMILY),
            Bytes.toBytes(String.valueOf(c)),
            timestamp,
            randomData);
      }

      out.output(row);
    }

    @StartBundle
    public void startBundle(StartBundleContext context) throws IOException {
      // Initialize the thread id the first time the thread actually runs in the worker
      if (workerThreadId == 0) {
        workerThreadId = random.nextInt(99999);  // This should not have conflict usually
        logger.warn(String.format("dg-debug id=%5d new-thread", splitFirstElement));
      }

      bundleNumber++;
      logger.warn(String.format("dg-debug id=%5d start-bundle bundleNumber=%d", workerThreadId,
          bundleNumber));
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext context) throws IOException {
      logger.warn(String.format(
          "dg-debug id=%5d finish-split bundleNumber=%d splitNumber=%d splitElementCount=%d firstElement=%s lastElement=%s spentSeconds=%f",
          workerThreadId, bundleNumber, splitNumber, splitElementCount, splitFirstElement,
          splitLastElement, (System.currentTimeMillis() - splitStartTime) / 1000.0));
      logger.warn(String.format(
          "dg-debug id=%5d finish-bundle bundleNumber=%d splitCount=%d bundleElementCount=%d",
          workerThreadId, bundleNumber, splitNumber, bundleElementCount));

      splitNumber = 0;
      splitElementCount = 0;
      splitFirstElement = null;
      splitLastElement = null;
      bundleElementCount = 0;
    }
  }

  public interface BigtablePipelineOptions extends DataflowPipelineOptions {

    @Description("The Bigtable instance ID")
    String getBigtableInstanceId();

    void setBigtableInstanceId(String bigtableInstanceId);

    @Description("The Bigtable table ID")
    String getBigtableTableId();

    void setBigtableTableId(String bigtableTableId);

    @Description("The number of bytes per column")
    Integer getBigtableBytesPerCol();

    void setBigtableBytesPerCol(Integer bigtableBytesPerCol);

    @Description("The number of columns per row")
    Integer getBigtableColsPerRow();

    void setBigtableColsPerRow(Integer bigtableColsPerRow);

    @Description("The number of rows")
    Long getBigtableRows();

    void setBigtableRows(Long bigtableRows);

    @Description("Number of rows in a group. Each group contains a contiguous range of row keys. Musts be powers of 10.")
    @Default.Long(1000)
    Long getBigtableRowsPerGroup();

    void setBigtableRowsPerGroup(Long bigtableRowsPerGroup);

    @Description("Generate HBase mutations and use CloudBigtableIO instead of BigtableIO.")
    @Default.Boolean(false)
    Boolean getBigtableGenerateHbaseMutations();

    void setBigtableGenerateHbaseMutations(Boolean hbase);
  }
}
