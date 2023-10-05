package bigtable;

import com.google.api.gax.rpc.UnaryCallSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.ConditionalRowMutation;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Mutation;
import com.google.common.base.Preconditions;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Random;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
A new data-generating pipeline used to benchmark Dataflow and Bigtable integration.
 */
public class DataGeneratorStreaming {

  static final long BYTES_PER_COL = 1024;
  static final String ROW_KEY_FORMAT = "%04d";
  static final String COLUMN_FAMILY = "cf";

  public static void main(String[] args) throws IOException, GeneralSecurityException {
    BigtablePipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(BigtablePipelineOptions.class);

    Preconditions.checkNotNull(options.getBigtableInstanceId());

    String generateLabel = String
        .format("Generate rows for table %s", options.getBigtableTableId());

    Pipeline p = Pipeline.create(options);

    // Using streaming mode to generate unbounded source by omitting calling .to()
    PCollection<Long> numbers = p.apply(generateLabel, GenerateSequence.from(0).withRate(20000,
        Duration.standardSeconds(1)));

    PCollection<KV<ByteString, Mutation>>
        mutations = numbers.apply("CreateMutationFromSequenceNumber",
        ParDo.of(new CreateMutationFn()));

    mutations.apply("CheckAndMutateRow",
        ParDo.of(new CheckAndMutateRowDoFn(options.getProject(), options.getBigtableInstanceId(),
            options.getBigtableTableId())));

    p.run();
  }

  static class CreateMutationFn extends DoFn<Long, KV<ByteString, Mutation>> {

    final Logger logger = LoggerFactory.getLogger(CreateMutationFn.class);

    transient private Random random;

    @Setup
    public void setup() {
      random = new Random();
    }

    @ProcessElement
    public void processElement(@Element Long number,
        OutputReceiver<KV<ByteString, Mutation>> out) {
      long timestamp = 0L;  // fixed timestamp for all rows

      // Randomly write into rows of a certain range
      HashFunction hf = Hashing.md5();
      HashCode hc = hf.newHasher().putLong(number).hash();
      number = Math.abs(hc.asLong()) % 8000;

      String rowKey = String.format(ROW_KEY_FORMAT, number);

      // Generate random bytes
      byte[] randomData = new byte[(int) BYTES_PER_COL];
      random.nextBytes(randomData);

      // Create a single mutation (for 1 column) for the row
      Mutation mutation = Mutation.create()
          .setCell(COLUMN_FAMILY, ByteString.copyFromUtf8(String.valueOf(0)), timestamp,
              ByteString.copyFrom(randomData));

      out.output(KV.of(ByteString.copyFromUtf8(rowKey), mutation));
    }
  }

  static class CheckAndMutateRowDoFn extends DoFn<KV<ByteString, Mutation>, Void> {

    static private BigtableDataClient client = null;

    private String projectId;
    private String instanceId;
    private String tableId;

    public CheckAndMutateRowDoFn(String projectId, String instanceId, String tableId)
        throws IOException {
      this.projectId = projectId;
      this.instanceId = instanceId;
      this.tableId = tableId;
    }

    @Setup
    synchronized public void setup() throws IOException {
      if (client != null) {
        return;
      }

      BigtableDataSettings.Builder settingsBuilder =
          BigtableDataSettings.newBuilder()
              .setProjectId(projectId)
              .setInstanceId(instanceId);

      UnaryCallSettings.Builder<ConditionalRowMutation, Boolean>
          checkAndMutateSettings =
          settingsBuilder.stubSettings().checkAndMutateRowSettings();

      checkAndMutateSettings.setRetrySettings(
          checkAndMutateSettings.getRetrySettings().toBuilder()
              .setMaxAttempts(3)
              .setInitialRpcTimeout(org.threeten.bp.Duration.ofSeconds(10))
              .setMaxRetryDelay(org.threeten.bp.Duration.ofSeconds(10))
              .setTotalTimeout(org.threeten.bp.Duration.ofSeconds(30))
              .build());

      client = BigtableDataClient.create(settingsBuilder.build());
    }

    @ProcessElement
    public void processElement(@Element KV<ByteString, Mutation> mutation) {

      // If the COLUMN_FAMILY is empty for this row, write something
      ConditionalRowMutation conditionalRowMutation = ConditionalRowMutation.create(
              tableId, mutation.getKey())
          .condition(
              Filters.FILTERS.chain().filter(Filters.FILTERS.family().exactMatch(COLUMN_FAMILY)))
          .otherwise(mutation.getValue());
      client.checkAndMutateRow(conditionalRowMutation); // Using sync API
    }
  }

  public interface BigtablePipelineOptions extends DataflowPipelineOptions {
    @Description("The Bigtable instance ID")
    String getBigtableInstanceId();

    void setBigtableInstanceId(String bigtableInstanceId);

    @Description("The Bigtable table ID")
    String getBigtableTableId();

    void setBigtableTableId(String bigtableTableId);
  }
}