/*
 * Copyright 2024 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package aiplatform;

import static com.google.common.truth.Truth.assertThat;
import static junit.framework.TestCase.assertNotNull;

import com.google.cloud.testing.junit4.MultipleAttemptsRule;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

public class PredictImageFromImageAndTextSampleTest {

  @Rule public final MultipleAttemptsRule multipleAttemptsRule = new MultipleAttemptsRule(3);

  private static final String PROJECT = System.getenv("UCAIP_PROJECT_ID");
  private static final String PUBLISHER = "google";
  private static final String LOCATION = "us-central1";
  private static final String MODEL = "multimodalembedding@001";
  private static final String BASE_IMAGE_PATH = "resources/image_flower_daisy.jpg";
  private static final String TEXT_PROMPT = "an impressionist painting";
  private static final Map<String, Object> PARAMETERS = new HashMap<String, Object>();

  static {
    PARAMETERS.put("sampleCount", 1);
  }

  private ByteArrayOutputStream bout;
  private PrintStream originalPrintStream;

  private static void requireEnvVar(String varName) {
    String errorMessage =
        String.format("Environment variable '%s' is required to perform these tests.", varName);
    assertNotNull(errorMessage, System.getenv(varName));
  }

  @BeforeClass
  public static void checkRequirements() {
    requireEnvVar("GOOGLE_APPLICATION_CREDENTIALS");
    requireEnvVar("UCAIP_PROJECT_ID");
  }

  @Before
  public void setUp() {
    bout = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(bout);
    originalPrintStream = System.out;
    System.setOut(out);
  }

  @After
  public void tearDown() {
    System.out.flush();
    System.setOut(originalPrintStream);
  }

  @Test
  public void testPredictImageFromImageAndText() throws IOException {
    // Act
    PredictImageFromImageAndTextSample.predictImageFromImageAndText(
        PROJECT, LOCATION, PUBLISHER, MODEL, TEXT_PROMPT, BASE_IMAGE_PATH, PARAMETERS);

    // Assert
    String got = bout.toString();
    assertThat(got).contains("Predict Response");
  }
}
