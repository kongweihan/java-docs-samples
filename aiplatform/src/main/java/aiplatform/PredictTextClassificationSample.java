/*
 * Copyright 2023 Google LLC
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

// [START aiplatform_sdk_classify_news_items]

import com.google.cloud.aiplatform.v1.EndpointName;
import com.google.cloud.aiplatform.v1.PredictResponse;
import com.google.cloud.aiplatform.v1.PredictionServiceClient;
import com.google.cloud.aiplatform.v1.PredictionServiceSettings;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

// Text Classification with a Large Language Model
public class PredictTextClassificationSample {

  public static void main(String[] args) throws IOException {
    // TODO(developer): Replace these variables before running the sample.
    String instance =
        "{ \"content\": \"What is the topic for a given news headline?\n"
            + "- business\n"
            + "- entertainment\n"
            + "- health\n"
            + "- sports\n"
            + "- technology\n"
            + "\n"
            + "Text: Pixel 7 Pro Expert Hands On Review, the Most Helpful Google Phones.\n"
            + "The answer is: technology\n"
            + "\n"
            + "Text: Quit smoking?\n"
            + "The answer is: health\n"
            + "\n"
            + "Text: Roger Federer reveals why he touched Rafael Nadals hand while they were"
            + " crying\n"
            + "The answer is: sports\n"
            + "\n"
            + "Text: Business relief from Arizona minimum-wage hike looking more remote\n"
            + "The answer is: business\n"
            + "\n"
            + "Text: #TomCruise has arrived in Bari, Italy for #MissionImpossible.\n"
            + "The answer is: entertainment\n"
            + "\n"
            + "Text: CNBC Reports Rising Digital Profit as Print Advertising Falls\n"
            + "The answer is:\"}";
    String parameters =
        "{\n"
            + "  \"temperature\": 0,\n"
            + "  \"maxDecodeSteps\": 5,\n"
            + "  \"topP\": 0,\n"
            + "  \"topK\": 1\n"
            + "}";
    String project = "YOUR_PROJECT_ID";
    String publisher = "google";
    String model = "text-bison@001";

    predictTextClassification(instance, parameters, project, publisher, model);
  }

  static void predictTextClassification(
      String instance, String parameters, String project, String publisher, String model)
      throws IOException {
    PredictionServiceSettings predictionServiceSettings =
        PredictionServiceSettings.newBuilder()
            .setEndpoint("us-central1-aiplatform.googleapis.com:443")
            .build();

    // Initialize client that will be used to send requests. This client only needs to be created
    // once, and can be reused for multiple requests.
    try (PredictionServiceClient predictionServiceClient =
        PredictionServiceClient.create(predictionServiceSettings)) {
      String location = "us-central1";
      final EndpointName endpointName =
          EndpointName.ofProjectLocationPublisherModelName(project, location, publisher, model);

      Value.Builder instanceValue = Value.newBuilder();
      JsonFormat.parser().merge(instance, instanceValue);
      List<Value> instances = new ArrayList<>();
      instances.add(instanceValue.build());

      Value.Builder parameterValueBuilder = Value.newBuilder();
      JsonFormat.parser().merge(parameters, parameterValueBuilder);
      Value parameterValue = parameterValueBuilder.build();

      PredictResponse predictResponse =
          predictionServiceClient.predict(endpointName, instances, parameterValue);
      System.out.println("Predict Response");
    }
  }
}
// [END aiplatform_sdk_classify_news_items]
