/*
 * Copyright 2023 Google LLC
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

package functions;

// [START functions_typed_greeting]
import com.google.cloud.functions.TypedFunction;
import com.google.gson.annotations.SerializedName;

public class Greeting implements TypedFunction<GreetingRequest, GreetingResponse> {
  @Override
  public GreetingResponse apply(GreetingRequest request) throws Exception {
    GreetingResponse response = new GreetingResponse();
    response.message = String.format("Hello %s %s!", request.firstName, request.lastName);
    return response;
  }
}
// [END functions_typed_greeting]
