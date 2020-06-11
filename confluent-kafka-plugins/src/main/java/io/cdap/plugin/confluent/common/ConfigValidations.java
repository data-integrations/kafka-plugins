/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.confluent.common;

import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.KeyValueListParser;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for config validation
 */
public class ConfigValidations {
  private ConfigValidations() {
    throw new AssertionError("Should not be initialized");
  }

  public static void validateBrokers(String brokers, String propertyName, FailureCollector collector) {
    Map<String, Integer> brokerMap = new HashMap<>();
    try {
      Iterable<KeyValue<String, String>> parsed = KeyValueListParser.DEFAULT.parse(brokers);
      for (KeyValue<String, String> hostAndPort : parsed) {
        String host = hostAndPort.getKey();
        String portStr = hostAndPort.getValue();
        try {
          brokerMap.put(host, Integer.parseInt(portStr));
        } catch (NumberFormatException e) {
          collector.addFailure(String.format("Invalid port '%s' for host '%s'.", portStr, host),
                               "It should be a valid port number.")
            .withConfigElement(propertyName, host + ":" + portStr);
        }
      }
    } catch (IllegalArgumentException e) {
      // no-op
    }

    if (brokerMap.isEmpty()) {
      collector.addFailure("Kafka brokers must be provided in host:port format.", null)
        .withConfigProperty(propertyName);
    }
  }
}
