/*
 * Copyright © 2021 Cask Data, Inc.
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
 *
 */

package io.cdap.plugin.connector;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.plugin.PluginConfig;

/**
 * Kafka connector config
 */
public class KafkaConnectorConfig extends PluginConfig {
  @Description("List of Kafka brokers specified in host1:port1,host2:port2 form. For example, " +
                 "host1.example.com:9092,host2.example.com:9092.")
  @Macro
  private String kafkaBrokers;

  public KafkaConnectorConfig() {
  }

  public KafkaConnectorConfig(String kafkaBrokers) {
    this.kafkaBrokers = kafkaBrokers;
  }

  public String getKafkaBrokers() {
    return kafkaBrokers;
  }
}
