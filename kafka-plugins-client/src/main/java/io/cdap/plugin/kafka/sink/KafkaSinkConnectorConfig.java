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

package io.cdap.plugin.kafka.sink;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginConfig;

import javax.annotation.Nullable;

/**
 * KafkaSink Connector Config. This class has been introduced to incorporate the backward incompatible change when
 * supporting connection in kafka sink to contain both fields "brokers" and "kafkaBrokers".
 */
public class KafkaSinkConnectorConfig extends PluginConfig {
  public static final String KAFKA_BROKERS = "kafkaBrokers";
  public static final String BROKERS = "brokers";

  @Name(KAFKA_BROKERS)
  @Description("List of Kafka brokers specified in host1:port1,host2:port2 form. For example, " +
    "host1.example.com:9092,host2.example.com:9092.")
  @Macro
  @Nullable
  private String kafkaBrokers;

  @Name(BROKERS)
  @Description("List of Kafka brokers specified in host1:port1,host2:port2 form. For example, " +
    "host1.example.com:9092,host2.example.com:9092.")
  @Macro
  @Nullable
  private String brokers;

  @Nullable
  public String getBrokers() {
    if (Strings.isNullOrEmpty(kafkaBrokers)) {
      return brokers;
    }
    return kafkaBrokers;
  }

  public KafkaSinkConnectorConfig(String brokers) {
    this.brokers = brokers;
  }
}
