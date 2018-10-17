/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.hydrator;

import co.cask.hydrator.plugin.batch.source.KafkaBatchSource;
import com.google.common.util.concurrent.Service;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Unit tests for our plugins.
 */
public class KafkaBatchSourceTest extends AbstractKafkaBatchSourceTest {

  @Override
  protected Service createKafkaServer(Properties kafkaConfig) {
    return new EmbeddedKafkaServer(kafkaConfig);
  }

  @Override
  protected List<Class<?>> getPluginClasses() {
    return Collections.singletonList(KafkaBatchSource.class);
  }

  @Override
  protected String getKafkaBatchSourceName() {
    return KafkaBatchSource.NAME;
  }
}
