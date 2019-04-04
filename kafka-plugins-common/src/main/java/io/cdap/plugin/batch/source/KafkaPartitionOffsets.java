/*
 * Copyright © 2017 Cask Data, Inc.
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
package io.cdap.plugin.batch.source;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 * A container class for holding the Kafka offsets for a set of partitions.
 */
public class KafkaPartitionOffsets {

  private final Map<Integer, Long> partitionOffsets;

  public KafkaPartitionOffsets(Map<Integer, Long> partitionOffsets) {
    this.partitionOffsets = new HashMap<>(partitionOffsets);
  }

  public void setPartitionOffset(int partition, long offset) {
    partitionOffsets.computeIfPresent(partition, (k, v) -> offset);
  }

  public long getPartitionOffset(int partition, long defaultValue) {
    return partitionOffsets.getOrDefault(partition, defaultValue);
  }

  /**
   * Loads the {@link KafkaPartitionOffsets} from the given input file.
   *
   * @param fc the hadoop {@link FileContext} to read from the input file
   * @param offsetFile the input file
   * @return a {@link KafkaPartitionOffsets} object decoded from the file
   * @throws IOException if failed to read the file
   */
  public static KafkaPartitionOffsets load(FileContext fc, Path offsetFile) throws IOException {
    try (Reader reader = new InputStreamReader(fc.open(offsetFile), StandardCharsets.UTF_8)) {
      return new Gson().fromJson(reader, KafkaPartitionOffsets.class);
    } catch (FileNotFoundException e) {
      // If the file does not exist, return an empty object
      return new KafkaPartitionOffsets(Collections.emptyMap());
    } catch (JsonSyntaxException e) {
      throw new IOException(e);
    }
  }

  /**
   * Saves the {@link KafkaPartitionOffsets} to the given file
   *
   * @param fc the hadoop {@link FileContext} to read from the input file
   * @param offsetFile the file to write to
   * @param partitionOffsets the {@link KafkaPartitionOffsets} object to save
   * @throws IOException if failed to write to the file
   */
  public static void save(FileContext fc, Path offsetFile,
                          KafkaPartitionOffsets partitionOffsets) throws IOException {
    try (Writer writer = new OutputStreamWriter(fc.create(offsetFile,
                                                          EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE),
                                                          Options.CreateOpts.perms(new FsPermission("600"))))) {
      new Gson().toJson(partitionOffsets, writer);
    }
  }
}

