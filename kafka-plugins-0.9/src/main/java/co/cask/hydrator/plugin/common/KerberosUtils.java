/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.hydrator.plugin.common;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;
import java.util.Set;
import javax.security.auth.login.Configuration;

/**
 * Utility class for Kerberos authentication
 */
public final class KerberosUtils {
  public static final String JAVA_AUTH_LOGIN_CONFIG = "java.security.auth.login.config";

  private static final Logger LOG = LoggerFactory.getLogger(KerberosUtils.class);

  // Util class cannot be instantiated
  private KerberosUtils() {
  }

  /**
   * Sets up a JAAS conf for Kafka client login with the given Kerberos principal and keytab
   *
   * @param pricipal Kerberos principal
   * @param keytabLocation Kerberos keytab for the principal
   * @throws IOException on any exception while writing the JAAS config file
   */
  public static void setupKerberosLogin(String pricipal, String keytabLocation) throws IOException {
    Set<PosixFilePermission> permissions = PosixFilePermissions.fromString("rw-------");
    FileAttribute<Set<PosixFilePermission>> fileAttributes = PosixFilePermissions.asFileAttribute(permissions);

    Path jaasConfFile = Files.createTempFile("kafka-client-jaas", "conf", fileAttributes);

    List<String> jassConfLines = ImmutableList.of(
      "KafkaClient { ",
        "com.sun.security.auth.module.Krb5LoginModule required ",
        "useKeyTab=true ",
        "storeKey=true ",
        "useTicketCache=false ",
        "renewTicket=true ",
        String.format("keyTab=\"%s\" ", keytabLocation),
        String.format("principal=\"%s\"; ", pricipal),
        "}; "
    );

    // TODO: merge with existing JAAS conf if any
    Files.write(jaasConfFile, jassConfLines, Charsets.UTF_8);
    System.setProperty(JAVA_AUTH_LOGIN_CONFIG, jaasConfFile.toAbsolutePath().toString());
    // Reset the configuration so that the new file gets loaded
    Configuration.setConfiguration(null);

    // TODO: change to debug
    if (LOG.isInfoEnabled()) {
      byte[] bytes = Files.readAllBytes(jaasConfFile.toAbsolutePath());
      LOG.info("Wrote JAAS login conf to file {}:\n{}", jaasConfFile.toAbsolutePath(),
               new String(bytes, Charsets.UTF_8));
    }
  }
}
