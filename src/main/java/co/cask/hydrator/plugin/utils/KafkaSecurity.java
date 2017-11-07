package co.cask.hydrator.plugin.utils;

import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import javax.security.auth.login.AppConfigurationEntry;

public class KafkaSecurity {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaSecurity.class);

  private KafkaSecurity() {

  }

  public static void SetupKafkaSecurity() {
    LOG.info("Initializing Security");
    // TODO do this only when its not set
    System.setProperty("java.security.auth.login.config", "dummy");
    LOG.info("Setting java.security.auth.login.config to {}", System.getProperty("java.security.auth.login.config"));
    final Map<String, String> properties = new HashMap<>();
    properties.put("doNotPrompt", "true");
    properties.put("useKeyTab", "true");
    properties.put("useTicketCache", "false");
    // TODO get the following from config
    properties.put("principal", "cdap/amr28827-1000.dev.continuuity.net@CONTINUUITY.NET");
    properties.put("keyTab", "/etc/security/keytabs/cdap.service.keytab");

    final AppConfigurationEntry configurationEntry = new AppConfigurationEntry(
      KerberosUtil.getKrb5LoginModuleName(), AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, properties);

    javax.security.auth.login.Configuration configuration = new javax.security.auth.login.Configuration() {
      @Override
      public AppConfigurationEntry[] getAppConfigurationEntry(String s) {
        return new AppConfigurationEntry[] { configurationEntry };
      }
    };

    // apply the configuration
    javax.security.auth.login.Configuration.setConfiguration(configuration);

    String loginContextName = "KafkaClient";
    AppConfigurationEntry[] configurationEntries = javax.security.auth.login.Configuration.getConfiguration().getAppConfigurationEntry(loginContextName);
    if (configurationEntries == null) {
      String errorMessage = "Could not find a '" + loginContextName + "' entry in this configuration.";
      LOG.error(errorMessage);
    }

    for (AppConfigurationEntry entry: configurationEntries) {
      LOG.info("Entry Options map is {} ", entry.getOptions());
    }
  }
}
