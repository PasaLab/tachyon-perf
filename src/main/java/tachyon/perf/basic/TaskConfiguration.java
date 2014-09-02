package tachyon.perf.basic;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.log4j.Logger;

import tachyon.perf.conf.PerfConf;
import tachyon.perf.util.SAXConfiguration;

/**
 * Manage the configurations for each task.
 */
public class TaskConfiguration {
  private static final Logger LOG = Logger.getLogger("");
  public static final boolean DEFAULT_BOOLEAN = false;
  public static final int DEFAULT_INTEGER = 0;
  public static final long DEFAULT_LONG = 0;
  public static final String DEFAULT_STRING = "";

  private static TaskConfiguration taskConf = null;

  /**
   * Get the configuration.
   * 
   * @param type the type of the benchmark task
   * @param fromFile if true, it will load the configuration file, otherwise it return an empty
   *        configuration.
   * @return the task configuration
   */
  public static synchronized TaskConfiguration get(String type, boolean fromFile) {
    if (taskConf == null) {
      if (fromFile) {
        try {
          taskConf =
              new TaskConfiguration(PerfConf.get().TACHYON_PERF_HOME + "/conf/testSuite/" + type
                  + ".xml");
        } catch (Exception e) {
          LOG.error("Error when parse conf/testSuite/" + type + ".xml", e);
          throw new RuntimeException("Failed to parse conf/testSuite/" + type + ".xml");
        }
      } else {
        taskConf = new TaskConfiguration();
      }
    }
    return taskConf;
  }

  private Map<String, String> mProperties;

  private TaskConfiguration() {
    mProperties = new HashMap<String, String>();
  }

  private TaskConfiguration(String xmlFileName) throws Exception {
    SAXParserFactory spf = SAXParserFactory.newInstance();
    SAXParser saxParser = spf.newSAXParser();
    File xmlFile = new File(xmlFileName);
    SAXConfiguration saxConfiguration = new SAXConfiguration();
    saxParser.parse(xmlFile, saxConfiguration);
    mProperties = saxConfiguration.getProperties();
  }

  public void addProperty(String name, String value) {
    mProperties.put(name, value);
  }

  public boolean getBooleanProperty(String property) {
    if (mProperties.containsKey(property)) {
      return Boolean.valueOf(mProperties.get(property));
    }
    return DEFAULT_BOOLEAN;
  }

  public int getIntProperty(String property) {
    if (mProperties.containsKey(property)) {
      return Integer.valueOf(mProperties.get(property));
    }
    return DEFAULT_INTEGER;
  }

  public long getLongProperty(String property) {
    if (mProperties.containsKey(property)) {
      return Long.valueOf(mProperties.get(property));
    }
    return DEFAULT_LONG;
  }

  public String getProperty(String property) {
    if (mProperties.containsKey(property)) {
      return mProperties.get(property);
    }
    return DEFAULT_STRING;
  }
}
