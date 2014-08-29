package tachyon.perf.util;

import java.io.File;
import java.util.Map;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.log4j.Logger;

import tachyon.perf.conf.PerfConf;

public class TaskType {
  private static final Logger LOG = Logger.getLogger("");

  private static TaskType taskType = null;

  public static synchronized TaskType get() {
    if (taskType == null) {
      try {
        taskType = new TaskType(PerfConf.get().TACHYON_PERF_HOME + "/conf/task-type.xml");
      } catch (Exception e) {
        LOG.error("Error when parse conf/task-type.xml", e);
        throw new RuntimeException("Failed to parse conf/task-type.xml");
      }
    }
    return taskType;
  }

  private Map<String, String> mTaskClasses;
  private Map<String, String> mTaskContextClasses;
  private Map<String, String> mTotalReportClasses;

  private TaskType(String xmlFileName) throws Exception {
    SAXParserFactory spf = SAXParserFactory.newInstance();
    SAXParser saxParser = spf.newSAXParser();
    File xmlFile = new File(xmlFileName);
    SAXTaskType saxTaskType = new SAXTaskType();
    saxParser.parse(xmlFile, saxTaskType);
    mTaskClasses = saxTaskType.getTaskClasses();
    mTaskContextClasses = saxTaskType.getTaskContextClasses();
    mTotalReportClasses = saxTaskType.getTotalReportClasses();
  }

  public String getTaskClass(String type) {
    return mTaskClasses.get(type);
  }

  public String getTaskContextClass(String type) {
    return mTaskContextClasses.get(type);
  }

  public String getTotalReportClass(String type) {
    return mTotalReportClasses.get(type);
  }
}
