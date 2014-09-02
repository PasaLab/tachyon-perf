package tachyon.perf.basic;

import java.io.File;
import java.util.Map;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.log4j.Logger;

import tachyon.perf.conf.PerfConf;
import tachyon.perf.util.SAXTaskType;

/**
 * Manage the different type of tasks.
 */
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

  /**
   * Get the task class of specified task. The class should be a subclass of PerfTask.
   * 
   * @param type the type of the benchmark task
   * @return the task class
   * @throws Exception
   */
  public PerfTask getTaskClass(String type) throws Exception {
    String taskClassName = mTaskClasses.get(type);
    return (PerfTask) Class.forName(taskClassName).newInstance();
  }

  /**
   * Get the task context class of specified task. The class should be a subclass of TaskContext.
   * 
   * @param type the type of the benchmark task
   * @return the task context class
   * @throws Exception
   */
  public TaskContext getTaskContextClass(String type) throws Exception {
    String taskContextClassName = mTaskContextClasses.get(type);
    return (TaskContext) Class.forName(taskContextClassName).newInstance();
  }

  /**
   * Get the task total report of specified task. The class should be a subclass of PerfTotalReport.
   * Note that this is not necessary for a benchmark. It only used when you want
   * TachyonPerfCollector to support to generate a total report for the benchmark.
   * 
   * @param type the type of the benchmark task
   * @return the task context class
   * @throws Exception
   */
  public PerfTotalReport getTotalReportClass(String type) throws Exception {
    String totalReportClassName = mTotalReportClasses.get(type);
    return (PerfTotalReport) Class.forName(totalReportClassName).newInstance();
  }
}
