package tachyon.perf.task;

import java.io.IOException;

public enum TaskType {
  /**
   * A read test
   */
  Read(1),
  /**
   * A write test
   */
  Write(2);

  public static TaskType getTaskType(String taskType) throws IOException {
    if (taskType.equals("Read")) {
      return Read;
    } else if (taskType.equals("Write")) {
      return Write;
    }
    throw new IOException("Unknown TaskType : " + taskType);
  }

  private final int mValue;

  private TaskType(int value) {
    mValue = value;
  }

  public int getValue() {
    return mValue;
  }

  public boolean isRead() {
    return mValue == Read.mValue;
  }

  public boolean isWrite() {
    return mValue == Write.mValue;
  }
}
