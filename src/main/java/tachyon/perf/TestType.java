package tachyon.perf;

import java.io.IOException;

/**
 * Different test type for Tachyon-Perf
 */
public enum TestType {
  /**
   * A read test
   */
  READ(1),
  /**
   * A write test
   */
  WRITE(2);

  public static TestType getTestType(String testType) throws IOException {
    if (testType.equals("Read")) {
      return READ;
    } else if (testType.equals("Write")) {
      return WRITE;
    }
    throw new IOException("Unknown TestType : " + testType);
  }

  private final int mValue;

  private TestType(int value) {
    mValue = value;
  }

  public int getValue() {
    return mValue;
  }

  public boolean isRead() {
    return mValue == READ.mValue;
  }

  public boolean isWrite() {
    return mValue == WRITE.mValue;
  }

  public String toString() {
    if (mValue == READ.mValue) {
      return "Read";
    } else if (mValue == WRITE.mValue) {
      return "Write";
    } else {
      return "Unknown TestType";
    }
  }
}
