package tachyon.perf.benchmark.read;

import java.io.IOException;

/**
 * Different read modes for a read test
 */
public enum ReadMode {
  /**
   * Read files randomly
   */
  RANDOM(1),
  /**
   * Read files sequentially
   */
  SEQUENCE(2);

  /**
   * Parse the read mode
   * 
   * @param readMode the string format of the read mode
   * @return the read mode
   * @throws IOException
   */
  public static ReadMode getReadMode(String readMode) throws IOException {
    if (readMode.equals("RANDOM")) {
      return RANDOM;
    } else if (readMode.equals("SEQUENCE")) {
      return SEQUENCE;
    }
    throw new IOException("Unknown ReadMode : " + readMode);
  }

  private int mValue;

  private ReadMode(int value) {
    mValue = value;
  }

  public int getValue() {
    return mValue;
  }

  public boolean isRandom() {
    return mValue == RANDOM.mValue;
  }

  public boolean isSequence() {
    return mValue == SEQUENCE.mValue;
  }
}
