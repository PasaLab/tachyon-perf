package tachyon.perf;

import java.io.IOException;

import tachyon.perf.basic.TaskContext;

/**
 * A simple task context class just used for unit tests.
 */
public class FooTaskContext extends TaskContext {
  private int mFoo;
  private boolean mReady;
  private boolean mWritten;

  public FooTaskContext(String nodeName) {
    mFoo = 0;
    mReady = false;
    mWritten = false;
  }

  public int getFoo() {
    return mFoo;
  }

  public boolean getReady() {
    return mReady;
  }

  public boolean getWritten() {
    return mWritten;
  }

  public void setFoo(int foo) {
    mFoo = foo;
  }

  public void setReady(boolean ready) {
    mReady = ready;
  }

  public void setWritten(boolean written) {
    mWritten = written;
  }

  @Override
  public void writeToFile(String fileName) throws IOException {
    mWritten = true;
  }
}
