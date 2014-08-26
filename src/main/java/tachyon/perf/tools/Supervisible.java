package tachyon.perf.tools;

public interface Supervisible {
  public String getTfsFailedPath();

  public String getTfsReadyPath();

  public String getTfsSuccessPath();
}
