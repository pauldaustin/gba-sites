package ca.bc.gov.gbasites.load.common;

public class IgnoreSiteException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  private boolean error = false;

  public IgnoreSiteException(final String countName) {
    super(countName);
  }

  public IgnoreSiteException(final String countName, final boolean error) {
    super(countName);
    this.error = error;
  }

  public String getCountName() {
    return getMessage();
  }

  public boolean isError() {
    return this.error;
  }
}
