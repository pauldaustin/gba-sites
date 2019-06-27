package ca.bc.gov.gbasites.load.common;

public class IgnoreSiteException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public static IgnoreSiteException error(final String countName) {
    return new IgnoreSiteException(countName, true);
  }

  public static IgnoreSiteException warning(final String countName) {
    return new IgnoreSiteException(countName, false);
  }

  private boolean error = false;

  private IgnoreSiteException(final String countName, final boolean error) {
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
