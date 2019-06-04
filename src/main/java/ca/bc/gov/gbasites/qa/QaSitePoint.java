package ca.bc.gov.gbasites.qa;

import java.util.function.Consumer;

import org.jeometry.common.data.identifier.Identifier;

import ca.bc.gov.gba.process.AbstractTaskByLocality;

public class QaSitePoint extends AbstractTaskByLocality {
  private static final long serialVersionUID = 1L;

  public static final String SITE_READ = "Site Read";

  public static final String SITE_VALIDATE = "Site Validate";

  public static final String TRANSPORT_LINE_READ = "Transport Line Read";

  public static void main(final String[] args) {
    start(QaSitePoint.class);
  }

  public QaSitePoint() {
    super(null, TRANSPORT_LINE_READ, "Transport Line Update", SITE_READ, SITE_VALIDATE,
      "Site Update", "Site Delete", ERROR, EXCLUDED);
  }

  @Override
  protected Consumer<Identifier> newLocalityHandler() {
    return new QaSitePointProcesss(this);
  }
}
