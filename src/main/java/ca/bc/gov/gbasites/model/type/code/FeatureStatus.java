package ca.bc.gov.gbasites.model.type.code;

import org.jeometry.common.data.identifier.Code;

import com.revolsys.record.Record;

public enum FeatureStatus implements Code {
  ACTIVE("A", "Active"), //
  PLANNED("P", "Planned"), //
  RETIRED("R", "Retired"), //
  IGNORED("I", "Ignored");

  public static FeatureStatus getEnum(final String featureStatusCode) {
    if ("A".equalsIgnoreCase(featureStatusCode) || "Active".equalsIgnoreCase(featureStatusCode)) {
      return FeatureStatus.ACTIVE;
    } else if ("P".equalsIgnoreCase(featureStatusCode)
      || "Planned".equalsIgnoreCase(featureStatusCode)) {
      return PLANNED;
    } else if ("R".equalsIgnoreCase(featureStatusCode)
      || "Retired".equalsIgnoreCase(featureStatusCode)) {
      return RETIRED;
    } else if ("I".equalsIgnoreCase(featureStatusCode)
      || "Ignored".equalsIgnoreCase(featureStatusCode)) {
      return IGNORED;
    } else {
      return ACTIVE;
    }
  }

  public static FeatureStatus getFeatureStatus(final Record record) {
    final String code = record.getUpperString("FEATURE_STATUS_CODE");
    if (code == null) {
      return ACTIVE;
    } else {
      return FeatureStatus.getEnum(code);
    }
  }

  private String description;

  private String code;

  private FeatureStatus(final String code, final String description) {
    this.code = code;
    this.description = description;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <C> C getCode() {
    return (C)this.code;
  }

  @Override
  public String getDescription() {
    return this.description;
  }

  public boolean isActive() {
    return this == FeatureStatus.ACTIVE;
  }

  public boolean isIgnored() {
    return this == FeatureStatus.IGNORED;
  }

  public boolean isPlanned() {
    return this == FeatureStatus.PLANNED;
  }

  public boolean isRetired() {
    return this == FeatureStatus.RETIRED;
  }

  @Override
  public String toString() {
    return this.code;
  }
}
