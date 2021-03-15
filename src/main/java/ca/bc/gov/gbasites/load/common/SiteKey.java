package ca.bc.gov.gbasites.load.common;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.jeometry.common.compare.CompareUtil;
import org.jeometry.common.data.identifier.Identifier;

import ca.bc.gov.gba.controller.GbaController;
import ca.bc.gov.gba.itn.model.NameDirection;
import ca.bc.gov.gbasites.model.type.SitePoint;

import com.revolsys.collection.map.Maps;
import com.revolsys.record.Record;
import com.revolsys.util.Property;
import com.revolsys.util.Strings;

public class SiteKey implements Comparable<SiteKey> {
  public static final Supplier<Map<SiteKey, List<Record>>> FACTORY_SITE_KEY_LIST_RECORD = Maps
    .factoryTree();

  public static SiteKey newSiteKey(final Record record) {
    final Identifier localityId = record.getIdentifier(SitePoint.LOCALITY_ID);
    final String unitDesriptor = record.getString(SitePoint.UNIT_DESCRIPTOR);
    final Integer civicNumber = record.getInteger(SitePoint.CIVIC_NUMBER);
    final String civicNumberSuffix = record.getString(SitePoint.CIVIC_NUMBER_SUFFIX);
    final Identifier structuredNameId = record.getIdentifier(SitePoint.STREET_NAME_ID);
    return new SiteKey(localityId, unitDesriptor, civicNumber, civicNumberSuffix, structuredNameId);
  }

  private final String unitDesriptor;

  private final Integer civicNumber;

  private final String civicNumberSuffix;

  private final String streetName;

  private String streetNameWithoutDirPrefix;

  private final String localityName;

  private NameDirection nameDirectionPrefix;

  private final Identifier structuredNameId;

  public SiteKey(final Identifier localityId, final String unitDesriptor, final Integer civicNumber,
    final String civicNumberSuffix, final Identifier structuredNameId) {
    if (!Property.hasValue(structuredNameId)) {
      throw new NullPointerException("structuredNameId cannot be null");
    }
    this.localityName = GbaController.getLocalities().getValue(localityId);
    this.unitDesriptor = unitDesriptor;
    this.civicNumber = civicNumber;
    this.civicNumberSuffix = civicNumberSuffix;
    this.structuredNameId = structuredNameId;
    this.streetName = GbaController.structuredNames.getValue(structuredNameId);
    final String firstPart = Strings.firstPart(this.streetName, ' ');
    try {
      this.nameDirectionPrefix = NameDirection.valueOf(firstPart);
      this.streetNameWithoutDirPrefix = this.streetName.substring(firstPart.length() + 1);
    } catch (final Throwable e) {
      this.streetNameWithoutDirPrefix = this.streetName;
    }
  }

  @Override
  public int compareTo(final SiteKey key2) {
    int compare = CompareUtil.compare(this.localityName, key2.localityName, true);
    if (compare == 0) {
      compare = CompareUtil.compare(this.streetNameWithoutDirPrefix,
        key2.streetNameWithoutDirPrefix, true);
      if (compare == 0) {
        compare = CompareUtil.compare(this.nameDirectionPrefix, key2.nameDirectionPrefix, true);
        if (compare == 0) {
          compare = CompareUtil.compare(this.civicNumber, key2.civicNumber, true);
          if (compare == 0) {
            compare = CompareUtil.compare(this.civicNumberSuffix, key2.civicNumberSuffix, true);
            if (compare == 0) {
              compare = CompareUtil.compare(this.unitDesriptor, key2.unitDesriptor, true);
            }
          }
        }
      }
    }
    return compare;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final SiteKey other = (SiteKey)obj;
    if (this.civicNumber != other.civicNumber) {
      return false;
    }
    if (this.civicNumberSuffix == null) {
      if (other.civicNumberSuffix != null) {
        return false;
      }
    } else if (!this.civicNumberSuffix.equals(other.civicNumberSuffix)) {
      return false;
    }
    if (this.localityName == null) {
      if (other.localityName != null) {
        return false;
      }
    } else if (!this.localityName.equals(other.localityName)) {
      return false;
    }
    if (this.streetName == null) {
      if (other.streetName != null) {
        return false;
      }
    } else if (!this.streetName.equals(other.streetName)) {
      return false;
    }
    if (this.unitDesriptor == null) {
      if (other.unitDesriptor != null) {
        return false;
      }
    } else if (!this.unitDesriptor.equals(other.unitDesriptor)) {
      return false;
    }
    return true;
  }

  public int getCivicNumber() {
    return this.civicNumber;
  }

  public String getCivicNumberSuffix() {
    return this.civicNumberSuffix;
  }

  public String getLocalityName() {
    return this.localityName;
  }

  public String getStreetName() {
    return this.streetName;
  }

  public Identifier getStructuredNameId() {
    return this.structuredNameId;
  }

  public String getUnitDesriptor() {
    return this.unitDesriptor;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + this.civicNumber;
    result = prime * result
      + (this.civicNumberSuffix == null ? 0 : this.civicNumberSuffix.hashCode());
    result = prime * result + (this.localityName == null ? 0 : this.localityName.hashCode());
    result = prime * result + (this.streetName == null ? 0 : this.streetName.hashCode());
    result = prime * result + (this.unitDesriptor == null ? 0 : this.unitDesriptor.hashCode());
    return result;
  }
}
