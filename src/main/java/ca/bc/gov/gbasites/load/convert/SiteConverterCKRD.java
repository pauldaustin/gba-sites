package ca.bc.gov.gbasites.load.convert;

import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jeometry.common.number.BigDecimals;

import ca.bc.gov.gbasites.load.common.IgnoreSiteException;
import ca.bc.gov.gbasites.load.common.SitePointProviderRecord;
import ca.bc.gov.gbasites.model.type.SitePoint;

import com.revolsys.collection.map.MapEx;
import com.revolsys.geometry.model.Point;
import com.revolsys.record.Record;
import com.revolsys.util.Debug;
import com.revolsys.util.Property;
import com.revolsys.util.Strings;

public class SiteConverterCKRD extends AbstractSiteConverter {
  private static final Pattern CIVIC_NUMBER_SUFFIX_PATTERN = Pattern
    .compile("(\\d+)(?:-)?([A-Z]|\\d+[A-Z]?| UT)");

  private static final Pattern UNIT_CIVIC_NUMBER_PATTERN = Pattern.compile("(\\w+|1/2) ?- ?(\\d+)");

  public static Function<MapEx, SiteConverterCKRD> newFactory(
    final Map<String, ? extends Object> config) {
    return properties -> new SiteConverterCKRD(properties.addAll(config));
  }

  private String civicNumberFieldName;

  private String streetNameFieldName;

  private String unitFieldName;

  public SiteConverterCKRD() {
  }

  public SiteConverterCKRD(final Map<String, ? extends Object> properties) {
    setProperties(properties);
  }

  @Override
  public SitePointProviderRecord convertRecordSite(final Record sourceRecord, final Point point) {
    final String addressFieldName = getAddressFieldName();
    final String custodianFullAddress = getUpperString(sourceRecord, addressFieldName);
    String fullAddress = custodianFullAddress;
    final String streetName = getUpperString(sourceRecord, this.streetNameFieldName);
    final String originalStreetName = streetName;

    String structuredName = streetName;

    String unitDescriptor = getUpperString(sourceRecord, this.unitFieldName);
    String streetNumber = getUpperString(sourceRecord, this.civicNumberFieldName);
    String civicNumberSuffix = "";
    Integer civicNumber = null;
    if (!Property.hasValue(streetNumber)) {
      if (Property.hasValue(unitDescriptor)) {
        if (unitDescriptor.matches("\\d+[A-Z]?")) {
          streetNumber = unitDescriptor;
          unitDescriptor = null;
          addError(sourceRecord, "UNIT_DESCRIPTOR is CIVIC_NUMBER");
        }
      }
    }
    if (Property.hasValue(streetNumber)) {
      try {
        civicNumber = Integer.valueOf(streetNumber);
      } catch (final RuntimeException e) {
        if (streetNumber.matches("\\d*\\.0*")) {
          civicNumber = Integer.valueOf(streetNumber.substring(0, streetNumber.indexOf('.')));
        } else {
          final Matcher unitCivicNumberMatcher = UNIT_CIVIC_NUMBER_PATTERN.matcher(streetNumber);
          if (unitCivicNumberMatcher.matches()) {
            if (unitDescriptor == null) {
              unitDescriptor = unitCivicNumberMatcher.group(1);
              civicNumber = Integer.valueOf(unitCivicNumberMatcher.group(2));
            } else {
              addError(sourceRecord, "Ignore " + this.civicNumberFieldName + " contains - and "
                + this.unitFieldName + " has a value");
              return null;
            }
          } else {
            final Matcher civicNumberSuffixMatcher = CIVIC_NUMBER_SUFFIX_PATTERN
              .matcher(streetNumber);
            if (civicNumberSuffixMatcher.matches()) {
              civicNumber = Integer.valueOf(civicNumberSuffixMatcher.group(1));
              final String suffixPart = civicNumberSuffixMatcher.group(2);
              if (BigDecimals.isNumber(suffixPart) || suffixPart.length() > 1) {
                unitDescriptor = suffixPart;
              } else {
                civicNumberSuffix = suffixPart;
              }
            } else {
              throw e;
            }
          }
        }
      }
      if (Property.hasValue(fullAddress)) {
        fullAddress = fullAddress.toUpperCase();
        if (Property.hasValue(unitDescriptor)) {
          if (fullAddress.startsWith(civicNumber + unitDescriptor)) {
            civicNumberSuffix = unitDescriptor;
            unitDescriptor = null;
          }
        }
        boolean matched = false;

        if (fullAddress.equalsIgnoreCase(Strings.toString(" ", unitDescriptor,
          civicNumber + civicNumberSuffix, originalStreetName))) {
          matched = true;
        } else if (fullAddress.equalsIgnoreCase(Strings.toString(" ",
          unitDescriptor + "-" + civicNumber + civicNumberSuffix, originalStreetName))) {
          matched = true;
        } else if (fullAddress.equalsIgnoreCase(Strings.toString(" ", unitDescriptor, "-",
          civicNumber + civicNumberSuffix, originalStreetName))) {
          matched = true;
        } else {
          if (fullAddress.contains("BROAD") || fullAddress.contains("RAILROAD")) {
            fullAddress = fullAddress.replace(" ROAD", " RD");
          } else {
            fullAddress = fullAddress.replace("ROAD", "RD");
          }
          if (!fullAddress.contains("CRESCENTVIEW")) {
            fullAddress = fullAddress.replaceAll("CRESCENT", "CRES");
          }
          fullAddress = fullAddress.replace("STREET", "ST");
          fullAddress = fullAddress.replace("AVENUE", "AVE");
          fullAddress = fullAddress.replace("DRIVE", "DR");
          fullAddress = fullAddress.replace("DRIVE", "DR");
          fullAddress = fullAddress.replace("LANE", "LN");
          fullAddress = fullAddress.replace("CRESCENT", "CRES");

          if (Property.hasValue(unitDescriptor)) {
            if (fullAddress.equalsIgnoreCase(Strings.toString(" ", unitDescriptor, "-",
              civicNumber + civicNumberSuffix, originalStreetName))) {
              matched = true;
            } else if (!matched && fullAddress.startsWith(civicNumber + civicNumberSuffix + " ")) {
              final String nameParts = fullAddress
                .substring((civicNumber + civicNumberSuffix + " ").length());
              if (nameParts.equals(originalStreetName)) {
                structuredName = nameParts;
                addWarning(sourceRecord, "FULL_ADDRESS missing UNIT");
                matched = true;
              } else if (nameParts.startsWith(originalStreetName + " ")) {
                structuredName = nameParts;
                addWarning(sourceRecord,
                  "FULL_ADDRESS missing UNIT, STREET_NAME missing STREET_TYPE");
                matched = true;
              } else {
                final String streetType1 = Strings.lastPart(nameParts, ' ');
                final String streetType2 = Strings.lastPart(streetName, ' ');
                final String streetName1 = Strings.firstPart(nameParts, ' ');
                final String streetName2 = Strings.firstPart(streetName, ' ');
                if (streetName1.equals(streetName2)) {
                  for (final String streetTypeAlias1 : getNameSuffixCodeByAlias(streetType1)) {
                    if (streetType2.equalsIgnoreCase(streetTypeAlias1)) {
                      addWarning(sourceRecord,
                        "FULL_ADDRESS and STREET_NAME have STREET_TYPE alias");
                      matched = true;
                    }
                  }
                }
              }
            }
            if (!matched) {
              Debug.noOp();
            }
          } else if (originalStreetName != null) {
            if (fullAddress
              .equalsIgnoreCase(Strings.toString(" ", civicNumber, originalStreetName))) {
              matched = true;
            } else if (fullAddress.equalsIgnoreCase(
              Strings.toString(" ", civicNumber + civicNumberSuffix, originalStreetName))) {
              matched = true;
            } else if (fullAddress.equalsIgnoreCase(
              Strings.toString(" ", civicNumber + "-" + civicNumberSuffix, originalStreetName))) {
              matched = true;
            } else if (fullAddress.equalsIgnoreCase(originalStreetName)) {
              matched = true;
              if (Property.hasValuesAny(unitDescriptor, civicNumber)) {
                addError(sourceRecord, "FULL_ADDRESS missing CIVIC_NUMBER");
              }
            }
            if (!matched && fullAddress.startsWith(civicNumber + civicNumberSuffix + " ")) {
              final String nameParts = fullAddress
                .substring((civicNumber + civicNumberSuffix + " ").length());
              if (nameParts.startsWith(originalStreetName + " ")) {
                structuredName = nameParts;
                addWarning(sourceRecord, "STREET_NAME missing STREET_TYPE");
                matched = true;
              } else {
                final String streetType1 = Strings.lastPart(nameParts, ' ');
                final String streetType2 = Strings.lastPart(streetName, ' ');
                final String streetName1 = Strings.firstPart(nameParts, ' ');
                final String streetName2 = Strings.firstPart(streetName, ' ');
                if (streetName1.equals(streetName2)) {
                  for (final String streetTypeAlias1 : getNameSuffixCodeByAlias(streetType1)) {
                    if (streetType2.equalsIgnoreCase(streetTypeAlias1)) {
                      addWarning(sourceRecord,
                        "FULL_ADDRESS and STREET_NAME have STREET_TYPE alias");
                      matched = true;
                    }
                  }
                }
              }

              matched = fixTypeAlias(sourceRecord, fullAddress, originalStreetName, civicNumber,
                matched, "AVENUE", "AVE");
              matched = fixTypeAlias(sourceRecord, fullAddress, originalStreetName, civicNumber,
                matched, "STREET", "ST");
              matched = fixTypeAlias(sourceRecord, fullAddress, originalStreetName, civicNumber,
                matched, "ROAD", "RD");
              matched = fixTypeAlias(sourceRecord, fullAddress, originalStreetName, civicNumber,
                matched, "DRIVE", "DR");
            }
          }
        }
        if (!matched) {
          throw IgnoreSiteException.error(IGNORED_FULL_ADDRESS_NOT_EQUAL_PARTS);
        }
      }
    }
    if (Property.isEmpty(structuredName)) {
      addWarning(sourceRecord, "Ignore " + this.streetNameFieldName + " not specified");
      return null;
    }
    if (Property.hasValue(streetNumber)) {
      final SitePointProviderRecord sitePoint = newSitePoint(this, point);
      sitePoint.setValue(CUSTODIAN_FULL_ADDRESS, custodianFullAddress);
      sitePoint.setValue(UNIT_DESCRIPTOR, unitDescriptor);
      sitePoint.setValue(CIVIC_NUMBER, civicNumber);
      sitePoint.setValue(CIVIC_NUMBER_SUFFIX, civicNumberSuffix);

      if (!setStructuredName(sourceRecord, sitePoint, 0, structuredName, originalStreetName)) {
        throw IgnoreSiteException.warning("STRUCTURED_NAME ignored");
      }
      SitePoint.updateFullAddress(sitePoint);
      setCustodianSiteId(sitePoint, sourceRecord);
      return sitePoint;
    } else {
      addError(sourceRecord, "Ignore CIVIC_NUMBER not specified");
      return null;
    }
  }

  private boolean fixTypeAlias(final Record sourceRecord, final String fullAddress,
    final String originalStreetName, final Integer civicNumber, boolean matched,
    final String fullType, final String shortType) {
    if (!matched) {
      final Matcher matcher = Pattern.compile(fullType).matcher(fullAddress);
      if (matcher.find()) {
        final String newFullAddress = fullAddress.replaceFirst(fullType, shortType);
        if (newFullAddress
          .equalsIgnoreCase(Strings.toString(" ", civicNumber, originalStreetName))) {
          matched = true;
          addError(sourceRecord, "FULL_ADDRESS has invalid replacement " + fullType + " in STREET");
          return true;
        }
      }
    }
    return matched;
  }

  public String getCivicNumberFieldName() {
    return this.civicNumberFieldName;
  }

  public String getStreetNameFieldName() {
    return this.streetNameFieldName;
  }

  public String getUnitFieldName() {
    return this.unitFieldName;
  }

  public void setCivicNumberFieldName(final String civicNumberFieldName) {
    this.civicNumberFieldName = civicNumberFieldName;
  }

  public void setStreetNameFieldName(final String streetNameFieldName) {
    this.streetNameFieldName = streetNameFieldName;
  }

  public void setUnitFieldName(final String unitFieldName) {
    this.unitFieldName = unitFieldName;
  }

  @Override
  public MapEx toMap() {
    final MapEx map = super.toMap();
    addToMap(map, "unitFieldName", this.unitFieldName);
    addToMap(map, "civicNumberFieldName", this.civicNumberFieldName);
    addToMap(map, "streetNameFieldName", this.streetNameFieldName);

    return map;
  }
}
