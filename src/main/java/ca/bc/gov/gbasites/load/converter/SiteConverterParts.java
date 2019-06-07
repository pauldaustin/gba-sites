package ca.bc.gov.gbasites.load.converter;

import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import ca.bc.gov.gba.model.type.code.NameDirection;
import ca.bc.gov.gbasites.load.common.IgnoreSiteException;
import ca.bc.gov.gbasites.load.common.ProviderSitePointConverter;
import ca.bc.gov.gbasites.load.common.SitePointProviderRecord;
import ca.bc.gov.gbasites.model.type.SitePoint;

import com.revolsys.collection.CollectionUtil;
import com.revolsys.collection.map.MapEx;
import com.revolsys.collection.map.Maps;
import com.revolsys.geometry.model.Point;
import com.revolsys.predicate.Predicates;
import com.revolsys.record.Record;
import com.revolsys.record.query.QueryValue;
import com.revolsys.util.Debug;
import com.revolsys.util.Property;
import com.revolsys.util.Strings;

public class SiteConverterParts extends AbstractSiteConverter {
  private static final Map<String, String> WORD_TO_NUMBER = Maps.<String, String> buildHash()//
    .add("FIRST", "1ST") //
    .add("SECOND", "2ND") //
    .add("THIRD", "3RD") //
    .add("FOURTH", "4TH") //
    .add("FIFTH", "5TH") //
    .add("SIXTH", "6TH") //
    .add("SEVENTH", "7TH") //
    .add("EIGHTH", "8TH") //
    .add("NINTH", "9TH") //
    .add("TENTH", "10TH") //
    .add("ELEVENTH", "11TH") //
    .add("TWELFTH", "12TH") //
    .add("THIRTEENTH", "13TH") //
    .add("FOURTEENTH", "14TH") //
    .add("FIFTEENTH", "15TH") //
    .add("SIXTEENTH", "16TH") //
    .add("SEVENTEENTH", "17TH") //
    .add("EIGHTEENTH", "18TH") //
    .add("NINETEENTH", "19TH") //
    .add("TWENTIETH", "20TH") //
    .add("TWENTY-FIRST", "21ST") //
    .getMap();

  private String unitFieldName;

  private String unitTypeFieldName;

  private String civicNumberFieldName;

  private String civicNumberSuffixFieldName;

  private String streetDirPrefixFieldName;

  private String structuredNameFieldName;

  private String streetTypeFieldName;

  private String streetDirSuffixFieldName;

  private String postalCodeFieldName;

  private boolean addressIncludesUnit;

  private boolean useZeroForNull;

  private final SiteConverterAddress siteConverterAddress = new SiteConverterAddress();

  private String activeQuery;

  private Predicate<Record> activeTest = Predicates.all();

  public SiteConverterParts() {
  }

  public SiteConverterParts(final Map<String, ? extends Object> properties) {
    setProperties(properties);
  }

  @Override
  public SitePointProviderRecord convert(final Record sourceRecord, final Point point) {
    final String partnerOrganizationShortName = getPartnerOrganizationShortName();
    final String addressFieldName = getAddressFieldName();
    if (!this.activeTest.test(sourceRecord)) {
      throw new IgnoreSiteException("Ignore inactive record");
    }
    final SitePointProviderRecord sitePoint = newSitePoint(this, point);
    String sourceFullAddress = getUpperString(sourceRecord, addressFieldName);
    ProviderSitePointConverter.setFeatureStatusCodeByFullAddress(sitePoint, sourceFullAddress);
    if ("NO CIVIC".equalsIgnoreCase(sourceFullAddress)) {
      throw new IgnoreSiteException(IGNORE_STREET_NAME_NOT_SPECIFIED);
    }
    String sourceUnitDescriptor = getUpperString(sourceRecord, this.unitFieldName);
    final String sourceCivicNumber = getUpperString(sourceRecord, this.civicNumberFieldName);
    final String sourceCivicNumberSuffix = getUpperString(sourceRecord,
      this.civicNumberSuffixFieldName);

    if ("KSRD".equals(partnerOrganizationShortName) && Property.hasValue(sourceFullAddress)) {
      if (sourceFullAddress.startsWith("LANDMARK: ")) {
        sourceFullAddress = sourceFullAddress.substring(10);
      }
      final String landmarkName = getUpperString(sourceRecord, "LandmarkName");
      sourceFullAddress = Strings.replaceWord(sourceFullAddress, landmarkName, "",
        SitePointProviderRecord.WORD_SEPARATORS);

      final int unitLabelIndex = sourceFullAddress.lastIndexOf(" UNIT: ");
      if (unitLabelIndex != -1) {
        final int unitStartIndex = unitLabelIndex + 7;
        int unitEndIndex = sourceFullAddress.indexOf(' ', unitStartIndex);
        if (unitEndIndex == -1) {
          unitEndIndex = sourceFullAddress.length();
        }
        final String unitSuffix = sourceFullAddress.substring(unitStartIndex, unitEndIndex);
        sourceFullAddress = unitSuffix + "-" + sourceFullAddress.substring(0, unitLabelIndex);
        if (Property.isEmpty(sourceUnitDescriptor) || !unitSuffix.equals(sourceUnitDescriptor)) {
          addError(sourceRecord, "FULL_ADDRESS ends with ' UNIT: UNIT_DESCRIPTOR'");
          sourceUnitDescriptor = unitSuffix;
        }
      }

    }
    sitePoint.setFullAddress(sourceFullAddress);
    final String streetDirPrefix = getUpperString(sourceRecord, this.streetDirPrefixFieldName);
    String streetName = getUpperString(sourceRecord, this.structuredNameFieldName);
    if (streetName != null && "NRD".equals(partnerOrganizationShortName)
      && streetName.contains("(")) {
      streetName = streetName.replaceAll(" \\(.+", "");
      addWarningCount("STREET_NAME ends with (... SUFFIX");
    }
    String streetType = getUpperString(sourceRecord, this.streetTypeFieldName);
    if ("N/A".equals(streetType)) {
      streetType = null;
    } else if ("HWY".equals(streetType)) {
      if (streetName.startsWith("HWY ")) {
        streetType = null;
      } else if (streetName.startsWith("HIGHWAY ")) {
        streetType = null;
      }
    } else if ("BLVD".equals(streetType)) {
      if ("BOULEVARD".equals(streetName)) {
        streetType = null;
        sitePoint.replaceAllInFullAddress("BOULEVARD  BLVD", "BOULEVARD");
      }
    }
    if (Property.hasValuesAll(streetName, streetType)) {
      if (streetName.endsWith(" " + streetType)) {
        addWarningCount("STREET_NAME ends with STREET_TYPE");
        streetType = "";
      }
    }
    String streetDirSuffix = getUpperString(sourceRecord, this.streetDirSuffixFieldName);
    String originalStreetName = Strings.toString(" ", streetDirPrefix, streetName, streetType,
      streetDirSuffix);
    if (Property.isEmpty(originalStreetName) && sitePoint.hasValue(FULL_ADDRESS)) {
      return this.siteConverterAddress.convert(sourceRecord, point);
    }
    if (sitePoint.hasValue(FULL_ADDRESS) && this.streetTypeFieldName != null) {
      final String fullAddress = sitePoint.getFullAddress();
      if (fullAddress.endsWith(originalStreetName)) {
      } else {
        if (fullAddress.contains(originalStreetName + " ")) {
          if (Property.isEmpty(streetDirSuffix)
            && !"Coquitlam".equals(partnerOrganizationShortName)) {
            try {
              final Matcher matcher = Pattern
                .compile(".+ " + originalStreetName + " ([NEWS])( .+)?")
                .matcher(fullAddress);
              if (matcher.matches()) {
                streetDirSuffix = matcher.group(1);
                originalStreetName += " " + streetDirSuffix;
                addError(sourceRecord, "FULL_ADDRESS has extra STREET_DIR_SUFFIX");
              }
            } catch (final Throwable e) {
            }
          }
        } else {
          boolean matched = false;
          String streetTypeCode = sourceRecord.getCodeValue(this.streetTypeFieldName);
          if (streetTypeCode != null) {
            streetTypeCode = streetTypeCode.toUpperCase();
            final String streetNameWithTypeCode = Strings.toString(" ", streetDirPrefix, streetName,
              streetTypeCode, streetDirSuffix);
            if (fullAddress.contains(streetNameWithTypeCode)) {
              streetType = streetTypeCode;
              originalStreetName = streetNameWithTypeCode;
              matched = true;
            }
          }
          if (!matched) {
            if (streetName.endsWith(streetType) && fullAddress
              .equals(Strings.toString(" ", streetDirPrefix, streetName, streetDirSuffix))) {
              originalStreetName = Strings.toString(" ", streetDirPrefix, streetName,
                streetDirSuffix);
            } else {
              Debug.noOp();
            }
          }
        }
      }
    }

    String prefixNameDirectionCode = streetDirPrefix;
    if (Property.hasValue(prefixNameDirectionCode)) {
      final NameDirection nameDirection = NameDirection.getDirection(prefixNameDirectionCode);
      if (nameDirection != null) {
        prefixNameDirectionCode = nameDirection.toString();
      }
    }

    String nameSuffixCode = streetType;
    if (Property.hasValue(nameSuffixCode)) {
      final Set<String> aliases = getNameSuffixCodeByAlias(nameSuffixCode);
      if (aliases.size() == 1) {
        nameSuffixCode = CollectionUtil.get(aliases, 0);
      }
    }
    String suffixNameDirectionCode = streetDirSuffix;

    if (Property.hasValue(suffixNameDirectionCode)) {
      final NameDirection nameDirection = NameDirection.getDirection(suffixNameDirectionCode);
      if (nameDirection != null) {
        suffixNameDirectionCode = nameDirection.toString();
      }
    }
    String structuredName;
    if ("HWY".equals(streetType) && streetName.matches("\\d+[A-Z]?")) {
      structuredName = Strings.toString(" ", prefixNameDirectionCode, "HWY", streetName,
        suffixNameDirectionCode);
    } else {
      structuredName = Strings.toString(" ", prefixNameDirectionCode, streetName, nameSuffixCode,
        suffixNameDirectionCode);
    }

    sitePoint.addUnitDescriptor(sourceUnitDescriptor);
    final String unitType = getUpperString(sourceRecord, this.unitTypeFieldName);
    if (Property.hasValue(sourceCivicNumber)) {
      if (SitePointProviderRecord.IGNORE_CIVIC_NUMBERS.contains(sourceCivicNumber)) {
        addError(sourceRecord, "Ignore " + this.civicNumberFieldName + " is not valid");
        return null;
      } else {
        sitePoint.setCivicNumber(sourceCivicNumber, Property.hasValue(this.unitFieldName));
      }
    }
    if (Property.hasValue(sourceCivicNumberSuffix)) {
      sitePoint.setCivicNumberSuffix(sourceCivicNumberSuffix);
    }

    String compareStreetName = originalStreetName;
    if (sitePoint.hasValue(FULL_ADDRESS)) {
      {
        final String streetNameNumber = WORD_TO_NUMBER.get(streetName);
        if (Property.hasValue(streetNameNumber)) {
          if (!sitePoint.getFullAddress().contains(streetName)
            && sitePoint.getFullAddress().contains(streetNameNumber)) {
            compareStreetName = Strings.toString(" ", streetDirPrefix, streetNameNumber, streetType,
              streetDirSuffix);
            structuredName = Strings.toString(" ", prefixNameDirectionCode, streetNameNumber,
              nameSuffixCode, suffixNameDirectionCode);
            if (!"BURNABY".equalsIgnoreCase(partnerOrganizationShortName)) {
              // Burnaby has a separate field for the number version of the
              // name
              addError(sourceRecord,
                "STREET_NAME contains spelled out number and FULL_ADDRESS contains numeric");
            }
          }
        }
      }
      {
        if (!compareStreetName.equalsIgnoreCase(structuredName)) {
          if (sitePoint.getFullAddress().endsWith(structuredName)) {
            compareStreetName = structuredName;
          }
        }
      }
    }

    if (sitePoint.validateFullAddress(originalStreetName, compareStreetName, structuredName,
      prefixNameDirectionCode, streetName, streetType, suffixNameDirectionCode, unitType,
      this.civicNumberFieldName, this.streetTypeFieldName,
      this.addressIncludesUnit) == Boolean.FALSE) {
      throw new IgnoreSiteException(IGNORED_FULL_ADDRESS_NOT_EQUAL_PARTS, true);
    }

    boolean unitDescriptorWithNoCivicNumber = false;
    if (Property.isEmpty(structuredName)) {
      addWarningCount(IGNORE_STREET_NAME_NOT_SPECIFIED);
      return null;
    } else {
      final Integer civicNumber = sitePoint.getCivicNumber();
      if ((civicNumber == null || this.useZeroForNull && civicNumber == 0)
        && !sitePoint.hasValue(CIVIC_NUMBER_RANGE)) {
        if (sitePoint.hasValue(UNIT_DESCRIPTOR)) {
          unitDescriptorWithNoCivicNumber = true;
        } else {
          throw new IgnoreSiteException("Ignore no valid CIVIC_NUMBER");
        }
      }
    }
    if (!setStructuredName(sourceRecord, sitePoint, 0, structuredName, originalStreetName)) {
      return null;
    }
    if (unitDescriptorWithNoCivicNumber) {
      throw new IgnoreSiteException("Ignore has UNIT_DESCRIPTOR but no valid CIVIC_NUMBER", true);
    }
    if (sitePoint.equalValue(CIVIC_NUMBER, 0)) {
      if (sitePoint.equalValue(FULL_ADDRESS, originalStreetName)) {
        throw new IgnoreSiteException("Ignore CIVIC_NUMBER was 0");
      } else {
        addError(sourceRecord, "CIVIC_NUMBER was 0");
      }
    }

    SitePoint.updateFullAddress(sitePoint);
    final String postalCode = sourceRecord.getString(this.postalCodeFieldName);
    sitePoint.setPostalCode(postalCode);
    setCustodianSiteId(sitePoint, sourceRecord);
    return sitePoint;
  }

  public String getActiveQuery() {
    return this.activeQuery;
  }

  public String getCivicNumberFieldName() {
    return this.civicNumberFieldName;
  }

  public String getCivicNumberSuffixFieldName() {
    return this.civicNumberSuffixFieldName;
  }

  public String getPostalCodeFieldName() {
    return this.postalCodeFieldName;
  }

  public String getStreetDirPrefixFieldName() {
    return this.streetDirPrefixFieldName;
  }

  public String getStreetDirSuffixFieldName() {
    return this.streetDirSuffixFieldName;
  }

  public String getStreetTypeFieldName() {
    return this.streetTypeFieldName;
  }

  public String getStructuredNameFieldName() {
    return this.structuredNameFieldName;
  }

  public String getUnitFieldName() {
    return this.unitFieldName;
  }

  public String getUnitTypeFieldName() {
    return this.unitTypeFieldName;
  }

  public boolean isAddressIncludesUnit() {
    return this.addressIncludesUnit;
  }

  public boolean isUseZeroForNull() {
    return this.useZeroForNull;
  }

  public void setActiveQuery(final String activeQuery) {
    this.activeQuery = activeQuery;
    if (Property.hasValue(activeQuery)) {
      this.activeTest = QueryValue.parseWhere(null, activeQuery);
    } else {
      this.activeTest = Predicates.all();
    }
  }

  public void setAddressIncludesUnit(final boolean addressIncludesUnit) {
    this.addressIncludesUnit = addressIncludesUnit;
  }

  public void setCivicNumberFieldName(final String civicNumberFieldName) {
    this.civicNumberFieldName = civicNumberFieldName;
  }

  public void setCivicNumberSuffixFieldName(final String civicNumberSuffixFieldName) {
    this.civicNumberSuffixFieldName = civicNumberSuffixFieldName;
  }

  public void setPostalCodeFieldName(final String postalCodeFieldName) {
    this.postalCodeFieldName = postalCodeFieldName;
  }

  @Override
  public void setProperties(final Map<String, ? extends Object> properties) {
    super.setProperties(properties);
    this.siteConverterAddress.setProperties(properties);
  }

  public void setStreetDirPrefixFieldName(final String streetDirPrefixFieldName) {
    this.streetDirPrefixFieldName = streetDirPrefixFieldName;
  }

  public void setStreetDirSuffixFieldName(final String streetDirSuffixFieldName) {
    this.streetDirSuffixFieldName = streetDirSuffixFieldName;
  }

  public void setStreetTypeFieldName(final String streetTypeFieldName) {
    this.streetTypeFieldName = streetTypeFieldName;
  }

  public void setStructuredNameFieldName(final String structuredNameFieldName) {
    this.structuredNameFieldName = structuredNameFieldName;
  }

  public void setUnitFieldName(final String unitFieldName) {
    this.unitFieldName = unitFieldName;
  }

  public void setUnitTypeFieldName(final String unitTypeFieldName) {
    this.unitTypeFieldName = unitTypeFieldName;
  }

  public void setUseZeroForNull(final boolean useZeroForNull) {
    this.useZeroForNull = useZeroForNull;
  }

  @Override
  public MapEx toMap() {
    final MapEx map = super.toMap();
    addToMap(map, "unitFieldName", this.unitFieldName);
    addToMap(map, "unitTypeFieldName", this.unitTypeFieldName);
    addToMap(map, "civicNumberFieldName", this.civicNumberFieldName);
    addToMap(map, "civicNumberSuffixFieldName", this.civicNumberSuffixFieldName);
    addToMap(map, "streetDirPrefixFieldName", this.streetDirPrefixFieldName);
    addToMap(map, "structuredNameFieldName", this.structuredNameFieldName);
    addToMap(map, "streetTypeFieldName", this.streetTypeFieldName);
    addToMap(map, "streetDirSuffixFieldName", this.streetDirSuffixFieldName);
    addToMap(map, "addressIncludesUnit", this.addressIncludesUnit);
    addToMap(map, "useZeroForNull", this.useZeroForNull);
    addToMap(map, "activeQuery", this.activeQuery);
    return map;
  }
}
