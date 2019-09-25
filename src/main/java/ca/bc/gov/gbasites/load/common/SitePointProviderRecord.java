package ca.bc.gov.gbasites.load.common;

import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jeometry.common.data.identifier.Identifier;
import org.jeometry.common.logging.Logs;
import org.jeometry.common.number.BigDecimals;
import org.jeometry.common.number.Numbers;

import ca.bc.gov.gba.controller.GbaController;
import ca.bc.gov.gba.model.type.code.PartnerOrganization;
import ca.bc.gov.gba.model.type.code.StructuredNames;
import ca.bc.gov.gbasites.controller.GbaSiteDatabase;
import ca.bc.gov.gbasites.load.convert.AbstractSiteConverter;
import ca.bc.gov.gbasites.model.type.SitePoint;
import ca.bc.gov.gbasites.model.type.SiteTables;

import com.revolsys.collection.CollectionUtil;
import com.revolsys.collection.range.RangeSet;
import com.revolsys.collection.set.Sets;
import com.revolsys.geometry.model.Point;
import com.revolsys.jdbc.io.JdbcRecordStore;
import com.revolsys.record.ArrayRecord;
import com.revolsys.record.DelegatingRecord;
import com.revolsys.record.Record;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.util.Debug;
import com.revolsys.util.Property;
import com.revolsys.util.Strings;

public class SitePointProviderRecord extends DelegatingRecord implements SitePoint {
  private static final StructuredNames STRUCTURED_NAMES = GbaController.getStructuredNames();

  private static final String CIVIC_NUMBER_INCLUDES_UNIT_DESCRIPTOR_SUFFIX = "CIVIC_NUMBER includes UNIT_DESCRIPTOR suffix";

  private static final String CIVIC_NUMBER_INCLUDES_UNIT_DESCRIPTOR_PREFIX = "CIVIC_NUMBER includes UNIT_DESCRIPTOR prefix";

  public static final Set<String> IGNORE_CIVIC_NUMBERS = Sets.newHash("(N OF)", "(N END OF)",
    "(E OF)", "(S OF)", "(W OF)", "(OFF OF)", "(END OF)", "(E END)", "(W OF )", "( EOF)", "BC");

  public static final Set<String> IGNORE_UNIT_DESCRIPTORS = Sets.newHash("OFF", "N OF", "N END OF",
    "E OF", "S OF", "W OF", "OFF OF", "END OF", "E END", "W OF", "EOF", "BC", "COMPLEX", "COMPLX",
    "CMPLEX", "CMPLX", "BLOCK", "PARK", "PRK", "FOOT", "BLOCK", "PENDRAY", "JOHNSON", "SONGHEES",
    "WEST BAY", "GARBALLY", "PT ELLICE", "SEAFORTH", "MARY ST", "LIME BAY", "LEASE", "SRW", "TO",
    "LANE", "BCH&P", "S.D.#71 MAINTENANCE", "COMOX VALLEY SPRTS CENTRE", "G.P.VANIER SCHOOL",
    "GOLF COURSE", "(PARK)", "COMRD", "GREENSPACE", "RC GARNETT", "FUTURE RM", "GREEN", "GREENWAY",
    "TBA", "CORNER");

  private static final Pattern NUMBER_RANGE_PATTERN = Pattern
    .compile("(\\d+)\\s?([,-/])\\s?(\\d+)");

  private static final Pattern NUMBER_AND_SUFFIX_PATTERN = Pattern
    .compile("(\\d+)\\s*[,]\\s*(\\d+)[-]?([A-Z])");

  private static final Pattern CIVIC_NUMBER_UNIT_PATTERN = Pattern
    .compile("(\\d+)(?: ?-|/| #|-#)?(\\d+)");

  private static final Pattern CIVIC_NUMBER_UNIT_RANGE_CIVIC_NUMBER_UNIT_RANGE_PATTERN = Pattern
    .compile("(\\d+)([A-Z])\\s*-\\s*(\\d+)([A-Z])");

  private static final Pattern UNIT_CIVIC_NUMBER_PATTERN = Pattern.compile("(\\w+|1/2) ?- ?(\\d+)");

  private static final Pattern CIVIC_NUMBER_SUFFIX_PATTERN = Pattern
    .compile("(\\d+)(?:[- ])?([A-Z]|\\d+[A-Z]?| UT|\\(?[A-Z].+)");

  public static final char[] WORD_SEPARATORS = new char[] {
    ' ', '-'
  };

  public static SitePointProviderRecord newSitePoint(final SiteKey siteKey, final Point point) {
    final JdbcRecordStore recordStore = GbaSiteDatabase.getRecordStore();
    final Record sitePoint = recordStore.newRecord(SiteTables.SITE_POINT);

    final String unitDesriptor = siteKey.getUnitDesriptor();
    final int civicNumber = siteKey.getCivicNumber();
    final String civicNumberSuffix = siteKey.getCivicNumberSuffix();
    final Identifier structuredNameId = siteKey.getStructuredNameId();

    sitePoint.setValue(UNIT_DESCRIPTOR, unitDesriptor);
    sitePoint.setValue(CIVIC_NUMBER_SUFFIX, civicNumberSuffix);
    sitePoint.setValue(CIVIC_NUMBER, civicNumber);
    sitePoint.setValue(STREET_NAME_ID, structuredNameId);
    sitePoint.setGeometryValue(point);

    final SitePointProviderRecord sitePointProviderRecord = new SitePointProviderRecord(sitePoint);
    return sitePointProviderRecord;
  }

  private AbstractSiteConverter siteConverter;

  public SitePointProviderRecord(final AbstractSiteConverter siteConverter,
    final RecordDefinition recordDefinition) {
    super(new ArrayRecord(recordDefinition));
    this.siteConverter = siteConverter;
  }

  public SitePointProviderRecord(final Record record) {
    super(record);
  }

  public void addError(final String message) {
    this.siteConverter.addError(this, message);
  }

  public void addUnitDescriptor(final Object unitDescriptor) {
    if (Property.hasValue(unitDescriptor)) {
      final String originalUnitDescriptor = getUnitDescriptor();
      String newUnitDescriptor = unitDescriptor.toString();
      int startIndex = 0;
      if (newUnitDescriptor.charAt(startIndex) == '(') {
        startIndex = 1;
      }
      int i = startIndex;
      int endIndex = newUnitDescriptor.length();
      if (newUnitDescriptor.charAt(endIndex - 1) == ')') {
        endIndex--;
      }
      while (i < endIndex && newUnitDescriptor.charAt(i) == '0') {
        i++;
      }
      if (i < newUnitDescriptor.length()) {
        if (i > startIndex) {
          addWarningCount("UNIT_DESCRIPTOR started with a 0");
          newUnitDescriptor = newUnitDescriptor.substring(i, endIndex).trim();
          replaceWordInFullAddress(unitDescriptor.toString(), newUnitDescriptor);
        } else if (startIndex > 0 && endIndex != newUnitDescriptor.length()) {
          newUnitDescriptor = newUnitDescriptor.substring(startIndex, endIndex).trim();
          replaceWordInFullAddress(unitDescriptor.toString(), newUnitDescriptor);
        }
        newUnitDescriptor = newUnitDescriptor //
          .replaceAll("-", "~") //
          .replaceAll("&", ",") //
        ;

        if (IGNORE_UNIT_DESCRIPTORS.contains(newUnitDescriptor)) {
          replaceAllInFullAddress("\\s*-\\s*", "-");
          if (newUnitDescriptor.contains(" ")) {
            String fullAddress = getFullAddress();
            fullAddress = fullAddress.replace(newUnitDescriptor, "");
            if (fullAddress.length() > 0 && fullAddress.charAt(0) == '-') {
              fullAddress = fullAddress.substring(1);
            }
            setFullAddress(fullAddress);
          } else {
            replaceWordInFullAddress(unitDescriptor.toString(), "");
          }
        } else {
          final RangeSet range = RangeSet.newRangeSet(newUnitDescriptor);
          if (Property.hasValue(originalUnitDescriptor)) {
            final RangeSet range2 = RangeSet.newRangeSet(originalUnitDescriptor);
            range.addRanges(range2);
          }
          setValue(UNIT_DESCRIPTOR, range.toString());
        }
      } else {
        replaceWordInFullAddress(unitDescriptor.toString(), "");
      }
    }
  }

  public void addWarningCount(final String message) {
    this.siteConverter.addWarning(message);
  }

  public void addWarningOrError(final String message, final boolean error) {
    if (error) {
      addError(message);
    } else {
      addWarningCount(message);
    }
  }

  public boolean clearCivicNumber() {
    return setValue(CIVIC_NUMBER, null);
  }

  public boolean clearCivicNumberSuffix() {
    return setValue(CIVIC_NUMBER_SUFFIX, null);
  }

  public boolean clearUnitDescriptor() {
    return setValue(UNIT_DESCRIPTOR, null);
  }

  @Override
  public SitePointProviderRecord clone() {
    final SitePointProviderRecord clone = new SitePointProviderRecord(this.siteConverter,
      getRecordDefinition());
    clone.setValues(this);
    return clone;
  }

  private boolean fixCivicNumber(String streetNumber, final boolean hasUnitField,
    final boolean civicNumberIncludesUnitPrefix) {
    if (streetNumber.matches("\\d*\\.0*")) {
      final Integer civicNumber = Integer
        .valueOf(streetNumber.substring(0, streetNumber.indexOf('.')));
      return setCivicNumber(civicNumber);
    } else if (streetNumber.matches("\\(\\d+\\)")) {
      final Integer civicNumber = Integer
        .valueOf(streetNumber.substring(1, streetNumber.length() - 1));
      return setCivicNumber(civicNumber);
    } else {
      if (streetNumber.matches("\\(.+\\)")) {
        streetNumber = streetNumber.substring(1, streetNumber.length() - 1);
      }
      final Matcher numberRangeMatcher = NUMBER_RANGE_PATTERN.matcher(streetNumber);
      if (numberRangeMatcher.matches()) {
        final int number1 = Integer.parseInt(numberRangeMatcher.group(1));
        String separator = numberRangeMatcher.group(2);
        final int number2 = Integer.parseInt(numberRangeMatcher.group(3));
        if (civicNumberIncludesUnitPrefix) {
          addUnitDescriptor(number1);
          return setCivicNumber(number2);
        } else {
          final int diff = number1 - number2;
          if (diff < -100 || number1 < 100 && number2 > 100) {
            addWarningOrError(CIVIC_NUMBER_INCLUDES_UNIT_DESCRIPTOR_PREFIX, hasUnitField);
            addUnitDescriptor(Integer.toString(number1));
            return setCivicNumber(number2);
          } else if (diff > 100 || number2 < 100 && number1 > 100) {
            addWarningOrError(CIVIC_NUMBER_INCLUDES_UNIT_DESCRIPTOR_SUFFIX, hasUnitField);
            addUnitDescriptor(Integer.toString(number2));
            return setCivicNumber(number1);
          } else {
            if ("CVRD".equals(this.siteConverter.getPartnerOrganizationShortName())) {
              addWarningCount(CIVIC_NUMBER_INCLUDES_UNIT_DESCRIPTOR_SUFFIX);
              addUnitDescriptor(number2);
              return setCivicNumber(number1);
            } else {
              String type;
              if ("/".equals(separator) || ",".equals(separator)) {
                separator = ",";
                type = "list";
              } else {
                separator = "~";
                type = "range";
              }
              if (number1 < number2) {
                addWarningCount("CIVIC_NUMBER is a " + type);
                final String civicNumberRange = number1 + separator + number2;
                replaceWordInFullAddress(streetNumber, civicNumberRange);
                return setCivicNumberRange(civicNumberRange);
              } else if (number1 > number2) {
                addWarningCount("CIVIC_NUMBER is a " + type);
                final String civicNumberRange = number2 + separator + number1;
                replaceWordInFullAddress(streetNumber, civicNumberRange);
                return setCivicNumberRange(civicNumberRange);
              } else {
                addError("CIVIC_NUMBER has duplicate value in " + type);
                return setCivicNumber(number1);
              }
            }
          }
        }
      } else {
        if (fixCivicNumberListWithSuffix(streetNumber)) {
          return true;
        } else if (streetNumber.matches("\\d+(\\s*[,/]\\s*\\d+)+")) {
          final String[] parts = streetNumber.split("\\s*[,/]\\s*");
          final Set<Integer> numbers = new TreeSet<>();
          for (final String part : parts) {
            final Integer number = Integer.valueOf(part);
            numbers.add(number);
          }
          if (numbers.size() == 1) {
            addError("CIVIC_NUMBER has duplicate value in list");
            final Integer civicNumber = CollectionUtil.get(numbers, 0);
            return setCivicNumber(civicNumber);
          } else {
            final String civicNumberRange = Strings.toString(",", numbers);
            addWarningCount("CIVIC_NUMBER is a list");
            replaceWordInFullAddress(streetNumber, civicNumberRange);
            return setCivicNumberRange(civicNumberRange);
          }
        } else {
          final Matcher civicUnitNumberMatcher = CIVIC_NUMBER_UNIT_PATTERN.matcher(streetNumber);
          if (civicUnitNumberMatcher.matches()) {
            addWarningOrError(CIVIC_NUMBER_INCLUDES_UNIT_DESCRIPTOR_SUFFIX, hasUnitField);
            addUnitDescriptor(civicUnitNumberMatcher.group(2));
            return setCivicNumber(Integer.valueOf(civicUnitNumberMatcher.group(1)));
          } else {
            final Matcher unitCivicNumberMatcher = UNIT_CIVIC_NUMBER_PATTERN.matcher(streetNumber);
            if (unitCivicNumberMatcher.matches()) {
              addWarningOrError(CIVIC_NUMBER_INCLUDES_UNIT_DESCRIPTOR_PREFIX, hasUnitField);
              addUnitDescriptor(unitCivicNumberMatcher.group(1));
              final Integer civicNumber = Integer.valueOf(unitCivicNumberMatcher.group(2));
              return setCivicNumber(civicNumber);
            } else {
              final Matcher civicNumberSuffixMatcher = CIVIC_NUMBER_SUFFIX_PATTERN
                .matcher(streetNumber);
              if (civicNumberSuffixMatcher.matches()) {
                final String numberGroup = civicNumberSuffixMatcher.group(1);
                if (numberGroup == null) {
                  return false;
                } else {
                  final String suffixPart = civicNumberSuffixMatcher.group(2);
                  if (Numbers.isNumber(suffixPart) || suffixPart.length() > 1) {
                    addUnitDescriptor(suffixPart);
                  } else {
                    if (hasValue(CIVIC_NUMBER_SUFFIX)) {
                      throw IgnoreSiteException
                        .error("Ignore CIVIC_NUMBER_SUFFIX != null and CIVIC_NUMBER has a suffix");
                    }
                    setCivicNumberSuffix(suffixPart);
                  }
                  return setCivicNumber(Integer.valueOf(numberGroup));
                }
              } else {
                final Matcher civicNumberUnitcivicNumberUnitRangeMatcher = CIVIC_NUMBER_UNIT_RANGE_CIVIC_NUMBER_UNIT_RANGE_PATTERN
                  .matcher(streetNumber);
                if (civicNumberUnitcivicNumberUnitRangeMatcher.matches()) {
                  final int civicNumber1 = Integer
                    .valueOf(civicNumberUnitcivicNumberUnitRangeMatcher.group(1));
                  final String civicNumberSuffix1 = civicNumberUnitcivicNumberUnitRangeMatcher
                    .group(2);
                  final int civicNumber2 = Integer
                    .valueOf(civicNumberUnitcivicNumberUnitRangeMatcher.group(3));
                  final String civicNumberSuffix2 = civicNumberUnitcivicNumberUnitRangeMatcher
                    .group(4);
                  if (civicNumber1 == civicNumber2) {
                    addError("CIVIC_NUMBER includes unit range");
                    addUnitDescriptor(civicNumberSuffix1 + "," + civicNumberSuffix2);
                    return setCivicNumber(civicNumber1);
                  } else {
                    throw IgnoreSiteException.error("Ignore CIVIC_NUMBER is not numeric");
                  }
                } else {
                  final Matcher civicNumberUnitRangeMatcher = CIVIC_NUMBER_UNIT_RANGE_CIVIC_NUMBER_UNIT_RANGE_PATTERN
                    .matcher(streetNumber);
                  if (civicNumberUnitRangeMatcher.matches()) {
                    addError("CIVIC_NUMBER includes unit range");
                    final String civicNumberSuffix1 = civicNumberUnitRangeMatcher.group(2);
                    final String civicNumberSuffix2 = civicNumberUnitRangeMatcher.group(3);
                    addUnitDescriptor(civicNumberSuffix1 + "," + civicNumberSuffix2);
                    final Integer civicNumber = Integer
                      .valueOf(civicNumberUnitRangeMatcher.group(1));
                    return setCivicNumber(civicNumber);
                  } else {
                    throw IgnoreSiteException.error("Ignore CIVIC_NUMBER is not numeric");
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  private boolean fixCivicNumberListWithSuffix(final String streetNumber) {
    final Matcher matcher = NUMBER_AND_SUFFIX_PATTERN.matcher(streetNumber);
    if (matcher.matches()) {
      final int number1 = Integer.parseInt(matcher.group(1));
      final int number2 = Integer.parseInt(matcher.group(2));
      final String suffix = matcher.group(3);

      if (number1 == number2) {
        addWarningCount("CIVIC_NUMBER is a list with suffix");
        replaceWordInFullAddress(streetNumber, "," + suffix + " " + number1);
        setCivicNumber(number1);
        setUnitDescriptor("," + suffix);
        return true;
      }
    }
    return false;
  }

  private boolean fixCivicNumberSuffix(String civicNumberSuffix) {
    if (civicNumberSuffix.startsWith("-")) {
      civicNumberSuffix = civicNumberSuffix.substring(1);

      try {
        final Integer number1 = getCivicNumber();
        final int number2 = Integer.parseInt(civicNumberSuffix);
        final int diff = number1 - number2;
        if (diff < -100 && number1 < 100) {
          addWarningCount("CIVIC_NUMBER_SUFFIX is [UNIT_DESCRIPTOR]");
          addUnitDescriptor(number1);
          return setCivicNumber(number2);
        } else if (diff > 100 && number2 < 100) {
          addError("CIVIC_NUMBER_SUFFIX is CIVIC_NUMBER and CIVIC_NUMBER is UNIT_DESCRIPTOR");
          addUnitDescriptor(number2);
          return setCivicNumber(number1);
        } else {
          clearCivicNumberSuffix();
          if (number1 < number2) {
            addError("CIVIC_NUMBER-CIVIC_NUMBER_SUFFIX is a range");
            clearCivicNumber();
            setCivicNumberRange(number1 + "-" + number2);
            return true;
          } else if (number1 < number2) {
            addError("CIVIC_NUMBER-CIVIC_NUMBER_SUFFIX is a range");
            clearCivicNumber();
            setCivicNumberRange(number2 + "-" + number1);
            return true;
          } else {
            addError("CIVIC_NUMBER = CIVIC_NUMBER_SUFFIX");
            return setCivicNumber(number1);
          }
        }
      } catch (final Throwable e) {
        Debug.noOp();
      }
    }
    if (civicNumberSuffix.matches("([A-Z]|1/2)")) {
      return setCivicNumberSuffix(civicNumberSuffix);
    } else if (civicNumberSuffix.equals("A&B")) {
      replaceWordInFullAddress("A&B", "A,B");
      addUnitDescriptor("A,B");
      clearCivicNumberSuffix();
      addWarningCount("CIVIC_NUMBER_SUFFIX is UNIT_DESCRIPTOR");
      return true;
    } else if (civicNumberSuffix.equals("ABC")) {
      replaceWordInFullAddress("ABC", "A,B,C");
      addUnitDescriptor("A,B,C");
      clearCivicNumberSuffix();
      addWarningCount("CIVIC_NUMBER_SUFFIX is UNIT_DESCRIPTOR");
      return true;
    } else if (civicNumberSuffix.matches("[A-Z0-9]+-[A-Z0-9]+")) {
      addUnitDescriptor(civicNumberSuffix);
      clearCivicNumberSuffix();
      addWarningCount("CIVIC_NUMBER_SUFFIX is UNIT_DESCRIPTOR range");
      return true;
    } else if (civicNumberSuffix.equals(".5")) {
      replaceWordInFullAddress(".5", "1/2");
      setCivicNumberSuffix("1/2");
      addWarningCount("CIVIC_NUMBER_SUFFIX=0.5 should be 1/2");
      return true;
    } else if (civicNumberSuffix.startsWith("/") || civicNumberSuffix.startsWith("&")) {
      replaceWordInFullAddress(civicNumberSuffix, "," + civicNumberSuffix.substring(1));
      setCivicNumberRange(getCivicNumber() + "," + civicNumberSuffix.substring(1));
      clearCivicNumber();
      clearCivicNumberSuffix();
      addError("CIVIC_NUMBER_SUFFIX is a range starting with / or &");
      return true;
    } else {
      // TODO change in FULL_ADDRESS
      addUnitDescriptor(civicNumberSuffix.replace(" ", ""));
      addWarningCount("CIVIC_NUMBER_SUFFIX is UNIT_DESCRIPTOR");
      return clearCivicNumberSuffix();
    }
  }

  public Integer getCivicNumber() {
    return getInteger(CIVIC_NUMBER);
  }

  public String getCivicNumberRange() {
    return getString(CIVIC_NUMBER_RANGE, "");
  }

  public String getCivicNumberRangeHyphen() {
    final String civicNumberRange = getCivicNumberRange();
    return Strings.replace(civicNumberRange, "~", "-");
  }

  public String getCivicNumberString() {
    return getString(CIVIC_NUMBER, "");
  }

  public String getCivicNumberSuffix() {
    return getString(CIVIC_NUMBER_SUFFIX, "");
  }

  public String getCivicNumberWithSuffix() {
    return getString(CIVIC_NUMBER, "") + getString(CIVIC_NUMBER_SUFFIX, "");
  }

  public String getFullAddress() {
    return getString(FULL_ADDRESS, "");
  }

  public String getStreetAddress() {
    final String structuredName = getStructuredName();
    final String civicNumber = Integer.toString(getCivicNumber());
    final String civicNumberSuffix = getCivicNumberSuffix();
    return SitePoint.getFullAddress(null, civicNumber, civicNumberSuffix, null, structuredName);
  }

  public String getStructuredName() {
    final String structuredName = getString(STREET_NAME);
    if (structuredName == null) {
      final Identifier structuredNameId = getIdentifier(STREET_NAME_ID);
      return STRUCTURED_NAMES.getValue(structuredNameId);
    } else {
      return structuredName;
    }
  }

  public String getUnitDescriptor() {
    return getString(UNIT_DESCRIPTOR, "");
  }

  public RangeSet getUnitDescriptorRanges() {
    final String unitDescriptor = getString(UNIT_DESCRIPTOR);
    return RangeSet.newRangeSet(unitDescriptor);
  }

  public boolean isUseInAddressRange() {
    final Object useInAddressRange = getValue(USE_IN_ADDRESS_RANGE_IND);
    return "Y".equals(useInAddressRange);
  }

  public boolean replaceAllInFullAddress(final String pattern, final String newValue) {
    String fullAddress = getFullAddress();

    fullAddress = Strings.replaceAll(fullAddress, pattern, newValue);
    return setFullAddress(fullAddress);
  }

  public boolean replaceWordInFullAddress(final String oldValue, final String newValue) {
    String fullAddress = getFullAddress();
    fullAddress = Strings.replaceWord(fullAddress, oldValue, newValue, WORD_SEPARATORS);
    return setFullAddress(fullAddress);
  }

  public boolean setCivicNumber(final Integer civicNumber) {
    return setValue(CIVIC_NUMBER, civicNumber);
  }

  public boolean setCivicNumber(String streetNumber, final boolean hasUnitField,
    final boolean civicNumberIncludesUnitPrefix) {
    if (IGNORE_UNIT_DESCRIPTORS.contains(streetNumber)) {
      return clearCivicNumber();
    } else if (Property.hasValue(streetNumber)) {
      if (streetNumber.charAt(0) == '*') {
        streetNumber = streetNumber.substring(1);
      }
      if (streetNumber.charAt(0) == '(' && streetNumber.endsWith(")")) {
        streetNumber = streetNumber.substring(1, streetNumber.length() - 1);
      }
      try {
        final int number = Integer.parseInt(streetNumber);
        return setCivicNumber(number);
      } catch (final RuntimeException e) {
        return fixCivicNumber(streetNumber, hasUnitField, civicNumberIncludesUnitPrefix);
      }
    } else {
      return clearCivicNumber();
    }
  }

  public boolean setCivicNumberRange(final String civicNumberRange) {
    return setValue(CIVIC_NUMBER_RANGE, civicNumberRange);
  }

  public boolean setCivicNumberSuffix(final String civicNumberSuffix) {
    if (Property.hasValue(civicNumberSuffix)) {
      if (civicNumberSuffix.matches("([A-Z]|1/2)")) {
        return setValue(CIVIC_NUMBER_SUFFIX, civicNumberSuffix);
      } else {
        return fixCivicNumberSuffix(civicNumberSuffix);
      }
    } else {
      return clearCivicNumberSuffix();
    }
  }

  public void setCreateModifyOrg(final PartnerOrganization partnerOrganization) {
    final Identifier partnerOrganizationId = partnerOrganization.getPartnerOrganizationId();
    final String partnerOrganizationName = partnerOrganization.getPartnerOrganizationName();
    // TODO setValue(CREATE_PARTNER_ORG_ID, partnerOrganizationId);
    setValue(CREATE_PARTNER_ORG, partnerOrganizationName);
    // TODO setValue(MODIFY_PARTNER_ORG_ID, partnerOrganizationId);
    setValue(MODIFY_PARTNER_ORG, partnerOrganizationName);
  }

  public void setCustodianOrg(final PartnerOrganization partnerOrganization) {
    final Identifier partnerOrganizationId = partnerOrganization.getPartnerOrganizationId();
    final String partnerOrganizationName = partnerOrganization.getPartnerOrganizationName();
    // TODO setValue(CUSTODIAN_PARTNER_ORG_ID, partnerOrganizationId);
    setValue(CUSTODIAN_PARTNER_ORG, partnerOrganizationName);
  }

  public boolean setFullAddress(String fullAddress) {
    if (fullAddress == null) {
      return setValue(FULL_ADDRESS, null);
    } else {
      if (".".equals(fullAddress)) {
        fullAddress = null;
      }
      if (fullAddress.endsWith(" (ALIAS)")) {
        fullAddress = fullAddress.substring(0, fullAddress.length() - 8);
      }
      return setValue(FULL_ADDRESS, Strings.trim(fullAddress));
    }
  }

  public void setPostalCode(String postalCode) {
    if (Property.hasValue(postalCode)) {
      boolean valid = false;
      postalCode = postalCode.toUpperCase().trim();
      if (postalCode.length() == 6 && postalCode.matches("[A-Z]\\d[A-Z]\\d[A-Z]\\d")) {
        valid = true;
        postalCode = postalCode.substring(0, 3) + " " + postalCode.substring(3);
      } else if (postalCode.length() == 7 && postalCode.matches("[A-Z]\\d[A-Z] \\d[A-Z]\\d")) {
        valid = true;
      }
      if (valid) {
        setValue(POSTAL_CODE, postalCode);
      } else {
        addError("Invalid postal code");
      }
    }
  }

  public boolean setUnitDescriptor(final RangeSet unitDescriptor) {
    return setValue(UNIT_DESCRIPTOR, unitDescriptor.toString());
  }

  public boolean setUnitDescriptor(final String unitDescriptor) {
    return setValue(UNIT_DESCRIPTOR, unitDescriptor);
  }

  public void setUseInAddressRange(final boolean useInAddressRange) {
    if (useInAddressRange) {
      setValue(USE_IN_ADDRESS_RANGE_IND, "Y");
    } else {
      setValue(USE_IN_ADDRESS_RANGE_IND, "N");
    }
  }

  public void updateFullAddress() {
    SitePoint.updateFullAddress(this);
  }

  public Boolean validateFullAddress(String originalStreetName, final String compareStreetName,
    String structuredName, final String prefixNameDirectionCode, final String streetName,
    final String streetType, final String suffixNameDirectionCode, final String unitType,
    final String civicNumberFieldName, final String streetTypeFieldName,
    final boolean addressIncludesUnit) {
    if (hasValue(FULL_ADDRESS)) {
      if (hasValue(UNIT_DESCRIPTOR)) {
        return validateFullAddressUnitDescriptor(compareStreetName, prefixNameDirectionCode,
          streetName, streetType, suffixNameDirectionCode, unitType, civicNumberFieldName,
          addressIncludesUnit);
      } else {
        final String fullAddress = getFullAddress();
        final String civicNumberString = getCivicNumberString();
        final String civicNumberSuffix = getCivicNumberSuffix();
        final String civicNumberRange = getCivicNumberRange();
        final String civicNumberRangeHyphen = getCivicNumberRangeHyphen();
        if (fullAddress
          .equalsIgnoreCase(Strings.toString(" ", civicNumberRangeHyphen, compareStreetName))) {
          // [NUMBER_RANGE] [NAME]
          return true;
        } else if (fullAddress
          .equalsIgnoreCase(Strings.toString(" ", civicNumberRange, compareStreetName))) {
          // [NUMBER_RANGE] [NAME]
          return true;
        } else if (fullAddress.equalsIgnoreCase(Strings.toString(" ",
          civicNumberString + civicNumberSuffix, civicNumberRangeHyphen, compareStreetName))) {
          // [NUMBER][SUFFIX] [NUMBER_RANGE] [NAME]
          return true;
        } else if (fullAddress
          .equalsIgnoreCase(civicNumberString + civicNumberSuffix + compareStreetName)) {
          addError("FULL_ADDRESS missing space before STREET_NAME");
          // [NUMBER][SUFFIX][NAME]
          return true;
        } else if (fullAddress
          .equalsIgnoreCase(Strings.toString(" ", civicNumberString + "-" + civicNumberSuffix,
            civicNumberRangeHyphen, compareStreetName))) {
          // [NUMBER]-[SUFFIX] [NUMBER_RANGE] [NAME]
          return true;
        } else if (fullAddress
          .equalsIgnoreCase(Strings.toString(" ", civicNumberString + " " + civicNumberSuffix,
            civicNumberRangeHyphen, compareStreetName))) {
          // [NUMBER] [SUFFIX] [NUMBER_RANGE] [NAME]
          return true;
        } else if (fullAddress
          .equalsIgnoreCase(Strings.toString(" ", civicNumberString + " -" + civicNumberSuffix,
            civicNumberRangeHyphen, compareStreetName))) {
          // [NUMBER] -[SUFFIX] [NUMBER_RANGE] [NAME]
          return true;
        } else if (fullAddress.equalsIgnoreCase(Strings.toString(" ",
          civicNumberString + civicNumberSuffix, compareStreetName, unitType))) {
          // [NUMBER][SUFFIX] [NAME] [UNIT_TYPE]
          return true;
        } else if (fullAddress.equalsIgnoreCase(
          Strings.toString(" ", civicNumberString + civicNumberSuffix + "UF", compareStreetName))) {
          // [NUMBER][SUFFIX]UF [NAME]
          addUnitDescriptor("UF");
          addError("FULL_ADDRESS has extra UNIT_DESCRIPTOR");
          return true;
        } else if (Property.hasValue(suffixNameDirectionCode) && fullAddress
          .equalsIgnoreCase(Strings.toString(" ", civicNumberString + civicNumberSuffix,
            civicNumberRangeHyphen, prefixNameDirectionCode, streetName, streetType))) {
          // [NUMBER] -[SUFFIX] [NUMBER_RANGE] [NAME_DIR] [NAME] [NAME_TYPE]
          addWarningCount("FULL_ADDRESS missing SUFFIX_NAME_DIRECTION_CODE");
          return true;
        } else {
          {
            try {
              final Matcher matcher = Pattern
                .compile(Strings.toString(" ", "(\\d+)-" + civicNumberString + civicNumberSuffix,
                  civicNumberRangeHyphen, originalStreetName))
                .matcher(fullAddress);
              if (matcher.matches()) {
                final String unitDescriptor = matcher.group(1);
                addUnitDescriptor(unitDescriptor);
                addError("FULL_ADDRESS has extra UNIT_DESCRIPTOR");
              }
            } catch (final Throwable e) {
              Logs.debug(this, e);
            }
          }
          if (Property.isEmpty(structuredName)) {
            if (!hasValuesAny(CIVIC_NUMBER, CIVIC_NUMBER_RANGE)) {
              throw IgnoreSiteException.warning("Ignore no valid CIVIC_NUMBER");
            } else if (fullAddress.startsWith(civicNumberString + " ")) {
              structuredName = fullAddress.substring((civicNumberString + " ").length());
              originalStreetName = structuredName;
              return true;
            }
          } else if (fullAddress.equalsIgnoreCase(compareStreetName)) {
            if (hasValue(CIVIC_NUMBER)) {
              if ("0".equals(civicNumberString)) {
                addError("Ignore FULL_ADDRESS missing CIVIC_NUMBER 0");
              } else {
                addError("FULL_ADDRESS missing CIVIC_NUMBER");
              }
              return true;
            } else {
              if (!Property.hasValue(civicNumberSuffix)) {
                return true;
              }
            }
          } else if (fullAddress.endsWith(" " + compareStreetName)) {
            final String numberPart = fullAddress.substring(0,
              fullAddress.length() - compareStreetName.length() - 1);
            if (Numbers.isNumber(numberPart)) {
              if (getCivicNumber() == null) {
                if (civicNumberRangeHyphen.contains(numberPart)) {
                  throw IgnoreSiteException
                    .error("Ignore FULL_ADDRESS missing CIVIC_NUMBER list/range");
                } else {
                  setCivicNumber(Integer.valueOf(numberPart));
                  if (Property.hasValue(civicNumberFieldName)) {
                    throw IgnoreSiteException.error("Ignore FULL_ADDRESS has extra CIVIC_NUMBER");
                  }
                  return true;
                }
              }
            } else if (hasValue(CIVIC_NUMBER)) {
              final Matcher civicNumberSuffixMatcher = CIVIC_NUMBER_SUFFIX_PATTERN
                .matcher(numberPart);
              if (civicNumberSuffixMatcher.matches()) {
                final String numberGroup = civicNumberSuffixMatcher.group(1);
                if (getCivicNumber() == Integer.parseInt(numberGroup)) {
                  final String suffixPart = civicNumberSuffixMatcher.group(2);
                  if (Numbers.isNumber(suffixPart) || suffixPart.length() > 1) {
                    setUnitDescriptor(suffixPart);
                    addError("FULL_ADDRESS has extra UNIT_DESCRIPTOR");
                  } else {
                    setCivicNumberSuffix(suffixPart);
                    addError("FULL_ADDRESS has extra CIVIC_NUMBER_SUFFIX");
                  }
                  return true;
                }
              }
            }
          } else if (fullAddress.startsWith(civicNumberString + " " + compareStreetName + " ")) {
            if (Property.hasValue(streetTypeFieldName)) {
              return false;
            } else {
              structuredName = fullAddress.substring((civicNumberString + " ").length());
              originalStreetName = structuredName;
              addWarningCount(
                "FULL_ADDRESS includes STREET_TYPE and No STREET_TYPE field specified");
              return true;
            }
          }
          return false;
        }
      }
    } else {
      return null;
    }
  }

  private Boolean validateFullAddressUnitDescriptor(final String compareStreetName,
    final String prefixNameDirectionCode, final String streetName, final String streetType,
    final String suffixNameDirectionCode, final String unitType, final String civicNumberFieldName,
    final boolean addressIncludesUnit) {
    final String fullAddress = getFullAddress();
    final String unitDescriptor = getUnitDescriptor();
    final String civicNumberString = getCivicNumberString();
    final String civicNumberSuffix = getCivicNumberSuffix();
    if (civicNumberFieldName == null) {
      if (fullAddress.endsWith(" " + compareStreetName)) {
        final String parts = fullAddress.substring(0,
          fullAddress.length() - compareStreetName.length() - 1);
        try {
          setCivicNumber(Integer.valueOf(parts));
          return true;
        } catch (final Throwable e) {
        }
      }

      return false;
    } else {
      if (!addressIncludesUnit && fullAddress.equalsIgnoreCase(
        Strings.toString(" ", civicNumberString + civicNumberSuffix, compareStreetName))) {
        return true;
      } else if (fullAddress.equalsIgnoreCase(Strings.toString(" ", unitDescriptor,
        civicNumberString + civicNumberSuffix, compareStreetName))) {
        return true;
      } else if (fullAddress.equalsIgnoreCase(Strings.toString(" ",
        unitDescriptor + "-" + civicNumberString + civicNumberSuffix, compareStreetName))) {
        return true;
      } else if (fullAddress.equalsIgnoreCase(Strings.toString(" ",
        unitDescriptor + "- " + civicNumberString + civicNumberSuffix, compareStreetName))) {
        return true;
      } else if (fullAddress.equalsIgnoreCase(Strings.toString(" ",
        civicNumberString + civicNumberSuffix + " -" + unitDescriptor, compareStreetName))) {
        return true;
      } else if (fullAddress.equalsIgnoreCase(Strings.toString(" ",
        civicNumberString + civicNumberSuffix + "-" + unitDescriptor, compareStreetName))) {
        return true;
      } else if (fullAddress.equalsIgnoreCase(Strings.toString(" ",
        civicNumberString + civicNumberSuffix, unitDescriptor, compareStreetName))) {
        return true;
      } else if (fullAddress.equalsIgnoreCase(Strings.toString(" ", "#" + unitDescriptor + ",",
        civicNumberString + civicNumberSuffix, compareStreetName))) {
        return true;
      } else if (fullAddress.equalsIgnoreCase(Strings.toString(" ",
        unitDescriptor + "-" + civicNumberString, "-" + civicNumberSuffix, compareStreetName))) {
        // [UNIT]-[CIVIC_NUMBER] -[CIVIC_NUMBER_SUFFIX] [STREET_NAME]
        return true;
      } else if (fullAddress.equalsIgnoreCase(
        Strings.toString(" ", unitDescriptor + ", " + civicNumberString, compareStreetName))) {
        // [UNIT], [CIVIC_NUMBER] [STREET_NAME]
        return true;
      } else if (fullAddress.equalsIgnoreCase(Strings.toString(" ", unitDescriptor,
        civicNumberString + "-" + civicNumberSuffix, compareStreetName))) {
        return true;
      } else if (fullAddress.equalsIgnoreCase(
        unitDescriptor + " - " + civicNumberString + civicNumberSuffix + " " + compareStreetName)) {
        return true;
      } else if (fullAddress.equalsIgnoreCase(
        unitDescriptor + "-" + civicNumberString + civicNumberSuffix + " " + compareStreetName)) {
        return true;
      } else if (fullAddress.equalsIgnoreCase(civicNumberString + civicNumberSuffix + " "
        + compareStreetName + " Unit: " + unitDescriptor)) {
        return true;
      } else if (fullAddress.equalsIgnoreCase(civicNumberString + " " + compareStreetName)) {
        addError("FULL_ADDRESS missing UNIT_DESCRIPTOR");
        return true;
      } else if (Property.hasValue(suffixNameDirectionCode) && fullAddress.equalsIgnoreCase(Strings
        .toString(" ", civicNumberString, prefixNameDirectionCode, streetName, streetType))) {
        addWarningCount("FULL_ADDRESS missing UNIT_DESCRIPTOR and SUFFIX_NAME_DIRECTION_CODE");
        return true;
      } else if (Property.hasValue(suffixNameDirectionCode) && fullAddress
        .equalsIgnoreCase(Strings.toString(" ", civicNumberString + "-" + unitDescriptor,
          prefixNameDirectionCode, streetName, streetType))) {
        addWarningCount("FULL_ADDRESS missing SUFFIX_NAME_DIRECTION_CODE");
        return true;
      } else if (Property.hasValue(suffixNameDirectionCode) && fullAddress
        .equalsIgnoreCase(Strings.toString(" ", civicNumberString + "" + unitDescriptor,
          prefixNameDirectionCode, streetName, streetType))) {
        addWarningCount("FULL_ADDRESS missing SUFFIX_NAME_DIRECTION_CODE");
        return true;
      } else if (fullAddress.equalsIgnoreCase(
        Strings.toString(" ", civicNumberString, compareStreetName, unitType, unitDescriptor))) {
        return true;
      } else if (fullAddress.equalsIgnoreCase(Strings.toString(" ",
        civicNumberString + civicNumberSuffix, compareStreetName, unitType, unitDescriptor))) {
        return true;
      } else if (fullAddress
        .equalsIgnoreCase(unitDescriptor + "-" + civicNumberString + " " + compareStreetName)) {
        return true;
      } else if (fullAddress.equals(civicNumberString + unitDescriptor + " " + compareStreetName)) {
        if (unitDescriptor.matches("([A-Z]|1/2)")) {
          setCivicNumberSuffix(unitDescriptor);
          clearUnitDescriptor();
        }
        return true;
      } else if (fullAddress.equalsIgnoreCase(
        civicNumberString + " " + compareStreetName + ", " + unitDescriptor + " Unit")) {
        if ("BSMT".equals(unitDescriptor)) {
          setUnitDescriptor("BASEMENT");
        }
        return true;
      } else if (fullAddress.equalsIgnoreCase(
        civicNumberString + " " + compareStreetName + " (" + unitDescriptor + ")")) {
        addError("UNIT is a type/location descriptor");
        clearUnitDescriptor();
        return true;
      } else if (fullAddress.equalsIgnoreCase(
        unitDescriptor + "-" + civicNumberString + civicNumberSuffix + compareStreetName)) {
        return true;
      } else if (fullAddress.equalsIgnoreCase(
        "-" + unitDescriptor + civicNumberString + civicNumberSuffix + " " + compareStreetName)) {
        return true;
      } else if (fullAddress
        .equalsIgnoreCase(Strings.toString(" ", civicNumberString + civicNumberSuffix,
          civicNumberString + civicNumberSuffix, "-" + unitDescriptor, compareStreetName))) {
        addError("FULL_ADDRESS has CIVIC_NUMBER twice");
        return true;
      } else {
        if (!hasValuesAny(CIVIC_NUMBER, CIVIC_NUMBER_RANGE)) {
          return true;
        } else {
          return false;
        }
      }
    }
  }
}
