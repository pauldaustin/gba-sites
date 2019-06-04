package ca.bc.gov.gbasites.load.provider.addressbc;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jeometry.common.data.identifier.Identifier;
import org.jeometry.common.number.Integers;

import ca.bc.gov.gba.model.type.StructuredName;
import ca.bc.gov.gbasites.load.common.IgnoreSiteException;
import ca.bc.gov.gbasites.load.common.ProviderSitePointConverter;
import ca.bc.gov.gbasites.load.common.converter.AbstractSiteConverter;
import ca.bc.gov.gbasites.model.type.SitePoint;

import com.revolsys.collection.list.Lists;
import com.revolsys.collection.map.Maps;
import com.revolsys.collection.range.RangeSet;
import com.revolsys.geometry.model.Point;
import com.revolsys.record.DelegatingRecord;
import com.revolsys.record.Record;
import com.revolsys.util.Debug;
import com.revolsys.util.Emptyable;
import com.revolsys.util.Property;
import com.revolsys.util.Strings;

public class AddressBcSite extends DelegatingRecord implements AddressBc, SitePoint {
  private static final String FULL_ADDRESS_HAS_EXTRA_CIVIC_NUMBER_SUFFIX = "FULL_ADDRESS has extra CIVIC_NUMBER_SUFFIX";

  private static final String FULL_ADDRESS_ENDS_WITH_LOCALITY_BC_POSTAL_CODE = "FULL_ADDRESS ends with '[LOCALITY] BC [POSTAL_CODE]'";

  private static final String MESSAGE_FUL_ADDRESS_ENDS_WITH_LOCALITY = "FULL_ADDRESS ends with '[LOCALITY]'";

  private static final List<String> ADDRESS_BC_INVALID_FULL_ADDRESSES = Arrays.asList(".", "V",
    "MARINE LEASE", "BLD");

  private static final Map<String, List<String>> ADDRESS_BC_STREET_TYPE_ALIAS = Maps
    .<String, List<String>> buildHash()//
    .add("ACCESS", Lists.newArray("ACC"))//
    .add("AVE", Lists.newArray("AV", "AVENUE"))//
    .add("BLVD", Lists.newArray("BOULEVARD"))//
    .add("CIR", Lists.newArray("CIRCLE"))//
    .add("CLOSE", Lists.newArray("CL"))//
    .add("CRES", Lists.newArray("CR", "CRESCENT"))//
    .add("CRT", Lists.newArray("CT", "COURT"))//
    .add("DR", Lists.newArray("DRIVE"))//
    .add("DIVERS", Lists.newArray("DIV"))//
    .add("EXTEN", Lists.newArray("EXTN"))//
    .add("ESTATES", Lists.newArray("ESTS"))//
    .add("FRONT", Lists.newArray("FRT"))//
    .add("FSR", Lists.newArray("FOREST SERVICE RD"))//
    .add("GR", Lists.newArray("GROVE"))//
    .add("GROVE", Lists.newArray("GR"))//
    .add("GRN", Lists.newArray("GREEN"))//
    .add("GREEN", Lists.newArray("GRN"))//
    .add("HTS", Lists.newArray("HEIGHTS", "HGHTS"))//
    .add("LANE", Lists.newArray("LN"))//
    .add("MEADOW", Lists.newArray("MEADOWS"))//
    .add("SQ", Lists.newArray("SQUARE"))//
    .add("LANDNG", Lists.newArray("LANDING", "LANDG"))//
    .add("LKOUT", Lists.newArray("LOOKOUT"))//
    .add("PASS", Lists.newArray("PASSAGE"))//
    .add("PKY", Lists.newArray("PARKWAY", "PKWY"))//
    .add("PL", Lists.newArray("PLACE"))//
    .add("PT", Lists.newArray("POINT"))//
    .add("RD", Lists.newArray("ROAD", "R", "RD."))//
    .add("RTE", Lists.newArray("RT"))//
    .add("SQ", Lists.newArray("SQUARE"))//
    .add("ST", Lists.newArray("STREET"))//
    .add("SUBDIV", Lists.newArray("SUB"))//
    .add("TERR", Lists.newArray("TERRACE", "TR"))//
    .add("TRAIL", Lists.newArray("TRL"))//
    .add("TRNABT", Lists.newArray("TURNABOUT", "TURNAROUND"))//
    .add("WAY", Lists.newArray("WY"))//
    .getMap();

  private static final List<String> DIRECTIONS = Lists.newArray("NORTH", "EAST", "SOUTH", "WEST",
    "N", "E", "S", "W", "NE", "NW", "SE", "SW");

  private static final Pattern POSTAL_CODE_PATTERN = Pattern.compile(".* V\\d[A-Z] \\d[A-Z]\\d");

  private static final String MESSAGE_FULL_ADDRESS_ENDS_WITH_LOCALITY_BC = "FULL_ADDRESS ends with ', [LOCALITY], BC'";

  private static final List<String> ADDRESS_BC_LOCALITY_SUFFIXES = Lists.newArray("PT ALICE",
    "PT HARDY", "PT MCNEILL", "SOINTULA", "QUATSINO", "ALERT BAY", "BEAVER COVE", "WOSS",
    "WINTER HARBOUR", "NANAIMO BC");

  private static final Pattern PATTERN_LOCALITY_BC = Pattern.compile("(.*),( \\w+)+, BC");

  public String addressParts;

  private String aliasName;

  private Integer civicNumber;

  public String civicNumberRange;

  public String civicNumberSuffix;

  public Map<String, Object> extendedData = new HashMap<>();

  public String fullAddress;

  private boolean ignored;

  public String nameBody;

  public String nameSuffixCode;

  public String namePrefixCode;

  private final Point point;

  public String prefixNameDirectionCode;

  public String siteLocationCode = "P";

  public String streetNumber;

  public String streetNumberPrefix;

  private String structuredName;

  public String suffixNameDirectionCode;

  public RangeSet unitDescriptor = new RangeSet();

  public String unitNumber;

  private final AddressBcSiteConverter converter;

  public AddressBcSite(final AddressBcSiteConverter converter, final Record sourceRecord,
    final Point point) {
    super(sourceRecord);
    this.converter = converter;
    this.unitNumber = getValue(UNIT_DESCRIPTOR);

    this.fullAddress = getValueCleanIntern("FULL_ADDRESS");
    this.streetNumberPrefix = getValueCleanIntern(STREET_NUMBER_PREFIX);
    if (this.streetNumberPrefix != null) {
      this.streetNumberPrefix = this.streetNumberPrefix.replace("#", "");
    }

    this.streetNumber = getValueCleanIntern(STREET_NUMBER);
    this.civicNumberSuffix = getValueCleanIntern(STREET_NUMBER_SUFFIX);
    if (this.civicNumberSuffix != null) {
      if (this.civicNumberSuffix.charAt(0) == '-') {
        final String newSuffix = this.civicNumberSuffix.substring(1);
        if (this.fullAddress != null) {
          this.fullAddress = this.fullAddress.replace(this.civicNumberSuffix, newSuffix);
        }
        this.civicNumberSuffix = newSuffix;
      }
    }
    this.prefixNameDirectionCode = getValueCleanIntern(STREET_DIR_PREFIX);
    this.nameBody = getValueCleanIntern(SitePoint.STREET_NAME);
    this.nameSuffixCode = getValueCleanIntern(STREET_TYPE);
    this.suffixNameDirectionCode = getValueCleanIntern(STREET_DIR_SUFFIX);

    if (Property.hasValuesAll(this.civicNumberSuffix, this.streetNumberPrefix)) {
      if (this.civicNumberSuffix.matches("[A-F]") && this.streetNumberPrefix.matches("[0-9]+")
        && this.unitNumber == null) {
        this.unitNumber = this.streetNumberPrefix;
        this.streetNumberPrefix = null;
      }
    }
    if (!fixMissingFullAddress()) {

    }
    if (this.streetNumberPrefix != null && this.streetNumberPrefix.equals(this.unitNumber)) {
      this.streetNumberPrefix = null;
      this.fullAddress = this.fullAddress.replace(this.unitNumber + "-" + this.unitNumber,
        this.unitNumber);
    }
    this.unitDescriptor = RangeSet.newRangeSet(this.unitNumber);
    this.unitDescriptor.add(this.streetNumberPrefix);

    this.addressParts = this.fullAddress;
    this.point = point;
  }

  public void addError(final String message) {
    this.converter.addError(this, message);
  }

  public void addWarning(final String message) {
    this.converter.addWarning(this, message);
  }

  private Boolean fixCivicNumberSuffixWithHyphen() {
    try {
      final int number1 = Integer.parseInt(this.streetNumber);
      final int number2 = Integer.parseInt(this.civicNumberSuffix.trim());
      final int diff = number1 - number2;
      if (diff < -100) {
        if (Property.isEmpty(this.unitDescriptor)) {
          this.unitDescriptor.add(number1);
          this.streetNumber = Integer.toString(number2);
          this.civicNumberSuffix = null;
          addError("CIVIC_NUMBER_SUFFIX is CIVIC_NUMBER and CIVIC_NUMBER is UNIT_DESCRIPTOR");
          return true;
        } else {
          addError(
            "Ignore CIVIC_NUMBER_SUFFIX includes [UNIT_DESCRIPTOR]- prefix and UNIT_DESCRIPTOR has a value");
          return null;
        }
      } else if (diff > 100) {
        if (Property.isEmpty(this.unitDescriptor)) {
          this.unitDescriptor.add(number2);
          this.streetNumber = Integer.toString(number1);
          this.civicNumberSuffix = null;
          addError("CIVIC_NUMBER_SUFFIX is UNIT_DESCRIPTOR");
          return true;
        } else {
          addError(
            "Ignore CIVIC_NUMBER_SUFFIX includes -[UNIT_DESCRIPTOR] suffix and UNIT_DESCRIPTOR has a value");
          return null;
        }
      } else {
        this.civicNumberSuffix = null;
        this.unitDescriptor = RangeSet.newRangeSet(number2);
        if (number1 < number2) {
          this.streetNumber = null;
          this.civicNumberRange = number1 + "-" + number2;
          addError("CIVIC_NUMBER_SUFFIX is a range");
        } else if (number1 < number2) {
          this.streetNumber = null;
          this.civicNumberRange = number2 + "-" + number1;
          addError("CIVIC_NUMBER_SUFFIX is a range");
        } else {
          this.streetNumber = Integer.toString(number1);
          addError("CIVIC_NUMBER_SUFFIX has duplicate value in range");
        }
        return true;
      }
    } catch (final Throwable e) {
    }
    return null;
  }

  private void fixFullAddress(final String localityName) {

    final String geographicDomain = this.getValue(AddressBc.GEOGRAPHIC_DOMAIN);
    if (this.addressParts.endsWith(", BC")) {
      if (this.addressParts.endsWith(", " + geographicDomain + ", BC")) {
        this.addressParts = this.addressParts.substring(0,
          this.addressParts.length() - 6 - geographicDomain.length());
        if (this.converter.equalsShortName("Bowen Island")
          || this.converter.equalsShortName("Kimberley")
          || this.converter.equalsShortName("Nanaimo")
          || this.converter.equalsShortName("Zeballos")) {
          addWarning(MESSAGE_FULL_ADDRESS_ENDS_WITH_LOCALITY_BC);
        } else {
          addError(MESSAGE_FULL_ADDRESS_ENDS_WITH_LOCALITY_BC);
        }
      } else if (this.addressParts.endsWith(", " + localityName.toUpperCase() + ", BC")) {
        this.addressParts = this.addressParts.substring(0,
          this.addressParts.length() - 6 - localityName.length());
        if (this.converter.equalsShortName("CCRD") || this.converter.equalsShortName("CMXRD")) {
          addWarning(MESSAGE_FULL_ADDRESS_ENDS_WITH_LOCALITY_BC);
        } else {
          addError(MESSAGE_FULL_ADDRESS_ENDS_WITH_LOCALITY_BC);
        }
      } else if (this.converter.equalsShortName("CMXRD")) {
        final Matcher matcher = PATTERN_LOCALITY_BC.matcher(this.addressParts);
        if (matcher.matches()) {
          this.addressParts = matcher.group(1);
          addWarning(MESSAGE_FULL_ADDRESS_ENDS_WITH_LOCALITY_BC);
        }
      }
    } else {
      if (geographicDomain != null) {
        int localityIndex = this.addressParts.indexOf(geographicDomain);
        if (localityIndex == -1) {
          localityIndex = this.addressParts.indexOf(localityName);
        }
        if (localityIndex != -1) {
          if (localityIndex == this.addressParts.length() - geographicDomain.length()) {
            this.addressParts = this.addressParts.substring(0, localityIndex - 1);
            if (this.converter.equalsShortName("Nanaimo")) {
              addWarning(MESSAGE_FUL_ADDRESS_ENDS_WITH_LOCALITY);
            } else {
              addError(MESSAGE_FUL_ADDRESS_ENDS_WITH_LOCALITY);
            }
          } else {
            final String localityBcPostalCode = ".* " + geographicDomain + " BC V.*";
            if (this.addressParts.matches(localityBcPostalCode)) {
              this.addressParts = this.addressParts.substring(0, localityIndex - 1);
              if (this.converter.equalsShortName("Nanaimo")) {
                addWarning(FULL_ADDRESS_ENDS_WITH_LOCALITY_BC_POSTAL_CODE);
              } else {
                addError(FULL_ADDRESS_ENDS_WITH_LOCALITY_BC_POSTAL_CODE);
              }
            }
          }
        }
      }
    }

    if (POSTAL_CODE_PATTERN.matcher(this.addressParts).matches()) {
      this.addressParts = this.addressParts.substring(0, this.addressParts.length() - 8);
      addError("FULL_ADDRESS ends with '[POSTAL_CODE]");
    }
    for (

    final String localitySuffix : Arrays.asList(" COAL HARBOUR", " HOLBERG", ", ZEBALLOS, BC")) {
      if (this.addressParts.endsWith(localitySuffix)) {
        this.addressParts = this.addressParts.replace(localitySuffix, "");
        addWarning("FULL_ADDRESS ends with locality");
      }
    }

    final int accIndex = this.addressParts.indexOf(" (ACC ");
    if (accIndex != -1) {
      this.addressParts = this.addressParts.substring(0, accIndex);
    }
    if (this.addressParts.endsWith(" (EAST)")) {
      this.addressParts = this.addressParts.substring(0, this.addressParts.length() - 6) + "E";
      if (this.suffixNameDirectionCode == null) {
        this.suffixNameDirectionCode = "E";
        addError("FULL_ADDRESS ends with (EAST) replaced with E");
      } else if ("E".equals(this.suffixNameDirectionCode)
        || "EAST".equals(this.suffixNameDirectionCode)) {
        this.suffixNameDirectionCode = "E";
        addError("FULL_ADDRESS ends with (EAST) replaced with E");
      } else {
        addError("FULL_ADDRESS ends with (EAST) replaced with STREET_DIR_SUFFIX="
          + this.suffixNameDirectionCode);
      }
    }
    if (this.addressParts.endsWith(" (WEST)")) {
      this.addressParts = this.addressParts.substring(0, this.addressParts.length() - 6) + "W";
      if (this.suffixNameDirectionCode == null) {
        this.suffixNameDirectionCode = "W";
        addError("FULL_ADDRESS ends with (WEST) replaced with W");
      } else if ("W".equals(this.suffixNameDirectionCode)
        || "WEST".equals(this.suffixNameDirectionCode)) {
        this.suffixNameDirectionCode = "W";
        addError("FULL_ADDRESS ends with (WEST) replaced with W");
      } else {
        addError("FULL_ADDRESS ends with (WEST) replaced with STREET_DIR_SUFFIX="
          + this.suffixNameDirectionCode);
      }
    }
    this.addressParts =

        replaceLocalities(this.addressParts);

    for (final String invalidFullName : ADDRESS_BC_INVALID_FULL_ADDRESSES) {
      if (this.addressParts.equals(invalidFullName)) {
        this.addressParts = getCalculatedFullAddress(this.streetNumberPrefix, this.streetNumber,
          this.civicNumberSuffix, this.prefixNameDirectionCode, this.nameBody, this.nameSuffixCode,
          this.suffixNameDirectionCode);
        addError("FULL_ADDRESS=" + invalidFullName + " not valid using parts");
      }
    }
    if (this.addressParts.endsWith("DWAYNEAHHFSJ.COM")) {
      this.addressParts = this.addressParts.replace("DWAYNEAHHFSJ.COM", "");
      addError("FULL_ADDRESS ends with DWAYNEAHHFSJ.COM");
    } else if (this.addressParts.endsWith("BLD.8 UNIT # 143,145,147,149,118,120,122,124")) {
      this.addressParts = this.addressParts.replace("BLD.8 UNIT # 143,145,147,149,118,120,122,124",
        "");
      addError("FULL_ADDRESS ends with BLD.8 UNIT # 143,145,147,149,118,120,122,124");
    }
    if (this.addressParts.endsWith(")")) {
      final Matcher aliasBracketMatcher = Pattern.compile("(.*) \\(([^)]+)\\)")
        .matcher(this.addressParts);
      if (aliasBracketMatcher.matches()) {
        this.addressParts = aliasBracketMatcher.group(1);
        this.setAliasName(aliasBracketMatcher.group(2));
        addError("FULL_ADDRESS has suffix in (...). Using suffix as secondary name.");
        if (this.nameBody != null) {
          this.nameBody = this.nameBody.replaceAll(" \\(" + this.getAliasName() + "\\)$", "");
        }
      } else {
        addError("FULL_ADDRESS ending with ) " + this.nameBody);
      }
    }
  }

  private boolean fixMissingFullAddress() {
    if (Property.hasValue(this.fullAddress)) {
      this.fullAddress = this.fullAddress.replaceAll("^\\#", "");
      return false;
    } else {
      // addError("FULL_ADDRESS not specified");
      if (Property.hasValue(this.streetNumberPrefix)) {
        if (this.streetNumberPrefix.matches("[A-Z]")) {
          this.civicNumberSuffix = this.streetNumberPrefix;
          this.streetNumberPrefix = null;
          addError("STREET_NUMBER_PREFIX is probably STREET_NUMBER_SUFFIX");
        } else {
          if (Property.hasValue(this.unitNumber)) {
            addError("STREET_NUMBER_PREFIX and UNIT_NUMBER both specified");
          } else {
            this.unitNumber = this.streetNumberPrefix;
            this.streetNumberPrefix = null;
            addError("STREET_NUMBER_PREFIX is probably UNIT_NUMBER");
          }
        }
      }
      return true;
    }
  }

  boolean fixSourceSite(final AddressBcSiteConverter converter, final String localityName) {
    final String structuredName = getStructuredName();
    final String providerName = this.converter.getPartnerOrganizationShortName().toUpperCase();
    String aliasName = converter.getStructuredNameFromAlias(providerName, structuredName);
    if (aliasName == null) {
      final String civicNumberSuffixStructuredName = this.civicNumberSuffix + " " + structuredName;
      aliasName = converter.getStructuredNameFromAlias(providerName,
        civicNumberSuffixStructuredName);
      if (aliasName == null) {
        {
          final String streetTypeWithSpace = " " + this.nameSuffixCode;
          if (this.nameBody != null && this.nameBody.endsWith(streetTypeWithSpace)) {
            this.nameBody = this.nameBody.substring(0,
              this.nameBody.length() - streetTypeWithSpace.length());
            // Remove duplicated suffix from full address
            if (this.fullAddress.endsWith(streetTypeWithSpace + streetTypeWithSpace)) {
              this.fullAddress = this.fullAddress.substring(0,
                this.fullAddress.length() - streetTypeWithSpace.length());
            }

            addWarning("STREET_NAME ends with STREET_TYPE");
          }
        }
        if ("HWY".equals(this.nameSuffixCode)) {
          if (this.nameBody != null && this.nameBody.startsWith("HWY ")) {
            this.namePrefixCode = "HWY";
            this.nameSuffixCode = null;
            this.nameBody = this.nameBody.substring(4);
            if (this.fullAddress.endsWith(" HWY")) {
              this.fullAddress = this.fullAddress.substring(0, this.fullAddress.length() - 4);
            }
            addWarning("STREET_NAME starts with STREET_TYPE (HWY)");
          }
        }
      } else {
        addWarning("Mapped from STRUCTURED_NAME_ALIAS.xlsx");
        setStructuredNameAndParts(aliasName);
        this.civicNumberSuffix = null;
        this.fullAddress = this.fullAddress.replace(civicNumberSuffixStructuredName, aliasName);
        this.addressParts = this.fullAddress;
      }
    } else {
      addWarning("Mapped from STRUCTURED_NAME_ALIAS.xlsx");
      setStructuredNameAndParts(aliasName);
      this.fullAddress = this.fullAddress.replace(structuredName, aliasName);
      this.addressParts = this.fullAddress;
    }
    if (isIgnored()) {
      return false;
    } else if (isMatched()) {

    } else if (Property.hasValue(this.addressParts) && !".".equals(this.addressParts)) {
      if (this.fullAddress.startsWith("ABC-")) {
        this.fullAddress = "A~C" + this.fullAddress.substring(3);
        this.addressParts = this.fullAddress;
        if (isMatched()) {
          return true;
        }
      }

      if (this.fullAddress.contains("?-")) {
        this.fullAddress = this.fullAddress.replace("?-", "-");
        addWarning("[FULL_ADDRESS] contains ? after [UNIT_DESCRIPTOR]");
        this.addressParts = this.fullAddress;
        if (isMatched()) {
          return true;
        }
      }

      fixSpecificAddresses();

      fixFullAddress(localityName);
      if (Property.hasValue(this.nameBody)) {

        fixStreetNameDirSuffix();
        fixStreetNameAlias();
        if (!fixStreetType()) {
          return false;
        }
        if (!fixStreetName()) {
          return false;
        }
        if (!fixStreetNameDirPrefix()) {
          return false;
        }
      } else {
        // providerData.addError(sourceSite,"STREET_NAME not specified");
        if (this.addressParts.startsWith(this.streetNumber)) {
          this
            .setStructuredNameAndParts(this.addressParts.substring(this.streetNumber.length() + 1));
          this.addressParts = this.streetNumber;
        } else {
          Debug.noOp();
        }
      }
      if (getCalculatedStreetNumber().equals(this.addressParts)) {
        if (Property.hasValue(this.streetNumberPrefix)) {
          if (hasUnitDescriptor()) {
            Debug.noOp();
          } else {
            this.unitDescriptor = RangeSet.newRangeSet(this.streetNumberPrefix);
            this.streetNumberPrefix = null;
          }
        } else if (Property.hasValue(this.civicNumberSuffix)) {
          if (this.civicNumberSuffix.matches("-[A-Z]")) {
            this.civicNumberSuffix = this.civicNumberSuffix.substring(1);
            addError("CIVIC_NUMBER_SUFFIX includes - prefix");
          } else if (this.civicNumberSuffix.startsWith("-")) {
            this.civicNumberSuffix = this.civicNumberSuffix.substring(1);
            fixCivicNumberSuffixWithHyphen();
          }
        }
      } else {
        Boolean matched = false;
        if (hasUnitDescriptor()) {
          matched = fixUnitDescriptor();
        } else {
          if (Property.hasValue(this.streetNumberPrefix)) {
            if (Property.hasValue(this.civicNumberSuffix)) {
              Debug.noOp();
            } else {
              matched = fixStreetNumberPrefix();
            }
          } else if (Property.hasValue(this.civicNumberSuffix)) {
            matched = fixStreetNumberSuffix();
          } else {
            matched = this.fixStreetNumber();
          }
        }
        if (matched == null) {
          return false;
        } else if (!matched) {
          if (!hasUnitDescriptor()) {
            final String fullAddress = getNewFullAddress();
            if (this.fullAddress.endsWith(fullAddress)) {
              final String prefix = this.fullAddress.substring(0,
                this.fullAddress.length() - fullAddress.length());
              final Pattern pattern = Pattern.compile("(\\d+(-?:\\d+?))[- ]+");
              final Matcher matcher = pattern.matcher(prefix);
              if (matcher.matches()) {
                this.unitDescriptor = RangeSet.newRangeSet(matcher.group(1));
              }
            }
          }
          if (Property.hasValue(this.unitNumber)) {
            final String unitPlusStreet = this.unitNumber.replace('~', '-') + " "
              + this.streetNumber;
            if (this.addressParts.equals(unitPlusStreet)) {
              addWarning("UNIT_NUMBER_SUFFIX not included in FULL_ADDRESS");
              return true;
            } else {
              try {
                final RangeSet range = RangeSet.newRangeSet(this.unitNumber);
                final String unitRanglePlusStreet = range.getFrom() + "-" + range.getTo() + " "
                  + this.streetNumber;
                if (this.addressParts.equals(unitRanglePlusStreet)) {
                  addWarning(
                    "UNIT_NUMBER is list and UNIT_NUMBER_SUFFIX not included in FULL_ADDRESS");
                  return true;
                }
              } catch (final Exception e) {
              }
            }
          }
          if (this.addressParts.replaceAll(" ", "")
            .equals(getSimplifiedUnitDescriptor() + "-" + this.streetNumber)) {
            return true;
          } else if (("A&B-" + this.streetNumber).equals(this.addressParts)
            && this.unitDescriptor.toString().equals("A~B")) {
            return true;
          } else if (("A/B-" + this.streetNumber).equals(this.addressParts)
            && this.unitDescriptor.toString().equals("A~B")) {
            return true;
          } else if (this.addressParts.startsWith("-") && this.addressParts.substring(1)
            .equals(getSimplifiedUnitDescriptor() + "-" + this.streetNumber)) {
            return true;
          } else {
            throw new IgnoreSiteException(
              AbstractSiteConverter.IGNORED_FULL_ADDRESS_NOT_EQUAL_PARTS, true);
          }
        }
      }
      if (Property.hasValue(this.streetNumberPrefix)) {
        if (hasUnitDescriptor()) {
          Debug.noOp();
        } else {
          this.unitDescriptor = RangeSet.newRangeSet(this.streetNumberPrefix);
          this.streetNumberPrefix = null;
        }
      }
    } else {
      addWarning("FULL_ADDRESS not specified");
    }

    return true;
  }

  private void fixSpecificAddresses() {
    if (this.addressParts.endsWith("SUMMIT AVENUESUMMIT AVENUE")) {
      this.addressParts = this.addressParts.replace("SUMMIT AVENUESUMMIT AVENUE", "SUMMIT AVENUE");
      addError("FULL_ADDRESS contains STREET_NAME twice");
    } else if (this.addressParts.endsWith("1911 WOODSIDE BLVD")
      && this.nameBody.equals("1911 WOODSIDE")) {
      if (this.unitNumber == null) {
        this.unitNumber = this.streetNumber;
        this.streetNumber = "1911";
        this.nameBody = "WOODSIDE";
        addError("STREET_NAME contains STREET_NUMBER, STREET_NUMBER is UNIT_NUMBER");
      }
    } else if (this.addressParts.equals("10520 MCDONALD PARK")) {
      if ("-1052".equals(this.civicNumberSuffix) && Property.hasValue(this.streetNumber)) {
        this.unitNumber = this.streetNumber;
        this.streetNumber = "10520";
        this.civicNumberSuffix = null;
        addError(
          "UNIT_NUMBER in STREET_NUMBER, STREET_NUMBER in CIVIC_NUMBER_SUFFIX and was truncated");
      }
    } else if (this.addressParts.equals("10500 MCDONALD PARK")) {
      if (("-1050".equals(this.civicNumberSuffix) || "- 105".equals(this.civicNumberSuffix))
        && Property.hasValue(this.streetNumber)) {
        this.unitNumber = this.streetNumber;
        this.streetNumber = "10500";
        this.civicNumberSuffix = null;
        addError(
          "UNIT_NUMBER in STREET_NUMBER, STREET_NUMBER in CIVIC_NUMBER_SUFFIX and was truncated");
      }
    }
  }

  private boolean fixStreetName() {
    if (this.nameBody.endsWith(" " + this.nameSuffixCode)) {
      // Remove duplicated suffix from nameBody
      this.nameBody = this.nameBody.substring(0,
        this.nameBody.length() - this.nameSuffixCode.length() - 1);
      if (this.addressParts.endsWith(" " + this.nameSuffixCode)) {
        this.addressParts = this.addressParts.substring(0,
          this.addressParts.length() - this.nameSuffixCode.length() - 1);
      }

      addWarning("STREET_NAME ends with STREET_TYPE");
    }
    for (final String streetTypeAlias : Maps.get(ADDRESS_BC_STREET_TYPE_ALIAS, this.nameSuffixCode,
      Collections.emptyList())) {
      if (this.nameBody.endsWith(" " + streetTypeAlias)) {
        this.nameBody = this.nameBody.substring(0,
          this.nameBody.length() - streetTypeAlias.length() - 1);
        if (this.addressParts.endsWith(" " + streetTypeAlias)) {
          this.addressParts = this.addressParts.substring(0,
            this.addressParts.length() - streetTypeAlias.length() - 1);
        }
        addWarning("STREET_NAME ends with STREET_TYPE alias");
      }
    }
    if (this.addressParts.endsWith(this.nameBody)) {
    } else {
      boolean error = false;
      final int spaceIndex = this.addressParts.lastIndexOf(' ');
      if (spaceIndex == -1) {
        error = true;
      } else {
        final String shortendedFullAddress = this.addressParts.substring(0, spaceIndex);
        if (shortendedFullAddress.endsWith(this.nameBody)) {
          this.nameBody += this.addressParts.substring(spaceIndex);
          addError("FULL_ADDRESS has extra STREET_NAME component");
        } else {
          if (getCalculatedStreetNumber().equals(shortendedFullAddress)) {
            setAliasName(getStructuredName());
            this.nameBody = this.addressParts.substring(spaceIndex + 1);

            addError(
              "FULL_ADDRESS is different from STREET_NAME etc. fields . Using STREET_NAME etc. fields in secondary name.");
          } else {
            error = true;
          }
        }
      }
      if (error) {
        addError("Ignore STREET_NAME is different from parts");
        return false;
      }
    }
    if (this.addressParts.equals(this.nameBody)) {
      this.addressParts = "";
    } else if (this.nameBody.length() > 0) {
      this.addressParts = Strings.removeFromEnd(this.addressParts, this.nameBody.length());
      if (this.addressParts.endsWith(" ")) {
        this.addressParts = Strings.removeFromEnd(this.addressParts, 1);
      } else {
        addError("FULL_ADDRESS missing space before STREET_NAME");
      }
    }
    return true;
  }

  private void fixStreetNameAlias() {
    if (this.nameBody.endsWith(")")) {
      final Matcher aliasBracketMatcher = Pattern.compile("(.*) \\(([^)]+)\\)")
        .matcher(this.nameBody);
      if (aliasBracketMatcher.matches()) {
        this.nameBody = aliasBracketMatcher.group(1);
        final String aliasName = aliasBracketMatcher.group(2);
        if ("PMT".equals(aliasName)) {
          addError("STREET_NAME ends with (PMT)");
        } else {
          this.setAliasName(aliasName);
          addError("STREET_NAME has suffix in (...). Using suffix as secondary name.");
        }
      } else {
        addError("STREET_NAME ending with ) " + this.nameBody);
      }
    } else if (this.nameBody.endsWith("(PDR")) {
      addError("STREET_NAME=RD (PDR is not valid");
      this.nameBody = this.nameBody.replaceAll("\\s*RD\\s*\\(PDR$", "");
    }
    if (this.addressParts.endsWith(")")) {
      final Matcher pdrMatcher = Pattern.compile("(.*)\\s*\\((PDR \\d+)\\)")
        .matcher(this.addressParts);
      if (pdrMatcher.matches()) {
        this.addressParts = pdrMatcher.group(1).trim();
        if (!Property.hasValue(this.nameBody)) {
          this.nameBody = Strings
            .lastPart(this.addressParts.replaceAll(" " + this.nameSuffixCode + "$", ""), ' ');
        }
        this.setAliasName(pdrMatcher.group(2));
        addError("FULL_ADDRESS has suffix in (...). Using suffix as secondary name.");
      }
    }
  }

  private boolean fixStreetNameDirPrefix() {
    if (Property.hasValue(this.prefixNameDirectionCode)) {
      if (!DIRECTIONS.contains(this.prefixNameDirectionCode)) {
        final String lastWord = Strings.lastPart(this.addressParts, ' ');
        if (lastWord.equals(this.prefixNameDirectionCode)
          || lastWord.startsWith(this.prefixNameDirectionCode)) {
          this.prefixNameDirectionCode = null;
          this.nameBody = lastWord + ' ' + this.nameBody;
          this.addressParts = Strings.removeFromEnd(this.addressParts, lastWord.length() + 1);
          addError("STREET_DIR_PREFIX should have been in STREET_NAME");
        } else if (this.nameBody.equals(this.prefixNameDirectionCode)) {
          this.prefixNameDirectionCode = null;
          addError("STREET_DIR_PREFIX same as STREET_NAME");
        } else if (this.addressParts
          .equals(this.prefixNameDirectionCode + "-" + this.streetNumber)) {
          this.unitNumber = this.prefixNameDirectionCode;
          this.addressParts = this.streetNumber;
          this.prefixNameDirectionCode = null;
          addError("STREET_DIR_PREFIX should have been in UNIT_NUMBER");
        } else {
          addError("Ignore STREET_DIR_PREFIX=" + this.prefixNameDirectionCode + " invalid");
          return false;
        }
      } else if (this.addressParts.endsWith(" " + this.prefixNameDirectionCode)) {
        this.addressParts = Strings.removeFromEnd(this.addressParts,
          this.prefixNameDirectionCode.length() + 1);
      } else {
        addError("Ignore STREET_DIR_PREFIX not same as FULL_ADDRESS");
        return false;
      }
    } else {
      final int spaceIndex = this.addressParts.lastIndexOf(' ');
      if (spaceIndex != -1) {
        final String lastWord = this.addressParts.substring(spaceIndex + 1);
        if (DIRECTIONS.contains(lastWord)) {
          this.prefixNameDirectionCode = lastWord;

          this.addressParts = this.addressParts.substring(0, spaceIndex);
          if (lastWord.equals(this.civicNumberSuffix)) {
            this.civicNumberSuffix = null;
            addError("CIVIC_NUMBER_SUFFIX should be in STREET_DIR_PREFIX");
          } else {
            addError("STREET_DIR_PREFIX missing");
          }
        }
      }
    }
    return true;
  }

  private void fixStreetNameDirSuffix() {
    if (Property.hasValue(this.suffixNameDirectionCode)) {
      if (this.addressParts.endsWith(" " + this.suffixNameDirectionCode)) {
        final int endIndex = this.addressParts.length() - this.suffixNameDirectionCode.length() - 1;
        this.addressParts = this.addressParts.substring(0, endIndex);
      } else {
        final String[] parts = this.addressParts.split(" ");
        if (parts.length > 1) {
          final String secondToLastWord = parts[parts.length - 2];
          if (secondToLastWord.equals(this.suffixNameDirectionCode)) {
            final String lastWord = parts[parts.length - 1];
            this.addressParts = this.addressParts
              .replace(this.suffixNameDirectionCode + " " + lastWord, lastWord);
          } else {
            // TODO Missing dir
            Debug.noOp();
          }
        } else {
          Debug.noOp();
        }
      }
    } else {
      final int spaceIndex = this.addressParts.lastIndexOf(' ');
      if (spaceIndex != -1) {
        final String lastWord = this.addressParts.substring(spaceIndex + 1);
        if (DIRECTIONS.contains(lastWord)) {
          if (lastWord.equals(this.prefixNameDirectionCode)) {
            this.suffixNameDirectionCode = this.prefixNameDirectionCode;
            this.prefixNameDirectionCode = null;
            this.addError(
              "FULL_ADDRESS has direction suffix, in STREET_DIR_PREFIX by mistake where: STREET_DIR_PREFIX=direction suffix, STREET_DIR_SUFFIX=null");
          } else {
            this.suffixNameDirectionCode = lastWord;
            this.addressParts = this.addressParts.substring(0, spaceIndex);
            if (lastWord.equals(this.civicNumberSuffix)) {
              this.civicNumberSuffix = null;
              if (ProviderSitePointConverter.getPartnerOrganizationShortName()
                .equals("PRINCE RUPERT")) {
                addWarning("CIVIC_NUMBER_SUFFIX should be in STREET_DIR_SUFFIX");
              } else {
                addError("CIVIC_NUMBER_SUFFIX should be in STREET_DIR_SUFFIX");
              }
            } else {
              addError("FULL_ADDRESS has extra direction suffix where: STREET_DIR_SUFFIX=null");
            }
          }
        }
      }
    }
  }

  public Boolean fixStreetNumber() {
    if (this.addressParts.matches("\\d+ " + this.streetNumber)) {
      // [UNIT] [NUMBER]
      this.unitNumber = Strings.firstPart(this.addressParts, ' ');
      addWarning(AbstractSiteConverter.FULL_ADDRESS_HAS_EXTRA_UNIT_DESCRIPTOR);
      return true;
    } else if (this.addressParts.matches("\\d+\\s*-\\s*" + this.streetNumber)) {
      // [UNIT]-[NUMBER]
      this.unitNumber = Strings.firstPart(this.addressParts, '-');
      addWarning(AbstractSiteConverter.FULL_ADDRESS_HAS_EXTRA_UNIT_DESCRIPTOR);
      return true;
    } else if (this.addressParts.matches("[A-Z] " + this.streetNumber)) {
      // [UNIT_NUMBER] [NUMBER]
      this.unitNumber = Strings.firstPart(this.addressParts, ' ');
      addError("FULL_ADDRESS has extra UNIT_NUMBER_SUFFIX");
      return true;
    } else if (this.addressParts.matches("[A-Z]-" + this.streetNumber)) {
      // [UNIT_NUMBER]-[NUMBER]
      this.unitNumber = Strings.firstPart(this.addressParts, '-');
      addError("FULL_ADDRESS has extra UNIT_NUMBER_SUFFIX");
      return true;
    } else if (this.addressParts.matches("[A-Z]\\d+ " + this.streetNumber)) {
      // [UNIT] [NUMBER]
      this.unitNumber = Strings.firstPart(this.addressParts, ' ');
      addWarning(AbstractSiteConverter.FULL_ADDRESS_HAS_EXTRA_UNIT_DESCRIPTOR);
      return true;
    } else if (this.addressParts.matches("\\d+[A-Z] " + this.streetNumber)) {
      // [UNIT][UNIT_NUMBER] [NUMBER]
      final String firstPart = Strings.firstPart(this.addressParts, ' ');
      this.unitNumber = firstPart.substring(0, firstPart.length() - 1);
      this.unitNumber = firstPart.substring(firstPart.length() - 1);
      addError("FULL_ADDRESS has extra UNIT_NUMBER & UNIT_NUMBER_SUFFIX");
      return true;
    } else if (this.addressParts.matches("\\d+[A-Z]-" + this.streetNumber)) {
      // [UNIT_NUMBER]-[NUMBER]
      this.unitNumber = Strings.firstPart(this.addressParts, '-');
      addError("FULL_ADDRESS has extra  UNIT_NUMBER & UNIT_NUMBER_SUFFIX");
      return true;
    } else if (this.addressParts.matches(this.streetNumber + " \\d+")) {
      // [NUMBER] [UNIT]
      this.unitNumber = Strings.lastPart(this.addressParts, ' ');
      addWarning(AbstractSiteConverter.FULL_ADDRESS_HAS_EXTRA_UNIT_DESCRIPTOR);
      return true;
    } else if (this.addressParts.matches(this.streetNumber + "[A-Z][0-9A-Z]+")) {
      // [NUMBER][UNIT]
      this.unitNumber = this.addressParts.substring(this.streetNumber.length());
      addWarning(AbstractSiteConverter.FULL_ADDRESS_HAS_EXTRA_UNIT_DESCRIPTOR);
      return true;
    } else if (this.addressParts.matches(this.streetNumber + "/\\d+")) {
      // [NUMBER]/[UNIT]
      this.unitNumber = Strings.lastPart(this.addressParts, '/');
      addWarning(AbstractSiteConverter.FULL_ADDRESS_HAS_EXTRA_UNIT_DESCRIPTOR);
      return true;
    } else if (this.addressParts.matches(this.streetNumber + "-\\d+")) {
      // [NUMBER]-[UNIT]
      this.unitNumber = Strings.lastPart(this.addressParts, '-');
      addWarning(AbstractSiteConverter.FULL_ADDRESS_HAS_EXTRA_UNIT_DESCRIPTOR);
      return true;
    } else if (this.addressParts.matches(this.streetNumber + "[A-Z]")) {
      // [NUMBER][NUMBER_SUFFIX]
      this.civicNumberSuffix = this.addressParts.substring(this.streetNumber.length());
      addError(AddressBcSite.FULL_ADDRESS_HAS_EXTRA_CIVIC_NUMBER_SUFFIX);
      return true;
    } else if (this.addressParts.matches(this.streetNumber + " [A-Z]")) {
      // [NUMBER][NUMBER_SUFFIX]
      this.civicNumberSuffix = this.addressParts.substring(this.streetNumber.length() + 1);
      addError(AddressBcSite.FULL_ADDRESS_HAS_EXTRA_CIVIC_NUMBER_SUFFIX);
      return true;
    } else if (this.addressParts.matches(this.streetNumber + "-[A-Z]")) {
      // [NUMBER]-[NUMBER_SUFFIX]
      this.civicNumberSuffix = this.addressParts.substring(this.streetNumber.length() + 1);
      addError(AddressBcSite.FULL_ADDRESS_HAS_EXTRA_CIVIC_NUMBER_SUFFIX);
      return true;
    } else if (this.addressParts.matches("\\d+-\\d+ " + this.streetNumber)) {
      // [UNIT]-[NUMBER]
      this.unitNumber = Strings.firstPart(this.addressParts, ' ');
      addError("FULL_ADDRESS has extra UNIT_NUMBER range");
      return true;
    } else if (this.addressParts.matches(this.streetNumber + "[A-Z] \\d+")) {
      // [UNIT][UNIT_NUMBER] [NUMBER]
      this.unitNumber = this.streetNumber
        + Strings.firstPart(this.addressParts, ' ').substring(this.unitNumber.length());
      this.streetNumber = Strings.lastPart(this.addressParts, ' ');
      addError(
        "STREET_NUMBER was UNIT_NUMBER, FULL_ADDRESS had extra UNIT_NUMBER_SUFFIX and STREET_NUMBER");
      return true;
    } else if (this.addressParts.matches("\\d+-" + this.streetNumber + "[A-Z]")) {
      // [UNIT] [NUMBER][NUMBER_SUFFIX]
      this.civicNumberSuffix = Strings.lastPart(this.addressParts, '-')
        .substring(this.streetNumber.length());
      this.unitNumber = Strings.firstPart(this.addressParts, '-');
      addError("FULL_ADDRESS had extra UNIT_NUMBER and CIVIC_NUMBER_SUFFIX");
      return true;
    } else if (this.addressParts.equals("")) {
      if (this.streetNumber.equals("0")) {
        this.streetNumber = null;
        addError("STREET_NUMBER=0 with no number in FULL_ADDRESS. Street number must not be 0.");
        return true;
      } else {
        addError("FULL_ADDRESS does not include the STREET_NUMBER.");
        return true;
      }
    } else if (this.streetNumber.length() == 8 && this.addressParts
      .equals(this.streetNumber.substring(0, 4) + "-" + this.streetNumber.substring(4))) {
      this.streetNumber = this.addressParts;
      addError("STREET_NUMBER missing - between range of numbers");
      return true;
    } else if (this.addressParts.matches("\\d+") && this.streetNumber.matches("\\d+")) {
      addError("Ignore STREET_NUMBER different from FULL_ADDRESS");
      return null;
    } else if (this.addressParts.equals(this.streetNumber + ".")) {
      addError("Ignore FULL_ADDRESS has . after STREET_NUMBER");
      return null;
    } else {
      return false;
    }
  }

  private Boolean fixStreetNumberPrefix() {
    if (this.addressParts.equals(this.streetNumberPrefix + this.streetNumber)) {
      this.unitDescriptor = RangeSet.newRangeSet(this.streetNumberPrefix);
      this.streetNumberPrefix = null;
      // [NUMBER_PREFIX][NUMBER]
      return true;
    } else if (this.addressParts.equals(this.streetNumberPrefix + "-" + this.streetNumber)) {
      this.unitDescriptor = RangeSet.newRangeSet(this.streetNumberPrefix);
      this.streetNumberPrefix = null;
      // [NUMBER_PREFIX]-[NUMBER]
      return true;
    } else if (this.addressParts
      .equals(this.streetNumberPrefix + "-" + this.streetNumberPrefix + " " + this.streetNumber)) {
      this.addressParts = this.streetNumberPrefix + " " + this.streetNumber;
      this.unitDescriptor = RangeSet.newRangeSet(this.streetNumberPrefix);
      this.streetNumberPrefix = null;
      // [NUMBER_PREFIX]-[NUMBER_PREFIX] [NUMBER]
      return true;
    } else if (this.addressParts.equals(this.streetNumberPrefix + " - " + this.streetNumber)) {
      this.unitDescriptor = RangeSet.newRangeSet(this.streetNumberPrefix);
      this.streetNumberPrefix = null;
      // [NUMBER_PREFIX] - [NUMBER]
      return true;
    } else if (this.addressParts.equals(this.streetNumberPrefix + "- " + this.streetNumber)) {
      this.unitDescriptor = RangeSet.newRangeSet(this.streetNumberPrefix);
      this.streetNumberPrefix = null;
      // [NUMBER_PREFIX]- [NUMBER]
      return true;
    } else if (this.addressParts.equals(this.streetNumber + this.streetNumberPrefix)) {
      // [NUMBER][NUMBER_PREFIX]
      this.civicNumberSuffix = this.streetNumberPrefix;
      this.streetNumberPrefix = null;
      addError("STREET_NUMBER_PREFIX is CIVIC_NUMBER_SUFFIX in FULL_ADDRESS");
      return true;
    } else if (this.addressParts.equals(this.streetNumber + " " + this.streetNumberPrefix)) {
      // [NUMBER] [NUMBER_PREFIX]
      this.civicNumberSuffix = this.streetNumberPrefix;
      this.streetNumberPrefix = null;
      addError("STREET_NUMBER_PREFIX is CIVIC_NUMBER_SUFFIX in FULL_ADDRESS");
      return true;
    } else if (this.addressParts.equals(this.streetNumber)) {
      // [NUMBER]
      this.unitDescriptor = RangeSet.newRangeSet(this.streetNumberPrefix);
      this.streetNumberPrefix = null;
      addError("FULL_ADDRESS missing STREET_NUMBER_PREFIX");
      return true;
    } else if (this.addressParts.matches(this.streetNumberPrefix + " \\d+")) {
      addError("Ignore STREET_NUMBER different from FULL_ADDRESS");
      return null;
    } else {
      return false;
    }
  }

  private Boolean fixStreetNumberSuffix() {
    final String fullAddressWithoutSpaces = this.addressParts.replace(" ", "");
    final String civicNumberSuffixWithoutSpaces = this.civicNumberSuffix.replace(" ", "");
    if (Integers.isInteger(civicNumberSuffixWithoutSpaces) && fullAddressWithoutSpaces
      .equals(this.streetNumber + " " + civicNumberSuffixWithoutSpaces)) {
      this.unitDescriptor = RangeSet.newRangeSet(civicNumberSuffixWithoutSpaces);
      this.civicNumberSuffix = null;
      return true;
    } else if (civicNumberSuffixWithoutSpaces.startsWith("&")
      && fullAddressWithoutSpaces.equals(this.streetNumber + civicNumberSuffixWithoutSpaces)) {
      this.civicNumberRange = this.streetNumber + "," + civicNumberSuffixWithoutSpaces;
      this.streetNumber = null;
      this.civicNumberSuffix = null;
      return true;
    } else if (civicNumberSuffixWithoutSpaces.equals("A&B")
      && fullAddressWithoutSpaces.equals(this.streetNumber + "A-" + this.streetNumber + "B")) {
      this.civicNumberSuffix = "A~B";
      addError("CIVIC_NUMBER_SUFFIX=A&B FULL_ADDRESS uses [NUMBER]A-[NUMBER]B");
      return true;
    } else if (civicNumberSuffixWithoutSpaces.equals("A&B&C") && fullAddressWithoutSpaces
      .equals(this.streetNumber + "A-" + this.streetNumber + "B-" + this.streetNumber + "C")) {
      this.civicNumberSuffix = "A~C";
      addError("CIVIC_NUMBER_SUFFIX=A&B&C FULL_ADDRESS uses [NUMBER]A-[NUMBER]C");
      return true;
    } else if (fullAddressWithoutSpaces.matches("\\d+(,\\d+\\w?)+")
      && fullAddressWithoutSpaces.startsWith(this.streetNumber + civicNumberSuffixWithoutSpaces)) {
      this.civicNumberSuffix = fullAddressWithoutSpaces.substring(this.streetNumber.length() + 1);
      addError("CIVIC_NUMBER_SUFFIX truncated");
      return true;
    } else if (this.civicNumberSuffix.equals("ACCES")
      && this.addressParts.equals(this.streetNumber + " ACCESS")) {
      this.addressParts = this.streetNumber;
      this.civicNumberSuffix = null;
      addError("CIVIC_NUMBER_SUFFIX ignored");
      return true;
    } else if (this.civicNumberSuffix.equals("PUMP")
      && this.addressParts.equals(this.streetNumber + " PUMP STA")) {
      this.addressParts = this.streetNumber;
      this.civicNumberSuffix = null;
      addError("CIVIC_NUMBER_SUFFIX ignored");
      return true;
    } else if (this.civicNumberSuffix.startsWith("-") || this.civicNumberSuffix.startsWith(",")
      || this.civicNumberSuffix.startsWith("/")) {
      final char character = this.civicNumberSuffix.charAt(0);
      this.civicNumberSuffix = this.civicNumberSuffix.substring(1);
      if (this.streetNumber.equals(civicNumberSuffixWithoutSpaces.substring(1))
        && this.streetNumber.equals(this.addressParts)) {
        this.civicNumberSuffix = null;
        addError("CIVIC_NUMBER_SUFFIX starts with " + character + ", is same as STREET_NUMBER");
        return true;
      } else {
        final Boolean x = fixCivicNumberSuffixWithHyphen();
        if (x != null) {
          return x;
        }
        if (this.streetNumber.equals(this.addressParts)) {
          this.civicNumberSuffix = this.civicNumberSuffix.substring(1);
          addError("FULL_ADDRESS missing CIVIC_NUMBER_SUFFIX which starts with " + character);
          return true;
        } else if (fullAddressWithoutSpaces
          .equals(this.streetNumber + civicNumberSuffixWithoutSpaces)) {
          this.civicNumberSuffix = this.civicNumberSuffix.substring(1);
          addError("CIVIC_NUMBER_SUFFIX starts with " + character);
          return true;
        } else if (fullAddressWithoutSpaces.substring(0, fullAddressWithoutSpaces.length() - 1)
          .equals(this.streetNumber + civicNumberSuffixWithoutSpaces)) {
          this.civicNumberSuffix = Strings.lastPart(fullAddressWithoutSpaces, character);
          addError("CIVIC_NUMBER_SUFFIX is truncated");
          return true;
        } else if (fullAddressWithoutSpaces.substring(0, fullAddressWithoutSpaces.length() - 2)
          .equals(this.streetNumber + civicNumberSuffixWithoutSpaces)) {
          this.civicNumberSuffix = Strings.lastPart(fullAddressWithoutSpaces, character);
          addError("CIVIC_NUMBER_SUFFIX is truncated");
          return true;
        } else {
          return false;
        }
      }
    } else if (this.civicNumberSuffix.startsWith("(")) {
      if (this.civicNumberSuffix.equals("(VILL") && this.addressParts.endsWith("(VILLAS)")) {
        this.civicNumberSuffix = "VILLAS";
        addError("CIVIC_NUMBER_SUFFIX ignored");
        return true;
      } else if (this.civicNumberSuffix.equals("(CLUB")
        && this.addressParts.endsWith("(CLUB HOUSE)")) {
        this.civicNumberSuffix = "CLUB HOUSE";
        addError("CIVIC_NUMBER_SUFFIX ignored");
        return true;
      } else if (this.addressParts.equals(this.streetNumber + " " + this.civicNumberSuffix + ")")) {
        this.civicNumberSuffix = this.civicNumberSuffix.substring(1);
        addError("CIVIC_NUMBER_SUFFIX starts with (");
        return true;
      } else {
        return false;
      }
    } else if (this.civicNumberSuffix.matches("\\w-\\d+")
      && this.addressParts.substring(0, this.addressParts.length() - 1)
        .equals(this.streetNumber + " " + this.civicNumberSuffix)) {
      this.unitNumber = this.streetNumber + this.civicNumberSuffix.substring(0, 1);
      this.streetNumber = this.civicNumberSuffix.substring(2)
        + this.addressParts.substring(this.addressParts.length() - 1);
      this.civicNumberSuffix = null;
      addError(
        "STREET_NUMBER is UNIT_NUMBER, CIVIC_NUMBER_SUFFIX is UNIT_NUMBER_SUFFIX-STREET_NUMBER (truncated)");
      return true;
    } else if (this.addressParts.equals(this.streetNumber + this.civicNumberSuffix)) {
      // [NUMBER][NUMBER_SUFFIX]
      return true;
    } else if (this.addressParts.matches("\\d+ " + this.streetNumber + this.civicNumberSuffix)) {
      // [UNIT] [NUMBER][NUMBER_SUFFIX]
      this.unitNumber = Strings.firstPart(this.addressParts, ' ');
      addError(AbstractSiteConverter.FULL_ADDRESS_HAS_EXTRA_UNIT_DESCRIPTOR);
      return true;
    } else if (this.addressParts.equals(this.streetNumber + " " + this.civicNumberSuffix)) {
      // [NUMBER] [NUMBER_SUFFIX]
      return true;
    } else if (this.addressParts.equals(this.streetNumber + "-" + this.civicNumberSuffix)) {
      // [NUMBER] [NUMBER_SUFFIX]
      return true;
    } else if (this.addressParts.equals(this.streetNumber)) {
      // [NUMBER]
      addError("FULL_ADDRESS missing CIVIC_NUMBER_SUFFIX");
      return true;
    } else if (this.addressParts.equals(this.civicNumberSuffix + "-" + this.streetNumber)) {
      // [NUMBER_SUFFIX]-[NUMBER]
      addError("CIVIC_NUMBER_SUFFIX is STREET_NUMBER_PREFIX in FULL_ADDRESS");
      return true;
    } else {
      return false;
    }
  }

  private boolean fixStreetType() {
    if (isMatched()) {
      return true;
    } else if (this.addressParts.endsWith(" " + this.nameSuffixCode)) {
      this.addressParts = Strings.removeFromEnd(this.addressParts,
        this.nameSuffixCode.length() + 1);
      if (this.addressParts.endsWith(" " + this.nameSuffixCode)) {
        this.addressParts = Strings.removeFromEnd(this.addressParts,
          this.nameSuffixCode.length() + 1);
        addWarning("FULL_ADDRESS has repeated STREET_TYPE");
      }
    } else {
      boolean matched = false;
      if ("WAY".equals(this.nameSuffixCode) && this.addressParts.endsWith("WAY")) {
        this.nameSuffixCode = null;
        addWarning("STREET_TYPE=WAY not required for STREET_NAME ending in WAY");
      }
      if ("BLVD".equals(this.nameSuffixCode) && this.nameBody.equals("BOULEVARD")) {
        this.nameSuffixCode = null;
        addWarning("STREET_TYPE=BLVD not required for STREET_NAME=BOULEVARD");
      }
      if ("HWY".equals(this.nameSuffixCode)) {
        final Matcher hwyMatcher = Pattern.compile(".* ((?:OLD )?(?:HIGHWAY|HWY)) (\\d+[A-Z]?)")
          .matcher(this.addressParts);
        if (hwyMatcher.matches()) {
          this.nameSuffixCode = null;
          final String highwayPrefix = hwyMatcher.group(1);
          final String highwayRoute = hwyMatcher.group(2);
          final String highwayName = highwayPrefix + " " + highwayRoute;
          if (this.nameBody.equals("OLD")) {
            this.nameBody = "OLD " + highwayName;
            addError("STREET_NAME=OLD should be OLD HWY [NUMBER]");
          } else if (this.nameBody.equals("HWY")) {
            this.nameBody = highwayName;
            addError("STREET_NAME=HWY should be HWY [NUMBER]");
          } else if (this.nameBody.equals("HIGHWAY")) {
            this.nameBody = highwayName;
            addError("STREET_NAME=HIGHWAY should be HIGHWAY [NUMBER]");
          } else if (this.nameBody.equals(highwayName)) {
            addWarning("STREET_TYPE=HWY not required for STREET_NAME starting in " + highwayPrefix);
          } else if (this.nameBody.equals("HIGHWAY " + highwayRoute)) {
            this.nameBody = highwayName;
            addError("STREET_NAME=HIGHWAY [NUMBER] should be " + highwayPrefix + " [NUMBER]");
          } else if (this.nameBody.equals("HWY " + highwayRoute)) {
            this.nameBody = highwayName;
            addError("STREET_NAME=HWY [NUMBER] should be " + highwayPrefix + " [NUMBER]");
          } else if (this.nameBody.equals("NO " + highwayRoute)) {
            this.nameBody = highwayName;
            addError("STREET_NAME=NO [NUMBER] should be STREET_NAME=[NUMBER]");
          } else {
            this.setAliasName(this.nameBody + " HWY");
            addError("FULL_ADDRESS is different from STREET_NAME. Using STREET_NAME in an alias.");
          }
        } else if (this.nameBody.startsWith("NO ")) {
          final Matcher streetMatcher = Pattern.compile(".* (\\d+) (ST|AVE)")
            .matcher(this.addressParts);
          if (streetMatcher.matches()) {
            matched = true;
            this.setAliasName("HWY " + this.nameBody.substring(3));
            this.nameBody = streetMatcher.group(1);
            this.nameSuffixCode = streetMatcher.group(2);
            final int endIndex = this.addressParts.length() - this.nameSuffixCode.length() - 1;
            this.addressParts = this.addressParts.substring(0, endIndex);

            addError(
              "FULL_ADDRESS is different from STREET_NAME etc. fields. Using STREET_NAME etc. fields in secondary name.");
          }
        }
      }

      if (this.addressParts.endsWith(this.nameBody + "" + this.nameSuffixCode)) {
        final int endIndex = this.addressParts.length() - this.nameSuffixCode.length();
        this.addressParts = this.addressParts.substring(0, endIndex);
        addError("FULL_ADDRESS missing space between STREET_NAME and STREET_TYPE");
      } else if (this.nameSuffixCode != null && !matched) {
        String matchedType = null;
        for (final String streetTypeAlias : Maps.get(ADDRESS_BC_STREET_TYPE_ALIAS,
          this.nameSuffixCode, Collections.emptyList())) {
          if (this.addressParts.endsWith(" " + streetTypeAlias)) {
            matchedType = streetTypeAlias;
            final int endIndex = this.addressParts.length() - streetTypeAlias.length() - 1;
            this.addressParts = this.addressParts.substring(0, endIndex);
          }
        }
        if (matchedType == null) {
          if (this.addressParts.endsWith(" " + this.nameBody)) {
            addError("FULL_ADDRESS does not include STREET_TYPE.");
          } else {
            final Matcher streetTypeMatcher = Pattern
              .compile(".* " + this.nameBody.replace("(", "\\(") + " (\\w+)")
              .matcher(this.addressParts);
            if (streetTypeMatcher.matches()) {
              final String oldStreetType = this.nameSuffixCode;
              this.nameSuffixCode = streetTypeMatcher.group(1);
              final int endIndex = this.addressParts.length() - this.nameSuffixCode.length() - 1;
              this.addressParts = this.addressParts.substring(0, endIndex);
              if (oldStreetType.equals("WAY") && this.nameBody.contains("WAY")) {
                addError("STREET_TYPE=WAY not correct");
              } else {
                this.setAliasName(this.nameBody + " " + oldStreetType);
                addError(
                  "FULL_ADDRESS is different from STREET_TYPE. Using STREET_TYPE in secondary name.");
              }
            } else {

              final String streetNumberDescriptor = Strings.toString(" ", this.streetNumberPrefix,
                this.streetNumber, this.civicNumberSuffix);
              if (this.addressParts.equals(streetNumberDescriptor)) {
                this.addressParts += " "
                  + Strings.toString(" ", this.prefixNameDirectionCode, this.nameBody);
              } else if (this.addressParts.startsWith(streetNumberDescriptor + " ")) {
                this.setAliasName(Strings.toString(" ", this.prefixNameDirectionCode, this.nameBody,
                  this.nameSuffixCode, this.suffixNameDirectionCode));
                addError(
                  "FULL_ADDRESS is different from STREET_NAME etc. fields . Using STREET_NAME etc. fields in secondary name.");
                final String[] parts = this.addressParts
                  .substring(streetNumberDescriptor.length() + 1)
                  .split(" ");
                int startIndex = 0;
                if (DIRECTIONS.contains(parts[0])) {
                  this.prefixNameDirectionCode = parts[0];
                  startIndex = 1;
                }
                int endIndex = parts.length - 1;
                if (DIRECTIONS.contains(parts[endIndex])) {
                  this.suffixNameDirectionCode = parts[endIndex];
                  endIndex -= 1;
                }
                this.nameSuffixCode = parts[endIndex];
                final StringBuilder newStreetName = new StringBuilder();
                for (int i = startIndex; i < endIndex; i++) {
                  if (i > startIndex) {
                    newStreetName.append(" ");
                  }
                  newStreetName.append(parts[i]);
                }
                this.nameBody = newStreetName.toString();
                this.addressParts = Strings.toString(" ", streetNumberDescriptor,
                  this.prefixNameDirectionCode, this.nameBody);
              } else {
                this.setAliasName(this.addressParts);
                this.addressParts = getCalculatedFullAddress(this.streetNumberPrefix,
                  streetNumberDescriptor, this.civicNumberSuffix, this.prefixNameDirectionCode,
                  this.nameBody, null, null);
                throw new IgnoreSiteException(
                  AbstractSiteConverter.IGNORED_FULL_ADDRESS_NOT_EQUAL_PARTS, true);
              }
            }
          }
        }
      }
    }
    return true;
  }

  private boolean fixUnitDescriptor() {
    final String unitDescriptor = getSimplifiedUnitDescriptor();
    final String calculatedStreetNumber = getCalculatedStreetNumber();
    if (this.addressParts.startsWith(unitDescriptor)
      && this.addressParts.endsWith(calculatedStreetNumber)) {
      // [UNIT] followed by [NUMBER] with an optional separator
      final String filler = this.addressParts.substring(unitDescriptor.length(),
        this.addressParts.length() - calculatedStreetNumber.length());
      if (filler.isEmpty() || " ".equals(filler) || "-".equals(filler)) {
        return true;
      } else if (filler.matches("[ \\-]+")) {
        return true;
      } else {
        return false;
      }
    } else if (this.addressParts
      .equals(unitDescriptor.replace(" ", "") + "-" + calculatedStreetNumber)) {
      // [UNIT]-[NUMBER]
      return true;
    } else if (this.addressParts.startsWith(calculatedStreetNumber)
      && this.addressParts.endsWith(unitDescriptor)) {
      // [NUMBER] followed by [UNIT] with an optional separator
      final String filler = this.addressParts.substring(calculatedStreetNumber.length(),
        this.addressParts.length() - unitDescriptor.length());
      if (filler.isEmpty() || " ".equals(filler) || "-".equals(filler)) {
        return true;
      } else if (filler.matches("[ \\-]+")) {
        return true;
      } else {
        return false;
      }
    } else if (this.addressParts
      .equals(unitDescriptor.replace(" ", "") + "-" + calculatedStreetNumber)) {
      // [UNIT]-[NUMBER]
      return true;
    } else if (this.addressParts.equals(calculatedStreetNumber + "(" + unitDescriptor + ")")) {
      // [NUMBER]([UNIT])
      return true;
    } else {
      return false;
    }
  }

  public String getAliasName() {
    return this.aliasName;
  }

  private String getCalculatedFullAddress(final String streetNumberPrefix,
    final String streetNumber, final String streetNumberSuffix, final String streetDirPrefix,
    final String streetName, final String streetType, final String streetDirSuffix) {
    return Strings.toString(" ", streetNumberPrefix, streetNumber, streetNumberSuffix,
      streetDirPrefix, streetName, streetType, streetDirSuffix);
  }

  public String getCalculatedStreetNumber() {
    return Strings.toString(" ", this.streetNumberPrefix, this.streetNumber,
      this.civicNumberSuffix);
  }

  public Integer getCivicNumber() {
    if (this.civicNumber == null) {
      if (Property.hasValue(this.streetNumber)) {
        try {
          this.civicNumber = Integer.valueOf(this.streetNumber);
        } catch (final Throwable e) {
          addError("STREET_NUMBER is not numeric, using highest number");
          this.extendedData.put("STREET_NUMBER", this.streetNumber);
          final String[] numberParts = this.streetNumber.split("[^\\d]+");
          for (final String part : numberParts) {
            final int partNumber = Integer.valueOf(part);
            if (this.civicNumber == null || partNumber > this.civicNumber) {
              this.civicNumber = partNumber;
            }
          }
        }
      }
    }
    return this.civicNumber;
  }

  public String getFullAddress() {
    return this.fullAddress;
  }

  public String getNewFullAddress() {
    final String structuredName = getStructuredName();
    Object civicNumber = this.civicNumber;
    if (civicNumber == null) {
      civicNumber = this.streetNumber;
    }
    final String civicNumberString = civicNumber.toString();
    final String fullAddress = SitePoint.getFullAddress(this.unitDescriptor, civicNumberString,
      this.civicNumberSuffix, this.civicNumberRange, structuredName);
    return fullAddress;
  }

  public String getNewFullAddressSpaceBetweenNumberSuffix() {
    final String structuredName = getStructuredName();
    Object civicNumber = this.civicNumber;
    if (civicNumber == null) {
      civicNumber = this.streetNumber;
    }
    final StringBuilder streetAddress = new StringBuilder();
    if (hasUnitDescriptor()) {
      streetAddress.append(this.unitDescriptor);
      if (Property.hasValuesAny(civicNumber.toString(), this.civicNumberSuffix,
        this.civicNumberRange, structuredName)) {
        streetAddress.append('-');
      }
    }
    final boolean hasCivicNumber = Property.hasValue(civicNumber.toString());
    if (hasCivicNumber) {
      streetAddress.append(civicNumber.toString());
    }
    final boolean hasCivicNumberSuffix = Property.hasValue(this.civicNumberSuffix);
    if (hasCivicNumberSuffix) {
      if (hasCivicNumber) {
        streetAddress.append(' ');
      }
      streetAddress.append(this.civicNumberSuffix);
    }
    if (Property.hasValue(this.civicNumberRange)) {
      if (hasCivicNumber || hasCivicNumberSuffix) {
        streetAddress.append(' ');
      }
      streetAddress.append(this.civicNumberRange);
    }
    if (Property.hasValue(structuredName)) {
      if (streetAddress.length() > 0) {
        streetAddress.append(' ');
      }
      streetAddress.append(structuredName);
    }
    final String fullAddress = streetAddress.toString();
    return fullAddress;
  }

  public String getNewStreetAddress() {
    final String structuredName = getStructuredName();
    Object civicNumber = this.civicNumber;
    if (civicNumber == null) {
      civicNumber = this.streetNumber;
    }
    final String fullAddress = SitePoint.getFullAddress(null, civicNumber.toString(),
      this.civicNumberSuffix, this.civicNumberRange, structuredName);
    return fullAddress;
  }

  public String getOriginalFullAddress() {
    return getValue("FULL_ADDRESS");
  }

  public Point getPoint() {
    return this.point;
  }

  private String getSimplifiedUnitDescriptor() {
    return SitePoint.getSimplifiedUnitDescriptor(this.unitDescriptor).replace('~', '-');
  }

  public String getStructuredName() {
    if (Property.hasValue(this.structuredName)) {
      return this.structuredName;
    } else {
      return Strings.toString(" ", this.prefixNameDirectionCode, this.namePrefixCode, this.nameBody,
        this.nameSuffixCode, this.suffixNameDirectionCode);
    }
  }

  public String getValueCleanIntern(final String fieldName) {
    String value = AddressBcSiteConverter.getCleanStringIntern(this, fieldName);
    if (Property.hasValue(value)) {
      if ("-".equals(value)) {
        addError(fieldName + "='-'");
        value = null;
      } else {
        value = value.intern();
      }
    }
    return value;
  }

  private boolean hasUnitDescriptor() {
    return Property.hasValue((Emptyable)this.unitDescriptor);
  }

  public boolean isIgnored() {
    return this.ignored;
  }

  public boolean isMatched() {
    if (this.fullAddress == null) {
      return false;
    } else {
      final String newFullAddress = getNewFullAddress().replace("~", "-");
      if (this.fullAddress.equals(newFullAddress)) {
        return true;
      } else if (this.fullAddress.charAt(0) == '0'
        && newFullAddress.equals(this.fullAddress.substring(1))) {
        addWarning("FULL_ADDRESS starts with 0");
        return true;
      } else if (this.fullAddress.equals(getNewFullAddressSpaceBetweenNumberSuffix())) {
        return true;
      } else {
        final String streetAddress = getNewStreetAddress();
        if (this.fullAddress.equals(streetAddress)) {
          addWarning("FULL_ADDRESS missing UNIT_DESCRIPTOR");
          return true;
        } else if (this.fullAddress.endsWith(streetAddress)) {
          if (hasUnitDescriptor()) {
            String unitPrefix = this.fullAddress.substring(0,
              this.fullAddress.length() - streetAddress.length());
            if (unitPrefix.endsWith(" ") || unitPrefix.endsWith("-")) {
              unitPrefix = unitPrefix.substring(0, unitPrefix.length() - 1);
              if (getSimplifiedUnitDescriptor().equals(unitPrefix)) {
                return true;
              } else if (this.unitDescriptor.toString().replace(',', '-').equals(unitPrefix)) {
                this.fullAddress = this.fullAddress.replace(unitPrefix,
                  this.unitDescriptor.toString());
                addWarning("Set FULL_ADDRESS (e.g. 1-10) from UNIT_DESCRIPTOR (e.g. 10,13)");
                return true;
              }
              try {
                final String rangeString = getSimplifiedUnitDescriptor();
                if (unitPrefix.equals(rangeString)) {
                  addWarning(
                    "FULL_ADDRESS has range (e.g. 1~10) but UNIT_DESCRIPTOR has ranges (e.g. 101~104,201~204)");
                  return true;
                }
              } catch (final Exception e) {
              }
              if (unitPrefix.equals(this.unitNumber)) {
                this.unitDescriptor = RangeSet.newRangeSet(unitPrefix);
                return true;
              } else {
                Debug.noOp();
              }
            }
          }
        }
        return false;
      }
    }
  }

  public boolean isStructuredNameExists() {
    final String structuredName = getStructuredName();
    final Identifier structuredNameId = AbstractSiteConverter.STRUCTURED_NAMES
      .getIdentifier(structuredName);
    return structuredNameId != null;
  }

  private String replaceLocalities(String fullAddress) {
    for (final String localitySuffix : ADDRESS_BC_LOCALITY_SUFFIXES) {
      if (fullAddress.endsWith(localitySuffix)) {
        fullAddress = fullAddress.replaceAll(" " + localitySuffix + "$", "");
        return fullAddress;
      }
    }
    if (fullAddress.endsWith(")")) {
      final Matcher matcher = Pattern.compile("(.*) (\\(\\w+\\)*)$").matcher(fullAddress);
      if (matcher.matches()) {
        addWarning("FULL_ADDRESS ends with locality");
        return matcher.group(1);
      }
    }
    return fullAddress;
  }

  public void setAliasName(final String aliasName) {
    this.aliasName = aliasName;
  }

  public void setIgnored(final boolean ignored) {
    this.ignored = ignored;
  }

  public void setStructuredName(final String structuredName) {
    this.structuredName = structuredName;
  }

  public void setStructuredNameAndParts(final String structuredName) {
    this.structuredName = structuredName;
    final Record name = AbstractSiteConverter.STRUCTURED_NAMES.getStructuredName(structuredName);
    if (name != null) {
      this.prefixNameDirectionCode = name.getString(StructuredName.PREFIX_NAME_DIRECTION_CODE);
      this.namePrefixCode = name.getValueByPath(StructuredName.NAME_PREFIX_CODE_DESCRIPTION);
      this.nameBody = name.getString(StructuredName.NAME_BODY);
      this.nameSuffixCode = name.getValueByPath(StructuredName.NAME_SUFFIX_CODE_DESCRIPTION);
      this.suffixNameDirectionCode = name.getString(StructuredName.SUFFIX_NAME_DIRECTION_CODE);
    }
  }

  @Override
  public String toString() {
    final StringBuilder string = new StringBuilder();
    string.append(getNewFullAddress());
    if (Property.hasValue(this.aliasName)) {
      string.append('\n');
      string.append(this.aliasName);
    }
    string.append('\n');
    string.append(super.toString());
    return string.toString();
  }
}
