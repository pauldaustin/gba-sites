package ca.bc.gov.gbasites.load.provider.geocoderca;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jeometry.common.data.identifier.Identifier;
import org.jeometry.common.logging.Logs;

import ca.bc.gov.gba.controller.GbaController;
import ca.bc.gov.gba.model.GbaTables;
import ca.bc.gov.gba.model.type.StructuredName;
import ca.bc.gov.gba.model.type.code.StructuredNames;
import ca.bc.gov.gbasites.controller.GbaSiteDatabase;
import ca.bc.gov.gbasites.load.common.IgnoreSiteException;
import ca.bc.gov.gbasites.load.common.ProviderSitePointConverter;
import ca.bc.gov.gbasites.model.type.SitePoint;

import com.revolsys.collection.map.LinkedHashMapEx;
import com.revolsys.collection.map.MapEx;
import com.revolsys.collection.map.Maps;
import com.revolsys.collection.range.RangeSet;
import com.revolsys.geometry.model.Point;
import com.revolsys.io.file.Paths;
import com.revolsys.record.DelegatingRecord;
import com.revolsys.record.Record;
import com.revolsys.record.io.RecordReader;
import com.revolsys.util.Debug;
import com.revolsys.util.Property;
import com.revolsys.util.Strings;

public class GeocoderCaSite extends DelegatingRecord implements GeocoderCa, SitePoint {

  private static final String GEOCODER_CA_DESCRIPTION = "GEOCODER_CA_DESCRIPTION";

  private static final String GEOCODER_CA_TYPE = "GEOCODER_CA_TYPE";

  private static final Set<String> IGNORE_STREETS = new HashSet<>();

  private static final Map<String, List<Record>> NAME_BODY_RECORD_MAP = new HashMap<>();

  private static final MapEx NAME_DIRECTION_CODE_MAP = new LinkedHashMapEx();

  private static final MapEx NAME_DIRECTION_LABEL_MAP = new LinkedHashMapEx();

  private static final Map<Identifier, Record> NAME_ID_RECORD_MAP = new HashMap<>();

  private static final String NO_STREET_NAME = "No street name";

  private static final Pattern NUMBER_AT_END_PATTERN = Pattern
    .compile("([\\w ]+) (?:UNIT |SUITE )?(\\d+)");

  private static final Pattern NUMBER_AT_START_PATTERN = Pattern.compile("^(\\d+) ");

  private static final Set<String> STREET_TYPE_NO_MAPPING = new HashSet<>();

  private static final Map<String, Record> STREET_TYPES = new HashMap<>();

  private static final Map<Identifier, Record> STRUCTURED_NAME_RECORD_BY_ID = new HashMap<>();

  private static final Path STREET_TYPES_FILE = ProviderSitePointConverter.SITE_CONFIG_DIRECTORY
    .resolve("GEOCODER_CA_STREET_TYPES.xlsx");

  public static final StructuredNames STRUCTURED_NAMES = GbaController.getStructuredNames();

  private static final Pattern SUITE_AT_END_PATTERN = Pattern
    .compile("(?:([\\w ]+) )?(?:SUITE|UNIT) (\\w+)");

  static {
    if (Paths.exists(STREET_TYPES_FILE)) {
      try (
        RecordReader reader = RecordReader.newRecordReader(STREET_TYPES_FILE)) {
        for (final Record record : reader) {
          final String type = record.getString(GEOCODER_CA_TYPE, "").toUpperCase();
          final String gbaValue = record.getString("GBA_VALUE", "").toUpperCase();
          record.setValue("GBA_VALUE", gbaValue);
          if (!type.isEmpty()) {
            STREET_TYPES.put(type, record);
            STREET_TYPES.put(type + ".", record);
          }
          if (!gbaValue.isEmpty()) {
            STREET_TYPES.put(gbaValue, record);
            STREET_TYPES.put(gbaValue + ".", record);
          }
        }
      }
    }

    final Path IGNORE_STREETS_FILE = ProviderSitePointConverter.SITE_CONFIG_DIRECTORY
      .resolve("GEOCODER_CA_IGNORE_NAMES.xlsx");
    if (Paths.exists(IGNORE_STREETS_FILE)) {
      try (
        RecordReader reader = RecordReader.newRecordReader(IGNORE_STREETS_FILE)) {
        for (final Record record : reader) {
          final String street = record.getString(STREET);
          if (Property.hasValue(street)) {
            IGNORE_STREETS.add(street.toUpperCase());
          }
        }
      }
    }
    for (final Record structuredName : GbaSiteDatabase.getRecordStore()
      .getRecords(GbaTables.STRUCTURED_NAME)) {
      final Identifier id = structuredName.getIdentifier();
      NAME_ID_RECORD_MAP.put(id, structuredName);
      final String nameBody = structuredName.getString(StructuredName.NAME_BODY);
      Maps.addToList(NAME_BODY_RECORD_MAP, nameBody.toUpperCase(), structuredName);
      STRUCTURED_NAME_RECORD_BY_ID.put(id, structuredName);
    }
    addDirection("N", "NORTH");
    addDirection("S", "SOUTH");
    addDirection("E", "EAST");
    addDirection("W", "WEST");
    addDirection("NE", "NORTH EAST");
    addDirection("NW", "NORTH WEST");
    addDirection("SE", "SOUTH EAST");
    addDirection("SW", "SOUTH WEST");
    NAME_DIRECTION_CODE_MAP.add("SO", "SW");
    NAME_DIRECTION_CODE_MAP.add("O", "W");
    NAME_DIRECTION_CODE_MAP.add("EST", "E");
  }

  private static void addDirection(final String code, final String label) {
    NAME_DIRECTION_CODE_MAP.add(code, code);
    NAME_DIRECTION_CODE_MAP.add(label, code);
    NAME_DIRECTION_CODE_MAP.add(label.replace(" ", ""), code);
    final char c1 = code.charAt(0);
    NAME_DIRECTION_CODE_MAP.add(code + ".", code);
    if (code.length() == 2) {
      final char c2 = code.charAt(1);
      NAME_DIRECTION_CODE_MAP.add(c1 + " " + c2, code);
      NAME_DIRECTION_CODE_MAP.add(c1 + "." + c2, code);
      NAME_DIRECTION_CODE_MAP.add(c1 + "." + c2 + ".", code);
      NAME_DIRECTION_CODE_MAP.add(c1 + ". " + c2 + ".", code);
      NAME_DIRECTION_CODE_MAP.add(c1 + " " + c2 + ".", code);
    }

    NAME_DIRECTION_LABEL_MAP.add(code, label);
  }

  private int civicNumber;

  private final GeocoderCaSiteConverter converter;

  private String nameBody;

  private String namePrefix;

  private String namePrefixDirection;

  private String nameSuffix;

  private String nameSuffixDirection;

  private final String postalCode;

  private String structuredName;

  private Identifier structuredNameId;

  private final RangeSet unitRange;

  public GeocoderCaSite(final GeocoderCaSiteConverter converter, final Record sourceRecord,
    final Point point) {
    super(sourceRecord);
    this.converter = converter;
    this.postalCode = getCleanString(POST_CODE);
    this.unitRange = RangeSet.newRangeSet(getString(UNIT));
    this.civicNumber = getInteger(NUMBER);
    this.namePrefixDirection = "";
    this.namePrefix = "";
    this.nameBody = getCleanString(GeocoderCa.STREET_NAME);
    this.nameSuffix = getCleanString(TYPE);

    if (!this.nameSuffix.isEmpty()) {
      final Record type = STREET_TYPES.get(this.nameSuffix.toUpperCase());
      if (type == null) {
        synchronized (STREET_TYPE_NO_MAPPING) {
          if (type == null) {
            if (STREET_TYPE_NO_MAPPING.add(this.nameSuffix)) {
              Logs.error(this, "Add " + this.nameSuffix + " to " + STREET_TYPES_FILE);
            }
          }
        }
      }
      final String gbaAffix = type.getString("GBA_VALUE");
      if (gbaAffix == null) {
        addError("Invalid [TYPE]=" + this.nameSuffix);
      } else {
        this.nameSuffix = gbaAffix;
      }
    }

    this.nameSuffixDirection = getCleanString(DIRECTON);
    this.structuredName = getCleanString(STREET);

    if (IGNORE_STREETS.contains(this.structuredName)) {
      throw new IgnoreSiteException("[STREET] ignored", false);
    }
    if (this.structuredName.equals("STN MAIN")) {
      this.structuredName = "MAIN ST";
      this.nameBody = "MAIN";
      this.nameSuffix = "ST";
    }
    converter.localityFixes.accept(this);
    if (this.structuredName.contains(" AND ")) {
      throw new IgnoreSiteException("[STREET] contains AND");
    }
    if (this.nameSuffixDirection.length() != 0) {
      if (this.nameSuffixDirection.endsWith(".")) {
        this.nameSuffixDirection = this.nameSuffixDirection.substring(0,
          this.nameSuffixDirection.length() - 1);
      }
      final String mappedDirection = NAME_DIRECTION_CODE_MAP.getString(this.nameSuffixDirection);
      if (mappedDirection == null) {
        addWarning("Invalid [DIRECTION]=" + this.nameSuffixDirection + " Added to body");

        if ("NO".equals(this.nameSuffixDirection)) {
          if ( // [BODY] [TYPE] NO
          this.structuredName.equals(Strings.toString(" ", this.nameBody, this.nameSuffix, "NO")) //
            // NO. [BODY] [TYPE]
            || this.structuredName
              .equals(Strings.toString(" ", "NO.", this.nameBody, this.nameSuffix)) //
            // NO. [BODY] [TYPE]
            || this.structuredName
              .equals(Strings.toString(" ", "NO", this.nameBody, this.nameSuffix)) //
          ) {
            this.nameBody = "NO " + this.nameBody;
            this.structuredName = "";
          } else {
            if ("RD".equals(this.nameSuffix)) {

              final Matcher matcher = Pattern.compile("(?:(\\d+) )?(NO.? \\d) (RD|ROAD)")
                .matcher(this.structuredName);
              if (matcher.matches()) {
                final String number = matcher.group(1);
                if (number != null) {
                  this.unitRange.add(this.civicNumber);
                  this.civicNumber = Integer.valueOf(number);
                }
                this.nameBody = matcher.group(2);
                this.structuredName = "";
              } else {
                Debug.noOp();
              }
            } else if ("HWY".equals(this.nameSuffix)) {
              final Matcher matcher = Pattern.compile("NO.? (\\d) (HWY|HIGHWAY)")
                .matcher(this.structuredName);
              if (matcher.matches()) {
                this.nameBody = matcher.group(1);
                this.namePrefix = "HWY";
                this.nameSuffix = "";
                this.structuredName = "";
              } else {
                Debug.noOp();
              }
            } else {
              Debug.noOp();
            }
          }
        }
        if (this.structuredName.startsWith(this.nameSuffixDirection)) {
          this.nameBody = this.nameSuffixDirection + " " + this.nameBody;
        } else if (this.structuredName.endsWith(this.nameSuffixDirection)) {
          this.nameBody += " " + this.nameSuffixDirection;
        } else {
          Debug.noOp();

        }
        this.nameSuffixDirection = "";
      } else {
        this.nameSuffixDirection = mappedDirection;
      }
    }

    removeSuffix("[Street] ends with [PostCode]", this.postalCode);
    final String postcodeWithSpace = this.postalCode.substring(0, 3) + " "
      + this.postalCode.substring(3);
    removeSuffix("[Street] ends with [PostCode] space", postcodeWithSpace);
    removeSuffix("[Street] ends with [PostCode] first 3", this.postalCode.substring(0, 3));
    removeSuffix("CANADA");
    removeSuffix("CA");
    removeSuffix("CITY VALUE");
    // removeSuffix("BR");
    removeSuffix("BC");
    removeBritishColumbia();
    final String city = getCleanString(CITY);
    final String localityNameUpper = this.converter.localityNameUpper;
    removeSuffix("[Street] ends with [Locality Name]", localityNameUpper);
    removeSuffix("[Street] ends with [City]", city);
    removePrefix("[Street] starts with [City]", city);
    removePrefix("[Street] starts with [Civic Number]", Integer.toString(this.civicNumber));
    if (Strings.containsWord(this.structuredName, "BOX")) {
      if (this.structuredName.contains("PO BOX") || this.structuredName.contains("P O BOX")
        || "BOX".equals(this.nameBody)) {
        throw new IgnoreSiteException("PO BOX address");
      } else if (this.structuredName.matches(".* BOX \\d+")
        || this.structuredName.matches("BOX \\d+ .*")) {
        throw new IgnoreSiteException("BOX address");
      } else {
        Debug.noOp();
      }
    }
    removePrefix("LOT");
    if (this.structuredName.indexOf('-') != -1) {
      if (removePrefix("BSMT-" + this.civicNumber)) {
        this.unitRange.add("BASEMENT");
      }
      if (removePrefix("MAIN-" + this.civicNumber)) {

      }
      if (removePrefix("PNTHSE-" + this.civicNumber)) {
        this.unitRange.add("PENTHOUSE");
      }
      final Matcher unitMatcher = Pattern.compile("^(\\w+)-(\\d+) ").matcher(this.structuredName);
      if (unitMatcher.find()) {
        final String unit = unitMatcher.group(1);
        final String numberString = unitMatcher.group(2);
        final int number = Integer.valueOf(numberString);
        final String prefix = unit + "-" + numberString + " ";
        this.structuredName = this.structuredName.substring(prefix.length());
        if (this.nameBody.startsWith(prefix)) {
          this.nameBody = this.nameBody.substring(prefix.length());
        } else if (this.nameBody.startsWith(prefix.substring(0, prefix.length() - 1))) {
          this.nameBody = this.nameBody.substring(prefix.length() - 1);
        } else {
          Debug.noOp();
        }

        if ("OCT".equals(unit)) {

        } else {
          if (this.civicNumber == 1) {
            this.civicNumber = number;
          } else if (this.civicNumber != number) {
            this.unitRange.add(number);
          }
          try {
            this.unitRange.add(Integer.valueOf(unit));
          } catch (final Exception e) {
            this.unitRange.add(unit);
          }
        }
      }
    }
    if (this.structuredName.isEmpty()) {
      this.structuredName = Strings.toString(" ", this.nameBody, this.nameSuffix,
        this.nameSuffixDirection);
    }

  }

  public void addError(final String message) {
    final String fullAddress = Strings.toString(":", this.unitRange, this.civicNumber,
      this.structuredName);
    final String fullAddressParts = Strings.toString(":", this.unitRange, this.civicNumber,
      this.namePrefixDirection, this.namePrefix, this.nameBody, this.nameSuffix,
      this.nameSuffixDirection);
    setValue(FULL_ADDRESS, fullAddress);
    setValue("FULL_ADDRESS_PARTS", fullAddressParts);
    this.converter.addError(this, message);
  }

  public void addWarning(final String message) {
    this.converter.addWarning(this, message);
  }

  private String findNameAffix(final String affix) {
    if (!affix.isEmpty()) {
      final Record type = STREET_TYPES.get(affix.toUpperCase());
      if (type == null) {
        return affix;
      }
      final String gbaAffix = type.getString("GBA_VALUE");
      if (gbaAffix == null) {
        addError("Invalid [TYPE]=" + affix);
      } else {
        return gbaAffix.toUpperCase();
      }
    }
    return affix;
  }

  private boolean findStructuredName(final String prefixDirection, final String prefix,
    final String body, final String suffix, final String suffixDirection) {
    final String fullName = Strings.toString(" ", prefixDirection, prefix, body, suffix,
      suffixDirection);
    if (fullName.length() == 0) {
      this.structuredName = null;
      return true;
    } else {
      if (setStructuredName(fullName)) {
        return true;
      }
      if (this.unitRange.isEmpty()) {
        final Matcher suiteEndMatcher = SUITE_AT_END_PATTERN.matcher(fullName);
        if (suiteEndMatcher.matches()) {
          final String fullNameWithoutNumberSuffix = suiteEndMatcher.group(1);
          if (setStructuredName(fullNameWithoutNumberSuffix)) {
            final String unit = suiteEndMatcher.group(2);
            try {
              final int unitNumber = Integer.parseInt(unit);
              this.unitRange.add(unitNumber);
            } catch (final NumberFormatException e) {
              this.unitRange.add(unit);
            }
            return true;
          }
        }
        final Matcher endMatcher = NUMBER_AT_END_PATTERN.matcher(fullName);
        if (endMatcher.matches()) {
          final String fullNameWithoutNumberSuffix = endMatcher.group(1);
          if (!fullNameWithoutNumberSuffix.endsWith("HWY")
            && setStructuredName(fullNameWithoutNumberSuffix)) {
            final int unitNumber = Integer.parseInt(endMatcher.group(2));
            this.unitRange.add(unitNumber);
            return true;
          }
        }
      }

      return false;
    }
  }

  public void fix108Mile() {
    removeSuffix("108 MILE");
  }

  public void fixAbbotsford() {
    if (this.structuredName.equals("STN MT LEHMAN")) {
      this.structuredName = "MT LEHMAN RD";
      this.nameBody = "MT LEHMAN";
      this.nameSuffix = "RD";
    }
    if (this.structuredName.equals("STN MATSQUI")) {
      this.structuredName = "MATSQUI PL";
      this.nameBody = "MATSQUI";
      this.nameSuffix = "PL";
    }
  }

  public void fixBurnaby() {
    if (this.structuredName.contains("CANADA TAKO")
      || this.structuredName.contains("WAY BURNABY BC CANADA")) {
      this.structuredName = "CANADA WAY";
      this.nameBody = "CANADA";
    }
  }

  public void fixChilliwack() {
    if (this.structuredName.startsWith("STN MAIN")
      || this.structuredName.equals("STN SARDIS MAIN")) {
      this.structuredName = "MAIN ST";
      this.nameBody = "MAIN";
      this.nameSuffix = "ST";
    }
  }

  private boolean fixDoubleAddress() {
    try {
      final String civicNumber = Integer.toString(this.civicNumber);
      final String fullAddress = Strings.toString(" ", civicNumber, this.structuredName);
      final int length = fullAddress.length();
      if (civicNumber.equals(fullAddress)) {
        throw new IgnoreSiteException(NO_STREET_NAME, false);
      } else if (length % 2 == 1) {
        final String part1 = fullAddress.substring(0, length / 2);
        final String part2 = fullAddress.substring(length / 2 + 1);
        if (part1.equals(part2)) {
          if (part1.matches("\\d+")) {
            throw new IgnoreSiteException(NO_STREET_NAME, false);
          }
          final String fullName = part1.substring(civicNumber.length() + 1);
          this.structuredName = fullName;
          if (!this.nameSuffixDirection.isEmpty()) {
            Debug.noOp();
          }
          this.nameBody = fullName.replaceAll(" " + this.nameSuffix + "$", "");
          addWarning("Full address duplicated in street name");
        }

      }
    } catch (final IgnoreSiteException e) {
      throw e;
    } catch (final Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return false;
  }

  private boolean fixEndsWithBody(final String name, final String body, final String type) {
    final int nameLength = name.length();
    final int bodyLength = body.length();
    final int bodyIndex = nameLength - bodyLength - 1;
    if (name.charAt(bodyIndex) == ' ') {
      String nameType = name.substring(0, bodyIndex);
      final int dotIndex = nameType.indexOf('.');
      if (dotIndex == 0) {
        if (nameType.charAt(1) == ' ') {
          nameType = nameType.substring(2);
        } else {
          nameType = nameType.substring(1);
        }
      } else if (dotIndex == nameType.length() - 1) {
        nameType = nameType.substring(0, nameType.length() - 1);
      }
      if (nameType.endsWith(" " + this.civicNumber)) {
        nameType = nameType.substring(0, nameType.lastIndexOf(' '));
      }
      nameType = findNameAffix(nameType);
      if (nameType.equals(type)) {
        return setStructuredName("", type, body, "", "");
      } else {
        return false;
      }
    } else {
      return false;
    }
  }

  public void fixMission() {
    if (this.structuredName.startsWith("AVES T")) {
      this.structuredName = "AVES TERR";
      this.nameBody = "AVES";
      this.nameSuffix = "TERR";
    }
  }

  public void fixParksville() {
    if (this.structuredName.equals("FINHOLM STN N")) {
      this.structuredName = "FINHOLM ST N";
      this.nameBody = "FINHOLM";
      this.nameSuffix = "ST";
      this.nameSuffixDirection = "N";
    }
  }

  boolean fixSourceSite() {
    fixUnitPrefix("UNIT ");
    fixUnitPrefix("SUITE ");
    if (this.unitRange.isEmpty()) {
      fixDoubleAddress();
    }
    if (this.structuredName.isEmpty()) {
      throw new IgnoreSiteException(NO_STREET_NAME, false);
    }
    {
      final Matcher unitEndMatcher = SUITE_AT_END_PATTERN.matcher(this.structuredName);
      if (unitEndMatcher.matches()) {
        this.structuredName = unitEndMatcher.group(1);

        final Matcher unitEndMatcherBody = SUITE_AT_END_PATTERN.matcher(this.nameBody);
        if (unitEndMatcherBody.matches()) {
          this.nameBody = unitEndMatcher.group(1);
        } else {
          final Matcher unitEndMatcherBodyDirection = SUITE_AT_END_PATTERN
            .matcher(this.nameBody + " " + this.nameSuffixDirection);
          if (unitEndMatcherBodyDirection.matches()) {
            this.nameBody = unitEndMatcherBodyDirection.group(1);
            this.nameSuffixDirection = "";
          } else {
            Debug.noOp();
          }
        }

        final String unit = unitEndMatcher.group(2);
        try {
          if ("1".equals(this.unitRange.toString())) {
            this.unitRange.clear();
          }

          final int unitNumber = Integer.parseInt(unit);
          this.unitRange.add(unitNumber);
        } catch (final NumberFormatException e) {
          this.unitRange.add(unit);
        }
      }
    }
    if (matchStructuredName(this.structuredName)) {
      return true;
    }
    if (matchStructuredName(this.nameBody, this.nameSuffix, this.nameSuffixDirection)) {
      return true;
    }
    final String mappedDirection = NAME_DIRECTION_CODE_MAP.getString(this.nameSuffixDirection);
    if (matchStructuredName(this.nameBody, this.nameSuffix, mappedDirection)) {
      return true;
    }
    if (!this.nameSuffix.isEmpty()) {
      final Record streetTypeRecord = STREET_TYPES.get(this.nameSuffix);
      if (streetTypeRecord != null) {
        for (final String typeDescription : streetTypeRecord.getString(GEOCODER_CA_DESCRIPTION, "")
          .split(",")) {
          if (matchStructuredName(this.nameBody, typeDescription, this.nameSuffixDirection)) {
            return true;
          }
          if (matchStructuredName(this.nameBody, typeDescription, mappedDirection)) {
            return true;
          }
        }
      }
    }

    if (this.structuredName.matches("^\\d+ .*$")) {
      final int spaceIndex = this.structuredName.indexOf(' ');
      final int number = Integer.valueOf(this.structuredName.substring(0, spaceIndex));
      final String name = this.structuredName.substring(spaceIndex + 1);
      if (matchStructuredName(name)) {
        if (number < this.civicNumber) {
          this.unitRange.add(number);
        } else {
          this.unitRange.add(this.civicNumber);
          this.civicNumber = number;
        }
        return true;
      }

    }
    final List<Record> records = this.converter.lineNameIndex.getRecordsDistance(getGeometry(), 10);
    if (!records.isEmpty()) {
      Debug.noOp();
    }
    return true;
  }

  private boolean fixStartsWithBody(final String name, String body, final String type,
    String direction) {
    final int nameLength = name.length();
    int bodyLength = body.length();
    if (nameLength == bodyLength) {
      return true;
    } else {
      if (name.charAt(bodyLength) == '.') {
        body += '.';
        bodyLength = body.length();
      }
      int beginIndex = bodyLength;
      if (name.charAt(bodyLength) == ' ') {
        beginIndex++;
      }
      String nameType = name.substring(beginIndex);
      final int dotIndex = nameType.indexOf('.');
      if (dotIndex == 0) {
        if (nameType.charAt(1) == ' ') {
          nameType = nameType.substring(2);
        } else {
          nameType = nameType.substring(1);
        }
      } else if (dotIndex == nameType.length() - 1) {
        nameType = nameType.substring(0, nameType.length() - 1);
      } else if (dotIndex == nameType.length() - 2) {
        final String directionSuffix = nameType.substring(nameType.length() - 1);
        if (NAME_DIRECTION_CODE_MAP.containsKey(directionSuffix)) {
          direction = directionSuffix;
          nameType = nameType.substring(0, nameType.length() - 2);
        }
      }
      if (nameType.charAt(0) == '@' || nameType.charAt(0) == '-') {
        nameType = nameType.substring(1);
      }
      if (!STREET_TYPES.containsKey(nameType) && nameType.length() > 0) {
        final String directionSuffix = nameType.substring(nameType.length() - 1);
        if (NAME_DIRECTION_CODE_MAP.containsKey(directionSuffix)) {
          final String typeNoDir = nameType.substring(0, nameType.length() - 1);
          if (!STREET_TYPES.containsKey(nameType)) {
            direction = directionSuffix;
            nameType = typeNoDir;
          }
        }
      }
      String affix;
      String extra;
      final int spaceIndex = nameType.indexOf(' ');
      if (spaceIndex == -1) {
        affix = findNameAffix(nameType);
        extra = "";
      } else {
        affix = findNameAffix(nameType.substring(0, spaceIndex));
        extra = nameType.substring(spaceIndex + 1);
      }
      if (affix.equals(type)) {
        if (!extra.isEmpty()) {
          if (extra.matches("NO \\d+")) {
            this.unitRange.add(Integer.valueOf(extra.substring(3)));
          } else if ("0".equals(extra)) {
          } else {
            Debug.noOp();
          }
        }
        return setStructuredName(null, null, body, type, direction);
      } else {
        return false;
      }
    }
  }

  private void fixUnitPrefix(final String prefix) {
    if (this.structuredName.startsWith(prefix)) {
      addWarning("[STREET] starts with " + prefix.trim());
      final int prefixLength = prefix.length();
      this.structuredName = this.structuredName.substring(prefixLength);
      if (this.nameBody.startsWith(prefix)) {
        this.nameBody = this.nameBody.substring(prefixLength);
      } else if (this.nameBody.startsWith(prefix.substring(0, prefixLength - 1))) {
        this.nameBody = this.nameBody.substring(prefixLength - 1);
      } else {
        Debug.noOp();
      }
      final Pattern unitPattern = Pattern.compile("^([A-Z]|\\d+|[A-Z]\\d+|\\d+[A-Z])[ -]+");
      final Matcher unitMatcher = unitPattern.matcher(this.structuredName);
      if (unitMatcher.find()) {
        final String matchText = unitMatcher.group(0);
        final String unit = unitMatcher.group(1);
        if (!this.unitRange.isEmpty()) {
          if (!this.unitRange.toString().equals(unit)) {
            addWarning("[STREET] contains unit and [UNIT] specified");
          }
        }
        this.unitRange.add(unit);
        this.structuredName = this.structuredName.substring(matchText.length());
        if (this.nameBody.startsWith(matchText)) {
          this.nameBody = this.nameBody.substring(matchText.length());
        } else if (this.nameBody.startsWith(unit)) {
          this.nameBody = this.nameBody.substring(unit.length());
        } else {
          Debug.noOp();
        }
        if (this.civicNumber == 1) {
          final Matcher numberMatcher = NUMBER_AT_START_PATTERN.matcher(this.structuredName);
          if (numberMatcher.find()) {
            addWarning("[STREET] contains [NUMBER]");
            final String numberText = numberMatcher.group(0);
            final String number = numberMatcher.group(1);
            this.civicNumber = Integer.parseInt(number);
            this.structuredName = this.structuredName.substring(numberText.length());
            if (this.nameBody.startsWith(numberText)) {
              this.nameBody = this.nameBody.substring(numberText.length());
            } else if (this.nameBody.startsWith(number)) {
              this.nameBody = this.nameBody.substring(number.length());
            } else {
              Debug.noOp();
            }
          }

        }
      }
    }
  }

  public void fixWestVancouver() {
    if (this.structuredName.startsWith("WEST VANCOUVER ")) {
      this.structuredName = this.structuredName.substring(15);
      if (this.nameBody.startsWith("VANCOUVER ")) {
        this.nameBody = this.nameBody.substring(10);
      }
    } else {
      int wvIndex = this.structuredName.indexOf(" WEST VANCOUVER BC " + this.postalCode);
      if (wvIndex != -1) {
        this.structuredName = this.structuredName.substring(0, wvIndex);
        final int vIndex = this.nameBody.indexOf("VANCOUVER BC " + this.postalCode);
        if (vIndex != -1) {
          this.nameBody = this.nameBody.substring(0, vIndex);
        }
      } else {
        wvIndex = this.structuredName.indexOf(" WEST VANCOUVER BC");
        if (wvIndex != -1) {
          this.structuredName = this.structuredName.substring(0, wvIndex);
          final int vIndex = this.nameBody.indexOf("VANCOUVER BC");
          if (vIndex != -1) {
            this.nameBody = this.nameBody.substring(0, vIndex);
          }
        } else {
          wvIndex = this.structuredName.indexOf(" AND WEST VANCOUVER");
          if (wvIndex != -1) {
            this.structuredName = this.structuredName.substring(0, wvIndex);
            final int vIndex = this.nameBody.indexOf(" ANDVANCOUVER");
            if (vIndex != -1) {
              this.nameBody = this.nameBody.substring(0, vIndex);
            }
          } else {
            wvIndex = this.structuredName.indexOf(" WEST VANCOUVER");
            if (wvIndex != -1) {
              this.structuredName = this.structuredName.substring(0, wvIndex);
              final int vIndex = this.nameBody.indexOf(" VANCOUVER");
              if (vIndex != -1) {
                this.nameBody = this.nameBody.substring(0, vIndex);
              }
            }
            wvIndex = this.structuredName.indexOf(" =WEST VANCOUVER");
            if (wvIndex != -1) {
              this.structuredName = this.structuredName.substring(0, wvIndex);
              final int vIndex = this.nameBody.indexOf(" =WEST VANCOUVER");
              if (vIndex != -1) {
                this.nameBody = this.nameBody.substring(0, vIndex);
              }
            }
          }
        }
      }
    }
    if ("W".equals(this.nameSuffixDirection) && !(Strings.containsWord(this.structuredName, "WEST")
      || Strings.containsWord(this.structuredName, "W"))) {
      this.nameSuffixDirection = "";
    }
  }

  public int getCivicNumber() {
    return this.civicNumber;
  }

  private String getCleanString(final String fieldName) {
    String string = getString(fieldName, "")//
      .trim()//
      .toUpperCase();

    string = removeStartOrEnd(string, "¿");
    string = removeStartOrEnd(string, "Â");
    string = removeStartOrEnd(string, "€");
    string = removeStartOrEnd(string, "Â"); // Duplicate for end after €
    string = removeStartOrEnd(string, "“");
    string = removeStartOrEnd(string, "Ï");
    string = replace(string, "]", "");
    string = replace(string, "% ", " ");
    string = replace(string, "%20", " ");
    string = replace(string, "%23", "#");
    string = replace(string, "%27", "'");
    string = replace(string, "%28", "(");
    string = replace(string, "%29", ")");
    string = replace(string, "%2B", "+");
    string = replace(string, "%2C", ",");
    string = replace(string, "%2F", "/");
    string = replace(string, "%26", "&");
    string = replace(string, "&#39;", "'");
    string = replace(string, "&QUOT%3B", "");
    string = replace(string, "’", "'");
    string = replace(string, "–", "-");
    string = replace(string, "“", "");
    string = replace(string, "Â€™", "'");
    string = replace(string, " Â€ ", " - ");
    string = replace(string, "Ã¨", "È");
    string = string//
      .replaceAll("\\s+", " ")//
      .trim();
    final String normalString = string.replaceAll("[^\\w\\+ '-@:\\.|&ÈÉÔ½/\\\\=!$#]+", " ")
      .replaceAll(" +", " ")
      .trim();
    return normalString;
  }

  private String getNameDirection(String direction) {
    if (direction.length() == 0) {
      return "";
    } else {
      if (direction.endsWith(".")) {
        direction = direction.substring(0, direction.length() - 1);
      }
      final String mappedDirection = NAME_DIRECTION_CODE_MAP.getString(direction);
      if (mappedDirection == null) {
        synchronized (NAME_DIRECTION_CODE_MAP) {
          if (NAME_DIRECTION_CODE_MAP.put(direction, direction) == null) {
            Logs.info(getClass(), "Added direction " + direction);
          }
        }
        return direction;
      } else {
        return mappedDirection;
      }
    }
  }

  public String getNewFullAddress() {
    return SitePoint.getFullAddress(this.unitRange, Integer.toString(this.civicNumber), null, null,
      this.structuredName);
  }

  public String getNewParts() {
    return Strings.toString(" ", this.namePrefixDirection, this.namePrefix, this.nameBody,
      this.nameSuffix, this.nameSuffixDirection);
  }

  public String getOriginalFullAddress() {
    final RangeSet unitDescriptor = RangeSet.newRangeSet(getString(UNIT));
    final String civicNumber = getString(NUMBER);
    final String streetName = getString(STREET);
    return SitePoint.getFullAddress(unitDescriptor, civicNumber, null, null, streetName)
      .toUpperCase();
  }

  public String getOriginalName() {
    return getString(STREET);
  }

  public String getOriginalParts() {
    return Strings.toString(" ", getString(GeocoderCa.STREET_NAME), getString(TYPE),
      getString(DIRECTON));
  }

  public String getOriginalStreetName() {
    return getString(STREET);
  }

  public String getPostalCode() {
    return this.postalCode;
  }

  public String getStructuredName() {
    return this.structuredName;
  }

  public Identifier getStructuredNameId() {
    return this.structuredNameId;
  }

  public String getUnitDescriptor() {
    return this.unitRange.toString();
  }

  public RangeSet getUnitRange() {
    return this.unitRange;
  }

  private boolean isChar(final String value, final int index, final char c) {
    if (index < value.length()) {
      return value.charAt(index) == c;
    } else {
      return false;
    }
  }

  public boolean isPartsEqual() {
    final String name = this.structuredName;
    String body = this.nameBody;
    final String type = this.nameSuffix;
    final String direction = this.nameSuffixDirection;
    if ("111 W GEORGIA STREET".equals(name)) {
      Debug.noOp();
    }
    if (name.isEmpty()) {
      this.structuredName = Strings.toString(" ", body, type, direction);
      return true;
    }
    if (body.isEmpty()) {

      final Record typeRecord = STREET_TYPES.get(name);
      if (name.equals(type) || typeRecord != null && typeRecord.equalValue("GBA_VALUE", type)) {
        throw new IgnoreSiteException("[body] not specified", true);
      }
      if (direction.isEmpty()) {
        if (name.startsWith(type + " ")) {
          body = name.substring(type.length() + 1);
          return setStructuredName("", type, body, "", "");
        }
      }
      if (!(direction.isEmpty() && type.isEmpty())) {
        return false;
      } else {
        return true;
      }

    } else {
      final boolean hasDirection = !direction.isEmpty();
      final boolean hasType = !type.isEmpty();
      // if (!hasType) {
      // if (body.endsWith("ROAD") && name.endsWith("ROAD")) {
      // body = body.substring(0, body.length() - 4);
      // type = "RD";
      // } else if (body.endsWith("RD") && name.endsWith("RD")) {
      // body = body.substring(0, body.length() - 2);
      // type = "RD";
      // }
      // hasType = !type.isEmpty();
      // }
      if (hasType) {
        if (hasDirection) {
          return isPartsEqualBodyTypeDirection(name, body, type, direction);
        } else {
          if (name.endsWith(" " + body)) {
            return fixEndsWithBody(name, body, type);
          } else if (name.startsWith(body)) {
            return fixStartsWithBody(name, body, type, direction);
          } else if (name.replace(".", "").startsWith(body)) {
            int nameI = 0;
            for (int bodyI = 0; bodyI < body.length(); bodyI++) {
              while (name.charAt(nameI) != body.charAt(bodyI)) {
                nameI++;
              }
              nameI++;
            }
            body = this.nameBody = name.substring(0, nameI);
            return fixStartsWithBody(name, body, type, direction);
          } else if (name.replace(".", "").endsWith(body)) {
            int nameI = name.length() - 1;
            for (int bodyI = body.length() - 1; bodyI >= 0; bodyI--) {
              while (name.charAt(nameI) != body.charAt(bodyI)) {
                nameI--;
              }
              nameI--;
            }
            body = this.nameBody = name.substring(nameI + 1);
            return fixEndsWithBody(name, body, type);
          } else {
            return false;
          }
        }
      } else {
        if (hasDirection) {
          return isPartsEqualBodyDirection(name, body, direction);
        } else {
          return isPartsEqualBody(name, body);
        }
      }
    }
  }

  private boolean isPartsEqualBody(final String name, final String body) {
    if (name.equals(body)) {
      return true;
    } else if (name.replaceAll("\\.", "").equals(body)) {
      return setStructuredName("", "", name, "", "");
    } else if (name.replaceAll("\\+", " ").equals(body)) {
      return setStructuredName("", "", name, "", "");
    }
    if (name.startsWith(body + " ")) {
      String type = name.substring(body.length() + 1);
      if (STREET_TYPES.containsKey(type)) {
        type = findNameAffix(type);
        return setStructuredName("", "", body, type, "");
      } else {
        return setStructuredName("", "", name, "", "");
      }
    } else {
      return false;
    }
  }

  private boolean isPartsEqualBodyDirection(final String name, String body,
    final String direction) {
    // Direction at end
    {
      int spaceIndex = name.lastIndexOf(' ');
      if (spaceIndex != -1) {
        String nameDirection = name.substring(spaceIndex + 1);
        nameDirection = NAME_DIRECTION_CODE_MAP.getString(nameDirection);
        if (direction.equals(nameDirection)) {
          final String nameWithoutDirection = name.substring(0, spaceIndex);
          return isPartsEqualBodyDirection(nameWithoutDirection, "", body, direction);
        } else if (direction.length() == 2) {
          spaceIndex = name.lastIndexOf(' ', spaceIndex - 1);
          if (spaceIndex != -1) {
            nameDirection = name.substring(spaceIndex + 1);
            nameDirection = NAME_DIRECTION_CODE_MAP.getString(nameDirection);
            if (direction.equals(nameDirection)) {
              final String nameWithoutDirection = name.substring(0, spaceIndex);
              return isPartsEqualBodyDirection(nameWithoutDirection, "", body, direction);
            }
          }
        }
      }
    }
    // Direction at start
    {
      int spaceIndex = name.indexOf(' ');
      if (spaceIndex != -1) {
        String nameDirection = name.substring(0, spaceIndex);
        nameDirection = NAME_DIRECTION_CODE_MAP.getString(nameDirection);
        if (direction.equals(nameDirection)) {
          final String nameWithoutDirection = name.substring(spaceIndex + 1);
          return isPartsEqualBodyDirection(nameWithoutDirection, direction, body, "");
        } else if (direction.length() == 2) {
          spaceIndex = name.indexOf(' ', spaceIndex + 1);
          if (spaceIndex != -1) {
            nameDirection = name.substring(0, spaceIndex);
            nameDirection = NAME_DIRECTION_CODE_MAP.getString(nameDirection);
            if (direction.equals(nameDirection)) {
              final String nameWithoutDirection = name.substring(spaceIndex + 1);
              return isPartsEqualBodyDirection(nameWithoutDirection, direction, body, "");
            }
          }
        }
      }
    }
    if (name.startsWith(body)) {
      final int bodyLength = body.length();
      if (name.length() == bodyLength) {
        return setStructuredName("", "", body, "", direction);
      } else if (name.charAt(bodyLength) == ' ') {
        final String nameDirection = getNameDirection(name.substring(bodyLength + 1));
        if (nameDirection.equals(direction)) {
          setStructuredName("", "", body, "", direction);
          return true;
        } else {
          return false;
        }
      } else {
        return false;
      }
    } else if (name.endsWith(body)) {
      final int nameLength = name.length();
      final int bodyLength = body.length();
      if (name.charAt(nameLength - bodyLength - 1) == ' ') {
        final String nameDirection = getNameDirection(
          name.substring(0, nameLength - bodyLength - 1));
        if (nameDirection.equals(direction)) {
          setStructuredName(direction, "", body, "", "");
          return true;
        } else {
          return false;
        }
      } else {
        return false;
      }
    } else {
      int directionLength = direction.length() + 2;
      int directionIndex = name.indexOf(" " + direction + " ");
      if (directionIndex == -1) {
        final String mappedDirection = NAME_DIRECTION_LABEL_MAP.getString(direction, direction);
        directionLength = mappedDirection.length() + 2;
        directionIndex = name.indexOf(" " + mappedDirection + " ");
        if (directionIndex == -1) {
          return false;
        }
      }
      final String before = name.substring(0, directionIndex);
      final String after = name.substring(directionIndex + directionLength);
      if (body.equals(before + after)) {
        if (STREET_TYPES.containsKey(after)) {
          body = body.substring(0, before.length());
          return setStructuredName("", "", body, after, direction);
        } else if (STREET_TYPES.containsKey(before)) {
          body = body.substring(before.length());
          return setStructuredName(direction, before, body, "", "");
        } else {
          return setStructuredName("", "", name, "", "");
        }
      } else {
        return false;
      }
    }
  }

  private boolean isPartsEqualBodyDirection(final String name, final String directionPrefix,
    final String body, final String directionSuffix) {
    final int index = name.indexOf(body);
    final int bodyLength = body.length();
    if (index == -1) {
      if (name.replace(".", "").equals(body)) {
        return setStructuredName(directionPrefix, "", name, "", directionSuffix);
      } else {
        if (!directionPrefix.isEmpty()) {
          if (body.startsWith(directionPrefix)) {
            final String directionLabel = NAME_DIRECTION_LABEL_MAP.getString(directionPrefix);
            if (name.startsWith(directionLabel + " ")) {
              final String bodyWithoutDirection = body.substring(directionPrefix.length());
              if (bodyWithoutDirection.equals(name.substring(directionLabel.length() + 1))) {
                return setStructuredName(directionPrefix, "", bodyWithoutDirection, "",
                  directionSuffix);
              }
            }
          }
        }
        return false;
      }
    } else {
      final int nameLength = name.length();
      if (index == 0) {
        if (nameLength == bodyLength) {
          return setStructuredName(directionPrefix, "", body, "", directionSuffix);
        } else {
          final String suffix = name.substring(bodyLength).trim();
          if (STREET_TYPES.containsKey(suffix)) {
            final String type = findNameAffix(suffix);
            return setStructuredName(directionPrefix, "", name, type, directionSuffix);
          } else {
            return setStructuredName(directionPrefix, "", name, "", directionSuffix);
          }
        }
      } else {
        final String prefix = name.substring(0, nameLength - bodyLength).trim();
        if (STREET_TYPES.containsKey(prefix)) {
          final String type = findNameAffix(prefix);
          return setStructuredName(directionPrefix, type, name, "", directionSuffix);
        } else {
          return setStructuredName(directionPrefix, "", name, "", directionSuffix);
        }
      }
    }

  }

  private boolean isPartsEqualBodyTypeDirection(final String name, final String body,
    final String type, final String direction) {
    if ("W. 6TH. AVE".equals(name)) {
      Debug.noOp();
    }
    if (name.equals(direction + " " + body + " " + type)) {
      return setStructuredName(direction, "", body, type, "");
    } else if (name.equals(body + " " + type + " " + direction)) {
      return setStructuredName("", "", body, type, direction);
    } else if (name.equals(direction + ". " + body + ". " + type)) {
      return setStructuredName(direction, "", body, type, "");
    } else if (name.equals(body + ". " + type + ". " + direction)) {
      return setStructuredName("", "", body, type, direction);
    } else if (name.equals(body + " . " + type + ". " + direction)) {
      return setStructuredName("", "", body, type, direction);
    } else if (name.equals(body + " . " + type + " " + direction)) {
      return setStructuredName("", "", body, type, direction);
    }

    final String directionLabel = NAME_DIRECTION_LABEL_MAP.getString(direction);
    // Direction at end
    {
      int spaceIndex = name.lastIndexOf(' ');
      if (spaceIndex != -1) {
        String nameDirection = name.substring(spaceIndex + 1);
        nameDirection = NAME_DIRECTION_CODE_MAP.getString(nameDirection);
        if (direction.equals(nameDirection)) {
          final String nameWithoutDirection = name.substring(0, spaceIndex);
          return isPartsEqualBodyTypeDirectionSuffix(nameWithoutDirection, body, type, direction);
        } else if (direction.length() == 2) {
          spaceIndex = name.lastIndexOf(' ', spaceIndex - 1);
          if (spaceIndex != -1) {
            nameDirection = name.substring(spaceIndex + 1);
            nameDirection = NAME_DIRECTION_CODE_MAP.getString(nameDirection);
            if (direction.equals(nameDirection)) {
              final String nameWithoutDirection = name.substring(0, spaceIndex);
              return isPartsEqualBodyTypeDirectionSuffix(nameWithoutDirection, body, type,
                direction);
            }
          }
        }
      }
    }
    // Direction at start
    {
      int spaceIndex = name.indexOf(' ');
      if (spaceIndex != -1) {
        String nameDirection = name.substring(0, spaceIndex);
        nameDirection = NAME_DIRECTION_CODE_MAP.getString(nameDirection);
        if (direction.equals(nameDirection)) {
          final String nameWithoutDirection = name.substring(spaceIndex + 1);
          return isPartsEqualBodyTypeDirectionPrefix(nameWithoutDirection, body, type, direction);
        } else if (direction.length() == 2) {
          spaceIndex = name.indexOf(' ', spaceIndex + 1);
          if (spaceIndex != -1) {
            nameDirection = name.substring(0, spaceIndex);
            nameDirection = NAME_DIRECTION_CODE_MAP.getString(nameDirection);
            if (direction.equals(nameDirection)) {
              final String nameWithoutDirection = name.substring(spaceIndex + 1);
              return isPartsEqualBodyTypeDirectionPrefix(nameWithoutDirection, body, type,
                direction);
            }
          }
        }
      }
    }

    // Type at end
    {
      final int spaceIndex = name.lastIndexOf(' ');
      if (spaceIndex != -1) {
        final String nameType = findNameAffix(name.substring(spaceIndex + 1));
        if (type.equals(nameType)) {
          final String nameWithoutType = name.substring(0, spaceIndex);
          return isPartsEqualBodyTypeDirectionMiddle2(nameWithoutType, direction, "", body, type);
        }
      }
    }

    // Direction in Middle
    if (name.contains(directionLabel)) {
      return isPartsEqualBodyTypeDirectionMiddle(name, body, type, direction, directionLabel);
    } else if (name.contains(" " + direction + " ")) {
      return isPartsEqualBodyTypeDirectionMiddle(name, body, type, direction, direction);
    } else if (name.contains(" " + direction + ". ")) {
      return isPartsEqualBodyTypeDirectionMiddle(name, body, type, direction, direction + ".");
    }

    if (name.equals(Strings.toString(" ", body, type, direction))) {
      return setStructuredName("", "", body, type, direction);
    } else if (name.equals(Strings.toString(" ", body, direction, type))) {
      return setStructuredName("", "", body, type, direction);
    } else if (name.equals(Strings.toString(" ", direction, body, type))) {
      return setStructuredName(direction, "", body, type, "");
    } else if (name.equals(Strings.toString(" ", direction, type, body))) {
      return setStructuredName(direction, type, body, type, "");
    } else {
      return false;
    }
  }

  private boolean isPartsEqualBodyTypeDirectionMiddle(final String name, String body,
    final String type, final String direction, final String directionLabel) {
    if ("OLD NORTH THOMPSON HWY".equals(name)) {
      Debug.noOp();
    }
    final int directionLength = directionLabel.length() + 2;
    final int directionIndex = name.indexOf(" " + directionLabel + " ");
    if (directionIndex == -1) {
      return false;
    } else {
      String before = name.substring(0, directionIndex);
      String after = name.substring(directionIndex + directionLength);
      final String civicNumberString = Integer.toString(this.civicNumber);
      if (before.startsWith(after)) {
        before = before.substring(after.length()).trim();
        if (before.startsWith(direction + " ")) {
          before = before.substring(direction.length() + 1);
        }
        if (after.endsWith(" " + type)) {
          after = after.substring(0, after.length() - type.length() - 1);
        }
        if (before.endsWith(" " + civicNumberString)) {
          final String unit = before.substring(0, before.length() - civicNumberString.length() - 1);
          try {
            this.unitRange.add(Integer.valueOf(unit));
          } catch (final Exception e) {
            this.unitRange.add(unit);
          }
          return setStructuredName(direction, "", body, type, "");
        }
        Debug.noOp();
      }
      if ("UNIT".equals(before)) {
        if (after.endsWith(" " + type)) {
          after = after.substring(0, after.length() - type.length() - 1);
        }
        if (after.indexOf(" ") == -1) {
          return setStructuredName(direction, "", after, type, "");
        } else {
          Debug.noOp();
          return false;
        }
      }
      {
        final int lastSpaceIndex = after.lastIndexOf(' ');
        if (lastSpaceIndex != -1) {
          final String lastWord = after.substring(lastSpaceIndex + 1);
          final String lastType = findNameAffix(lastWord);
          if (type.equals(lastType)) {
            after = after.substring(0, lastSpaceIndex + 1) + type;
          }
        }
      }
      if ((body + " " + type).equals(before + after)) {
        after = after.substring(0, after.length() - type.length() - 1);

        if (before.startsWith("SUITE ")) {
          before = before.substring(6);
        } else if (before.startsWith("UNIT ")) {
          before = before.substring(5);
        }
        if (before.endsWith(civicNumberString)) {
          before = before.substring(0, before.length() - civicNumberString.length()).trim();
        }
        if (before.endsWith("-")) {
          before = before.substring(0, before.length() - 1).trim();
        }
        if (before.isEmpty()) {
          return setStructuredName(direction, "", after, type, "");
        } else if (before.equals(civicNumberString)) {
          return setStructuredName(direction, "", after, type, "");
        } else if (before.matches("[A-Z]")) {
          this.unitRange.add(before);
          return setStructuredName(direction, "", after, type, "");
        } else {
          if (before.matches("\\d+ \\d+")) {
            try {
              final int beforeSpaceIndex = before.indexOf(' ');
              final int unit = Integer.parseInt(before.substring(0, beforeSpaceIndex));
              this.unitRange.add(unit);
              this.civicNumber = Integer.parseInt(before.substring(beforeSpaceIndex + 1));
              return setStructuredName(direction, "", after, type, "");
            } catch (final Exception e) {
            }
          }
          try {
            final int number = Integer.parseInt(before);
            if (this.unitRange.isEmpty()) {
              this.unitRange.add(this.civicNumber);
              this.civicNumber = number;
              return setStructuredName(direction, "", after, type, "");
            }

          } catch (final Exception e) {
            if (after.contains("AND ")) {
              throw new IgnoreSiteException("Address is intersection", false);
            }
          }
          return false;
        }
      }
      if (body.equals(before + after)) {
        if (STREET_TYPES.containsKey(after)) {
          body = body.substring(0, before.length());
          return setStructuredName("", "", body, after, direction);
        } else if (STREET_TYPES.containsKey(before)) {
          body = body.substring(before.length());
          return setStructuredName(direction, before, body, "", "");
        } else {
          return setStructuredName("", "", name, "", "");
        }
      }
      return false;
    }
  }

  private boolean isPartsEqualBodyTypeDirectionMiddle2(final String name,
    final String prefixDirection, final String namePrefix, final String body,
    final String nameSuffix) {
    int index = name.indexOf(" " + prefixDirection + " ");
    int length;
    if (index == -1) {
      final String nameLabel = NAME_DIRECTION_LABEL_MAP.getString(prefixDirection);
      index = name.indexOf(" " + nameLabel + " ");
      if (index == -1) {
        index = name.indexOf(" " + prefixDirection + ". ");
        if (index == -1) {
          return false;
        } else {
          length = 4;
        }
      } else {
        length = nameLabel.length() + 2;
      }
    } else {
      length = 3;
    }
    final String before = name.substring(0, index);
    final String after = name.substring(index + length);
    if (before.matches("\\d+")) {
      final int number = Integer.parseInt(before);
      if (number != this.civicNumber) {
        this.civicNumber = number;
      }
      return setStructuredName(prefixDirection, namePrefix, after, nameSuffix, "");
    } else {
      if (before.startsWith("BOX ")) {
        throw new IgnoreSiteException("BOX", false);
      }
      if (!"LOT".equals(before)) {
        this.unitRange.add(before);
      }
      return setStructuredName(prefixDirection, namePrefix, after, nameSuffix, "");
    }
  }

  private boolean isPartsEqualBodyTypeDirectionPrefix(final String name, final String body,
    final String type, final String direction) {
    final int nameLength = name.length();
    if (name.startsWith(body + " ")) {
      String nameType = name.substring(body.length() + 1);
      if (nameType.equals("AVEE")) {
        nameType = "AVE";
      }
      nameType = findNameAffix(nameType);
      if (nameType.equals(type)) {
        return setStructuredName(direction, "", body, type, "");
      } else {
        return false;
      }
    }
    if (name.endsWith(" " + body)) {
      String nameType = name.substring(0, nameLength - body.length() - 1);
      nameType = findNameAffix(nameType);
      if (nameType.endsWith(".")) {
        nameType = nameType.substring(0, nameType.length() - 1);
      }
      if (nameType.equals(type)) {
        return setStructuredName(direction, type, body, "", "");
      } else {
        return false;
      }
    }
    // TODO beginning end duplicate
    return false;
  }

  private boolean isPartsEqualBodyTypeDirectionSuffix(final String name, final String body,
    final String type, final String direction) {
    final int nameLength = name.length();
    if (name.endsWith(" " + body)) {
      String nameType = name.substring(0, nameLength - body.length() - 1);
      nameType = findNameAffix(nameType);
      if (nameType.equals(type)) {
        return setStructuredName("", type, body, "", direction);
      } else {
        return false;
      }
    }
    if (name.startsWith(body + " ")) {
      String nameType = name.substring(body.length() + 1);
      if (nameType.endsWith(".")) {
        nameType = nameType.substring(0, nameType.length() - 1);
      }
      nameType = findNameAffix(nameType);
      if (nameType.equals(type)) {
        return setStructuredName("", "", body, type, direction);
      } else {
        return false;
      }
    }
    return false;
  }

  private boolean matchSingleStructuredName() {
    if (matchStructuredName(this.structuredName)) {
      return true;
    }

    final List<Record> structuredNameRecords = NAME_BODY_RECORD_MAP.get(this.nameBody);
    if (structuredNameRecords != null) {
      Record structuredNameRecord = null;
      for (final Record record : structuredNameRecords) {
        final Identifier structuredNameId = record.getIdentifier();
        if (this.converter.structuredNameIds.contains(structuredNameId)) {
          if (structuredNameRecord == null) {
            structuredNameRecord = record;
          } else {
            return false;
          }
        }
      }
      if (structuredNameRecord != null) {
        addWarning("Mapped name without suffix");
        return setStructuredName(structuredNameRecord);
      }
    }
    return false;
  }

  private boolean matchStructuredName(final String name) {
    Identifier structuredNameId = STRUCTURED_NAMES.getIdentifier(name);
    if (structuredNameId == null) {
      final List<Identifier> structuredNameIds = STRUCTURED_NAMES.getMatchingNameIds(name);
      structuredNameIds.retainAll(this.converter.structuredNameIds);
      if (structuredNameIds.isEmpty()) {
        return false;
      }
      if (structuredNameIds.size() == 1) {
        structuredNameId = structuredNameIds.get(0);
      } else {
        return false;
      }
    }
    final Record structuredNameRecord = STRUCTURED_NAME_RECORD_BY_ID.get(structuredNameId);
    return setStructuredName(structuredNameRecord);
  }

  private boolean matchStructuredName(final String body, final String type,
    final String direction) {
    final String structuredName = this.structuredName;
    if (structuredName.equalsIgnoreCase(Strings.toString(" ", body, type, direction))) {
      // BODY TYPE DIR
      return setStructuredNameId("", "", body, type, direction);
    } else if (structuredName.equalsIgnoreCase(Strings.toString(" ", type, body, direction))) {
      // TYPE BODY DIR
      return setStructuredNameId("", type, body, "", direction);
    } else if (structuredName.equalsIgnoreCase(Strings.toString(" ", direction, body, type))) {
      // DIR BODY TYPE
      return setStructuredNameId(direction, "", body, type, "");
    } else if (structuredName.equalsIgnoreCase(Strings.toString(" ", direction, body, type))) {
      // DIR TYPE BODY
      return setStructuredNameId(direction, type, body, "", "");
    } else if (structuredName.equalsIgnoreCase(Strings.toString(" ", body, direction, type))) {
      // BODY DIR TYPE
      return setStructuredNameId("", "", body + " " + direction, type, "");
    } else if (structuredName.equalsIgnoreCase(Strings.toString(" ", body, direction, type))) {
      // BODY DIR TYPE
      return setStructuredNameId("", "", body + " " + direction, type, "");
    } else {
      return false;
    }
  }

  private void removeBritishColumbia() {
    final String newBody = removeBritishColumbia(this.nameBody);
    if (newBody != this.nameBody) {
      this.nameBody = newBody;
    }
    final String newName = removeBritishColumbia(this.structuredName);
    if (newName != this.structuredName) {
      this.structuredName = newName;
    }
  }

  private String removeBritishColumbia(final String value) {
    final int bcIndex = value.indexOf("BRITISH C");
    if (bcIndex == -1) {
      return value;
    } else {
      int endIndex = bcIndex + 9;
      if (isChar(value, endIndex, 'O')) {
        endIndex++;
        if (isChar(value, endIndex, 'L')) {
          endIndex++;
          if (isChar(value, endIndex, 'U')) {
            endIndex++;
            if (isChar(value, endIndex, 'M')) {
              endIndex++;
              if (isChar(value, endIndex, 'B')) {
                endIndex++;
                if (isChar(value, endIndex, 'I')) {
                  endIndex++;
                  if (isChar(value, endIndex, 'A')) {
                    endIndex++;
                  }
                }
              }
            }
          }
        }
      }
      final String prefix = value.substring(0, bcIndex);
      final String newValue = prefix + value.substring(endIndex);
      return newValue.trim().replaceAll("  +", " ");
    }
  }

  private boolean removePrefix(final String prefix) {
    final String message = "[Street] starts with " + prefix;
    return removePrefix(message, prefix);
  }

  private boolean removePrefix(final String message, final String prefix) {
    if (!prefix.isEmpty()) {
      if (this.nameBody.startsWith(prefix)) {
        if (this.nameBody.length() == prefix.length()) {
        } else if (this.nameBody.charAt(prefix.length()) == ' ') {
          this.nameBody = this.nameBody.substring(prefix.length()).trim();
          if (this.structuredName.startsWith(prefix)) {
            this.structuredName = this.structuredName.substring(prefix.length()).trim();
          }
          addWarning(message);
          return true;
        }
      }
    }
    return false;
  }

  private String removeStartOrEnd(final String string, final String find) {
    final int index = string.indexOf(find);
    if (index != -1) {
      final int length = find.length();
      if (index == 0) {
        if (string.charAt(length) == ' ') {
          return string.substring(length + 1);
        } else {
          return string.substring(length);
        }
      } else {
        final int stringLength = string.length();
        if (index == stringLength - 1) {
          if (string.charAt(stringLength - length - 1) == ' ') {
            final String substring = string.substring(0, stringLength - length - 1);
            return substring;
          } else {
            final String substring = string.substring(0, stringLength - length);
            return substring;
          }
        }
      }
    }
    return string;
  }

  public void removeSuffix(final String suffix) {
    final String message = "[Street] ends with " + suffix;
    removeSuffix(message, suffix);
  }

  private void removeSuffix(final String message, final String suffix) {
    if (this.structuredName.endsWith(suffix) && !this.nameBody.equals(suffix)) {
      this.structuredName = this.structuredName
        .substring(0, this.structuredName.length() - suffix.length())
        .trim();
      if (this.nameBody.endsWith(suffix)) {
        this.nameBody = this.nameBody.substring(0, this.nameBody.length() - suffix.length()).trim();
      }
      addWarning(message);
    }
  }

  private String replace(String string, final String find, final String replace) {
    if (string.indexOf(find) != -1) {
      string = string.replaceAll(find, replace);
    }
    return string;
  }

  public void setStreetName(final String streetName) {
    this.structuredName = streetName;
  }

  private boolean setStructuredName(final Record record) {
    this.structuredNameId = record.getIdentifier();
    this.structuredName = record.getString(StructuredName.FULL_NAME);
    this.namePrefixDirection = record.getString(StructuredName.PREFIX_NAME_DIRECTION_CODE);
    this.namePrefix = record.getCodeValue(StructuredName.NAME_PREFIX_CODE);
    this.nameBody = record.getString(StructuredName.NAME_BODY);
    this.nameSuffix = record.getCodeValue(StructuredName.NAME_SUFFIX_CODE);
    this.nameSuffixDirection = record.getString(StructuredName.SUFFIX_NAME_DIRECTION_CODE, "");
    return true;
  }

  private boolean setStructuredName(final String fullName) {
    final Identifier structuredNameId = STRUCTURED_NAMES.getIdentifier(fullName);
    if (structuredNameId == null) {
      return false;
    } else {
      final Record structuredName = NAME_ID_RECORD_MAP.get(structuredNameId);
      setStructuredName(structuredName);
      return true;
    }
  }

  private boolean setStructuredName(final String namePrefixDirection, final String namePrefix,
    final String nameBody, final String nameSuffix, final String nameSuffixDirection) {
    this.namePrefixDirection = namePrefixDirection;
    this.namePrefix = namePrefix;
    this.nameBody = nameBody;
    this.nameSuffix = nameSuffix;
    this.nameSuffixDirection = nameSuffixDirection;
    this.structuredName = Strings.toString(" ", namePrefixDirection, namePrefix, nameBody,
      nameSuffix, nameSuffixDirection);
    return true;
  }

  private boolean setStructuredNameId(final String namePrefixDirection, final String namePrefix,
    final String nameBody, final String nameSuffix, final String nameSuffixDirection) {
    this.namePrefixDirection = namePrefixDirection;
    this.namePrefix = findNameAffix(namePrefix);
    this.nameBody = nameBody;
    this.nameSuffix = findNameAffix(nameSuffix);
    this.nameSuffixDirection = nameSuffixDirection;
    if (findStructuredName(this.namePrefixDirection, this.namePrefix, this.nameBody,
      this.nameSuffix, this.nameSuffixDirection)) {
      return true;
    }
    if (findStructuredName(this.namePrefixDirection, this.nameSuffix, this.nameBody,
      this.namePrefix, this.nameSuffixDirection)) {
      // Switch prefix and suffix
      return true;
    }
    final String civicNumberWithSpace = Integer.toString(this.civicNumber) + " ";
    if (this.nameBody.startsWith(civicNumberWithSpace)) {
      this.nameBody = nameBody.substring(civicNumberWithSpace.length());
      if (findStructuredName(this.namePrefixDirection, this.namePrefix, this.nameBody,
        this.nameSuffix, this.nameSuffixDirection)) {
        return true;
      }
    }
    if (matchSingleStructuredName()) {
      return true;
    }
    return false;
  }

  @Override
  public String toString() {
    final String newAddress = Strings.toString("-", this.unitRange, this.civicNumber);
    final String newName = this.structuredName;
    final String newParts = getNewParts();
    final String oldAddress = Strings.toString("-", getString(UNIT), getString(NUMBER));
    final String oldName = getOriginalName().toUpperCase();
    final String oldParts = getOriginalParts();
    final StringBuilder s = new StringBuilder();
    s.append(newAddress);
    s.append("|");
    s.append(newName);
    if (!newName.equals(newParts)) {
      s.append("|");
      s.append(newParts);
    }
    s.append("\n");
    s.append(oldAddress);
    s.append("|");
    s.append(oldName);
    if (!oldName.equals(oldParts)) {
      s.append("|");
      s.append(oldParts);
    }
    return s.toString();
  }
}
