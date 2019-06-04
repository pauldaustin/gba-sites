package ca.bc.gov.gbasites.model.type;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jeometry.common.data.identifier.Identifier;

import ca.bc.gov.gba.controller.GbaController;
import ca.bc.gov.gba.model.type.GbaType;
import ca.bc.gov.gba.ui.layer.SessionRecordIdentifierComparator;
import ca.bc.gov.gbasites.model.rule.StreetBlock;
import ca.bc.gov.gbasites.model.type.code.SiteType;

import com.revolsys.collection.range.AbstractRange;
import com.revolsys.collection.range.CharRange;
import com.revolsys.collection.range.IntRange;
import com.revolsys.collection.range.RangeInvalidException;
import com.revolsys.collection.range.RangeSet;
import com.revolsys.collection.range.StringSingletonRange;
import com.revolsys.geometry.model.Point;
import com.revolsys.record.Record;
import com.revolsys.record.Records;
import com.revolsys.util.Emptyable;
import com.revolsys.util.Property;
import com.revolsys.util.Strings;
import com.revolsys.util.Uuid;

public interface SitePoint extends GbaType {
  String ADDRESS_COMMENT = "ADDRESS_COMMENT";

  String CIVIC_NUMBER = "CIVIC_NUMBER";

  String CIVIC_NUMBER_RANGE = "CIVIC_NUMBER_RANGE";

  String CIVIC_NUMBER_SUFFIX = "CIVIC_NUMBER_SUFFIX";

  String COMMUNITY_ID = "COMMUNITY_ID";

  String CUSTODIAN_SITE_ID = "CUSTODIAN_SITE_ID";

  String CUSTODIAN_FULL_ADDRESS = "CUSTODIAN_FULL_ADDRESS";

  String EMERGENCY_MANAGEMENT_SITE_IND = "EMERGENCY_MANAGEMENT_SITE_IND";

  String FULL_ADDRESS = "FULL_ADDRESS";

  String LOCALITY_ID = "LOCALITY_ID";

  String OPEN_DATA_IND = "OPEN_DATA_IND";

  String POSTAL_CODE = "POSTAL_CODE";

  String PARENT_SITE_ID = "PARENT_SITE_ID";

  String REGIONAL_DISTRICT_ID = "REGIONAL_DISTRICT_ID";

  String SITE_COMMENT = "SITE_COMMENT";

  String SITE_ID = "SITE_ID";

  SessionRecordIdentifierComparator SITE_ID_COMPARATOR = new SessionRecordIdentifierComparator(
    SITE_ID);

  String SITE_LOCATION_CODE = "SITE_LOCATION_CODE";

  String SITE_NAME_1 = "SITE_NAME_1";

  String SITE_NAME_2 = "SITE_NAME_2";

  String SITE_NAME_3 = "SITE_NAME_3";

  String SITE_TYPE_CODE = "SITE_TYPE_CODE";

  String STREET_NAME_ID = "STREET_NAME_ID";

  String STREET_NAME = "STREET_NAME";

  String STREET_NAME_ALIAS_1_ID = "STREET_NAME_ALIAS_1_ID";

  List<String> SITE_STRUCTURED_NAME_FIELD_NAMES = Arrays.asList(STREET_NAME_ID,
    STREET_NAME_ALIAS_1_ID);

  String TRANSPORT_LINE_ID = "TRANSPORT_LINE_ID";

  String UNDER_CONSTRUCTION_IND = "UNDER_CONSTRUCTION_IND";

  String UNIT_DESCRIPTOR = "UNIT_DESCRIPTOR";

  String USE_IN_ADDRESS_RANGE_IND = "USE_IN_ADDRESS_RANGE_IND";

  String USE_SITE_NAME_IN_ADDRESS_IND = "USE_SITE_NAME_IN_ADDRESS_IND";

  Path SITES_DIRECTORY = GbaController.getDataDirectory("Sites");

  Pattern UNIT_SUFFIX_RANGE_PATTERN = Pattern
    .compile("(\\d+)([A-Z](?::[A-Z])?(?:;[A-Z](?::[A-Z]))*)");

  static void addRanges(final RangeSet ranges, final Iterable<Record> sites) {
    for (final Record site : sites) {
      final Integer civicNumber = site.getInteger(SitePoint.CIVIC_NUMBER);
      ranges.add(civicNumber);
    }
  }

  static int getBlock(final Record site) {
    if (site != null) {
      final Integer civicNumber = site.getInteger(CIVIC_NUMBER);
      if (civicNumber != null) {
        return StreetBlock.getBlockFrom(civicNumber);
      }
    }
    return -1;
  }

  static int getCivicNumber(final Record site) {
    return site.getInteger(SitePoint.CIVIC_NUMBER, -1);
  }

  static String getFullAddress(final RangeSet unitDescriptor, final String civicNumber,
    final String civicNumberSuffix, final String civicNumberRange, final String streetName) {
    final StringBuilder streetAddress = new StringBuilder();
    if (Property.hasValue((Emptyable)unitDescriptor)) {
      final String simplifiedUnitDescriptor = getSimplifiedUnitDescriptor(unitDescriptor);
      streetAddress.append(simplifiedUnitDescriptor);
      if (Property.hasValuesAny(civicNumber, civicNumberSuffix, civicNumberRange, streetName)) {
        streetAddress.append('-');
      }
    }
    final boolean hasCivicNumber = Property.hasValue(civicNumber);
    if (hasCivicNumber) {
      streetAddress.append(civicNumber);
    }
    final boolean hasCivicNumberSuffix = Property.hasValue(civicNumberSuffix);
    if (hasCivicNumberSuffix) {
      if (hasCivicNumber) {
        final char suffixFirstChar = civicNumberSuffix.charAt(0);
        if (suffixFirstChar >= 'A' && suffixFirstChar <= 'Z') {
        } else {
          streetAddress.append(' ');
        }
      }
      streetAddress.append(civicNumberSuffix);
    }
    if (Property.hasValue(civicNumberRange)) {
      if (hasCivicNumber || hasCivicNumberSuffix) {
        streetAddress.append(' ');
      }
      streetAddress.append(civicNumberRange);
    }
    if (Property.hasValue(streetName)) {
      if (streetAddress.length() > 0) {
        streetAddress.append(' ');
      }
      streetAddress.append(streetName);
    }
    return streetAddress.toString();
  }

  static String getFullAddress(final Record site) {
    final String unitDescriptor = site.getString(UNIT_DESCRIPTOR);
    final String civicNumber = site.getString(CIVIC_NUMBER);
    final String civicNumberSuffix = Strings.upperCase(site.getString(CIVIC_NUMBER_SUFFIX));
    final String civicNumberRange = site.getString(CIVIC_NUMBER_RANGE);
    final Identifier structuredNameId = site.getIdentifier(STREET_NAME_ID);
    final String fullName;
    if (structuredNameId == null) {
      fullName = site.getString(STREET_NAME);
    } else {
      fullName = GbaController.structuredNames.getValue(structuredNameId);
    }
    final RangeSet unitRange = RangeSet.newRangeSet(unitDescriptor);
    return getFullAddress(unitRange, civicNumber, civicNumberSuffix, civicNumberRange, fullName);
  }

  static RangeSet getRanges(final Iterable<Record> sites) {
    final RangeSet ranges = new RangeSet();
    addRanges(ranges, sites);
    return ranges;
  }

  static String getSimplifiedUnitDescriptor(final RangeSet unitDescriptorRange) {
    final RangeSet simplifiedRange = new RangeSet();
    int min = Integer.MAX_VALUE;
    int max = Integer.MIN_VALUE;
    for (final AbstractRange<?> range : unitDescriptorRange.getRanges()) {
      if (range instanceof IntRange) {
        final IntRange intRange = (IntRange)range;
        final int from = intRange.getFrom();
        if (from < min) {
          min = from;
        }
        final int to = intRange.getTo();
        if (to > max) {
          max = to;
        }
      } else if (range instanceof CharRange) {
        final CharRange charRange = (CharRange)range;
        final char from = charRange.getFrom();
        final char to = charRange.getTo();
        simplifiedRange.addRange(from, to);
      } else if (range instanceof StringSingletonRange) {
        final String part = range.toString();
        final Matcher matcher = UNIT_SUFFIX_RANGE_PATTERN.matcher(part);
        if (matcher.matches()) {
          final int number = Integer.parseInt(matcher.group(1));
          if (number < min) {
            min = number;
          }
          if (number > max) {
            max = number;
          }
        } else {
          simplifiedRange.add(part);
        }
      }
    }
    if (min != Integer.MAX_VALUE) {
      if (min == max) {
        return unitDescriptorRange.toString();
      } else {
        simplifiedRange.add(min + "~" + max);
      }
    }
    return simplifiedRange.toString();
  }

  static RangeSet getUnitDescriptorRanges(final Record site) {
    final String unitDescriptor = site.getString(UNIT_DESCRIPTOR);
    return RangeSet.newRangeSet(unitDescriptor);
  }

  static boolean hasNumericUnitDescriptor(final Record site) {
    try {
      for (final Object unit : getUnitDescriptorRanges(site)) {
        if (unit instanceof Number) {
          return true;
        }
      }
    } catch (final RangeInvalidException e) {
    }
    return false;
  }

  static boolean isInLocality(final Record site, final Identifier localityId) {
    if (site.equalValue(LOCALITY_ID, localityId)) {
      return true;
    }
    return false;
  }

  static boolean isUseInAddressRange(final Record site) {
    return Records.getBoolean(site, USE_IN_ADDRESS_RANGE_IND);
  }

  static boolean isVirtual(final Record site) {
    final String siteTypeCode = site.getString(SITE_TYPE_CODE);
    return Strings.startsWith(siteTypeCode, SiteType.VIRTUAL);
  }

  static boolean isVirtualBlockEnd(final Record site) {
    final String siteType = site.getString(SITE_TYPE_CODE);
    return SiteType.VIRTUAL_BLOCK_FROM.equals(siteType)
      || SiteType.VIRTUAL_BLOCK_TO.equals(siteType);
  }

  static void setUseInAddressRange(final Record site, final boolean useInAddressRange) {
    String value;
    if (useInAddressRange) {
      value = "Y";
    } else {
      value = "N";
    }
    site.setValue(USE_IN_ADDRESS_RANGE_IND, value);
  }

  static void updateCustodianSiteId(final String uuidNamespace, final Record sitePoint) {
    final Point point = sitePoint.getGeometry();
    final double x = Math.round(point.getX());
    final double y = Math.round(point.getY());
    final String fullAddress = sitePoint.getString(FULL_ADDRESS);
    final String idParts = Strings.toString(",", x, y, fullAddress.toUpperCase());
    final String custodianSiteId = Uuid.sha1(uuidNamespace, idParts).toString();
    sitePoint.setValue(CUSTODIAN_SITE_ID, custodianSiteId);
  }

  static boolean updateFullAddress(final Record site) {
    final String fullAddress = getFullAddress(site);
    return site.setValue(FULL_ADDRESS, fullAddress);
  }
}
