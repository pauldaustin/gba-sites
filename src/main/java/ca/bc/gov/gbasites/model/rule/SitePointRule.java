package ca.bc.gov.gbasites.model.rule;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.function.Predicate;

import org.jeometry.common.data.identifier.Identifier;
import org.jeometry.common.data.type.DataType;
import org.jeometry.common.logging.Logs;
import org.jeometry.common.number.Doubles;
import org.jeometry.common.number.Numbers;

import ca.bc.gov.gba.controller.GbaController;
import ca.bc.gov.gba.model.Gba;
import ca.bc.gov.gba.model.GbaTables;
import ca.bc.gov.gba.model.message.QaMessageDescription;
import ca.bc.gov.gba.model.type.GbaType;
import ca.bc.gov.gba.model.type.StructuredName;
import ca.bc.gov.gba.model.type.TransportLine;
import ca.bc.gov.gba.model.type.code.HouseNumberScheme;
import ca.bc.gov.gba.model.type.code.StructuredNames;
import ca.bc.gov.gba.rule.AbstractRecordRule;
import ca.bc.gov.gba.rule.RecordRule;
import ca.bc.gov.gba.rule.RecordRuleThreadProperties;
import ca.bc.gov.gba.rule.fix.SetValue;
import ca.bc.gov.gba.rule.fix.SetValues;
import ca.bc.gov.gba.rule.impl.FieldValueRule;
import ca.bc.gov.gba.rule.transportline.AddressRange;
import ca.bc.gov.gba.ui.layer.SessionRecordIdentifierComparator;
import ca.bc.gov.gbasites.model.type.SitePoint;
import ca.bc.gov.gbasites.model.type.SiteTables;
import ca.bc.gov.gbasites.qa.QaSitePoint;

import com.revolsys.collection.CollectionUtil;
import com.revolsys.collection.SetValueHolderRunnable;
import com.revolsys.collection.ValueHolder;
import com.revolsys.collection.map.Maps;
import com.revolsys.collection.range.IntMinMax;
import com.revolsys.collection.range.RangeInvalidException;
import com.revolsys.collection.range.RangeSet;
import com.revolsys.geometry.algorithm.GeometryFactoryIndexedPointInAreaLocator;
import com.revolsys.geometry.graph.Edge;
import com.revolsys.geometry.graph.Graph;
import com.revolsys.geometry.graph.Node;
import com.revolsys.geometry.index.RecordSpatialIndex;
import com.revolsys.geometry.model.End;
import com.revolsys.geometry.model.Geometry;
import com.revolsys.geometry.model.LineString;
import com.revolsys.geometry.model.Lineal;
import com.revolsys.geometry.model.Point;
import com.revolsys.geometry.model.Side;
import com.revolsys.geometry.model.metrics.PointLineStringMetrics;
import com.revolsys.geometry.operation.distance.DistanceWithPoints;
import com.revolsys.io.FileUtil;
import com.revolsys.io.Writer;
import com.revolsys.record.Record;
import com.revolsys.record.Records;
import com.revolsys.record.code.CodeTableValueComparator;
import com.revolsys.record.filter.ClosestRecordFilter;
import com.revolsys.record.io.RecordWriter;
import com.revolsys.record.io.format.tsv.Tsv;
import com.revolsys.record.io.format.tsv.TsvWriter;
import com.revolsys.util.Counter;
import com.revolsys.util.Property;
import com.revolsys.util.Strings;

public class SitePointRule extends AbstractRecordRule implements Cloneable, SitePoint {
  private static final Function<Integer, StreetBlock> CREATE_MESSAGE_BLOCK_FUNCTION = (block) -> {
    return new StreetBlock(block);
  };

  public static final SetValues FIX_USE_IN_ADDRESS_RANGE_IND_NO = new SetValues(
    "Set Fb(USE_IN_ADDRESS_RANGE_IND)=Vb(N),Fb(TRANSPORT_LINE_ID)=Vb(null)",
    Maps.<String, Object> buildLinkedHash()
      .add(USE_IN_ADDRESS_RANGE_IND, "N")
      .add(TRANSPORT_LINE_ID, null));

  public static final QaMessageDescription MESSAGE_BLOCK_NO_SITES = new QaMessageDescription(
    "BLK_NOSITE", "No Tb(SITE_POINT) records found for Tb(TRANSPORT_LINE) in street block",
    "A street block must either have site points for each side of the street block.",
    "No Tb(SITE_POINT) for {side} side(s) of block Vb({blockRange}) Vb({name})", false);

  public static final QaMessageDescription MESSAGE_BLOCK_NO_TRANSPORT_LINE_HOUSE_NUMBERS = new QaMessageDescription(
    "BLK_NOTLHN", "No Tb(TRANSPORT_LINE) house number range for Tb(SITE_POINT) in street block",
    "A street block must have a Tb(TRANSPORT_LINE) house number range for Tb(SITE_POINT).",
    "No Tb(TRANSPORT_LINE) house number range for {side} side(s) of block Vb({blockRange}) Vb({name})",
    false);

  public static final QaMessageDescription MESSAGE_BLOCK_TRANSPORT_LINE_SITE_RANGE_DIFFERENT = new QaMessageDescription(
    "BLK_SRNTLR",
    "The Tb(TRANSPORT_LINE) house number range is different from the Tb(SITE_POINT) derived range in street block",
    "A street block must have a Tb(TRANSPORT_LINE) house number range matching the Tb(SITE_POINT) range.",
    "Tb(TRANSPORT_LINE) {streetRange} != {siteRange} Tb(SITE_POINT) for {side} side(s) of block Vb({blockRange}) Vb({name})",
    false);

  public static final List<String> MESSAGE_DUPLICATE_EQUAL_EXCLUDE = Arrays.asList(OBJECTID,
    SITE_ID, ADDRESS_COMMENT, TRANSPORT_LINE_ID, CREATE_INTEGRATION_SESSION_ID,
    MODIFY_INTEGRATION_SESSION_ID, CREATE_PARTNER_ORG_ID, MODIFY_PARTNER_ORG_ID, CAPTURE_DATE,
    EXTENDED_DATA, EXCLUDED_RULES);

  public static final QaMessageDescription MESSAGE_MISSING_MAIN_SITE_POINT_FOR_UNIT_NUMBER = new QaMessageDescription(
    "SP_MHNUN", "Missing main Tb(SITE_POINT) for Fb(UNIT_DESCRIPTOR)",
    "Tb(SITE_POINT) records with a Fb(UNIT_DESCRIPTOR) must be have a Tb(SITE_POINT) with the same Fb(CIVIC_NUMBER) and no Fb(UNIT_DESCRIPTOR).",
    "Missing Tb(SITE_POINT) Fb(CIVIC_NUMBER)=Vb({civicNumber}) Fb(Structured Name 1)=Vb({name}).",
    false);

  public static final QaMessageDescription MESSAGE_SITE_POINT_DUPLICATE_EQUAL = new QaMessageDescription(
    "SP_DUPEQ", "Exact Duplicate",
    "Tb(SITE_POINT) records where Fb(USE_IN_ADDRESS_RANGE_IND)=Vb(Y) must not have the same field values.",
    "Exact duplicate for Vb({fullAddress}) with Tb(SITE_POINT)=Vb({otherId}).", false) //
      .addQuickFix(FIX_USE_IN_ADDRESS_RANGE_IND_NO) //
      .addQuickFix(FIX_COMPARE_RECORDS) //
      .addQuickFix(FIX_DELETE_RECORD) //
      .addQuickFix(FIX_DELETE_OTHER_RECORD);

  public static final QaMessageDescription MESSAGE_SITE_POINT_DUPLICATE_FULL_ADDRESS = new QaMessageDescription(
    "SP_DUPFA", "Full Address Duplicate",
    "Tb(SITE_POINT) records where Fb(USE_IN_ADDRESS_RANGE_IND)=Vb(Y) must not have the same Fb(FULL_ADDRESS).",
    "Duplicate for Vb({fullAddress}) with Tb(SITE_POINT)=Vb({otherId}).", false) //
      .addQuickFix(FIX_USE_IN_ADDRESS_RANGE_IND_NO) //
      .addQuickFix(FIX_COMPARE_RECORDS) //
      .addQuickFix(FIX_DELETE_RECORD) //
      .addQuickFix(FIX_DELETE_OTHER_RECORD);

  public static final QaMessageDescription MESSAGE_SITE_POINT_DUPLICATE_UNIT = new QaMessageDescription(
    "SP_DUPUD", "Unit Duplicate",
    "Tb(SITE_POINT) records where Fb(USE_IN_ADDRESS_RANGE_IND)=Vb(Y) must not have the same unit in Fb(UNIT_DESCRIPTOR).",
    "Duplicate for Vb({fullAddress}) with Tb(SITE_POINT)=Vb({otherId}).", false) //
      .addQuickFix(FIX_USE_IN_ADDRESS_RANGE_IND_NO) //
      .addQuickFix(FIX_COMPARE_RECORDS) //
      .addQuickFix(FIX_DELETE_RECORD) //
      .addQuickFix(FIX_DELETE_OTHER_RECORD);

  public static final QaMessageDescription MESSAGE_SITE_POINT_MISSING_UNIT_DESCRIPTOR = new QaMessageDescription(
    "SP_NOUD", "Missing Fb(UNIT_DESCRIPTOR)",
    "Tb(SITE_POINT) records that are closest to a Tb(TRANSPORT_LINE) with a Fb(SINGLE_HOUSE_NUMBER) should have a Fb(UNIT_DESCRIPTOR).",
    "Missing Fb(UNIT_DESCRIPTOR) for Vb({fullAddress}).", true);

  public static final QaMessageDescription MESSAGE_SITE_POINT_TOO_CLOSE = new QaMessageDescription(
    "SP_CLOSE", "Site Point Too Close",
    "Tb(SITE_POINT) should be at least 1m from other site point records.",
    "Tb(SITE_POINT) Vb({fullAddress}) < 1m from Tb(SITE_POINT)=Vb({otherId}).", true) //
      .addQuickFix(FIX_COMPARE_RECORDS) //
      .addQuickFix(FIX_DELETE_RECORD) //
      .addQuickFix(FIX_DELETE_OTHER_RECORD);

  public static final QaMessageDescription MESSAGE_SITE_POINT_TRANSPORT_LINE_CIVIC_AND_SINGLE_DIFFER = new QaMessageDescription(
    "SPTL_SHNCN", "Fb(CIVIC_NUMBER) != Tb(TRANSPORT_LINE) Fb(SINGLE_HOUSE_NUMBER)",
    "Tb(SITE_POINT) records that are closest to a Tb(TRANSPORT_LINE) with a Fb(SINGLE_HOUSE_NUMBER) should have Fb(CIVIC_NUMBER)=Fb(SINGLE_HOUSE_NUMBER).",
    "Fb(CIVIC_NUMBER) Vb({civicNumber}) != Vb({singleCivicNumber}) Tb(TRANSPORT_LINE).Fb(SINGLE_HOUSE_NUMBER).",
    true);

  public static final QaMessageDescription MESSAGE_SITE_POINT_TRANSPORT_LINE_CIVIC_NUMBER_NOT_IN_MESSAGE_BLOCK_RANGE = new QaMessageDescription(
    "SPTL_NCNBR", "Fb(CIVIC_NUMBER) not in Tb(TRANSPORT_LINE) address block range",
    "The Tb(SITE_POINT) records that are closest to a Tb(TRANSPORT_LINE) should have Fb(CIVIC_NUMBER) in the same street block number as the address range. "
      + "Either change the Tb(TRANSPORT_LINE) address range, Fb(CIVIC_NUMBER), Fb(STRUCTURED_NAME_1_ID) or set Fb(US_IN_ADDRESS_RANGE)=Vb(No).",
    "Vb({fullAddress}) not in Tb(TRANSPORT_LINE) block Vb({addressRange}).", true) //
      .addQuickFix(FIX_USE_IN_ADDRESS_RANGE_IND_NO);

  public static final QaMessageDescription MESSAGE_SITE_POINT_TRANSPORT_LINE_DUPLICATE_CIVIC_NUMBER = new QaMessageDescription(
    "SPTL_DUPCN", "Tb(SITE_POINT) -> Tb(TRANSPORT_LINE) Multiple Matches for Fb(CIVIC_NUMBER)",
    "For each Fb(CIVIC_NUMBER) there should only be one Tb(TRANSPORT_LINE) matching Tb(SITE_POINT).",
    "Tb(SITE_POINT) -> Tb(TRANSPORT_LINE) Multiple Matches for Vb({fullAddress})", false);

  public static final QaMessageDescription MESSAGE_SITE_POINT_TRANSPORT_LINE_LOCALITY_DIFFERENT = new QaMessageDescription(
    "SPTL_LOCID", "Tb(TRANSPORT_LINE) has Different Fb(LOCALITY_ID)",
    "Tb(SITE_POINT) records should have the same Fb(LOCALITY_ID) as the matching Tb(TRANSPORT_LINE). "
      + "Verify the Fb(LOCALITY_ID) or add an exclusion as required.",
    "Fb(LOCALITY_ID)=Vb({localityName}) != Eb({otherLocalityName}) from Tb(TRANSPORT_LINE)=Vb({transportLineId}).",
    true) //
      .addQuickFix(new SetValue("{otherLocalityId}"));

  public static final QaMessageDescription MESSAGE_SITE_POINT_TRANSPORT_LINE_NAME_DIFFERENT = new QaMessageDescription(
    "SPTL_DIFNM", "Tb(TRANSPORT_LINE) has Different street names",
    "Tb(SITE_POINT) records should have the same street names fields as the matching Tb(TRANSPORT_LINE). "
      + "Verify the street names or add an exclusion as required.",
    "Fb({fieldTitle})=Vb({name}) != Eb({otherName}) from Tb(TRANSPORT_LINE)=Vb({transportLineId}).",
    true) //
      .addQuickFix(new SetValue("{otherValue}"));

  public static final QaMessageDescription MESSAGE_SITE_POINT_TRANSPORT_LINE_NAME_NONE = new QaMessageDescription(
    "SPTL_NONAM", "No Tb(TRANSPORT_LINE) With Same Fb(STRUCTURED_NAME_*_ID)",
    "Tb(SITE_POINT) records should have a Tb(TRANSPORT_LINE) with one common STRUCTURED_NAME_*_ID. Verify the STRUCTURED_NAME_*_ID or add an exclusion as required.",
    "No Tb(TRANSPORT_LINE) for Fb(STRUCTURED_NAME_*_ID) in (Vb({names})).", true);

  public static final QaMessageDescription MESSAGE_SITE_POINT_TRANSPORT_LINE_NO_CLOSE = new QaMessageDescription(
    "SPTL_NONE", "No Tb(TRANSPORT_LINE) With Same Fb(STRUCTURED_NAME_*_ID) < 1000m",
    "Tb(SITE_POINT) records with Fb(USE_IN_ADDRESS_RANGE_IND)=Vb(Y) should have a Tb(TRANSPORT_LINE) with one common Fb(STRUCTURED_NAME_*_ID) within 1000m."
      + "Verify the Fb(STRUCTURED_NAME_*_ID) on the Tb(SITE_POINT) and Tb(TRANSPORT_LINE). "
      + "If needed manually set the Fb(TRANSPORT_LINE_ID). "
      + "Alternatively move this Tb(SITE_POINT) or Construct a new different Tb(SITE_POINT) closer to the Tb(TRANSPORT_LINE).",
    "No Tb(TRANSPORT_LINE) with same Fb(STRUCTURED_NAME_*_ID) in (Vb({names})) < Vb(1000m).", true);

  public static final QaMessageDescription MESSAGE_SITE_POINT_TRANSPORT_LINE_NOT_CLOSE = new QaMessageDescription(
    "SPTL_NOTCL", "Distance to Tb(TRANSPORT_LINE) > 1000m",
    "Tb(SITE_POINT) records with Fb(USE_IN_ADDRESS_RANGE_IND)=Vb(Y) should have a Fb(TRANSPORT_LINE_ID) < 1000m. "
      + "Move this Tb(SITE_POINT) or Construct a new different Tb(SITE_POINT) that is closer to the Tb(TRANSPORT_LINE) or add an exclusion.",
    "Tb(TRANSPORT_LINE) distance Vb({distance}m) &gt; Vb(1000m).", true);

  public static final QaMessageDescription MESSAGE_SITE_POINT_TRANSPORT_LINE_NOT_CLOSEST = new QaMessageDescription(
    "SPTL_NOCLS", "Tb(TRANSPORT_LINE) Not Closest",
    "Tb(SITE_POINT) records with Fb(USE_IN_ADDRESS_RANGE_IND)=Vb(Y) should have a Fb(TRANSPORT_LINE_ID) that is closest to the Tb(SITE_POINT)."
      + "Move this Tb(SITE_POINT) or Construct a new different Tb(SITE_POINT) that is closer to the Tb(TRANSPORT_LINE) or add an exclusion.",
    "Fb(TRANSPORT_LINE_ID)=Vb({transportLineId}) distance Vb({distance}m) > Vb({otherDistance}m) from closest Vb({otherId}).",
    true);

  public static final QaMessageDescription MESSAGE_SITE_POINT_TRANSPORT_LINE_SCHEME_DIFFER = new QaMessageDescription(
    "SPTL_CNSHD", "Fb(CIVIC_NUMBER) != Tb(TRANSPORT_LINE)'s Scheme",
    "The Fb(CIVIC_NUMBER) for a Tb(SITE_POINT) must match the Fb(*_HOUSE_NUMBERING_SCHEME) on the Tb(TRANSPORT_LINE).",
    "Vb({fullAddress}) is not Vb({scheme}).", false) //
      .addQuickFix(FIX_USE_IN_ADDRESS_RANGE_IND_NO);

  public static final QaMessageDescription MESSAGE_SITE_POINT_VIRTUAL_LOCATION = new QaMessageDescription(
    "SP_VRTLOC", "Virtual Tb(SITE_POINT) not in Correct Location",
    "Virtual Tb(SITE_POINT) not in Correct Location",
    "Virtual Tb(SITE_POINT) not in Correct Location", true);

  public static final QaMessageDescription MESSAGE_SITE_POINT_VIRTUAL_MIDDLE_OF_RANGE = new QaMessageDescription(
    "SP_VRTMID", "Virtual Tb(SITE_POINT) in middle of range",
    "Virtual site points should only be at the ends of the street ranges",
    "Virtual Tb(SITE_POINT) in middle of range", true);

  public static final QaMessageDescription MESSAGE_SITE_POINT_VIRTUAL_NO_CLOSE = new QaMessageDescription(
    "SP_VRTNCLS", "Virtual Tb(SITE_POINT) has no Tb(TRANSPORT_LINE) < 100m",
    "Virtual Tb(SITE_POINT) has no Tb(TRANSPORT_LINE) < 100m",
    "Virtual Tb(SITE_POINT) has no Tb(TRANSPORT_LINE) < 100m.", true);

  public static final QaMessageDescription MESSAGE_STRUCTURED_NAME_DUPLICATE_IN_LOCALITY = new QaMessageDescription(
    "SN_LCDUP", "Duplicate Structured Name in Locality",
    "Structured names which have the same spelling but different capitalization, punctuation or spelling differences (e.g. Tenth vs 10th) are not allowed in the same locality.",
    "Duplicate Structured Name in Locality Vb({name})", true);

  public static final QaMessageDescription MESSAGE_TRANSPORT_LINE_SITE_POINT_MESSAGE_BLOCK_NONE = new QaMessageDescription(
    "TLSP_NOBLK", "No Sites for Block",
    "A Tb(TRANSPORT_LINE) with an address range should have Tb(SITE_POINT).",
    "No Tb(SITE_POINT) for block Vb({block}) Vb({name})", false);

  public static final QaMessageDescription MESSAGE_TRANSPORT_LINE_SITE_POINT_NAME_NONE = new QaMessageDescription(
    "TLSP_NONAM", "No Tb(SITE_POINT) for Tb(TRANSPORT_LINE) with Name",
    "There must exist Tb(SITE_POINT)  records for Tb(TRANSPORT_LINE) with a given structured name.",
    "No Tb(SITE_POINT) for Tb(TRANSPORT_LINE) with Fb(Structured Name 1)=Vb({name}).", true);

  private static final int TRANSPORT_LINE_DISTANCE_TOLERANCE = 1000;

  public static void addDataAddressRange(final Map<String, Object> data,
    final Record transportLine) {
    if (transportLine != null) {
      final StringBuilder range = new StringBuilder();
      final HouseNumberScheme leftScheme = TransportLine.getHouseNumberScheme(transportLine,
        Side.LEFT);
      range.append(leftScheme);
      if (!leftScheme.isNone()) {
        final Integer fromLeft = transportLine.getInteger(TransportLine.FROM_LEFT_HOUSE_NUMBER);
        final Integer toLeft = transportLine.getInteger(TransportLine.TO_LEFT_HOUSE_NUMBER);
        range.append(' ');
        range.append(fromLeft);
        range.append('-');
        range.append(toLeft);
      }
      range.append(", ");
      final HouseNumberScheme rightScheme = TransportLine.getHouseNumberScheme(transportLine,
        Side.RIGHT);
      range.append(rightScheme);
      if (!rightScheme.isNone()) {
        final Integer fromRight = transportLine.getInteger(TransportLine.FROM_RIGHT_HOUSE_NUMBER);
        final Integer toRight = transportLine.getInteger(TransportLine.TO_RIGHT_HOUSE_NUMBER);
        range.append(' ');
        range.append(fromRight);
        range.append('-');
        range.append(toRight);
      }
      data.put("addressRange", range);
    }
  }

  public static void addDataFullAddress(final Map<String, Object> data, final Record site) {
    final String fullAddress = site.getString(SitePoint.FULL_ADDRESS);
    data.put("fullAddress", fullAddress);
  }

  // private AddressRange expandAddressRange(
  // final Map<Integer, Record> transportLineByHouseNumber,
  // final Edge<Record> edge, final AddressRange numberRange) {
  // final int fromHouseNumber = numberRange.getFromHouseNumberDirectional();
  // final int previousBlockStart = (fromHouseNumber / 100 - 1) * 100;
  // final int toHouseNumber = numberRange.getToHouseNumberDirectional();
  // final int nextBlockEnd = (toHouseNumber / 100 + 2) * 100;
  // Record previousTransportLine = null;
  // int rangeStart = previousBlockStart;
  // int rangeEnd = previousBlockStart;
  // for (int houseNumber = previousBlockStart; houseNumber < nextBlockEnd;
  // houseNumber++) {
  // final Record transportLine = transportLineByHouseNumber.get(houseNumber);
  // if (previousTransportLine == transportLine) {
  // rangeEnd = houseNumber;
  // } else if (transportLine != null) {
  // if (previousTransportLine != null) {
  // System.out.println(rangeStart + "-" + rangeEnd + " "
  // + previousTransportLine.getIdentifier());
  // }
  // previousTransportLine = transportLine;
  // rangeStart = houseNumber;
  // rangeEnd = houseNumber;
  // }
  // }
  // if (previousTransportLine != null) {
  // System.out.println(rangeStart + "-" + rangeEnd + " "
  // + previousTransportLine.getIdentifier());
  // }
  // return null;
  // }

  /**
   * Get the list of {@link GbaTables#SITE_POINT} records with the current {@link RecordRuleThreadProperties#getLocalityId()}.
   *
   * @return The list of sites.
   */
  public static List<Record> getSites() {
    List<Record> sites = RecordRuleThreadProperties.getProperty("sites");
    if (sites == null) {
      final Map<Integer, Record> sitesById = new TreeMap<>();
      final Counter totalCounter = RecordRuleThreadProperties
        .getTotalCounter(QaSitePoint.SITE_READ);
      final Identifier localityId = RecordRuleThreadProperties.getLocalityId();
      final List<Record> records = RecordRuleThreadProperties.getRecords(totalCounter, localityId,
        SiteTables.SITE_POINT, false);
      final GeometryFactoryIndexedPointInAreaLocator locator = GbaController.getLocalities()
        .getPointLocator(localityId);
      for (final Record site : RecordRuleThreadProperties.i(records)) {
        final Point point = site.getGeometry();
        if (locator.intersects(point)) {
          sitesById.put(site.getInteger(SITE_ID), site);
        } else {
          totalCounter.add(-1);
        }
      }
      sites = new ArrayList<>(sitesById.values());
      RecordRuleThreadProperties.setProperty("sites", sites);
    }
    return sites;
  }

  public static StreetBlock getStreetBlock(final int number) {
    final Map<Integer, StreetBlock> blocksByNumber = RecordRuleThreadProperties
      .getProperty("streetBlocksByNumber", Maps.factoryTree());
    return blocksByNumber.get(number);
  }

  public static List<Record> getTransportLines() {
    final Identifier localityId = RecordRuleThreadProperties.getLocalityId();
    final Counter totalCounter = RecordRuleThreadProperties
      .getTotalCounter(QaSitePoint.TRANSPORT_LINE_READ);
    final List<Record> records = RecordRuleThreadProperties.getRecords(totalCounter, localityId,
      GbaTables.TRANSPORT_LINE, false);
    final RecordSpatialIndex<Record> index = RecordSpatialIndex.quadTree(Gba.GEOMETRY_FACTORY_2D)
      .addRecords(records);
    RecordRuleThreadProperties.setSpatialIndex(GbaTables.TRANSPORT_LINE, index);
    return records;
  }

  public static boolean hasAddresses(final List<Record> transportLines) {
    for (final Record transportLine : transportLines) {
      if (TransportLine.hasHouseNumbers(transportLine)) {
        return true;
      }
    }
    return false;
  }

  public static boolean isInBlockRange(final Record transportLine, final Integer civicNumber) {
    final Integer min = TransportLine.getMinimumCivicNumber(transportLine);
    final Integer max = TransportLine.getMaximumCivicNumber(transportLine);
    if (min == null) {
      return true;
    } else if (StreetBlock.isInBlock(civicNumber, min, max)) {
      return true;
    } else {
      return false;
    }
  }

  public static String replace(final String string, final String find, final String replacement) {
    if (string == null) {
      return null;
    } else if (find == null) {
      return string;
    } else {
      return string.replaceAll(find.replaceAll("([\\(\\)])", "\\\\$1").replaceAll("\\?", "\\\\?"),
        replacement);
    }
  }

  private Writer<Record> deletedWriter;

  // private boolean validateAddressRange(
  // final Map<Integer, Record> transportLineByHouseNumber,
  // final Edge<Record> edge, final Record transportLine,
  // final Map<Record, Set<Record>> addressesByTransportLine,
  // final Map<Integer, Record> siteByHouseNumber,
  // final String schemeField, final String fromField, final String toField) {
  // final LineString line = transportLine.getGeometry();
  // final List<Record> addresses =
  // CollectionUtil.list(addressesByTransportLine.get(transportLine));
  // // Collections.sort(addresses, new
  // // HouseNumberSitePointDistanceComparator(
  // // line));
  // final HouseNumberScheme scheme = transportLine.getString(schemeField);
  // final Integer fromHouseNumber = transportLine.getInteger(fromField);
  // final Integer toHouseNumber = transportLine.getInteger(toField);
  // if (addresses.isEmpty()) {
  // if (HouseNumberingScheme.NONE.equals(scheme)) {
  // return true;
  // } else {
  // // System.err.println("no house numbers");
  // }
  // } else {
  // final RecordLogController recordLog =
  // RecordRuleThreadProperties.getRecordLog();
  // if (HouseNumberingScheme.NONE.equals(scheme)) {
  // // System.err.println("numbers found for scheme none");
  // // update
  // } else {
  // final AddressRange range = new AddressRange(scheme, fromHouseNumber,
  // toHouseNumber);
  // boolean schemeValid = true;
  // final List<Integer> houseNumbers = new ArrayList<>();
  // for (final Record site : addresses) {
  // final WmsIdentifier addressId = site.getIdentifier();
  // final Geometry point = site.getGeometry();
  // final Integer houseNumber =
  // site.getValue(CIVIC_NUMBER);
  // if (houseNumber <= 0) {
  // recordLog.log(SiteTables.SITE_POINT, addressId,
  // FieldValueRule.FIELD_MINIMUM, "House number <= 0 " + houseNumber,
  // point, CIVIC_NUMBER);
  // schemeValid = false;
  // } else {
  // houseNumbers.add(houseNumber);
  // if (scheme.equalsIgnoreCase(HouseNumberingScheme.ODD_INCREASING)) {
  // if (houseNumber % 2 == 0) {
  // recordLog.log(SiteTables.SITE_POINT, addressId,
  // AddressRangeRule.NUMBER_ODD, "House number not odd "
  // + houseNumber, point, CIVIC_NUMBER);
  // schemeValid = false;
  // }
  // } else if (scheme.equalsIgnoreCase(HouseNumberingScheme.EVEN_INCREASING)) {
  // if (houseNumber % 2 == 1) {
  // recordLog.log(SiteTables.SITE_POINT, addressId,
  // AddressRangeRule.NUMBER_EVEN, "House number not even "
  // + houseNumber, point, CIVIC_NUMBER);
  // schemeValid = false;
  // }
  // }
  // }
  // if (schemeValid && !range.contains(houseNumber)) {
  // recordLog.log(SiteTables.SITE_POINT, addressId, "ADDR_NITLR",
  // "House number not in address range " + houseNumber + " " + range,
  // point, CIVIC_NUMBER);
  // }
  // }
  // if (!houseNumbers.isEmpty()) {
  // final AddressRange numberRange = AddressRange.create(houseNumbers);
  // final AddressRange expandedRange = expandAddressRange(
  // transportLineByHouseNumber, edge, numberRange);
  // final boolean matchedStart = fromHouseNumber.equals(houseNumbers.get(0));
  // final boolean matchedEnd =
  // toHouseNumber.equals((houseNumbers.get(houseNumbers.size() - 1)));
  // if (matchedStart) {
  // if (matchedEnd) {
  // return true;
  // } else {
  // // System.err.println("Matched Start");
  // }
  // } else if (matchedEnd) {
  // // System.err.println("Matched End");
  // } else {
  // final boolean reverseStart = toHouseNumber.equals(houseNumbers.get(0));
  // final boolean reverseEnd =
  // fromHouseNumber.equals((houseNumbers.get(houseNumbers.size() - 1)));
  // if (reverseStart) {
  // if (reverseEnd) {
  // // System.err.println("Reversed");
  // } else {
  // // System.err.println("Reversed Start");
  // }
  // } else if (reverseEnd) {
  // // System.err.println("Reversed End");
  // } else {
  //
  // }
  // }
  // }
  // }
  // }
  // return false;
  // }

  private Map<String, Identifier> localityNameIdBySimplifiedNameMap;

  private Map<Record, List<Record>> sitesByTransportLine;

  private Identifier structuredNameId;

  private Map<Identifier, Integer> transportLineMissingSiteNameCount;

  private Map<Identifier, Record> transportLinesById;

  private Map<Identifier, List<Record>> transportLinesByStructuredNameId;

  public SitePointRule() {
    super(null, TRANSPORT_LINE_DISTANCE_TOLERANCE);
    setFieldNames(STREET_NAME_ALIAS_1_ID, COMMUNITY_ID, FULL_ADDRESS, CIVIC_NUMBER,
      CIVIC_NUMBER_SUFFIX, LOCALITY_ID, REGIONAL_DISTRICT_ID, EMERGENCY_MANAGEMENT_SITE_IND,
      SITE_LOCATION_CODE, SITE_NAME_1, SITE_NAME_2, SITE_NAME_3, SITE_TYPE_CODE, STREET_NAME_ID,
      TRANSPORT_LINE_ID, UNDER_CONSTRUCTION_IND, UNIT_DESCRIPTOR, USE_IN_ADDRESS_RANGE_IND);
  }

  protected void addDataName(final Map<String, Object> data) {
    final String name = getStructuredName();
    data.put("name", name);
  }

  public void addMessages(final Street street) {
    final int count = street.getTransportLineCount();
    if (street.hasExactMatch()) {
      addCount("Info", "Site -> Transport Line Address match", count);
    }
    for (final Side side : Side.VALUES) {
      final StreetSide address = street.getStreetSide(side);
      if (address.hasExactMatch()) {
        addCount("Info", "Site -> Transport Line Address match " + side, count);
      }
      for (final End lineEnd : End.VALUES) {
        if (address.hasMatch(lineEnd)) {
          addCount("Info", "Site -> Transport Line Address match " + side + " " + lineEnd, count);
        } else if (address.hasMatchReverse(lineEnd)) {
          addCount("Info",
            "Site -> Transport Line Address match " + side + " " + lineEnd + " reverse", count);
        }
      }
    }
  }

  private void addStreetToBlock(final Map<Integer, StreetBlock> blocksById, int block,
    final Street street) {
    block = StreetBlock.getBlockFrom(block);
    if (block >= 0) {
      final StreetBlock streetBlock = Maps.get(blocksById, block, CREATE_MESSAGE_BLOCK_FUNCTION);
      streetBlock.addStreet(street);
    }
  }

  private void addStreetToBlock(final Map<Integer, StreetBlock> streetBlocksById,
    final Street street, final Integer from, final Integer to) {
    if (from == null || from < 0) {
      if (to != null && to >= 0) {
        final int block = StreetBlock.getBlockFrom(to);
        addStreetToBlock(streetBlocksById, block, street);
      }
    } else {
      if (to == null || to < 0) {
        final int block = StreetBlock.getBlockFrom(from);
        addStreetToBlock(streetBlocksById, block, street);
      } else {
        int blockFrom = StreetBlock.getBlockFrom(from);
        int blockTo = StreetBlock.getBlockFrom(to);

        if (blockFrom > blockTo) {
          final int temp = blockFrom;
          blockFrom = blockTo;
          blockTo = temp;
        }
        for (int block = blockFrom; block <= blockTo; block++) {
          addStreetToBlock(streetBlocksById, block, street);
        }
      }
    }
  }

  @Override
  public void clearLocality() {
    super.clearLocality();
    this.structuredNameId = null;
    this.localityNameIdBySimplifiedNameMap = null;
    this.transportLinesById = null;
    this.transportLinesByStructuredNameId = null;
    this.transportLineMissingSiteNameCount = null;
    this.sitesByTransportLine = null;
    FileUtil.closeSilent(this.deletedWriter);
    this.deletedWriter = null;
  }

  public void deleteSite(final Record site, final String message) {
    writeDeleted(site);
    final RecordSpatialIndex<Record> index = getIndex();
    if (index != null) {
      index.removeRecord(site);
    }
    if (deleteRecord(site)) {
      addCount("Fixed", message);
    }
  }

  private boolean equalExceptNameType(final String name, final String addressBcName) {
    final StructuredNames structuredNames = GbaController.structuredNames;
    final Record structuredName = structuredNames.getStructuredName(name);
    if (structuredName.hasValue(StructuredName.NAME_SUFFIX_CODE)) {
      final List<String> parts = new ArrayList<>();
      StructuredNames.addPart(parts, structuredName, StructuredName.PREFIX_NAME_DIRECTION_CODE,
        null);
      StructuredNames.addPart(parts, structuredName, StructuredName.NAME_PREFIX_CODE,
        structuredNames.getPrefixCodeTable());
      StructuredNames.addPart(parts, structuredName, StructuredName.NAME_BODY, null);

      parts.add("[A-Z0-9]+");

      StructuredNames.addPart(parts, structuredName, StructuredName.SUFFIX_NAME_DIRECTION_CODE,
        null);
      StructuredNames.addPart(parts, structuredName, StructuredName.NAME_DESCRIPTOR_CODE,
        structuredNames.getDescriptorCodeTable());
      final String namePattern = Strings.toString(" ", parts).toUpperCase().replace("?", "\\?");
      if (addressBcName.matches(namePattern)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Get all the full addresses for the site. If the {@link SitePoint#UNIT_DESCRIPTOR} is
   * a range then there will be one address for each unit when expanding the range.
   * @param site
   * @return
   */
  private List<String> getFullAddresses(final Record site) {
    final String fullAddress = site.getString(FULL_ADDRESS);
    final String unitDescriptor = site.getString(UNIT_DESCRIPTOR);
    if (Property.hasValue(unitDescriptor)) {
      try {
        final RangeSet ranges = RangeSet.newRangeSet(unitDescriptor);
        if (ranges.size() > 1) {
          final List<String> addresses = new ArrayList<>();
          for (final Object unit : ranges) {
            final String address = fullAddress.replace(unitDescriptor, unit.toString());
            addresses.add(address);
          }
          return addresses;
        }
      } catch (final RangeInvalidException e) {
        Logs.debug(this, "Invalid range: " + site, e);
      }
    }
    return Collections.singletonList(fullAddress);
  }

  private Identifier getLocalityStructuredNameId(final Identifier structuredNameId) {
    final String simplifiedName = GbaController.structuredNames.getSimplifiedName(structuredNameId);
    return Maps.get(this.localityNameIdBySimplifiedNameMap, simplifiedName, structuredNameId);
  }

  public Map<Record, List<Record>> getSitesByTransportLine() {
    return this.sitesByTransportLine;
  }

  public String getStructuredName() {
    return GbaController.structuredNames.getValue(this.structuredNameId);
  }

  public Record getTransportLine(final Record site) {
    final Identifier transportLineId = site.getIdentifier(TRANSPORT_LINE_ID);
    if (transportLineId != null) {
      return this.transportLinesById.get(transportLineId);
    }
    return null;
  }

  @Override
  public void initLocality() {
    super.initLocality();
    getSites();
    final List<Record> transportLines = new ArrayList<>();

    initTransportLines(transportLines);
    initTransportLinesByStructuredNameId(transportLines);
    initLocalityNameIdBySimplifiedNameMap();
    this.transportLineMissingSiteNameCount = new TreeMap<>(GbaController.structuredNames);
  }

  private void initLocalityNameIdBySimplifiedNameMap() {
    this.localityNameIdBySimplifiedNameMap = new HashMap<>();
    for (final Identifier structuredNameId : RecordRuleThreadProperties
      .i(this.transportLinesByStructuredNameId.keySet())) {
      final String simplifiedName = GbaController.structuredNames
        .getSimplifiedName(structuredNameId);
      final Identifier currentStructuredNameId = this.localityNameIdBySimplifiedNameMap
        .get(simplifiedName);
      if (currentStructuredNameId == null) {
        this.localityNameIdBySimplifiedNameMap.put(simplifiedName, structuredNameId);
      } else {
        final int currentSize = this.transportLinesByStructuredNameId.get(currentStructuredNameId)
          .size();
        final int size = this.transportLinesByStructuredNameId.get(structuredNameId).size();
        if (size > currentSize) {
          // TODO move all the transport lines too!

          this.localityNameIdBySimplifiedNameMap.put(simplifiedName, structuredNameId);
        }
      }
    }
  }

  /**
   * initialize the map from {@link SitePoint#STREET_NAME_ID} to list of {@link GbaTables#SITE_POINT} records.
   *
   * @param sitesByStructuredNameId Map from {@link SitePoint#STREET_NAME_ID} to list of {@link GbaTables#SITE_POINT} records.
   * @param sitesWithNoStructuredName List of {@link GbaTables#SITE_POINT} records that don't have a {@link SitePoint#STREET_NAME_ID}.
   */
  private void initSitesByStructuredNameId(
    final Map<Identifier, List<Record>> sitesByStructuredNameId,
    final List<Record> sitesWithNoStructuredName) {
    final List<Record> localitySites = getSites();
    for (final Record site : ndi(localitySites)) {
      Identifier structuredNameId = site.getIdentifier(STREET_NAME_ID);
      if (structuredNameId == null) {
        sitesWithNoStructuredName.add(site);
      } else {
        structuredNameId = getLocalityStructuredNameId(structuredNameId);
        Maps.addToList(sitesByStructuredNameId, structuredNameId, site);
      }
    }
  }

  public Collection<StreetBlock> initStreetBlocks(final Graph<Street> streetGraph) {
    final Map<Integer, StreetBlock> streetBlocksByNumber = new TreeMap<>();
    RecordRuleThreadProperties.setProperty("streetBlocksByNumber", streetBlocksByNumber);
    for (final Street street : streetGraph.getEdgeObjects()) {
      for (final Side side : Side.VALUES) {
        if (street.localityEqual(this, side)) {
          final Integer from = street.getTransportLineNumber(End.FROM, side);
          final Integer to = street.getTransportLineNumber(End.TO, side);
          addStreetToBlock(streetBlocksByNumber, street, from, to);

          final StreetSide streetSide = street.getStreetSide(side);
          final IntMinMax siteMinMax = streetSide.getSiteMinMax();
          if (!siteMinMax.isEmpty()) {
            final int siteMin = siteMinMax.getMin();
            final int siteMax = siteMinMax.getMax();
            addStreetToBlock(streetBlocksByNumber, street, siteMin, siteMax);
          }
        }
      }
    }
    final Collection<StreetBlock> streetBlocks = streetBlocksByNumber.values();
    return streetBlocks;
  }

  private void initStreets(final List<Street> streets, final List<Record> transportLines) {
    for (final Record transportLine : transportLines) {
      if (!transportLine.hasValue(TransportLine.SINGLE_HOUSE_NUMBER)) {
        final Street street = new Street(this, transportLine, null);
        streets.add(street);
      }
    }
  }

  private void initStreetStrata(final List<Street> streets, final List<Record> transportLines,
    final Integer singleCivicNumber) {
    if (transportLines != null) {
      for (final Record transportLine : transportLines) {
        if (transportLine.getInteger(TransportLine.SINGLE_HOUSE_NUMBER) == singleCivicNumber) {
          final Street street = new Street(this, transportLine, singleCivicNumber);
          streets.add(street);
        }
      }
    }
  }

  private void initTransportLines(final List<Record> transportLines) {
    this.transportLinesById = new HashMap<>();
    RecordRuleThreadProperties.setProperty("siteRule.TransportLines", transportLines);

    final Identifier localityId = RecordRuleThreadProperties.getLocalityId();
    final Counter totalCounter = RecordRuleThreadProperties
      .getTotalCounter(QaSitePoint.TRANSPORT_LINE_READ);
    final List<Record> records = getTransportLines();
    for (final Record transportLine : RecordRuleThreadProperties.i(records)) {
      final boolean inLocality = TransportLine.isInLocality(transportLine, localityId);
      if (inLocality && TransportLine.isDemographic(transportLine)) {
        final Identifier transportLineId = transportLine.getIdentifier();
        this.transportLinesById.put(transportLineId, transportLine);
        transportLines.add(transportLine);
      } else {
        totalCounter.add(-1);
      }
    }
  }

  private void initTransportLinesByStructuredNameId(final List<Record> transportLines) {
    this.transportLinesByStructuredNameId = new TreeMap<>(
      new CodeTableValueComparator(GbaController.structuredNames));
    final Map<String, Map<Identifier, List<Record>>> recordsBySimplifiedNameAndNameId = new TreeMap<>();
    for (final Record transportLine : transportLines) {
      Identifier selectedStucturedNameId = null;
      for (final String fieldName : TransportLine.STRUCTURED_NAME_FIELD_NAMES) {
        final Identifier structuredNameId = transportLine.getIdentifier(fieldName);
        if (structuredNameId != null) {
          final String simplifiedName = GbaController.structuredNames
            .getSimplifiedName(structuredNameId);
          Maps.addToList(recordsBySimplifiedNameAndNameId, simplifiedName, structuredNameId,
            transportLine);
          if (TransportLine.STRUCTURED_NAME_1_ID.equals(fieldName)) {
            selectedStucturedNameId = structuredNameId;
          } else {
            if (GbaController.structuredNames.hasFieldValue(selectedStucturedNameId,
              StructuredName.NAME_DESCRIPTOR_CODE)) {
              if (!GbaController.structuredNames.hasFieldValue(structuredNameId,
                StructuredName.NAME_DESCRIPTOR_CODE)) {
                selectedStucturedNameId = structuredNameId;
              }
            }
          }
        }
      }
      if (selectedStucturedNameId != null) {
        Maps.addToList(this.transportLinesByStructuredNameId, selectedStucturedNameId,
          transportLine);
      }
    }
    for (final Record site : fi(getSites(), RecordRule.notDeleted())) {
      for (final String fieldName : SITE_STRUCTURED_NAME_FIELD_NAMES) {
        final Identifier structuredNameId = site.getIdentifier(fieldName);
        if (structuredNameId != null) {
          final String simplifiedName = GbaController.structuredNames
            .getSimplifiedName(structuredNameId);
          if (simplifiedName != null) {
            Maps.addToList(recordsBySimplifiedNameAndNameId, simplifiedName, structuredNameId,
              site);
          }
        }
      }
    }
    for (final Map<Identifier, List<Record>> recordsByNameId : recordsBySimplifiedNameAndNameId
      .values()) {
      if (recordsByNameId.size() > 1) {
        for (final Entry<Identifier, List<Record>> entry : recordsByNameId.entrySet()) {
          final Identifier structuredNameId = entry.getKey();
          final String name = GbaController.structuredNames.getValue(structuredNameId);
          final List<Record> records = entry.getValue();

          final Map<String, Object> data = new HashMap<>();
          data.put("name", name);
          final Record structuredName = GbaController.structuredNames.getRecord(structuredNameId);
          Geometry geometry = Records.unionGeometry(records);
          if (geometry != null) {
            final List<LineString> lines = geometry.getGeometries(LineString.class);

            for (final Point point : geometry.getGeometries(Point.class)) {
              final double x = point.getX();
              final double y = point.getY();
              final LineString line = Gba.GEOMETRY_FACTORY_2D.lineString(2, x, y, x + 1, y);
              lines.add(line);
            }
            final Geometry line = Gba.GEOMETRY_FACTORY_2D.geometry(lines);
            geometry = line.union();
          }
          addMessage(structuredName, MESSAGE_STRUCTURED_NAME_DUPLICATE_IN_LOCALITY, geometry, data,
            StructuredName.FULL_NAME);

        }
      }
    }
  }

  private boolean isCloser(final PointLineStringMetrics metrics,
    final PointLineStringMetrics matchedMetrics) {
    return matchedMetrics == null || metrics.getDistance() < matchedMetrics.getDistance();
  }

  protected boolean isHasStreetErrors() {
    return RecordRuleThreadProperties.isBooleanProperty("SitePointRule.hasStreetErrors");
  }

  private void linkStreets(final Graph<Street> graph) {
    for (final Node<Street> node : graph.nodes()) {
      if (node.getDegree() == 2) {
        final Edge<Street> edge1 = node.getEdge(0);
        final Edge<Street> edge2 = node.getEdge(1);
        if (!edge1.equals(edge2)) {
          final Street street1 = edge1.getObject();
          final Street street2 = edge2.getObject();
          if (edge1.getEnd(node).isFrom()) {
            street1.setPreviousStreet(street2);
          } else {
            street1.setNextStreet(street2);
          }
          if (edge2.getEnd(node).isFrom()) {
            street2.setPreviousStreet(street1);
          } else {
            street2.setNextStreet(street1);
          }
        }
      }
    }
  }

  private void logMissingMainSitePointForUnitNumber(final int civicNumber,
    final List<Record> strataSites) {
    if (strataSites != null) {
      final int siteCount = strataSites.size();
      if (siteCount > 1) {
        final Geometry geometry = Records.unionGeometry(strataSites);
        final Record logRecord = strataSites.get(0);
        final Map<String, Object> data = new HashMap<>();
        data.put("civicNumber", civicNumber);
        addDataName(data);
        addMessage(logRecord, MESSAGE_MISSING_MAIN_SITE_POINT_FOR_UNIT_NUMBER, geometry, data,
          CIVIC_NUMBER);
      }
    }
  }

  private void logNameCounts(final Map<Identifier, Integer> nameCounts, final String fileName,
    final String countName) {
    if (!nameCounts.isEmpty()) {
      final Path logDirectory = getLogDirectory();
      final Path logFile = logDirectory.resolve(fileName);
      try (
        TsvWriter writer = Tsv.plainWriter(logFile)) {
        writer.write(StructuredName.STRUCTURED_NAME_ID, StructuredName.FULL_NAME, countName);
        for (final Entry<Identifier, Integer> entry : nameCounts.entrySet()) {
          final Identifier structuredNameId = entry.getKey();
          final String name = GbaController.structuredNames.getValue(structuredNameId);
          final Integer count = entry.getValue();
          writer.write(structuredNameId, name, count);
        }
      }
    }
  }

  /**
   * Log an error for all transport lines where the structured name doesn't have any
   * site points for the locality.
   *
   * @param structuredNameId The name id.
   * @param sitesByStructuredNameId The list of transport line records.
   */
  private boolean logSitePointsWithNoTransportLines(
    final Map<Identifier, List<Record>> sitesByStructuredNameId) {
    boolean valid = true;
    final Map<Identifier, Integer> nameCounts = new TreeMap<>(GbaController.structuredNames);
    for (final Entry<Identifier, List<Record>> entry : sitesByStructuredNameId.entrySet()) {
      final Identifier structuredNameId = entry.getKey();
      final List<Record> sites = entry.getValue();
      for (final Record site : sites) {
        final Identifier localityId = getLocalityId();
        if (SitePoint.isUseInAddressRange(site) && SitePoint.isInLocality(site, localityId)) {
          Maps.addCount(nameCounts, structuredNameId);
          final List<Identifier> structuredNameIds = Records.getIdentifiers(site,
            SITE_STRUCTURED_NAME_FIELD_NAMES);
          final Map<String, Object> data = new TreeMap<>();
          addDataNames(data, structuredNameIds);
          valid &= addMessage(site, MESSAGE_SITE_POINT_TRANSPORT_LINE_NAME_NONE, data,
            STREET_NAME_ID);
        }
      }
    }
    logNameCounts(nameCounts, "SITE_POINT_NAME_NO_TRANSPORT_LINE.tsv", "SITE_POINT_COUNT");
    return valid;
  }

  /**
   * Log an error for all transport lines where the structured name doesn't have any
   * site points for the locality.
   *
   * @param structuredNameId The structured name id
   * @param transportLines The list of transport line records.
   * @param structuredNameId The name id.
   */
  private void logTransportLinesWithNoSitePoints(final Identifier structuredNameId,
    final List<Record> transportLines) {
    for (final Record transportLine : transportLines) {
      if (TransportLine.isInLocality(transportLine, this.getLocalityId())) {
        if (TransportLine.hasHouseNumbers(transportLine)
          && !Street.isStructureUnencumbered(transportLine)) {
          final String name = GbaController.structuredNames.getValue(structuredNameId);
          final Map<String, String> data = Collections.singletonMap("name", name);
          addMessage(transportLine, MESSAGE_TRANSPORT_LINE_SITE_POINT_NAME_NONE, data,
            TransportLine.STRUCTURED_NAME_1_ID);
          Maps.addCount(this.transportLineMissingSiteNameCount, structuredNameId);
        }
      }
    }
  }

  public void mergeStreet(final List<Street> streets, final Graph<Street> graph,
    final Node<Street> node, final Edge<Street> edge1, final Edge<Street> edge2) {
    final Street street1 = edge1.getObject();
    final Street street2 = edge2.getObject();
    final End end1 = edge1.getEnd(node);
    final End end2 = edge2.getEnd(node);
    int blockEqualCount = 0;
    int schemeNoneCount1 = 0;
    int schemeNoneCount2 = 0;
    // Only merge if localities and schemes are the same
    for (final Side side : Side.VALUES) {
      Side side2 = side;
      if (!end1.isOpposite(end2)) {
        side2 = side2.opposite();
      }
      final Identifier localityId1 = street1.getLocalityId(side);
      final Identifier localityId2 = street2.getLocalityId(side2);
      if (!DataType.equal(localityId1, localityId2)) {
        return;
      }
      final StreetSide streetSide1 = street1.getStreetSide(side);
      final StreetSide streetSide2 = street2.getStreetSide(side2);
      final HouseNumberScheme scheme1 = streetSide1.getTransportLineScheme();
      final HouseNumberScheme scheme2 = streetSide2.getTransportLineScheme();
      if (scheme1.isNone()) {
        if (scheme2.isNone() || !streetSide1.hasSiteNumbers()
          || streetSide1.hasSiteBlockOverlap(streetSide2.getTransportLineMinMax())) {
          schemeNoneCount1++;
        }
        // TODO check what part of range
      } else if (scheme2.isNone()) {
        if (!streetSide2.hasSiteNumbers()
          || streetSide2.hasSiteBlockOverlap(streetSide1.getTransportLineMinMax())) {
          schemeNoneCount2++;
        }
        schemeNoneCount2++;
        // TODO check what part of range
      } else if (DataType.equal(scheme1, scheme2)) {
        final Integer civicNumber1 = streetSide1.getTransportLineNumber(end1);
        final Integer civicNumber2 = streetSide2.getTransportLineNumber(end2);

        final int block1 = StreetBlock.getBlockFrom(civicNumber1);
        final int block2 = StreetBlock.getBlockFrom(civicNumber2);

        if (block1 == block2) {
          blockEqualCount++;
        }
        if (Math.abs(block1 - block2) > 100) {
          return;
        }
        final int delta = Math.abs(civicNumber1 - civicNumber2);
        if (delta == 1 || delta == 2 && scheme1.isEvenOrOdd()) {
          // OK to merge
        } else {
          final int step = scheme1.getStep();
          int start;
          int end;
          if (civicNumber1 > civicNumber2) {
            start = civicNumber2 + step;
            end = civicNumber1;
          } else {
            start = civicNumber1 + step;
            end = civicNumber2;
          }
          for (int civicNumber = start; civicNumber < end; civicNumber += step) {
            for (final Street street : streets) {
              if (street.containsTransportLineNumber(civicNumber)) {
                return;
              }
            }
          }
        }
      } else {
        return;
      }
    }
    if (blockEqualCount == 0) {
      if (schemeNoneCount1 == 2) {
      } else if (schemeNoneCount2 == 2) {
      } else {
        return;
      }
    }
    for (final String fieldName : new String[] {
      TransportLine.TRANSPORT_LINE_DIVIDED_CODE
    }) {
      final Object value1 = street1.getTransportLineFieldValue(fieldName);
      final Object value2 = street2.getTransportLineFieldValue(fieldName);
      if (!DataType.equal(value1, value2)) {
        return;
      }
    }

    final Street mergedStreet = street1.merge(end1, street2, end2);
    graph.addEdge(mergedStreet, mergedStreet.getLine());
    graph.remove(edge1);
    graph.remove(edge2);
  }

  public void mergeStreets(final Graph<Street> graph, final List<Street> streets) {
    graph.forEachNode((node) -> {
      final List<Edge<Street>> edges = node.getEdges();
      if (edges.size() == 2) {
        final Edge<Street> edge1 = edges.get(0);
        final Edge<Street> edge2 = edges.get(1);
        mergeStreet(streets, graph, node, edge1, edge2);
      }
    }, Node.filterDegree(2));
  }

  public boolean setForeignKeyIdentifier(final Record record, final String fieldName,
    final Record referencedRecord) {
    Identifier identifier = null;
    if (referencedRecord != null) {
      identifier = getIdentifier(referencedRecord);
    }
    return record.setValue(fieldName, identifier);
  }

  private void setTransportLine(final Record site, final Record transportLine) {
    // TODO if (setForeignKeyIdentifier(site, TRANSPORT_LINE_ID, transportLine))
    // {
    // if (transportLine == null) {
    // addCount("Fixed", "Clear TRANSPORT_LINE_ID");
    // } else {
    // addCount("Fixed", "Set TRANSPORT_LINE_ID");
    // }
    //
    // }
  }

  @Override
  public String toString() {
    if (this.getLocalityId() == null) {
      return "Idle";
    } else {
      final String localityName = RecordRuleThreadProperties.getLocalityName();
      if (this.structuredNameId == null) {
        return localityName;
      } else {
        return getStructuredName() + ", " + localityName;
      }
    }
  }

  /**
   * Validate the {@link GbaTables#SITE_POINT} records within the locality.
   */
  @Override
  public boolean validateLocality() {
    boolean valid = true;
    final Map<Identifier, List<Record>> sitesByStructuredNameId = Identifier.newTreeMap();
    final List<Record> sitesWithNoStructuredName = new ArrayList<>();
    initSitesByStructuredNameId(sitesByStructuredNameId, sitesWithNoStructuredName);

    validateSitesWithNoNames(sitesByStructuredNameId, sitesWithNoStructuredName);

    for (final Entry<Identifier, List<Record>> entry : i(
      this.transportLinesByStructuredNameId.entrySet())) {
      this.structuredNameId = entry.getKey();
      final List<Record> streetTransportLines = entry.getValue();
      final List<Record> streetSites = sitesByStructuredNameId.remove(this.structuredNameId);
      valid &= validateLocalityStreet(this.structuredNameId, streetTransportLines, streetSites);
    }

    valid &= logSitePointsWithNoTransportLines(sitesByStructuredNameId);
    logNameCounts(this.transportLineMissingSiteNameCount, "TRANSPORT_LINE_NAME_NO_SITE_POINT.tsv",
      "SITE_POINT_COUNT");
    return valid;
  }

  private boolean validateLocalityStreet(final Identifier structuredNameId,
    final List<Record> streetTransportLines, final List<Record> streetSites) {
    final boolean valid = true;
    if (GbaController.structuredNames.isGeneric(structuredNameId)) {
    } else if (streetSites == null) {
      logTransportLinesWithNoSitePoints(structuredNameId, streetTransportLines);
    } else {

      final Set<Integer> mainCivicNumbers = new HashSet<>();
      final List<Record> mainTransportLines = new ArrayList<>();
      final List<Record> mainSites = new ArrayList<>();

      final Set<Integer> strataCivicNumbers = new TreeSet<>();
      final Map<Integer, List<Record>> strataSitesByCivicNumber = new TreeMap<>();
      final Map<Integer, List<Record>> strataTransportLinesByCivicNumber = new TreeMap<>();

      for (final Record transportLine : RecordRuleThreadProperties.fi(streetTransportLines,
        RecordRule.notDeleted())) {
        final Integer strataCivicNumber = transportLine
          .getInteger(TransportLine.SINGLE_HOUSE_NUMBER);
        if (strataCivicNumber == null) {
          mainTransportLines.add(transportLine);
        } else {
          if (strataCivicNumber > 0) {
            strataCivicNumbers.add(strataCivicNumber);
            Maps.addToList(strataTransportLinesByCivicNumber, strataCivicNumber, transportLine);
          }
        }
      }

      for (final Record site : RecordRuleThreadProperties.fi(streetSites,
        RecordRule.notDeleted())) {
        final boolean useInAddressRange = SitePoint.isUseInAddressRange(site);
        if (useInAddressRange) {
          final Integer civicNumber = site.getInteger(CIVIC_NUMBER);
          if (Property.hasValue(civicNumber) && civicNumber >= 0) {
            if (site.hasValue(UNIT_DESCRIPTOR) && strataCivicNumbers.contains(civicNumber)) {
              Maps.addToList(strataSitesByCivicNumber, civicNumber, site);
            } else {
              mainCivicNumbers.add(civicNumber);
              mainSites.add(site);
            }
          }
        }
      }

      validateLocalityStreetBlocks(mainTransportLines, mainSites);

      for (final Integer strataCivicNumber : strataCivicNumbers) {
        final List<Record> strataSites = strataSitesByCivicNumber.get(strataCivicNumber);
        final List<Record> strataTransportLines = strataTransportLinesByCivicNumber
          .get(strataCivicNumber);

        validateLocalityStreetStrata(mainCivicNumbers, strataCivicNumber, strataTransportLines,
          strataSites);
      }
    }
    return valid;
  }

  private void validateLocalityStreetBlocks(final List<Record> transportLines,
    final List<Record> sites) {

    this.sitesByTransportLine = new HashMap<>();

    for (final Record site : sites) {
      final Record transportLine = getTransportLine(site);
      Maps.addToList(this.sitesByTransportLine, transportLine, site);
    }

    final List<Street> streets = new ArrayList<>();
    initStreets(streets, transportLines);

    final Graph<Street> streetGraph = new Graph<>();
    for (final Street street : streets) {
      final LineString line = street.getLine();
      streetGraph.addEdge(street, line);
    }
    mergeStreets(streetGraph, streets);
    linkStreets(streetGraph);

    final Collection<StreetBlock> streetBlocks = initStreetBlocks(streetGraph);

    final List<StreetBlock> reverseStreetBlocks = new ArrayList<>(streetBlocks);
    Collections.reverse(reverseStreetBlocks);

    for (final StreetBlock streetBlock : streetBlocks) {
      streetBlock.validateCivicNumberDuplicates(this);

      streetBlock.expand(this);
    }
    // Repeat the expand in the opposite direction to pick up additional matches
    for (final StreetBlock streetBlock : reverseStreetBlocks) {
      streetBlock.expand(this);
    }

    for (final StreetBlock streetBlock : streetBlocks) {
      addCount("Info", "Block");
      if (streetBlock.validate(this)) {
        addCount("Info", "Block matched");
      }
    }
  }

  private void validateLocalityStreetStrata(final Set<Integer> mainCivicNumbers,
    final int civicNumber, final List<Record> transportLines, final List<Record> strataSites) {
    if (mainCivicNumbers.contains(civicNumber)) {
      logMissingMainSitePointForUnitNumber(civicNumber, strataSites);
    }
    validateLocalityStreetStrataDuplicate(strataSites);

    validateLocalityStreetStrataBlocks(transportLines, strataSites, civicNumber);
  }

  private void validateLocalityStreetStrataBlocks(final List<Record> transportLines,
    final List<Record> sites, final Integer singleCivicNumber) {
    this.sitesByTransportLine = new HashMap<>();
    if (sites != null) {
      for (final Record site : sites) {
        final Record transportLine = getTransportLine(site);
        Maps.addToList(this.sitesByTransportLine, transportLine, site);
      }
    }

    final List<Street> streets = new ArrayList<>();
    initStreetStrata(streets, transportLines, singleCivicNumber);

    final Graph<Street> streetGraph = new Graph<>();
    for (final Street street : streets) {
      final LineString line = street.getLine();
      streetGraph.addEdge(street, line);
    }
    linkStreets(streetGraph);

  }

  private void validateLocalityStreetStrataDuplicate(final List<Record> strataSites) {
    final Map<String, List<Record>> sitesByFullAddress = new TreeMap<>();
    if (strataSites != null) {
      for (final Record site : ndi(strataSites)) {
        if (SitePoint.isUseInAddressRange(site)) {
          final List<String> fullAddresses = getFullAddresses(site);
          for (final String fullAddress : fullAddresses) {
            Maps.addToList(sitesByFullAddress, fullAddress, site);
          }
        }
      }
    }
    for (final Entry<String, List<Record>> entry : sitesByFullAddress.entrySet()) {
      final String fullAddress = entry.getKey();
      final List<Record> sitesForFullAddress = entry.getValue();
      if (sitesForFullAddress.size() > 1) {
        int i = 1;
        for (final Record site : sitesForFullAddress) {
          final Record duplicateSite = sitesForFullAddress.get(i);
          final Map<String, Object> data = Maps.newLinkedHash("fullAddress", fullAddress);
          addDataOtherId(data, site, false);
          addMessage(duplicateSite, MESSAGE_SITE_POINT_DUPLICATE_UNIT, data, UNIT_DESCRIPTOR);
          i = 0;
        }
      }
    }
  }

  @Override
  protected boolean validateRecordDo(final Record site) {
    boolean valid = true;

    if (SitePoint.isVirtual(site)) {
      valid &= validateRecordVirtual(site);
    } else {
      if (SitePoint.isUseInAddressRange(site)) {
        valid &= validateRecordMatchTransportLine(site);
      } else {
        if (site.setValue(TRANSPORT_LINE_ID, null)) {
          addCount("Fixed", "Secondary address set TRANSPORT_LINE_ID=null");
        }
      }
      valid &= validateRecordDuplicateExactOrClose(site);
    }
    return valid;
  }

  private boolean validateRecordDuplicateExactOrClose(final Record site1) {
    boolean valid = true;
    final Point point1 = site1.getGeometry();
    final Collection<Record> sitesWithinDistance = queryDistance(point1, 2);
    final boolean fixingAllowed = isFixingAllowed();
    if (sitesWithinDistance.size() > 1) {
      final String fullAddress1 = site1.getString(FULL_ADDRESS);
      final Map<Identifier, Record> closeSites = Identifier.newTreeMap();
      for (final Record site2 : ndi(sitesWithinDistance)) {
        final Identifier identifier2 = site2.getIdentifier();
        final String fullAddress2 = site2.getString(FULL_ADDRESS);
        if (isSame(site1, site2)) {
        } else if (!site1.equalValue(STREET_NAME_ID, site2.getValue(STREET_NAME_ID))) {
          if (SitePoint.isUseInAddressRange(site2)) {
            closeSites.put(identifier2, site2);
          }
        } else if (DataType.equal(site1, site2, MESSAGE_DUPLICATE_EQUAL_EXCLUDE)) {
          if (fixingAllowed) {
            if (SITE_ID_COMPARATOR.compare(site1, site2) < 0) {
              deleteSite(site2, "Duplicate (exact) deleted");
            } else {
              deleteSite(site1, "Duplicate (exact) deleted");
              return true;
            }
          } else {
            final Map<String, Object> data = Maps.newLinkedHash("fullAddress", fullAddress1);
            addDataOtherId(data, site2, false);
            valid &= addMessage(site1, MESSAGE_SITE_POINT_DUPLICATE_EQUAL, data, FULL_ADDRESS);
          }
        } else if (Strings.equals(fullAddress1, fullAddress2)) {
          if (fixingAllowed) {
            // boolean canDelete1 =
            // !site1.equalValue(EMERGENCY_MANAGEMENT_SITE_IND, "Y");
            // boolean canDelete2 =
            // !site2.equalValue(EMERGENCY_MANAGEMENT_SITE_IND, "Y");
            // for (final String fieldName : Arrays.asList(SITE_TYPE_CODE,
            // SITE_LOCATION_CODE,
            // STREET_NAME_ALIAS_1_ID, FEATURE_STATUS_CODE, SITE_NAME_1,
            // SITE_NAME_2, SITE_NAME_3)) {
            // final String value1 = site1.getString(fieldName);
            // final String value2 = site2.getString(fieldName);
            // if (value1 == null) {
            // if (value2 != null) {
            // canDelete2 = false;
            // }
            // } else if (value2 == null) {
            // canDelete1 = false;
            // } else if (!value1.equals(value2)) {
            // canDelete1 = false;
            // canDelete2 = false;
            // }
            // }
            // if (canDelete1) {
            // if (!canDelete2) {
            // deleteSite(site1, "FIXED: Duplicate (full_address/close)
            // deleted");
            // }
            // } else if (canDelete2) {
            // deleteSite(site2, "FIXED: Duplicate (full_address/close)
            // deleted");
            // }
            if (SitePoint.isUseInAddressRange(site2)) {
              closeSites.put(identifier2, site2);
            }
          } else {
            if (SitePoint.isUseInAddressRange(site2)) {
              closeSites.put(identifier2, site2);
            }
          }
        } else {
          if (SitePoint.isUseInAddressRange(site2)) {
            closeSites.put(identifier2, site2);
          }
        }
      }
      if (!closeSites.isEmpty()) {
        final Record site2 = CollectionUtil.get(closeSites.values(), 0);
        if (SitePoint.isUseInAddressRange(site1)) {
          // Only log the id of the first match
          final Map<String, Object> data = Maps.newLinkedHash("fullAddress", fullAddress1);
          addDataOtherId(data, site2, true);
          valid &= addMessage(site1, MESSAGE_SITE_POINT_TOO_CLOSE, data, GbaType.GEOMETRY);
        }
      }
    }
    return valid;
  }

  private boolean validateRecordMatchTransportLine(final Record site) {
    boolean valid = true;
    if (site.hasValuesAll(CIVIC_NUMBER, STREET_NAME_ID, LOCALITY_ID)) {
      final int civicNumber = site.getInteger(CIVIC_NUMBER);
      final List<Identifier> structuredNameIds = Records.getIdentifiers(site,
        SITE_STRUCTURED_NAME_FIELD_NAMES);
      Identifier transportLineId = getIdentifier(site, TRANSPORT_LINE_ID);

      PointLineStringMetrics matchedMetrics = null;
      Record matchedTransportLine = null;

      final Point sitePoint = site.getGeometry();
      final int startDistance = 100;
      final int distanceTolerance = TRANSPORT_LINE_DISTANCE_TOLERANCE;

      for (int maxDistance = startDistance; matchedTransportLine == null
        && maxDistance <= distanceTolerance; maxDistance += 200) {
        final Predicate<Record> filter = new SitePointClosestTransportLineFilter(sitePoint,
          maxDistance, structuredNameIds);

        final Collection<Record> closeTransportLines = queryDistance(GbaTables.TRANSPORT_LINE,
          sitePoint, maxDistance, filter);
        for (final Record transportLine : closeTransportLines) {
          final LineString line = transportLine.getGeometry();
          final PointLineStringMetrics metrics = line.getMetrics(sitePoint);
          Boolean bestMatch = true;
          if (matchedTransportLine != null) {
            bestMatch = validateRecordMatchTransportLineStrata(site, civicNumber, transportLine,
              matchedTransportLine);
            if (bestMatch == null) {
              bestMatch = validateRecordMatchTransportLineDistance(site, civicNumber, transportLine,
                metrics, matchedTransportLine, matchedMetrics);
            }
          }
          if (bestMatch) {
            matchedMetrics = metrics;
            matchedTransportLine = transportLine;
          }
        }
      }

      Record transportLine;
      if (matchedTransportLine == null) {
        if (SitePoint.isVirtual(site)) {
          transportLineId = null;
        }
        if (transportLineId == null) {
          transportLine = null;
          final Map<String, Object> data = new TreeMap<>();
          addDataNames(data, structuredNameIds);
          valid &= addMessage(site, MESSAGE_SITE_POINT_TRANSPORT_LINE_NO_CLOSE, data,
            TRANSPORT_LINE_ID);
        } else {
          transportLine = loadRecord(GbaTables.TRANSPORT_LINE, transportLineId);
          if (transportLine == null) {
            return false;
          } else {
            final LineString line = transportLine.getGeometry();

            final Map<String, Object> data = Maps.newLinkedHash("transportLineId",
              (Object)transportLineId.toString());
            final double distance = sitePoint.distance(line);
            data.put("distance", Doubles.toString(Doubles.makePrecise(10, distance)));

            final List<Point> nearestPoints = DistanceWithPoints.nearestPoints(sitePoint, line);
            final LineString errorLine = Gba.GEOMETRY_FACTORY_2D.lineString(nearestPoints);

            valid &= addMessage(site, MESSAGE_SITE_POINT_TRANSPORT_LINE_NOT_CLOSE, errorLine, data,
              TRANSPORT_LINE_ID);
          }
        }
      } else {
        double matchedDistance = matchedMetrics.getDistance();
        if (transportLineId == null) {
          transportLine = matchedTransportLine;
        } else {
          final Identifier matchedTransportLineId = getIdentifier(matchedTransportLine);
          if (transportLineId.equals(matchedTransportLineId)) {
            transportLine = matchedTransportLine;
          } else {
            transportLine = loadRecord(GbaTables.TRANSPORT_LINE, transportLineId);
            if (transportLine == null) {
              if (matchedTransportLineId == null) {
                return false;
              } else {
                transportLine = matchedTransportLine;
              }
            } else {
              final LineString line = transportLine.getGeometry();
              final LineString closestLine = matchedTransportLine.getGeometry();

              final Map<String, Object> data = Maps.newLinkedHash("transportLineId",
                (Object)transportLineId.toString());
              data.put("otherId", matchedTransportLineId);
              final double distance = sitePoint.distance(line);
              data.put("distance", Doubles.toString(Doubles.makePrecise(10, distance)));
              if (matchedDistance == Double.MAX_VALUE) {
                matchedDistance = closestLine.distancePoint(sitePoint);
              }
              data.put("otherDistance", Doubles.toString(Doubles.makePrecise(10, matchedDistance)));

              final List<Point> nearestPoints1 = DistanceWithPoints.nearestPoints(sitePoint, line);
              final LineString errorLine1 = Gba.GEOMETRY_FACTORY_2D.lineString(nearestPoints1);
              final List<Point> nearestPoints2 = DistanceWithPoints.nearestPoints(sitePoint,
                closestLine);
              final LineString errorLine2 = Gba.GEOMETRY_FACTORY_2D.lineString(nearestPoints2);
              final Lineal errorLine = Gba.GEOMETRY_FACTORY_2D.lineal(errorLine1, errorLine2);

              final SetValueHolderRunnable<Boolean> fixTransportLine = new SetValueHolderRunnable<>(
                false, true);
              valid &= addMessage(site, MESSAGE_SITE_POINT_TRANSPORT_LINE_NOT_CLOSEST, errorLine,
                data, TRANSPORT_LINE_ID, fixTransportLine);
              if (fixTransportLine.getValue()) {
                transportLine = matchedTransportLine;
              }
            }
          }
        }
      }
      if (transportLine != null) {
        final Integer singleCivicNumber = transportLine
          .getInteger(TransportLine.SINGLE_HOUSE_NUMBER);
        if (singleCivicNumber == null) {
          if (isInBlockRange(transportLine, civicNumber)) {
            final LineString line = transportLine.getGeometry();
            final PointLineStringMetrics metrics = line.getMetrics(sitePoint);
            final Side side = metrics.getSide();
            if (side != null) {
              boolean schemeValid = true;
              final HouseNumberScheme scheme = TransportLine.getHouseNumberScheme(transportLine,
                side);
              if (scheme.isOdd()) {
                schemeValid = Numbers.isOdd(civicNumber);
              } else if (scheme.isEven()) {
                schemeValid = Numbers.isEven(civicNumber);
              }
              if (!schemeValid && metrics.isBesideLine()) {
                final String schemeName = scheme.getDescription();
                final Map<String, Object> data = Maps.newLinkedHash("scheme", schemeName);
                SitePointRule.addDataFullAddress(data, site);
                addMessage(site, SitePointRule.MESSAGE_SITE_POINT_TRANSPORT_LINE_SCHEME_DIFFER,
                  data, SitePoint.CIVIC_NUMBER);
                transportLine = null;
              }
            }
          } else {
            final Map<String, Object> data = Maps.newLinkedHash("fullAddress",
              site.getString(FULL_ADDRESS));
            addDataAddressRange(data, transportLine);
            addMessage(site,
              MESSAGE_SITE_POINT_TRANSPORT_LINE_CIVIC_NUMBER_NOT_IN_MESSAGE_BLOCK_RANGE, data,
              SitePoint.CIVIC_NUMBER);
            transportLine = null;
          }
        }
      }
      setTransportLine(site, transportLine);

      if (transportLine != null) {
        transportLineId = getIdentifier(transportLine);

        validateRecordMatchTransportLineLocality(site, transportLine);

        final Integer singleCivicNumber = transportLine
          .getInteger(TransportLine.SINGLE_HOUSE_NUMBER);
        if (singleCivicNumber == null) {
          // TODO ?
        } else {
          if (singleCivicNumber.equals(civicNumber)) {
            if (!site.hasValue(UNIT_DESCRIPTOR)) {
              final Map<String, Object> data = Maps.newLinkedHash("fullAddress",
                site.getString(FULL_ADDRESS));
              addMessage(site, MESSAGE_SITE_POINT_MISSING_UNIT_DESCRIPTOR, data, UNIT_DESCRIPTOR);
            }
          } else {
            final Map<String, Object> data = new HashMap<>();
            data.put("civicNumber", civicNumber);
            data.put("singleCivicNumber", singleCivicNumber);
            addMessage(site, MESSAGE_SITE_POINT_TRANSPORT_LINE_CIVIC_AND_SINGLE_DIFFER, data,
              CIVIC_NUMBER);
          }
        }

        if (!validateSiteTransportLineName(site, STREET_NAME_ID, transportLine,
          TransportLine.STRUCTURED_NAME_1_ID)) {
          valid = false;
        }
        if (!validateSiteTransportLineName(site, STREET_NAME_ALIAS_1_ID, transportLine,
          TransportLine.STRUCTURED_NAME_2_ID)) {
          valid = false;
        }
      }
    }
    return valid;
  }

  public boolean validateRecordMatchTransportLineDistance(final Record site, final int civicNumber,
    final Record transportLine, final PointLineStringMetrics metrics,
    final Record matchedTransportLine, final PointLineStringMetrics matchedMetrics) {
    final double distance = metrics.getDistance();
    final double matchedDistance = matchedMetrics.getDistance();

    if (distance > 30 && matchedDistance > 30) {
      if (AddressRange.isInAddressRange(transportLine, civicNumber)) {
        return true;
      } else if (AddressRange.isInAddressRange(matchedTransportLine, civicNumber)) {
        return false;
      }
    }
    if (metrics.withinDistanceFromEnds(5)) {
      if (AddressRange.isInAddressRange(transportLine, civicNumber)) {
        return true;
      }
    }
    if (matchedMetrics.withinDistanceFromEnds(5)) {
      if (AddressRange.isInAddressRange(matchedTransportLine, civicNumber)) {
        return false;
      }
    }
    final boolean besideLine = metrics.isBesideLine();
    final boolean matchedBesideLine = matchedMetrics.isBesideLine();
    if (besideLine == matchedBesideLine) {
      if (besideLine) {
        if (AddressRange.isInAddressRange(transportLine, civicNumber)) {
          return true;
        } else if (AddressRange.isInAddressRange(matchedTransportLine, civicNumber)) {
          return false;
        }
      }
      if (distance == matchedDistance) {
        final int compare = SessionRecordIdentifierComparator.compareIdentifier(transportLine,
          matchedTransportLine, TransportLine.TRANSPORT_LINE_ID);
        if (compare < 1) {
          return true;
        }
      } else if (distance < matchedDistance) {
        return true;
      }
      return false;
    } else {
      if (besideLine) {
        if (distance > matchedDistance) {
        }
        return true;
      } else {
        if (distance < matchedDistance) {
        }
        return false;
      }
    }
  }

  private void validateRecordMatchTransportLineLocality(final Record site,
    final Record transportLine) {
    final Identifier localityId = site.getIdentifier(LOCALITY_ID);
    String localityFieldName;
    final Point point = site.getGeometry();
    final LineString line = transportLine.getGeometry();
    final Side side = line.getSide(point);
    if (Side.isLeft(side)) {
      localityFieldName = TransportLine.LEFT_LOCALITY_ID;
    } else {
      localityFieldName = TransportLine.RIGHT_LOCALITY_ID;
    }
    final Identifier transportLineLocalityId = transportLine.getIdentifier(localityFieldName);
    if (localityId != null && transportLineLocalityId != null) {
      if (!localityId.equals(transportLineLocalityId)) {
        final Identifier transportLineId = getIdentifier(transportLine);
        final Map<String, Object> data = Maps.newLinkedHash("transportLineId", transportLineId);
        addDataLocality(data, "locality", localityId);
        addDataLocality(data, "otherLocality", transportLineLocalityId);
        addMessage(site, MESSAGE_SITE_POINT_TRANSPORT_LINE_LOCALITY_DIFFERENT, data, LOCALITY_ID);
      }
    }
  }

  /**
   * <ol>
   *   <li>True if this site has a numerical unit descriptor, the <code>transportLine</code>'s
   *   {@link TransportLine#SINGLE_HOUSE_NUMBER} is equal to the site's {@link SitePoint#CIVIC_NUMBER},
   *   and the <code>matchedTransportLine</code>'s is not.</li>
   *   <li>False if this site has a numerical unit descriptor, the <code>matchedTransportLine</code>'s
   *   {@link TransportLine#SINGLE_HOUSE_NUMBER} is equal to the site's {@link SitePoint#CIVIC_NUMBER},
   *   and the <code>transportLine</code>'s is not.</li>
   *   <li>null otherwise</li>
   * </ol>
   *
   * @param site The site.
   * @param siteCivicNumber The civic number of the site.
   * @param transportLine The transport line.
   * @param matchedTransportLine The current best matched transport line.
   * @return true if best match, false if closest was best match, null if the best match could not be determined.
   */
  private Boolean validateRecordMatchTransportLineStrata(final Record site,
    final int siteCivicNumber, final Record transportLine, final Record matchedTransportLine) {
    if (SitePoint.hasNumericUnitDescriptor(site)) {
      final Integer currentSingleCivicNumber = transportLine
        .getInteger(TransportLine.SINGLE_HOUSE_NUMBER);
      final Integer matchedSingleCivicNumber = matchedTransportLine
        .getInteger(TransportLine.SINGLE_HOUSE_NUMBER);
      if (matchedSingleCivicNumber == null) {
        if (currentSingleCivicNumber != null) {
          if (siteCivicNumber == currentSingleCivicNumber) {
            return true;
          } else {
            return false;
          }
        }
      } else {
        if (currentSingleCivicNumber == null) {
          if (siteCivicNumber == matchedSingleCivicNumber) {
            return false;
          }
        } else {
          if (currentSingleCivicNumber != matchedSingleCivicNumber) {
            if (siteCivicNumber == currentSingleCivicNumber) {
              return true;
            } else if (siteCivicNumber == matchedSingleCivicNumber) {
              return false;
            }
          }
        }
      }
    }
    return null;
  }

  private boolean validateRecordVirtual(final Record site) {
    boolean valid = true;
    if (site.hasValuesAll(CIVIC_NUMBER, STREET_NAME_ID, LOCALITY_ID)
      && !site.hasValuesAny(UNIT_DESCRIPTOR, CIVIC_NUMBER_SUFFIX)) {
      final Point point = site.getGeometry();
      final int civicNumber = site.getInteger(CIVIC_NUMBER);
      PointLineStringMetrics matchedMetrics = null;
      Record matchedTransportLine = getTransportLine(site);
      if (matchedTransportLine != null) {
        final LineString line = matchedTransportLine.getGeometry();
        matchedMetrics = line.getMetrics(point);
      }
      final Identifier structuredNameId = GbaType.getStructuredName1Id(site);
      final Predicate<Record> filter = new SitePointClosestTransportLineFilter(point, 100,
        structuredNameId);

      final Collection<Record> closeTransportLines = queryDistance(GbaTables.TRANSPORT_LINE, point,
        100, filter);
      for (final Record closeTransportLine : closeTransportLines) {
        if (!closeTransportLine.hasValue(TransportLine.SINGLE_HOUSE_NUMBER)) {
          final LineString line = closeTransportLine.getGeometry();
          final PointLineStringMetrics metrics = line.getMetrics(point);
          boolean bestMatch = false;
          final boolean inAddressRange = AddressRange.isInAddressRange(closeTransportLine,
            civicNumber);
          final boolean matchedInAddressRange = AddressRange.isInAddressRange(matchedTransportLine,
            civicNumber);
          if (inAddressRange) {
            if (!matchedInAddressRange || isCloser(metrics, matchedMetrics)) {
              // TODO overlaps
              bestMatch = true;
            }
          } else if (!matchedInAddressRange) {
            final boolean inBlockRange = StreetBlock.isInBlockRange(closeTransportLine,
              civicNumber);
            final boolean matchedInBlockRange = StreetBlock.isInBlockRange(closeTransportLine,
              civicNumber);
            if (inBlockRange) {
              if (!matchedInBlockRange || isCloser(metrics, matchedMetrics)) {
                // TODO overlaps
                bestMatch = true;
              }
            } else if (!matchedInBlockRange) {
              if (isCloser(metrics, matchedMetrics)) {
                bestMatch = true;
              }
            }
          }
          if (bestMatch) {
            matchedMetrics = metrics;
            matchedTransportLine = closeTransportLine;
          }
        }
      }

      final ValueHolder<Boolean> moved = new ValueHolder<>(false);
      if (matchedTransportLine == null) {
        final Map<String, Object> data = new TreeMap<>();
        addDataName(data);
        valid &= addMessage(site, MESSAGE_SITE_POINT_VIRTUAL_NO_CLOSE, data, TRANSPORT_LINE_ID);
      } else {
        validateRecordMatchTransportLineLocality(site, matchedTransportLine);
        final Point newPoint = new Street(matchedTransportLine).getVirtualPoint(civicNumber);
        if (!newPoint.equals(point)) {
          valid &= addMessage(site, MESSAGE_SITE_POINT_VIRTUAL_LOCATION, newPoint, GEOMETRY, () -> {
            setGeometry(site, newPoint);
            moved.setValue(true);
          });
        }
      }
      valid &= validateRecordDuplicateExactOrClose(site);
      if (!RecordRule.isDeleted(site)) {
        if (moved.getValue()) {
          addCount("Fixed", "Moved virtual site");
        }
        setTransportLine(site, matchedTransportLine);
      }
    }

    return valid;

  }

  private void validateSitesWithNoNames(final Map<Identifier, List<Record>> sitesByStructuredNameId,
    final List<Record> sitesWithNoStructuredName) {
    for (final Record site : RecordRuleThreadProperties.i(sitesWithNoStructuredName)) {
      final Point sitePoint = site.getGeometry();

      final ClosestRecordFilter filter = new ClosestRecordFilter(sitePoint, 100,
        TransportLine::isDemographic);
      queryDistance(GbaTables.TRANSPORT_LINE, sitePoint, 100, filter);
      final Record closestTransportLine = filter.getClosestRecord();
      if (closestTransportLine != null) {
        final String name = GbaType.getStructuredName1(closestTransportLine);
        final String addressBcName = GbaType.getExtendedData(site, "FULL_NAME");
        if (Property.hasValue(addressBcName)) {
          final String upperName = name.toUpperCase();
          boolean nameMatched = upperName.contains(addressBcName)
            || addressBcName.contains(upperName);
          if (!nameMatched) {
            nameMatched = Strings.equalExceptOneCharacter(addressBcName, upperName);
            if (!nameMatched) {
              nameMatched = Strings.equalExceptOneExtraCharacter(addressBcName, upperName);
              if (!nameMatched) {
                nameMatched = equalExceptNameType(name, addressBcName);
                if ("HWY".equals(addressBcName)) {
                  if (upperName.matches("HWY \\d+\\w?( AND \\d+\\w?)?")) {
                    nameMatched = true;
                  }
                }
              }
            }
          }
          if (nameMatched) {
            final Identifier structuredNameId = GbaType.getStructuredName1Id(closestTransportLine);
            site.setValue(STREET_NAME_ID, structuredNameId);
            removeMessages(site, FieldValueRule.MESSAGE_FIELD_REQUIRED, STREET_NAME_ID);
            SitePointFieldValueRule.setFullAddress(this, site);
            setTransportLine(site, closestTransportLine);
            Maps.addToList(sitesByStructuredNameId, structuredNameId, site);
          } else {
            // System.out.println(site.getValue(EXTENDED_DATA) + "\t"
            // + name);
          }
        }
      }
    }
  }

  private boolean validateSiteTransportLineName(final Record site, final String siteNameFieldName,
    final Record transportLine, final String transportLineNameFieldName) {
    final Identifier siteStructuredNameId = site.getIdentifier(siteNameFieldName);
    if (siteStructuredNameId != null) {
      final Identifier transportStructuredNameId = transportLine
        .getIdentifier(transportLineNameFieldName);
      if (!siteStructuredNameId.equals(transportStructuredNameId)) {
        // this.siteNameAlias.put(site,
        // transportLine.getIdentifier(TransportLine.STRUCTURED_NAME_1_ID));
        final Object siteStructuredName = GbaController.structuredNames
          .getValue(siteStructuredNameId);
        final Object transportStructuredName = GbaController.structuredNames
          .getValue(transportStructuredNameId);
        final Map<String, Object> data = Maps.newLinkedHash("name", siteStructuredName);
        data.put("otherName", transportStructuredName);
        final Identifier transportLineId = getIdentifier(transportLine);
        data.put("transportLineId", transportLineId.toString());
        return addMessage(site, MESSAGE_SITE_POINT_TRANSPORT_LINE_NAME_DIFFERENT, data,
          siteNameFieldName);
      }
    }
    return true;
  }

  protected void writeDeleted(final Record site) {
    if (this.deletedWriter == null) {
      final Path logFile = getLogDirectory().resolve("site_point_deleted.tsv");
      this.deletedWriter = RecordWriter.newRecordWriter(site.getRecordDefinition(), logFile);
    }
    this.deletedWriter.write(site);
  }

}

// private void validateDuplicate(final List<Record> streetSites) {
// final Map<String, List<Record>> sitesByFullAddress = new TreeMap<>();
// streetSites.sort(SITE_ID_COMPARATOR);
// for (final Record site : streetSites) {
// final List<String> fullAddresses = getFullAddresses(site);
// if (SitePoint.isUseInAddressRange(site) && !RecordRule.isDeleted(site)) {
// for (final String fullAddress : fullAddresses) {
// Maps.addToList(sitesByFullAddress, fullAddress, site);
// }
// }
// }
// for (final Entry<String, List<Record>> entry : sitesByFullAddress.entrySet())
// {
// final String fullAddress = entry.getKey();
// final List<Record> sitesForFullAddress = entry.getValue();
// if (sitesForFullAddress.size() > 1) {
// for (int i = 0; i < sitesForFullAddress.size();) {
// final Record site1 = sitesForFullAddress.get(i);
// if (!site1.hasValuesAny(CIVIC_NUMBER_SUFFIX, UNIT_DESCRIPTOR)) {
// final Integer civicNumber = site1.getInteger(CIVIC_NUMBER);
// if (civicNumber != null) {
// validateRecordDuplicateExactOrClose(site1);
//
// final WmsIdentifier transportLineId1 = getIdentifier(site1,
// TRANSPORT_LINE_ID);
// for (int j = i + 1; j < sitesForFullAddress.size() &&
// !RecordRule.isDeleted(site1);) {
// final Record site2 = sitesForFullAddress.get(j);
// final WmsIdentifier transportLineId2 = getIdentifier(site2,
// TRANSPORT_LINE_ID);
// if (transportLineId1 != null && transportLineId1.equals(transportLineId2)) {
// final Record transportLine1 = getTransportLine(site1);
// if (transportLine1 != null) {
// final LineString line = transportLine1.getGeometry();
// final PointLineStringMetrics metrics1 = line.getMetrics(site1.getGeometry());
// final PointLineStringMetrics metrics2 = line.getMetrics(site2.getGeometry());
// final End end = TransportLine.getClosestEnd(transportLine1, civicNumber);
// // System.out.println(end);
// }
// }
// if (RecordRule.isDeleted(site2)) {
// sitesForFullAddress.remove(j);
// } else {
// j++;
// }
// }
// if (RecordRule.isDeleted(site1)) {
// sitesForFullAddress.remove(i);
// } else {
// i++;
// }
// }
// }
// }
//
// final List<Record> virtualSites = new ArrayList<>();
// final List<Record> realSites = new ArrayList<>();
// for (final Record site : sitesForFullAddress) {
// if (SitePoint.isVirtual(site)) {
// virtualSites.add(site);
// } else {
// realSites.add(site);
// }
// }
// if (realSites.isEmpty()) {
// if (virtualSites.size() > 1) {
// int i = 1;
// for (final Record site : virtualSites) {
// final Record duplicateSite = virtualSites.get(i);
// logDuplicateSite(site, duplicateSite);
// i = 0;
// }
// }
// } else {
// for (final Record virtualSite : virtualSites) {
// final Record realSite = realSites.get(0);
// if (isFixingAllowed()) {
// deleteSite(virtualSite, "FIXED: DELETED virtual site with real site");
// } else {
// logDuplicateSite(realSite, virtualSite);
// }
// }
// }
// }
// }
//
// final Map<String, Map<WmsIdentifier, Record>>
// siteByFullAddressAndTransportLine
// = new HashMap<>();
//
// final Map<String, Set<Record>> sitesWithNoTransportLineByFullAddress = new
// HashMap<>();
//
// for (final Record site : streetSites) {
// final List<String> fullAddresses = getFullAddresses(site);
// for (final String fullAddress : fullAddresses) {
// if (SitePoint.isUseInAddressRange(site) &&
// !site.getState().equals(RecordState.DELETED)) {
// // validateDuplicateSite(siteByFullAddressAndTransportLine,
// // sitesWithNoTransportLineByFullAddress, fullAddress, site);
// }
// }
// }
// for (final String fullAddress : sitesByFullAddress.keySet()) {
// final Map<WmsIdentifier, Record> siteByTransportLine =
// siteByFullAddressAndTransportLine
// .get(fullAddress);
// final Set<Record> sitesWithNoTransportLine =
// sitesWithNoTransportLineByFullAddress
// .get(fullAddress);
// if (Property.hasValue(siteByTransportLine)) {
//
// if (Property.hasValue(sitesWithNoTransportLine)) {
// final Record site = siteByTransportLine.values().iterator().next();
// for (final Record duplicateSite : sitesWithNoTransportLine) {
// fixOrLogDuplicateSite(site, duplicateSite);
// }
// }
// } else if (Property.hasValue(sitesWithNoTransportLine)) {
// Record site = null;
// for (final Record otherSite : sitesWithNoTransportLine) {
// if (site == null) {
// site = otherSite;
// } else {
// final int compareIdentifier =
// SessionRecordIdentifierComparator.compareIdentifier(site,
// otherSite, SITE_ID);
// if (compareIdentifier < 0) {
// fixOrLogDuplicateSite(site, otherSite);
// } else {
// fixOrLogDuplicateSite(otherSite, site);
// site = otherSite;
// }
// }
// }
// }
// }
//
// }
//
// private void fixOrLogDuplicateSite(final Record site, final Record
// duplicateSite) {
// if (isFixingAllowed()) {
// QaLogMessagesByRecord.addMessageCount(SiteTables.SITE_POINT, "FIXED:
// Secondary
// address set "
// + USE_IN_ADDRESS_RANGE_IND + "=N, " + TRANSPORT_LINE_ID + "=null");
// duplicateSite.setValue(TRANSPORT_LINE_ID, null);
// SitePoint.setUseInAddressRange(duplicateSite, false);
// } else {
// logDuplicateSite(site, duplicateSite);
// }
// }
