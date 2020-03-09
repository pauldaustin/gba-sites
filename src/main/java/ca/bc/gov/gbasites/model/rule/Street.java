package ca.bc.gov.gbasites.model.rule;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.jeometry.common.data.identifier.Identifier;
import org.jeometry.common.data.type.DataType;
import org.jeometry.common.number.Numbers;

import ca.bc.gov.gba.controller.GbaController;
import ca.bc.gov.gba.model.Gba;
import ca.bc.gov.gba.model.message.QaMessageDescription;
import ca.bc.gov.gba.model.type.GbaType;
import ca.bc.gov.gba.model.type.TransportLine;
import ca.bc.gov.gba.model.type.code.HouseNumberScheme;
import ca.bc.gov.gba.model.type.code.TransportLineDivided;
import ca.bc.gov.gba.ui.layer.SessionRecordIdentifierComparator;
import ca.bc.gov.gbasites.model.type.SitePoint;

import com.revolsys.collection.list.Lists;
import com.revolsys.collection.map.Maps;
import com.revolsys.collection.range.IntMinMax;
import com.revolsys.collection.range.RangeSet;
import com.revolsys.geometry.model.Direction;
import com.revolsys.geometry.model.End;
import com.revolsys.geometry.model.EndAndSide;
import com.revolsys.geometry.model.LineString;
import com.revolsys.geometry.model.Point;
import com.revolsys.geometry.model.Side;
import com.revolsys.geometry.model.metrics.PointLineStringMetrics;
import com.revolsys.geometry.util.LineStringUtil;
import com.revolsys.record.Record;
import com.revolsys.util.Debug;
import com.revolsys.util.Property;

/**
 * A street contains one or more connected transport lines. It also has left and right addressing which can be empty.
 */
public class Street implements Comparable<Street>, Comparator<Record> {

  public static boolean isModifiedByGeoBc(final Record transportLine) {
    return transportLine.getInteger(GbaType.MODIFY_INTEGRATION_SESSION_ID) > 60;
  }

  public static boolean isStructureUnencumbered(final Record transportLine) {
    final String name = GbaType.getStructuredName1(transportLine);
    final String structure = TransportLine.getTransportLineStructure(transportLine);
    if (Property.hasValue(name) && Property.hasValue(structure) && name.contains(structure)) {
      if (isModifiedByGeoBc(transportLine)) {
        return true;
      } else {
      }
    }
    return false;
  }

  public static Point newVirtualPoint(final LineString line, final End lineEnd, final Side side,
    double endOffset, double sideOffset) {
    if (Side.isRight(side)) {
      sideOffset = -sideOffset;
    }
    endOffset = Math.min(endOffset, line.getLength() / 4);
    final Point point = LineStringUtil.pointOffset(line, lineEnd, endOffset, sideOffset);
    return Gba.GEOMETRY_FACTORY_2D_1M.point(point);
  }

  public static Point newVirtualPoint(final LineString line, final EndAndSide endAndSide) {
    final End end = endAndSide.getEnd();
    final Side side = endAndSide.getSide();
    return newVirtualPoint(line, end, side, 10, 5);
  }

  private final LineString line;

  private Street nextStreet;

  private final RangeSet numberRange;

  private Street previousStreet;

  private SitePointRule rule;

  private final Integer singleCivicNumber;

  private final List<Record> sites = new ArrayList<>();

  private final StreetSide streetSideLeft;

  private final StreetSide streetSideRight;

  private List<Direction> transportLineDirections = new ArrayList<>();

  private List<Record> transportLines = new ArrayList<>();

  public Street(final Record transportLine) {
    this.line = transportLine.getGeometry();
    this.transportLines = Lists.newArray(transportLine);
    this.transportLineDirections.add(Direction.FORWARDS);
    this.streetSideLeft = new StreetSide(this, Side.LEFT);
    this.streetSideRight = new StreetSide(this, Side.RIGHT);

    this.singleCivicNumber = transportLine.getInteger(TransportLine.SINGLE_HOUSE_NUMBER);

    this.numberRange = new RangeSet();
  }

  public Street(final SitePointRule rule, final LineString line,
    final Collection<Record> transportLines, final Collection<Direction> transportLineDirections,
    final Integer singleCivicNumber) {
    this.rule = rule;
    this.line = line;
    this.transportLines = Lists.toArray(transportLines);
    this.transportLineDirections = Lists.toArray(transportLineDirections);
    while (this.transportLineDirections.size() < this.transportLines.size()) {
      this.transportLineDirections.add(Direction.FORWARDS);
    }
    this.streetSideLeft = new StreetSide(this, Side.LEFT);
    this.streetSideRight = new StreetSide(this, Side.RIGHT);
    this.singleCivicNumber = singleCivicNumber;

    final List<Record> sitesLeft = new ArrayList<>();
    final List<Record> sitesRight = new ArrayList<>();

    final List<Record> sitesBefore = new ArrayList<>();
    final List<Record> sitesAfter = new ArrayList<>();
    final List<Record> sitesOn = new ArrayList<>();

    final Map<Record, List<Record>> sitesByTransportLine = rule.getSitesByTransportLine();
    for (final Record transportLine : transportLines) {
      final List<Record> sites = Maps.get(sitesByTransportLine, transportLine);
      if (sites != null) {
        this.sites.addAll(sites);
        for (final Record site : sites) {
          final int civicNumber = SitePoint.getCivicNumber(site);
          if (this.streetSideLeft.isInTransportLineRange(civicNumber)) {
            sitesLeft.add(site);
          } else if (this.streetSideRight.isInTransportLineRange(civicNumber)) {
            sitesRight.add(site);
          } else {
            final boolean inSchemeLeft = this.streetSideLeft.isInTransportLineScheme(civicNumber);
            final boolean inSchemeRight = this.streetSideRight.isInTransportLineScheme(civicNumber);
            if (inSchemeLeft && !inSchemeRight) {
              sitesLeft.add(site);
            } else if (inSchemeRight && !inSchemeLeft) {
              sitesRight.add(site);
            } else {
              final Point point = site.getGeometry();
              final PointLineStringMetrics metrics = line.getMetrics(point);
              if (metrics.isBeforeLine()) {
                sitesBefore.add(site);
              } else if (metrics.isAfterLine()) {
                sitesAfter.add(site);
              } else if (metrics.getDistance() < 2) {
                sitesOn.add(site);
              } else {
                final Side side = metrics.getSide();
                if (Side.isLeft(side)) {
                  sitesLeft.add(site);
                } else if (Side.isRight(side)) {
                  sitesRight.add(site);
                } else {
                  sitesOn.add(site);
                }
              }
            }
          }
        }
      }
    }

    this.streetSideLeft.addSites(sitesLeft);
    this.streetSideRight.addSites(sitesRight);

    if (!isUseUnitDescriptor()) {
      for (final Side side : Side.VALUES) {
        final StreetSide streetSide = getStreetSide(side);
        final HouseNumberScheme streetScheme = streetSide.getStreetScheme();
        if (!streetScheme.isContinuous()) {
          if (!streetScheme.isNone()) {
            final HouseNumberScheme siteSchemeNonVirtual = streetSide.getSiteSchemeNonVirtual();
            if (siteSchemeNonVirtual.isNone() || siteSchemeNonVirtual.isSame(streetScheme)) {
              streetSide.moveVirtualNotMatchingSchemeToOpposite(streetScheme);
            }
          }
        }
      }
    }
    for (final Side side : Side.VALUES) {
      final StreetSide streetSide = getStreetSide(side);
      streetSide.addSitesMatchingScheme(sitesBefore);
      streetSide.addSitesMatchingScheme(sitesOn);
      streetSide.addSitesMatchingScheme(sitesAfter);

      if (!isUseUnitDescriptor()) {
        for (final Record sideSite : streetSide.getSites()) {
          if (SitePoint.isVirtual(sideSite)) {
            final int civicNumber = sideSite.getInteger(SitePoint.CIVIC_NUMBER);
            if (streetSide.isStreetNumbersContains(civicNumber)) {
              if (!hasTransportLineNumber(civicNumber)) {
                rule.addMessage(sideSite, SitePointRule.MESSAGE_SITE_POINT_VIRTUAL_MIDDLE_OF_RANGE,
                  SitePoint.CIVIC_NUMBER, () -> {
                    rule.deleteSite(sideSite, "Delete virtual site in middle of range");
                  });
              }
            }
          }
        }
      }
    }

    this.numberRange = new RangeSet(this.streetSideLeft.getSiteNumbers());
    this.numberRange.addValues(this.streetSideRight.getSiteNumbers());
  }

  public Street(final SitePointRule rule, final LineString line,
    final Collection<Record> transportLines, final Integer singleCivicNumber) {
    this(rule, line, transportLines, null, singleCivicNumber);
  }

  public Street(final SitePointRule rule, final Record transportLine,
    final Integer singleCivicNumber) {
    this(rule, transportLine.getGeometry(), Lists.newArray(transportLine), singleCivicNumber);
  }

  public void addToTransportLineRange(final RangeSet rangeSet) {
    for (final Side side : Side.VALUES) {
      final HouseNumberScheme scheme = getTransportLineScheme(side);
      if (!scheme.isNone()) {
        Integer fromNumber = getTransportLineNumber(End.FROM, side);
        Integer toNumber = getTransportLineNumber(End.TO, side);
        if (fromNumber == null) {
          if (toNumber != null) {
            rangeSet.add(toNumber);
          }
        } else if (toNumber == null) {
          rangeSet.add(fromNumber);
        } else {
          if (fromNumber > toNumber) {
            final int temp = fromNumber;
            fromNumber = toNumber;
            toNumber = temp;
          }
          if (scheme.isContinuous()) {
            rangeSet.addRange(fromNumber, toNumber);
          } else if (scheme.isOdd()) {
            if (!Numbers.isOdd(fromNumber)) {
              rangeSet.add(fromNumber);
              fromNumber++;
            }
            if (!Numbers.isOdd(toNumber)) {
              rangeSet.add(toNumber);
              toNumber--;
            }
            for (int i = fromNumber; i <= toNumber; i++) {
              rangeSet.add(i);
            }
          } else if (scheme.isEven()) {
            if (!Numbers.isEven(fromNumber)) {
              rangeSet.add(fromNumber);
              fromNumber++;
            }
            if (!Numbers.isEven(toNumber)) {
              rangeSet.add(toNumber);
              toNumber--;
            }
            for (int i = fromNumber; i <= toNumber; i++) {
              rangeSet.add(i);
            }
          }
        }
      }
    }
  }

  @Override
  public int compare(final Record record1, final Record record2) {
    if (record1 == record2) {
      return 0;
    } else {
      final Point point1 = record1.getGeometry();
      final double distance1 = this.line.distanceAlong(point1);

      final Point point2 = record2.getGeometry();
      final double distance2 = this.line.distanceAlong(point2);
      int compare = Double.compare(distance1, distance2);
      if (compare == 0) {
        compare = SessionRecordIdentifierComparator.compareIdentifier(record1, record2,
          SitePoint.SITE_ID);
      }
      return compare;
    }
  }

  @Override
  public int compareTo(final Street street) {
    final int min1 = getMin();
    final int min2 = street.getMin();
    final int compare = Integer.compare(min1, min2);
    return compare;
  }

  public boolean containsTransportLineNumber(final int civicNumber) {
    if (this.streetSideLeft.containsTransportLineNumber(civicNumber)) {
      return true;
    } else if (this.streetSideLeft.containsTransportLineNumber(civicNumber)) {
      return true;
    } else {
      return false;
    }
  }

  public void expandSingleBlock(final SitePointRule rule, final int blockNumber,
    final IntMinMax blockRange) {
    for (final Side side : Side.VALUES) {
      if (localityEqual(rule, side)) {
        final StreetSide streetSide = getStreetSide(side);
        streetSide.expandSingleBlock(blockNumber, blockRange);
      }
    }
  }

  public List<Integer> getBlockNumbers() {
    final Set<Integer> blockNumbers = new TreeSet<>();
    for (final Record site : this.sites) {
      final int blockNumber = SitePoint.getBlock(site);
      blockNumbers.add(blockNumber);
    }
    for (final Side side : Side.VALUES) {
      final Integer from = getTransportLineNumber(End.FROM, side);
      if (from != null) {
        blockNumbers.add(StreetBlock.getBlockFrom(from));
      }
      final Integer to = getTransportLineNumber(End.TO, side);
      if (to != null) {
        blockNumbers.add(StreetBlock.getBlockFrom(to));
      }
    }
    return new ArrayList<>(blockNumbers);
  }

  public EndAndSide getEndAndSide(final int civicNumber) {
    final StreetSide leftStreetSide = getStreetSide(Side.LEFT);
    final double leftRatio = leftStreetSide.getStreetNumberRatio(civicNumber);

    final StreetSide rightStreetSide = getStreetSide(Side.RIGHT);
    final double rightRatio = rightStreetSide.getStreetNumberRatio(civicNumber);

    Side side = null;
    if (leftRatio == Double.MAX_VALUE) {
      if (rightRatio == Double.MAX_VALUE) {
        if (Numbers.isEven(civicNumber)) {
          side = Side.LEFT;
        } else {
          side = Side.RIGHT;
        }
        if (civicNumber % 100 < 50) {
          return EndAndSide.get(End.FROM, side);
        } else {
          return EndAndSide.get(End.TO, side);
        }
      } else {
        side = Side.RIGHT;
      }
    } else if (rightRatio == Double.MAX_VALUE) {
      side = Side.LEFT;
    } else {
      final boolean leftContains = leftStreetSide.isStreetNumbersContains(civicNumber);
      final boolean rightContains = rightStreetSide.isStreetNumbersContains(civicNumber);
      if (leftContains) {
        if (!rightContains) {
          side = Side.LEFT;
        }
      } else if (rightContains) {
        side = Side.RIGHT;
      }
      if (side == null) {
        if (Numbers.isOdd(civicNumber)) {
          final boolean leftOdd = leftStreetSide.getTransportLineScheme().isOdd();
          final boolean rightOdd = rightStreetSide.getTransportLineScheme().isOdd();
          if (leftOdd) {
            if (!rightOdd) {
              side = Side.LEFT;
            }
          } else if (rightOdd) {
            side = Side.RIGHT;
          }
        } else {
          final boolean leftEven = leftStreetSide.getTransportLineScheme().isEven();
          final boolean rightEven = rightStreetSide.getTransportLineScheme().isEven();
          if (leftEven) {
            if (!rightEven) {
              side = Side.LEFT;
            }
          } else if (rightEven) {
            side = Side.RIGHT;
          }
        }
      }
      if (side == null) {

        // The abs finds which side has a closer range. Not perfect!
        if (Math.abs(leftRatio - 0.5) <= Math.abs(rightRatio - 0.5)) {
          side = Side.LEFT;
        } else {
          side = Side.RIGHT;
        }
      }
    }
    double ratio;
    if (Side.isLeft(side)) {
      ratio = leftRatio;
    } else {
      ratio = rightRatio;
    }
    if (ratio <= 0.5) {
      return EndAndSide.get(End.FROM, side);
    } else {
      return EndAndSide.get(End.TO, side);
    }
  }

  public LineString getLine() {
    return this.line;
  }

  public Identifier getLocalityId(Side side) {
    if (!this.transportLines.isEmpty()) {
      final Record transportLine = this.transportLines.get(0);
      final Direction direction = this.transportLineDirections.get(0);
      if (!direction.isForwards()) {
        side = side.opposite();
      }
      return TransportLine.getLocality(transportLine, side);
    }
    return null;
  }

  private int getMin() {
    int min = Integer.MAX_VALUE;
    if (!this.numberRange.isEmpty()) {
      min = ((Number)this.numberRange.iterator().next()).intValue();
    }
    if (!getTransportLineScheme(Side.LEFT).isNone()) {
      min = Math.min(min, this.streetSideLeft.getTransportLineMinMax().getMin());
    }
    if (!getTransportLineScheme(Side.RIGHT).isNone()) {
      min = Math.min(min, this.streetSideRight.getTransportLineMinMax().getMin());
    }
    return min;
  }

  public Street getNextStreet() {
    return this.nextStreet;
  }

  public Street getPreviousStreet() {
    return this.previousStreet;
  }

  public List<Record> getSites() {
    return new ArrayList<>(this.sites);
  }

  public List<Record> getSites(final int blockNumber) {
    final ArrayList<Record> sites = new ArrayList<>();
    for (final Record site : this.sites) {
      final Integer civicNumber = site.getInteger(SitePoint.CIVIC_NUMBER);
      if (StreetBlock.isBlockEqual(blockNumber, civicNumber)) {
        sites.add(site);
      }
    }
    return sites;
  }

  public RangeSet getSitesRange() {
    final RangeSet ranges = new RangeSet();
    for (final Record site : this.sites) {
      final Integer civicNumber = site.getInteger(SitePoint.CIVIC_NUMBER);
      ranges.add(civicNumber);
    }

    return ranges;
  }

  public StreetSide getStreetSide(final Side side) {
    if (Side.isLeft(side)) {
      return this.streetSideLeft;
    } else if (Side.isRight(side)) {
      return this.streetSideRight;
    } else {
      return null;
    }
  }

  public StreetSide getStreetSideLeft() {
    return this.streetSideLeft;
  }

  public StreetSide getStreetSideRight() {
    return this.streetSideRight;
  }

  public List<StreetSide> getStreetSides() {
    return Lists.newArray(this.streetSideLeft, this.streetSideRight);
  }

  public int getTransportLineCount() {
    return this.transportLines.size();
  }

  public List<Direction> getTransportLineDirections() {
    return this.transportLineDirections;
  }

  public <V> V getTransportLineFieldValue(final String fieldName) {
    V value = null;
    for (final Record transportLine : this.transportLines) {
      final V recordValue = transportLine.getValue(fieldName);
      if (Property.hasValue(recordValue)) {
        if (value == null) {
          value = recordValue;
        } else if (!DataType.equal(value, recordValue)) {
          throw new IllegalStateException(
            "Values different on street " + value + "!= " + recordValue + "\n" + this);
        }
      }
    }
    return value;
  }

  public <V> List<V> getTransportLineFieldValues(final String fieldName) {
    final Set<V> values = new LinkedHashSet<>();
    for (final Record transportLine : this.transportLines) {
      final V recordValue = transportLine.getValue(fieldName);
      if (Property.hasValue(recordValue)) {
        values.add(recordValue);
      }
    }
    return new ArrayList<>(values);
  }

  public IntMinMax getTransportLineMinMax(final Side side) {
    if (side.isLeft()) {
      return this.streetSideLeft.getTransportLineMinMax();
    } else {
      return this.streetSideRight.getTransportLineMinMax();
    }
  }

  public Integer getTransportLineNumber(final End end, final Side side) {
    final HouseNumberScheme scheme = getTransportLineScheme(side);
    if (scheme.isNone()) {
      return null;
    } else {
      for (int i = 0; i < getTransportLineCount(); i++) {
        int index = i;
        if (end.isTo()) {
          index = getTransportLineCount() - i - 1;
        }
        final Record transportLine = this.transportLines.get(index);
        final Direction direction = this.transportLineDirections.get(index);
        End transportLineEnd = end;
        Side transportLineSide = side;
        if (direction.isBackwards()) {
          transportLineEnd = transportLineEnd.opposite();
          transportLineSide = transportLineSide.opposite();
        }
        final HouseNumberScheme transportLineScheme = TransportLine
          .getHouseNumberScheme(transportLine, transportLineSide);
        final String fieldName = TransportLine.getHouseNumberFieldName(transportLineEnd,
          transportLineSide);
        final Integer number = transportLine.getInteger(fieldName);
        if (number == null) {
          if (!transportLineScheme.isNone()) {
            Debug.noOp();
          }
        } else {
          if (transportLineScheme.isNone()) {
            Debug.noOp();
          }
          return number;
        }
      }
      return null;
    }
  }

  public RangeSet getTransportLineRange() {
    final RangeSet ranges = new RangeSet();
    ranges.addRange(this.streetSideLeft.getTransportLineMinMax());
    ranges.addRange(this.streetSideRight.getTransportLineMinMax());
    return ranges;
  }

  public List<Record> getTransportLines() {
    return this.transportLines;
  }

  public HouseNumberScheme getTransportLineScheme(final Side side) {
    if (side == null) {
      return HouseNumberScheme.NONE;
    } else if (side.isLeft()) {
      return this.streetSideLeft.getTransportLineScheme();
    } else {
      return this.streetSideRight.getTransportLineScheme();
    }
  }

  public Point getVirtualPoint(final int civicNumber) {
    final EndAndSide endAndSide = getEndAndSide(civicNumber);
    return newVirtualPoint(this.line, endAndSide);
  }

  public boolean hasExactMatch() {
    return this.streetSideLeft.hasExactMatch() && this.streetSideRight.hasExactMatch();
  }

  public boolean hasTransportLine(final int id) {
    for (final Record transportLine : this.transportLines) {
      if (transportLine.getIdentifier().getInteger(0).equals(id)) {
        return true;
      }
    }
    return false;

  }

  public boolean hasTransportLineFieldValue(final String fieldName, final Object value) {
    for (final Record transportLine : this.transportLines) {
      final Object recordValue = transportLine.getValue(fieldName);
      if (DataType.equal(recordValue, value)) {
        return true;
      }
    }
    return false;
  }

  public boolean hasTransportLineNumber(final int civicNumber) {
    for (final Record transportLine : this.transportLines) {
      for (final String houseNumberField : TransportLine.HOUSE_NUMBER_FIELD_NAMES) {
        if (transportLine.equalValue(houseNumberField, civicNumber)) {
          return true;
        }
      }
    }
    return false;
  }

  public boolean isDivided() {
    final String transportLineDividedCode = getTransportLineFieldValue(
      TransportLine.TRANSPORT_LINE_DIVIDED_CODE);
    return TransportLineDivided.isDivided(transportLineDividedCode);
  }

  public boolean isSameBlock(final int blockNumber) {
    for (final Record site : this.sites) {
      final int siteBlockNumber = SitePoint.getBlock(site);
      if (siteBlockNumber != blockNumber) {
        return false;
      }
    }
    for (final Side side : Side.VALUES) {
      for (final End end : End.FROM_TO) {
        final Integer number = getTransportLineNumber(end, side);
        if (number != null) {
          if (!StreetBlock.isBlockEqual(blockNumber, number)) {
            return false;
          }
        }
      }
    }
    return true;
  }

  public boolean isUseUnitDescriptor() {
    return this.singleCivicNumber != null;
  }

  public boolean localityEqual(final SitePointRule rule, final Side side) {
    final Identifier localityId = rule.getLocalityId();
    final Identifier sideLocalityId = getLocalityId(side);
    return localityId.equals(sideLocalityId);
  }

  private void logSideCount(final SitePointRule rule, final int blockNumber, final String prefix,
    final QaMessageDescription messageDescription, final String side) {
    int i = 0;
    for (final Record transportLine : this.transportLines) {
      final Direction direction = this.transportLineDirections.get(i);
      final Map<String, Object> data = Maps.newLinkedHash("side", side);
      final String structuredName = rule.getStructuredName();
      data.put("name", structuredName);
      data.put("blockRange", blockNumber + "-" + (blockNumber + 99) + " ");
      final RangeSet siteRange = new RangeSet();
      final RangeSet streetRange = new RangeSet();
      String schemeField = TransportLine.LEFT_HOUSE_NUM_SCHEME_CODE;
      if ("Left".equals(side)) {
        if (direction.isBackwards()) {
          schemeField = TransportLine.RIGHT_HOUSE_NUM_SCHEME_CODE;
        }
        siteRange.add(this.streetSideLeft.getSiteStreetMinMax());
        streetRange.add(this.streetSideLeft.getTransportLineMinMax());
      } else if ("Right".equals(side)) {
        if (direction.isForwards()) {
          schemeField = TransportLine.RIGHT_HOUSE_NUM_SCHEME_CODE;
        }
        siteRange.add(this.streetSideRight.getSiteStreetMinMax());
        streetRange.add(this.streetSideRight.getTransportLineMinMax());
      } else {
        siteRange.add(this.streetSideLeft.getSiteStreetMinMax());
        streetRange.add(this.streetSideLeft.getTransportLineMinMax());
        siteRange.add(this.streetSideRight.getSiteStreetMinMax());
        streetRange.add(this.streetSideRight.getTransportLineMinMax());
      }
      data.put("siteRange", siteRange);
      data.put("streetRange", streetRange);

      rule.addMessage(transportLine, messageDescription, data, schemeField);
      i++;
    }
  }

  public void logSideCounts(final SitePointRule rule, final int blockNumber, final String prefix,
    final QaMessageDescription leftMessage, final QaMessageDescription rightMessage) {
    rule.addCount("Info", "Block " + prefix);
    if (leftMessage == null) {
      if (rightMessage == null) {
        rule.addCount("Info", "Block " + prefix + " Matched");
      } else {
        logSideCount(rule, blockNumber, prefix, rightMessage, "Right");
      }
    } else {
      if (rightMessage == null) {
        logSideCount(rule, blockNumber, prefix, leftMessage, "Left");
      } else if (leftMessage.equals(rightMessage)) {
        logSideCount(rule, blockNumber, prefix, rightMessage, "Both");
      } else {
        logSideCount(rule, blockNumber, prefix, leftMessage, "Left");
        logSideCount(rule, blockNumber, prefix, rightMessage, "Right");
      }
    }
  }

  public Street merge(final End end1, final Street street, final End end2) {
    final List<Record> transportLines = new ArrayList<>();
    final List<Direction> transportLineDirections = new ArrayList<>();

    final List<Record> transportLines1 = getTransportLines();
    final List<Direction> transportLineDirections1 = getTransportLineDirections();
    final List<Record> transportLines2 = street.getTransportLines();
    final List<Direction> transportLineDirections2 = street.getTransportLineDirections();

    if (end1.isFrom()) {
      if (end2.isFrom()) {
        // <--*--> = <----
        for (int i = transportLineDirections2.size() - 1; i >= 0; i--) {
          final Record transportLine = transportLines2.get(i);
          transportLines.add(transportLine);
          final Direction transportLineDirection = transportLineDirections2.get(i);
          transportLineDirections.add(transportLineDirection.opposite());
        }
        transportLines.addAll(transportLines1);
        transportLineDirections.addAll(transportLineDirections1);
      } else {
        // <--*<-- = <----
        transportLines.addAll(transportLines2);
        transportLineDirections.addAll(transportLineDirections2);

        transportLines.addAll(transportLines1);
        transportLineDirections.addAll(transportLineDirections1);
      }
    } else {
      if (end2.isFrom()) {
        // -->*--> = ---->
        transportLines.addAll(transportLines1);
        transportLineDirections.addAll(transportLineDirections1);

        transportLines.addAll(transportLines2);
        transportLineDirections.addAll(transportLineDirections2);
      } else {
        // -->*<-- = ---->
        transportLines.addAll(transportLines1);
        transportLineDirections.addAll(transportLineDirections1);

        for (int i = transportLineDirections2.size() - 1; i >= 0; i--) {
          final Record transportLine = transportLines2.get(i);
          transportLines.add(transportLine);
          final Direction transportLineDirection = transportLineDirections2.get(i);
          transportLineDirections.add(transportLineDirection.opposite());
        }
      }
    }
    final LineString line1 = getLine();
    final LineString line2 = street.getLine();
    final Point point = line1.getPoint(end1);
    final LineString line = line1.merge(point, line2);

    final Street mergedStreet = new Street(this.rule, line, transportLines, transportLineDirections,
      this.singleCivicNumber);
    return mergedStreet;
  }

  public void setNextStreet(final Street nextStreet) {
    this.nextStreet = nextStreet;
  }

  public void setPreviousStreet(final Street previousStreet) {
    this.previousStreet = previousStreet;
  }

  @Override
  public String toString() {
    final StringBuilder string = new StringBuilder();
    if (this.transportLines.isEmpty()) {
      string.append(GbaType.getStructuredName1(this.sites.get(0)));
      string.append("\n  ");
    } else {
      string.append(GbaType.getStructuredName1(this.transportLines.get(0)));
      final Identifier leftLocalityId = getLocalityId(Side.LEFT);
      final Identifier rightLocalityId = getLocalityId(Side.RIGHT);
      string.append(" - ");
      string.append((String)GbaController.getLocalities().getValue(leftLocalityId));
      if (!leftLocalityId.equals(rightLocalityId)) {
        string.append(", ");
        string.append((String)GbaController.getLocalities().getValue(rightLocalityId));
      }
      string.append("\n  ");
      string.append(getTransportLineFieldValues(TransportLine.TRANSPORT_LINE_ID));
      string.append("\n  ");
    }
    string.append(this.streetSideLeft);
    string.append("\n  ");
    string.append(this.streetSideRight);
    return string.toString();
  }

  public boolean validateMultipleSameStreetBlock(final SitePointRule rule, final int blockNumber,
    final IntMinMax blockRange) {
    boolean valid = true;
    QaMessageDescription leftMessage = null;
    QaMessageDescription rightMessage = null;

    for (final Side side : Side.VALUES) {
      if (localityEqual(rule, side)) {
        QaMessageDescription message = null;
        final StreetSide streetSide = getStreetSide(side);
        if (!streetSide.isMultipleBlockSameMatched(blockNumber, blockRange)) {
          final StreetSide oppositeSide = getStreetSide(side.opposite());
          final HouseNumberScheme siteScheme = streetSide.getSiteStreetScheme();
          if (siteScheme.isNone()) {
            if (getTransportLineScheme(side).opposite().isSame(oppositeSide.getSiteStreetScheme())
              && oppositeSide.isMultipleBlockSameMatched(blockNumber, blockRange)) {
              // Valid
              // TODO update range?
            } else {
              valid = false;
              message = SitePointRule.MESSAGE_BLOCK_NO_SITES;
            }
          } else if (streetSide.getStreetScheme().isNone()) {
            valid = false;
            message = SitePointRule.MESSAGE_BLOCK_NO_TRANSPORT_LINE_HOUSE_NUMBERS;
          } else {
            valid = false;
            message = SitePointRule.MESSAGE_BLOCK_TRANSPORT_LINE_SITE_RANGE_DIFFERENT;
          }
        }
        if (side.isLeft()) {
          leftMessage = message;
        } else {
          rightMessage = message;
        }
      }
    }
    if (!valid) {
      // Debug.println(this);
    }

    logSideCounts(rule, blockNumber, "Multiple Same Street", leftMessage, rightMessage);
    return valid;
  }

  public boolean validateSingleSpanStreetBlock(final SitePointRule rule, final int blockNumber,
    final IntMinMax blockRange) {
    boolean valid = true;

    QaMessageDescription leftMessage = null;
    QaMessageDescription rightMessage = null;
    for (final Side side : Side.VALUES) {
      if (localityEqual(rule, side)) {
        QaMessageDescription message = null;
        final StreetSide streetSide = getStreetSide(side);
        if (!streetSide.isSingleBlockSpanMatched(blockNumber, blockRange)) {
          final HouseNumberScheme siteScheme = streetSide.getSiteStreetScheme();
          if (siteScheme.isNone()) {
            valid = false;
            message = SitePointRule.MESSAGE_BLOCK_NO_SITES;
          } else if (streetSide.getStreetScheme().isNone()) {
            valid = false;
            message = SitePointRule.MESSAGE_BLOCK_NO_TRANSPORT_LINE_HOUSE_NUMBERS;
          } else {
            valid = false;
            message = SitePointRule.MESSAGE_BLOCK_TRANSPORT_LINE_SITE_RANGE_DIFFERENT;
          }
        }
        if (side.isLeft()) {
          leftMessage = message;
        } else {
          rightMessage = message;
        }
      }
    }
    logSideCounts(rule, blockNumber, "Single Span", leftMessage, rightMessage);
    return valid;
  }

  public boolean validateSingleStreetBlock(final SitePointRule rule, final int blockNumber,
    final IntMinMax blockRange) {
    boolean valid = true;
    for (final Side side : Side.VALUES) {
      final StreetSide streetSide = getStreetSide(side);
      streetSide.logSitesWithWrongScheme(rule, blockNumber);
    }

    QaMessageDescription leftMessage = null;
    QaMessageDescription rightMessage = null;
    for (final Side side : Side.VALUES) {
      QaMessageDescription message = null;
      if (localityEqual(rule, side)) {
        final StreetSide streetSide = getStreetSide(side);
        if (!streetSide.isSingleBlockMatched(blockNumber, blockRange)) {
          final HouseNumberScheme siteScheme = streetSide.getSiteScheme();
          if (siteScheme.isNone()) {
            valid = false;
            message = SitePointRule.MESSAGE_BLOCK_NO_SITES;
          } else if (streetSide.getStreetScheme().isNone()) {
            valid = false;
            message = SitePointRule.MESSAGE_BLOCK_NO_TRANSPORT_LINE_HOUSE_NUMBERS;
          } else {
            valid = false;
            message = SitePointRule.MESSAGE_BLOCK_TRANSPORT_LINE_SITE_RANGE_DIFFERENT;
          }
        }
      }
      if (side.isLeft()) {
        leftMessage = message;
      } else {
        rightMessage = message;
      }
    }
    logSideCounts(rule, blockNumber, "Single Same", leftMessage, rightMessage);
    return valid;
  }

}
