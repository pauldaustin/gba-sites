package ca.bc.gov.gbasites.model.rule;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.jeometry.common.data.type.DataType;
import org.jeometry.common.number.Numbers;

import ca.bc.gov.gba.controller.GbaController;
import ca.bc.gov.gba.itn.model.HouseNumberScheme;
import ca.bc.gov.gba.model.type.TransportLines;
import ca.bc.gov.gba.rule.transportline.AddressRange;
import ca.bc.gov.gbasites.model.type.SitePoint;

import com.revolsys.collection.list.Lists;
import com.revolsys.collection.map.Maps;
import com.revolsys.collection.range.IntMinMax;
import com.revolsys.collection.range.RangeInvalidException;
import com.revolsys.geometry.model.Direction;
import com.revolsys.geometry.model.End;
import com.revolsys.geometry.model.LineString;
import com.revolsys.geometry.model.Point;
import com.revolsys.geometry.model.Side;
import com.revolsys.record.Record;
import com.revolsys.util.Debug;
import com.revolsys.util.Parity;
import com.revolsys.util.Property;
import com.revolsys.util.StringBuilders;

public class StreetSide {

  public static IntMinMax getMinMaxInBlock(final IntMinMax intMinMax, final IntMinMax blockRange,
    final HouseNumberScheme scheme) {
    final Parity parity = scheme.getParity();
    final IntMinMax minMaxInBlock = intMinMax.clip(blockRange, parity);
    return minMaxInBlock;
  }

  private Direction direction = Direction.FORWARDS;

  private final Side side;

  private final IntMinMax siteMinMax = new IntMinMax();

  private List<Integer> siteNumbers = new ArrayList<>();

  private final List<Record> sites = new ArrayList<>();

  private HouseNumberScheme siteScheme = HouseNumberScheme.NONE;

  private final Street street;

  private final IntMinMax streetMinMax;

  private boolean streetRangeUpdated;

  private HouseNumberScheme streetScheme = HouseNumberScheme.NONE;

  private final IntMinMax transportLineMinMax;

  private HouseNumberScheme transportLineScheme = HouseNumberScheme.NONE;

  private final List<Integer> updatedStreetBlocks = new ArrayList<>();

  public StreetSide(final Street street, final Side side) {
    this.street = street;
    this.side = side;
    this.transportLineScheme = this.getTransportLineScheme(side);
    this.transportLineMinMax = getTransportLineMinMax(side);
    this.streetScheme = this.transportLineScheme;
    this.streetMinMax = this.transportLineMinMax.clone();
  }

  public void addSite(final Record site) {
    this.sites.add(site);
    updateSiteNumbers();
  }

  public void addSites(final Collection<Record> sites) {
    Lists.addAll(this.sites, sites);
    updateSiteNumbers();
  }

  public void addSitesMatchingScheme(final List<Record> sites) {
    if (!sites.isEmpty()) {
      Boolean odd = null;
      if (this.siteScheme.isOdd()) {
        odd = Boolean.TRUE;
      } else if (this.siteScheme.isEven()) {
        odd = Boolean.FALSE;
      } else if (this.siteScheme.isNone()) {
        if (getStreetScheme().isOdd()) {
          odd = Boolean.TRUE;
        } else if (getStreetScheme().isEven()) {
          odd = Boolean.FALSE;
        }
      }
      if (odd != null) {
        for (final Record site : sites) {
          if (this.street.isUseUnitDescriptor()) {
            boolean allMatched = true;
            boolean hasMatch = false;
            try {
              for (final Object unit : SitePoint.getUnitDescriptorRanges(site)) {
                if (unit instanceof Number) {
                  final int number = ((Number)unit).intValue();
                  if (Numbers.isOdd(number) == odd) {
                    hasMatch = true;
                  } else {
                    allMatched = false;
                  }
                }
              }
            } catch (final RangeInvalidException e) {
            }
            if (hasMatch && allMatched) {
              addSite(site);
            }
          } else {
            final Integer number = site.getInteger(SitePoint.CIVIC_NUMBER);
            if (Property.hasValue(number) && Numbers.isOdd(number) == odd) {
              if (isInBlock(number)) {
                addSite(site);
              }
            }
          }
        }
      }
    }
  }

  public void append(final StringBuilder string, final HouseNumberScheme scheme, final Integer from,
    final Integer to) {
    final String code = scheme.getCode();
    string.append(code);
    if (!scheme.isNone()) {
      string.append(' ');
      string.append(from);
      string.append('-');
      string.append(to);
    }
  }

  public boolean containsTransportLineNumber(final int civicNumber) {
    if (this.transportLineScheme.isNone()) {
      return false;
    } else if (this.transportLineScheme.isEven()) {
      if (Numbers.isEven(civicNumber)) {
        return getTransportLineMinMax().contains(civicNumber);
      } else {
        return false;
      }
    } else if (this.transportLineScheme.isOdd()) {
      if (Numbers.isOdd(civicNumber)) {
        return getTransportLineMinMax().contains(civicNumber);
      } else {
        return false;
      }
    }
    return false;
  }

  public void expandSingleBlock(final int blockNumber, final IntMinMax blockRange) {
    // final boolean divided = this.street.isDivided();
    // TODO check which side of divided based on travel direction
    boolean expand = false;
    final HouseNumberScheme siteScheme = getSiteScheme();
    HouseNumberScheme streetScheme = this.streetScheme;
    final StreetSide oppositeStreetSide = getOppositeStreetSide();
    final HouseNumberScheme oppositeSiteScheme = oppositeStreetSide.getSiteScheme();
    if (siteScheme.isNone() && oppositeSiteScheme.isNone()) {
      for (int i = -100; i < 101 && !expand; i += 200) {
        final int blockNumber2 = blockNumber + i;
        final StreetBlock block2 = SitePointRule.getStreetBlock(blockNumber2);
        if (block2 != null) {
          final StreetSide streetSide2 = block2.getTouchingStreetSide(this);
          if (streetSide2 != null) {
            final HouseNumberScheme scheme2 = streetSide2.getSiteStreetScheme();
            if (scheme2 == this.transportLineScheme) {
              if (scheme2.isEvenOrOdd()) {
                if (streetSide2.hasStreetSiteInBlock(blockNumber2)) {
                  streetScheme = scheme2;
                  expand = true;
                }
              }
            }
          }
        }
      }
    } else if (oppositeStreetSide.overlaps(blockRange)) {
      final HouseNumberScheme oppositeTransportLineScheme = oppositeStreetSide
        .getTransportLineScheme();

      if (siteScheme.isNone()) {
        if (oppositeSiteScheme.isEven() || oppositeSiteScheme.isOdd()) {
          expand = true;
          streetScheme = oppositeSiteScheme.opposite();
        }
      } else if (siteScheme.isEven()) {
        if (oppositeSiteScheme.isOdd()) {
          expand = true;
          streetScheme = oppositeSiteScheme.opposite();
        } else if (oppositeSiteScheme.isNone() && oppositeTransportLineScheme.isOdd()) {
          expand = true;
          streetScheme = oppositeTransportLineScheme.opposite();
        }
      } else if (siteScheme.isOdd()) {
        if (oppositeSiteScheme.isEven()) {
          expand = true;
          streetScheme = oppositeSiteScheme.opposite();
        } else if (oppositeSiteScheme.isNone() && oppositeTransportLineScheme.isEven()) {
          expand = true;
          streetScheme = oppositeTransportLineScheme.opposite();
        }
      }
    } else {
      expand = true;
    }

    if (expand) {
      if (streetScheme.isSame(this.streetScheme) || this.streetScheme.isNone()) {
        final IntMinMax parityBlockRange = blockRange.convert(this.streetScheme.getParity());
        this.streetMinMax.add(parityBlockRange);

        this.streetRangeUpdated = true;
        this.updatedStreetBlocks.add(blockNumber);
      }
    }
  }

  public Direction getDirection() {
    return this.direction;
  }

  public HouseNumberScheme getDirectionalScheme() {
    if (this.direction.isForwards()) {
      return this.siteScheme;
    } else {
      return this.siteScheme.reverse();
    }
  }

  public int getFromNumber() {
    if (this.direction.isForwards()) {
      return getSiteMin();
    } else {
      return getSiteMax();
    }
  }

  public LineString getLine() {
    if (this.street != null) {
      return this.street.getLine();
    }
    return null;
  }

  private Integer getNumber(final End lineEnd) {
    if (End.isFrom(lineEnd)) {
      return getFromNumber();
    } else if (End.isTo(lineEnd)) {
      return getToNumber();
    }
    return null;
  }

  public StreetSide getOppositeAddress() {
    return getOppositeStreetSide();
  }

  public StreetSide getOppositeRange() {
    if (this.street != null) {
      if (Side.isLeft(this.side)) {
        return this.street.getStreetSideRight();
      } else {
        return this.street.getStreetSideLeft();
      }
    }
    return null;
  }

  public Side getOppositeSide() {
    if (this.side != null) {
      if (Side.isLeft(this.side)) {
        return Side.RIGHT;
      } else {
        return Side.LEFT;
      }
    }
    return null;
  }

  public StreetSide getOppositeStreetSide() {
    final Side oppositeSide = Side.opposite(this.side);
    return this.street.getStreetSide(oppositeSide);
  }

  public Point getPoint(final End lineEnd) {
    final LineString line = getLine();
    if (line != null) {
      return line.getPoint(lineEnd);
    }
    return null;
  }

  public Side getSide() {
    return this.side;
  }

  public int getSiteBlockMax() {
    if (getSiteMax() == -1) {
      return -1;
    } else {
      int blockMax = getSiteMax() / 100 * 100 + 99;
      if (isSchemeEven()) {
        blockMax -= 1;
      }
      return blockMax;
    }
  }

  public int getSiteBlockMin() {
    if (getSiteMin() == -1) {
      return -1;
    } else {
      int blockMin = getSiteMin() / 100 * 100;
      if (this.siteScheme.isOdd()) {
        blockMin += 1;
      }
      return blockMin;
    }
  }

  public int getSiteMax() {
    return this.siteMinMax.getMax();
  }

  public int getSiteMin() {
    return this.siteMinMax.getMin();
  }

  public IntMinMax getSiteMinMax() {
    return this.siteMinMax;
  }

  public IntMinMax getSiteMinMax(final IntMinMax blockRange) {
    final IntMinMax intMinMax = getSiteMinMax();
    final HouseNumberScheme scheme = getSiteScheme();
    return getMinMaxInBlock(intMinMax, blockRange, scheme);
  }

  public List<Integer> getSiteNumbers() {
    return this.siteNumbers;
  }

  public List<Record> getSites() {
    return new ArrayList<>(this.sites);
  }

  public HouseNumberScheme getSiteScheme() {
    return this.siteScheme;
  }

  public HouseNumberScheme getSiteSchemeNonVirtual() {
    boolean hasEven = false;
    boolean hasOdd = false;
    for (final Record site : this.sites) {
      if (!SitePoint.isVirtual(site)) {
        final int civicNumber = site.getInteger(SitePoint.CIVIC_NUMBER);
        if (Numbers.isEven(civicNumber)) {
          hasEven = true;
        } else {
          hasOdd = true;
        }
      }
    }
    return HouseNumberScheme.getScheme(hasEven, hasOdd);
  }

  public IntMinMax getSiteStreetMinMax() {
    if (this.streetRangeUpdated) {
      return getStreetMinMax();
    } else {
      return getSiteMinMax();
    }
  }

  public IntMinMax getSiteStreetMinMax(final int blockNumber, final IntMinMax blockRange) {
    IntMinMax intMinMax;
    HouseNumberScheme scheme;
    if (this.updatedStreetBlocks.contains(blockNumber)) {
      intMinMax = getStreetMinMax();
      scheme = getStreetScheme();
    } else {
      intMinMax = getSiteMinMax();
      scheme = getSiteScheme();
    }
    return getMinMaxInBlock(intMinMax, blockRange, scheme);
  }

  public HouseNumberScheme getSiteStreetScheme() {
    if (this.streetRangeUpdated) {
      return getStreetScheme();
    } else {
      return getSiteScheme();
    }
  }

  public Street getStreet() {
    return this.street;
  }

  public Integer getStreetFromNumber() {
    if (this.direction.isForwards()) {
      return this.streetMinMax.getFrom();
    } else {
      return this.streetMinMax.getTo();
    }
  }

  public IntMinMax getStreetMinMax() {
    return this.streetMinMax;
  }

  public double getStreetNumberRatio(final int civicNumber) {
    final double ratio = Numbers.ratio(civicNumber, getStreetFromNumber(), getStreetToNumber());
    return ratio;
  }

  public HouseNumberScheme getStreetScheme() {
    return this.streetScheme;
  }

  public Integer getStreetToNumber() {
    if (this.direction.isForwards()) {
      return this.streetMinMax.getTo();
    } else {
      return this.streetMinMax.getFrom();
    }
  }

  public int getToNumber() {
    if (this.direction.isForwards()) {
      return getSiteMax();
    } else {
      return getSiteMin();
    }
  }

  /**
   * Check if the other addressRange touches the specified point of this address range.
   * @param addressRange
   * @param from
   * @return
   */
  public End getTouchingEnd(final StreetSide addressRange, final End lineEnd) {
    final Point point = getPoint(lineEnd);
    if (point == null) {
      return null;
    } else if (point.equals(addressRange.getPoint(End.FROM))) {
      return End.FROM;
    } else if (point.equals(addressRange.getPoint(End.TO))) {
      return End.TO;
    } else {
      return null;
    }
  }

  public IntMinMax getTransportLineMinMax() {
    return this.transportLineMinMax;
  }

  public IntMinMax getTransportLineMinMax(final int blockNumber) {
    final IntMinMax intMinMax = new IntMinMax();
    if (this.street != null) {
      for (final End end : End.FROM_TO) {
        final Integer number = getTransportLineNumber(end);
        intMinMax.add(number);
      }
    }
    return intMinMax;
  }

  public IntMinMax getTransportLineMinMax(final IntMinMax blockRange) {
    final IntMinMax transportLineMinMax = getTransportLineMinMax();
    return getMinMaxInBlock(transportLineMinMax, blockRange, this.transportLineScheme);
  }

  private IntMinMax getTransportLineMinMax(final Side side) {
    final IntMinMax intMinMax = new IntMinMax();
    final List<Direction> transportLineDirections = this.street.getTransportLineDirections();
    final List<Record> transportLines = this.street.getTransportLines();
    for (int i = 0; i < transportLines.size(); i++) {
      final Record transportLine = transportLines.get(i);
      final Direction direction = transportLineDirections.get(i);
      Side transportLineSide = side;
      if (direction.isBackwards()) {
        transportLineSide = transportLineSide.opposite();
      }
      final HouseNumberScheme scheme = TransportLines.getHouseNumberScheme(transportLine,
        transportLineSide);
      for (final End end : End.FROM_TO) {
        final String fieldName = TransportLines.getHouseNumberFieldName(end, transportLineSide);
        final Integer number = transportLine.getInteger(fieldName);
        if (number == null) {
          if (!scheme.isNone()) {
            Debug.noOp();
          }
        } else {
          if (scheme.isNone()) {
            Debug.noOp();
          }
          intMinMax.add(number);
        }
      }
    }
    return intMinMax;
  }

  public Integer getTransportLineNumber(final End end) {
    return this.street.getTransportLineNumber(end, this.side);
  }

  public HouseNumberScheme getTransportLineScheme() {
    return this.transportLineScheme;
  }

  private HouseNumberScheme getTransportLineScheme(final Side side) {
    final List<Direction> transportLineDirections = this.street.getTransportLineDirections();
    final List<Record> transportLines = this.street.getTransportLines();
    int i = 0;
    for (final Record transportLine : transportLines) {
      Side transportLineSide = side;
      if (transportLineDirections.get(i).isBackwards()) {
        transportLineSide = transportLineSide.opposite();
      }
      final HouseNumberScheme scheme = TransportLines.getHouseNumberScheme(transportLine,
        transportLineSide);
      if (!scheme.isNone()) {
        return scheme;
      }
      i++;
    }
    return HouseNumberScheme.NONE;
  }

  public boolean hasEven() {
    final HouseNumberScheme scheme = getSiteScheme();
    return scheme.isContinuous() || scheme.isEven();
  }

  public boolean hasExactMatch() {
    if (DataType.equal(getDirectionalScheme(), this.transportLineScheme)) {
      if (hasMatch(End.FROM)) {
        if (hasMatch(End.TO)) {
          return true;
        }
      }
    }
    return false;
  }

  public boolean hasMatch(final End lineEnd) {
    final Integer number = getNumber(lineEnd);
    final Integer transportLineNumber = getTransportLineNumber(lineEnd);
    return DataType.equal(number, transportLineNumber);
  }

  public boolean hasMatchReverse(final End lineEnd) {
    final End oppositeEnd = End.opposite(lineEnd);
    final Integer number = getNumber(lineEnd);
    final Integer transportLineNumber = getTransportLineNumber(oppositeEnd);
    return DataType.equal(number, transportLineNumber);
  }

  public boolean hasOdd() {
    final HouseNumberScheme scheme = getSiteScheme();
    return scheme.isContinuous() || scheme.isOdd();
  }

  public boolean hasOverlap(final StreetSide addressRange) {
    final HouseNumberScheme scheme1 = getSiteScheme();
    final HouseNumberScheme scheme2 = addressRange.getSiteScheme();
    if (scheme1.equals(HouseNumberScheme.EVEN_INCREASING)) {
      if (scheme2.equals(HouseNumberScheme.ODD_INCREASING)) {
        return false;
      }
    }
    if (scheme1.equals(HouseNumberScheme.ODD_INCREASING)) {
      if (scheme2.equals(HouseNumberScheme.EVEN_INCREASING)) {
        return false;
      }
    }
    if (getSiteMin() < addressRange.getSiteMin()) {
      return false;
    } else if (getSiteMax() > addressRange.getSiteMax()) {
      return false;
    } else {
      return true;
    }
  }

  public boolean hasSiteBlockOverlap(final IntMinMax intMinMax) {
    final IntMinMax siteMinMax = getSiteMinMax();
    return StreetBlock.blockOverlaps(intMinMax, siteMinMax);
  }

  public boolean hasSiteNumbers() {
    return !this.siteNumbers.isEmpty();
  }

  public boolean hasStreetSiteInBlock(final int blockNumber) {
    if (this.updatedStreetBlocks.contains(blockNumber)) {
      return true;
    } else {
      return StreetBlock.blockOverlaps(getSiteMinMax(), blockNumber);
    }
  }

  public boolean hasTransportLine() {
    return false;
  }

  public boolean isInBlock(final int number) {
    int blockMin = getSiteBlockMin();
    int blockMax = getSiteBlockMax();
    if (blockMin == -1 || blockMax == -1) {
      blockMin = StreetBlock.getBlockFrom(getStreetMinMax().getMin());
      blockMax = StreetBlock.getBlockTo(getStreetMinMax().getMax());
    }
    return StreetBlock.isInBlock(number, blockMin, blockMax);
  }

  public boolean isInTransportLineRange(final int civicNumber) {
    if (getTransportLineMinMax().contains(civicNumber)) {
      if (this.transportLineScheme.isEven()) {
        if (!Numbers.isEven(civicNumber)) {
          return false;
        }
      } else if (this.transportLineScheme.isOdd()) {
        if (!Numbers.isOdd(civicNumber)) {
          return false;
        }
      }
      return true;
    } else {
      return false;
    }
  }

  public boolean isInTransportLineScheme(final int number) {
    return this.transportLineScheme.isNumberValid(number);
  }

  public boolean isMultipleBlockSameMatched(final int blockNumber, final IntMinMax blockRange) {
    final HouseNumberScheme siteScheme = getSiteStreetScheme();
    final IntMinMax siteMinMaxInBlock = getSiteStreetMinMax(blockNumber, blockRange);
    final IntMinMax transportLineMinMaxInBlock = getTransportLineMinMax(blockRange);

    if (siteMinMaxInBlock.isEmpty()) {
      if (transportLineMinMaxInBlock.isEmpty()) {
        return true;
      } else {
        return false;
      }
    } else if (transportLineMinMaxInBlock.isEmpty()) {
      return false;
    } else {
      if (siteScheme.equals(this.transportLineScheme)) {
        if (transportLineMinMaxInBlock.contains(siteMinMaxInBlock)) {
          return true;
        } else {
          Debug.noOp();
        }
      }
    }
    return false;
  }

  public boolean isSchemeEven() {
    final HouseNumberScheme scheme = getSiteScheme();
    return scheme.isEven();
  }

  /**
   * Check if the site address range is larger than (contains) the transport lines range and
   * has the same scheme. Only use for a single block.
   * @param blockRange
   * @return
   */
  public boolean isSingleBlockMatched(final int blockNumber, final IntMinMax blockRange) {
    final HouseNumberScheme siteScheme = getSiteStreetScheme();
    final IntMinMax siteMinMaxInBlock = getSiteStreetMinMax(blockNumber, blockRange);
    final IntMinMax transportLineMinMaxInBlock = getTransportLineMinMax(blockRange);

    if (siteMinMaxInBlock.isEmpty()) {
      if (transportLineMinMaxInBlock.isEmpty()) {
        return true;
      } else {
        return false;
      }
    } else if (transportLineMinMaxInBlock.isEmpty()) {
      if (this.updatedStreetBlocks.contains(blockNumber)) {
        return true;
      } else {
        return false;
      }
    } else {
      if (siteScheme.equals(this.transportLineScheme)) {
        if (siteMinMaxInBlock.contains(transportLineMinMaxInBlock)) {
          return true;
        }
      }
    }
    return false;
  }

  public boolean isSingleBlockSpanMatched(final int blockNumber, final IntMinMax blockRange) {
    final HouseNumberScheme siteScheme = getSiteStreetScheme();
    final IntMinMax siteMinMaxInBlock = getSiteStreetMinMax(blockNumber, blockRange);
    final IntMinMax transportLineMinMaxInBlock = getTransportLineMinMax(blockRange);

    if (siteMinMaxInBlock.isEmpty()) {
      if (transportLineMinMaxInBlock.isEmpty()) {
        return true;
      } else {
        return false;
      }
    } else if (transportLineMinMaxInBlock.isEmpty()) {
      return false;
    } else {
      if (siteScheme.equals(this.transportLineScheme)) {
        if (siteMinMaxInBlock.contains(transportLineMinMaxInBlock)) {
          return true;
        } else {
          return false;
        }
      } else {
        return false;
      }
    }
  }

  public boolean isStreetNumbersContains(final int civicNumber) {
    return AddressRange.contains(civicNumber, this.streetScheme, getStreetFromNumber(),
      getStreetToNumber());
  }

  public void logSitesWithWrongScheme(final SitePointRule rule, final int blockNumber) {
    final StreetSide oppositeStreetSide = getOppositeStreetSide();
    final HouseNumberScheme oppositeSiteScheme = oppositeStreetSide.getTransportLineScheme();
    if (this.transportLineScheme.isOpposite(oppositeSiteScheme)) {
      final boolean isEven = this.transportLineScheme.isEven();
      for (final Record site : getSites()) {
        final int civicNumber = SitePoint.getCivicNumber(site);
        if (StreetBlock.isBlockEqual(blockNumber, civicNumber)) {
          if (Numbers.isEven(civicNumber) != isEven) {
            final HouseNumberScheme schemeName = GbaController.schemes
              .getValue(this.transportLineScheme);
            final Map<String, Object> data = Maps.newLinkedHash("scheme", schemeName);
            SitePointRule.addDataFullAddress(data, site);
            rule.addMessage(site, SitePointRule.MESSAGE_SITE_POINT_TRANSPORT_LINE_SCHEME_DIFFER,
              data, SitePoint.CIVIC_NUMBER);
            removeSite(site);
          }
        }
      }
    }
  }

  public void moveVirtualNotMatchingSchemeToOpposite(final HouseNumberScheme scheme) {
    final StreetSide oppositeStreetSide = getOppositeStreetSide();
    for (final Record site : new ArrayList<>(this.sites)) {
      if (SitePoint.isVirtual(site)) {
        final int civicNumber = site.getInteger(SitePoint.CIVIC_NUMBER);
        boolean move = false;
        if (scheme.isEven() && !Numbers.isEven(civicNumber)) {
          move = true;
        }
        if (scheme.isOdd() && !Numbers.isOdd(civicNumber)) {
          move = true;
        }
        if (move) {
          removeSite(site);
          oppositeStreetSide.addSite(site);
        }
      }
    }
  }

  public boolean overlaps(final IntMinMax blockRange) {
    if (this.siteMinMax.overlaps(blockRange)) {
      return true;
    } else if (this.streetMinMax.overlaps(blockRange)) {
      return true;
    } else if (this.transportLineMinMax.overlaps(blockRange)) {
      return true;
    } else {
      return false;
    }
  }

  protected void recalculate() {
    this.direction = Direction.FORWARDS;
    this.siteMinMax.clear();
    if (Property.hasValue(this.siteNumbers)) {
      if (this.siteNumbers.get(0) > this.siteNumbers.get(this.siteNumbers.size() - 1)) {
        this.direction = Direction.BACKWARDS;
      }
      boolean hasEven = false;
      boolean hasOdd = false;
      for (final Integer civicNumber : this.siteNumbers) {
        if (Numbers.isEven(civicNumber)) {
          hasEven = true;
        } else {
          hasOdd = true;
        }
        this.siteMinMax.add(civicNumber);
      }
      this.siteScheme = HouseNumberScheme.getScheme(hasEven, hasOdd);
    } else {
      this.siteScheme = HouseNumberScheme.NONE;
    }
  }

  public void removeSite(final Record site) {
    if (this.sites.remove(site)) {
      updateSiteNumbers();
    }
  }

  protected void setSiteNumbers(final Collection<Integer> numbers) {
    this.siteNumbers = Lists.toArray(numbers);
    recalculate();
  }

  @Override
  public String toString() {
    final StringBuilder string = new StringBuilder();
    final int from = getFromNumber();
    final int to = getToNumber();
    final HouseNumberScheme scheme = getDirectionalScheme();
    append(string, scheme, from, to);
    if (this.street != null) {
      if (this.streetRangeUpdated) {
        string.append(" {");
        final Integer streetFromNumber = getStreetFromNumber();
        final Integer streetToNumber = getStreetToNumber();
        append(string, this.streetScheme, streetFromNumber, streetToNumber);
        string.append('}');
      }
      final Integer transportLineFrom = getTransportLineNumber(End.FROM);
      final Integer transportLineTo = getTransportLineNumber(End.TO);
      string.append(" (");
      append(string, this.transportLineScheme, transportLineFrom, transportLineTo);
      string.append(')');
    }
    if (!this.sites.isEmpty()) {
      string.append(" [");
      StringBuilders.append(string, this.siteNumbers, ",");
      string.append(']');
    }
    return string.toString();
  }

  /**
   * Check if the other addressRange touches the specified point of this address range.
   * @param addressRange
   * @param from
   * @return
   */
  public boolean touches(final StreetSide addressRange, final End lineEnd) {
    final Point point = getPoint(lineEnd);
    if (point == null) {
      return false;
    } else if (point.equals(addressRange.getPoint(End.FROM))) {
      return true;
    } else if (point.equals(addressRange.getPoint(End.TO))) {
      return true;
    } else {
      return false;
    }
  }

  private void updateSiteNumbers() {
    this.sites.sort(this.street);
    this.siteNumbers.clear();
    for (final Record site : this.sites) {
      if (this.street.isUseUnitDescriptor()) {
        try {
          for (final Object unit : SitePoint.getUnitDescriptorRanges(site)) {
            if (unit instanceof Number) {
              final int number = ((Number)unit).intValue();
              if (number >= 0) {
                this.siteNumbers.add(number);
              }
            }
          }
        } catch (final RangeInvalidException e) {
        }
      } else {
        final Integer number = site.getInteger(SitePoint.CIVIC_NUMBER);
        if (number >= 0) {
          this.siteNumbers.add(number);
        }
      }
    }
    recalculate();
  }
}
