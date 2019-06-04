package ca.bc.gov.gbasites.model.rule;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.jeometry.common.number.Integers;

import ca.bc.gov.gba.model.type.TransportLine;
import ca.bc.gov.gba.model.type.code.HouseNumberScheme;
import ca.bc.gov.gbasites.model.type.SitePoint;

import com.revolsys.collection.map.Maps;
import com.revolsys.collection.range.IntMinMax;
import com.revolsys.collection.set.Sets;
import com.revolsys.geometry.model.End;
import com.revolsys.geometry.model.Geometry;
import com.revolsys.geometry.model.LineString;
import com.revolsys.geometry.model.Side;
import com.revolsys.record.Record;
import com.revolsys.record.Records;

public class StreetBlock {
  public static boolean blockOverlaps(final int min, final int max, final int block) {
    final int blockMin = getBlockFrom(block);
    final int blockMax = getBlockTo(block);

    return Integers.overlaps(min, max, blockMin, blockMax);
  }

  public static boolean blockOverlaps(final int min, final int max, int blockMin, int blockMax) {
    blockMin = getBlockFrom(blockMin);
    blockMax = getBlockTo(blockMax);
    return Integers.overlaps(min, max, blockMin, blockMax);
  }

  public static boolean blockOverlaps(final IntMinMax intMinMax, final int block) {
    if (intMinMax.isEmpty()) {
      return false;
    } else {
      final int min = intMinMax.getMin();
      final int max = intMinMax.getMax();
      return blockOverlaps(min, max, block);
    }
  }

  public static boolean blockOverlaps(final IntMinMax minMax1, final IntMinMax minMax2) {
    if (minMax1.isEmpty() || minMax2.isEmpty()) {
      return false;
    } else {
      final int min1 = minMax1.getMin();
      final int max1 = minMax1.getMax();
      final int min2 = minMax2.getMin();
      final int max2 = minMax2.getMax();
      return blockOverlaps(min1, max1, min2, max2);
    }
  }

  public static int getBlockFrom(final HouseNumberScheme scheme, final int number) {
    if (number < 0) {
      return -1;
    } else {
      int from = number / 100 * 100;
      if (scheme.isOdd()) {
        from++;
      }
      return from;
    }
  }

  public static int getBlockFrom(final int number) {
    if (number < 0) {
      return -1;
    } else {
      return number / 100 * 100;
    }
  }

  /**
   * If the number is greater than the end of the block return the end of the block.
   * @param blockNumber
   * @param scheme
   * @param number
   * @return
   */
  public static int getBlockMax(final int blockNumber, final HouseNumberScheme scheme,
    final int number) {
    if (number < 0) {
      return -1;
    } else {
      final int blockTo = getBlockTo(scheme, blockNumber);
      if (number > blockTo) {
        return blockTo;
      } else {
        return number;
      }
    }
  }

  /**
   * If the number is less than the start of the block return the start of the block.
   * @param blockNumber
   * @param scheme
   * @param number
   * @return
   */
  public static int getBlockMin(final int blockNumber, final HouseNumberScheme scheme,
    final int number) {
    if (number < 0) {
      return -1;
    } else {
      final int blockFrom = getBlockFrom(scheme, blockNumber);
      if (number < blockFrom) {
        return blockFrom;
      } else {
        return number;
      }
    }
  }

  public static int getBlockTo(final HouseNumberScheme scheme, final int number) {
    if (number < 0) {
      return -1;
    } else {
      int to = number / 100 * 100 + 99;
      if (scheme.isEven()) {
        to--;
      }
      return to;
    }
  }

  public static int getBlockTo(final int number) {
    if (number < 0) {
      return -1;
    } else {
      return number / 100 * 100 + 99;
    }
  }

  static boolean isBlockEqual(final int houseNumber1, final int houseNumber2) {
    if (houseNumber1 < 0 || houseNumber2 < 0) {
      return false;
    } else {
      final int block1 = houseNumber1 / 100;
      final int block2 = houseNumber2 / 100;
      return block1 == block2;
    }
  }

  public static boolean isInBlock(final int number, int blockMin, int blockMax) {
    blockMin = getBlockFrom(blockMin);
    blockMax = getBlockTo(blockMax);
    if (blockMin <= number && number <= blockMax) {
      return true;
    } else if (blockMax <= number && number <= blockMin) {
      return true;
    } else {
      return false;
    }
  }

  public static boolean isInBlockRange(final Record record, final int civicNumber) {
    if (record != null && TransportLine.isDemographic(record)) {
      final Integer singleHouseNumber = record.getInteger(TransportLine.SINGLE_HOUSE_NUMBER);
      if (singleHouseNumber == null) {
        for (final Side side : Side.VALUES) {
          if (isInBlockRange(record, side, civicNumber)) {
            return true;
          }
        }
      }
    }
    return false;
  }

  public static boolean isInBlockRange(final Record record, final Side side,
    final int civicNumber) {
    final Integer fromHouseNumber = TransportLine.getHouseNumber(record, End.FROM, side);
    final Integer toHouseNumber = TransportLine.getHouseNumber(record, End.TO, side);
    if (StreetBlock.isInBlock(civicNumber, fromHouseNumber, toHouseNumber)) {
      return true;
    }
    return false;
  }

  private final int blockNumber;

  private final IntMinMax blockRange;

  private final List<Record> sites = new ArrayList<>();

  private final List<Street> streets = new ArrayList<>();

  public StreetBlock(final int number) {
    this.blockNumber = number;
    this.blockRange = new IntMinMax(number, number + 99);
  }

  public void addSite(final Record site) {
    if (!this.sites.contains(site)) {
      this.sites.add(site);
    }
  }

  public void addStreet(final Street street) {
    if (!this.streets.contains(street)) {
      this.streets.add(street);
    }
  }

  public void expand(final SitePointRule rule) {
    if (streetCount() == 1) {
      final Street street = this.streets.get(0);
      street.expandSingleBlock(rule, this.blockNumber, this.blockRange);
    }
  }

  public int getMax() {
    return this.blockNumber + 99;
  }

  public int getMin() {
    return this.blockNumber;
  }

  public int getNumber() {
    return this.blockNumber;
  }

  public List<Record> getSites() {
    return this.sites;
  }

  public List<Street> getStreets() {
    return this.streets;
  }

  public StreetSide getTouchingStreetSide(final StreetSide streetSide1) {
    final Side side1 = streetSide1.getSide();
    StreetSide streetSide2 = null;
    final LineString line1 = streetSide1.getLine();
    for (final Street street2 : this.streets) {
      final LineString line2 = street2.getLine();
      final End[] touchingEnds = line2.touchingEnds(line1);
      if (touchingEnds != null) {
        if (streetSide2 != null) {
          // don't allow multiple matches (e.g. loops or middle of lines).
          return null;
        }
        if (touchingEnds[0] == touchingEnds[1]) {
          streetSide2 = street2.getStreetSide(side1.opposite());
        } else {
          streetSide2 = street2.getStreetSide(side1);
        }
      }
    }
    return streetSide2;
  }

  public int getTransportLineCount() {
    int count = 0;
    for (final Street street : this.streets) {
      count += street.getTransportLineCount();
    }
    return count;
  }

  public boolean hasSites() {
    for (final Street street : this.streets) {
      final List<Record> sites = street.getSites();
      if (!sites.isEmpty()) {
        return true;
      }
    }
    return !this.sites.isEmpty();
  }

  protected boolean isAllSameBlock() {
    for (final Record site : this.sites) {
      if (SitePoint.getBlock(site) != this.blockNumber) {
        return false;
      }
    }
    for (final Street street : this.streets) {
      if (!street.isSameBlock(this.blockNumber)) {
        return false;
      }
    }
    return true;
  }

  private int streetCount() {
    return this.streets.size();
  }

  @Override
  public String toString() {
    final StringBuilder string = new StringBuilder();
    string.append(this.blockNumber);
    for (final Street street : this.streets) {
      string.append('\n');
      string.append(street);
    }
    return string.toString();
  }

  public boolean validate(final SitePointRule rule) {
    boolean valid = true;
    final Boolean singleStreetValid = validateSingleStreet(rule);
    if (singleStreetValid == null) {
      valid &= validateMultipleStreets(rule);
    } else {
      valid &= singleStreetValid;
    }
    return valid;
  }

  public boolean validateCivicNumberDuplicates(final SitePointRule rule) {
    boolean valid = true;
    if (!this.streets.isEmpty()) {
      final Map<Integer, Set<Street>> streetsByCivicNumber = new TreeMap<>();
      final Map<Integer, Set<Record>> sitesByCivicNumber = new TreeMap<>();
      for (final Street street : this.streets) {
        for (final Record site : street.getSites()) {
          final int civicNumber = site.getInteger(SitePoint.CIVIC_NUMBER);
          Maps.addToCollection(Sets.linkedHashFactory(), streetsByCivicNumber, civicNumber, street);
          Maps.addToCollection(Sets.linkedHashFactory(), sitesByCivicNumber, civicNumber, site);
        }
      }
      for (final Entry<Integer, Set<Street>> entry : streetsByCivicNumber.entrySet()) {
        final int civicNumber = entry.getKey();
        final Set<Street> streets = entry.getValue();
        if (streets.size() > 1) {
          valid = false;
          final Set<Record> sites = sitesByCivicNumber.get(civicNumber);
          final Geometry errorGeometry = Records.getGeometry(sites);

          for (final Record site : sites) {
            final String fullAddress = site.getString(SitePoint.FULL_ADDRESS);
            final Map<String, Object> data = Maps.newLinkedHash("fullAddress", fullAddress);
            rule.addMessage(site,
              SitePointRule.MESSAGE_SITE_POINT_TRANSPORT_LINE_DUPLICATE_CIVIC_NUMBER, errorGeometry,
              data, SitePoint.CIVIC_NUMBER);
          }
        }
      }
    }
    return valid;
  }

  private boolean validateMultipleStreets(final SitePointRule rule) {
    if (isAllSameBlock()) {
      rule.addCount("Info", "Block Multiple Same");
      boolean matched = true;
      for (final Street street : this.streets) {
        if (street.validateMultipleSameStreetBlock(rule, this.blockNumber, this.blockRange)) {
        } else {
          matched = false;
        }
      }
      if (matched) {
        rule.addCount("Info", "Block Multiple Same Matched");
      }
      return matched;
    } else {
      rule.addCount("Info", "Block Multiple Span");
      // Debug.println(this);
    }
    return false;
  }

  private Boolean validateSingleStreet(final SitePointRule rule) {
    if (streetCount() == 1) {
      final Street street = this.streets.get(0);
      if (isAllSameBlock()) {
        return street.validateSingleStreetBlock(rule, this.blockNumber, this.blockRange);
      } else {
        return street.validateSingleSpanStreetBlock(rule, this.blockNumber, this.blockRange);
      }
    }
    return null;
  }

}
