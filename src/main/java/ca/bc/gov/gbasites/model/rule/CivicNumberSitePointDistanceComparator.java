package ca.bc.gov.gbasites.model.rule;

import java.util.Comparator;
import java.util.Map;

import org.jeometry.common.number.Doubles;

import ca.bc.gov.gbasites.model.type.SitePoint;

import com.revolsys.geometry.model.LineString;
import com.revolsys.geometry.model.Point;
import com.revolsys.record.Record;
import com.revolsys.record.Records;

public class CivicNumberSitePointDistanceComparator implements Comparator<Integer> {
  private final LineString line;

  private final Map<Integer, Record> siteByHouseNumber;

  public CivicNumberSitePointDistanceComparator(final LineString line,
    final Map<Integer, Record> siteByHouseNumber) {
    this.line = line;
    this.siteByHouseNumber = siteByHouseNumber;
  }

  @Override
  public int compare(final Integer number1, final Integer number2) {
    if (Doubles.equal(number1, number2)) {
      return 0;
    } else {
      final Record address1 = this.siteByHouseNumber.get(number1);
      final Point point1 = address1.getGeometry();
      final double distance1 = this.line.distanceAlong(point1);
      final Record address2 = this.siteByHouseNumber.get(number2);
      final Point point2 = address2.getGeometry();
      final double distance2 = this.line.distanceAlong(point2);
      final int distanceCompare = Double.compare(distance1, distance2);
      if (distanceCompare == 0) {
        final int houseNumberCompare = Records.compareNullFirst(address1, address2,
          SitePoint.CIVIC_NUMBER, SitePoint.SITE_ID);
        return houseNumberCompare;
      } else {
        return distanceCompare;
      }
    }
  }
}
