package ca.bc.gov.gbasites.model.rule;

import java.util.Comparator;

import org.jeometry.common.io.PathName;

import ca.bc.gov.gbasites.model.type.SitePoint;
import ca.bc.gov.gbasites.model.type.SiteTables;

import com.revolsys.record.Record;
import com.revolsys.record.Records;

public class SiteAddressComparator implements Comparator<Record> {

  private static final String[] FIELD_NAMES1 = new String[] {
    SitePoint.LOCALITY_ID, SitePoint.STREET_NAME_ID, SitePoint.CIVIC_NUMBER
  };

  public static final SiteAddressComparator INSTANCE = new SiteAddressComparator();

  public static int compareDo(final Record site1, final Record site2) {
    if (site1 == site2) {
      return 0;
    } else {
      final PathName typePath1 = site1.getPathName();
      if (typePath1.equals(SiteTables.SITE_POINT)) {
        final PathName typePath2 = site2.getPathName();
        if (typePath2.equals(SiteTables.SITE_POINT)) {
          int compare = Records.compareNullLast(site1, site2, FIELD_NAMES1);
          if (compare == 0) {
            compare = Records.compareNullFirst(site1, site2, SitePoint.UNIT_DESCRIPTOR);
            if (compare == 0) {
              compare = Records.compareNullLast(site1, site2, SitePoint.SITE_ID);
            }
          }
          return compare;
        } else {
          return -1;
        }
      } else {
        return 1;
      }
    }
  }

  @Override
  public int compare(final Record site1, final Record site2) {
    return compareDo(site1, site2);
  }

}
