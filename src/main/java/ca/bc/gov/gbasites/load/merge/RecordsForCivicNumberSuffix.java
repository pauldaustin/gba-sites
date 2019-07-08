package ca.bc.gov.gbasites.load.merge;

import java.util.ArrayList;
import java.util.List;

import ca.bc.gov.gbasites.model.type.SitePoint;

import com.revolsys.collection.list.Lists;
import com.revolsys.collection.range.RangeSet;
import com.revolsys.geometry.model.Punctual;
import com.revolsys.record.Record;
import com.revolsys.util.Counter;

public class RecordsForCivicNumberSuffix {

  private final List<Record> records = new ArrayList<>();

  private final String civicNumberSuffix;

  public RecordsForCivicNumberSuffix(final String civicNumberSuffix) {
    this.civicNumberSuffix = civicNumberSuffix;
  }

  public void addRecord(final Record record) {
    this.records.add(record);
  }

  public void addRecord(final Record record, final RecordMergeCounters counters) {
    if (matchFullAddress(record, counters)) {
      // Don't add if matched, merged or duplicate
    } else if (matchUnitDescriptor(record, counters)) {
      // Don't add if matched, merged or duplicate
    } else if (matchPoint(record, counters)) {
      // Don't add if matched, merged or duplicate
    } else {
      addRecord(record);
    }
  }

  public void deleteRecords(final Counter counter) {
    counter.add(getRecordCount());
  }

  public int getRecordCount() {
    return this.records.size();
  }

  public List<Record> getRecords() {
    return Lists.toArray(this.records);
  }

  /**
   * Match records with the same full address. If the create org is the same then create a union of
   * the points. Otherwise if the new record is address BC then ignore it.
   * @param record
   * @param counters
   * @return
   */
  private boolean matchFullAddress(final Record record, final RecordMergeCounters counters) {
    for (final Record record1 : this.records) {
      if (record1.equalValue(record, SitePoint.FULL_ADDRESS)) {
        if (SitePointMerger.isAddressBc(record)) {
          counters.matchAddress.add();
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Match and merge all records which have points within 2m. May result in a MultiPoint.
   *
   * @param record
   * @param counters
   * @return
   */
  private boolean matchPoint(final Record record, final RecordMergeCounters counters) {
    final Punctual point = record.getGeometry();
    for (final Record record1 : this.records) {
      final Punctual point1 = record1.getGeometry();
      if (point.intersects(point1)) {
        SitePointMerger.setMergedPoint(record1, record);
        SitePointMerger.setMergedUnit(record1, record);
        if (record.equalValue(record1, SitePoint.CREATE_PARTNER_ORG)) {
          counters.mergePoint.add();
        } else if (SitePointMerger.isAddressBc(record)) {
          counters.matchPoint.add();
        } else {
          counters.mergePoint.add();
        }
        return true;
      }
    }
    return false;
  }

  /**
   * Merge any address BC unit descriptors into records from other providers.
   *
   * @param record
   * @param counters
   * @return
   */
  private boolean matchUnitDescriptor(final Record record, final RecordMergeCounters counters) {
    if (this.records.size() > 1 && SitePointMerger.isAddressBc(record)) {
      final String unitDescriptor = record.getString(SitePoint.UNIT_DESCRIPTOR, "");
      if (unitDescriptor.length() > 0) {
        final RangeSet unitRange = RangeSet.newRangeSet(unitDescriptor);
        for (final Record record1 : this.records) {
          final String unitDescriptor1 = record1.getString(SitePoint.UNIT_DESCRIPTOR, "");
          if (unitDescriptor1.length() > 0) {
            final RangeSet unitRange1 = RangeSet.newRangeSet(unitDescriptor1);
            unitRange.removeRange(unitRange1);
            if (unitRange.isEmpty()) {
              counters.matchUnit.add();
              return true;
            }
          }
        }
        record.setValue(SitePoint.UNIT_DESCRIPTOR, unitDescriptor.toString());
        SitePoint.updateFullAddress(record);
      }
    }
    return false;

  }

  /**
   * Merge records with the same full address
   * @param addressBc
   * @param counters
   */
  protected void mergeFullAddress(final RecordMergeCounters counters) {
    for (int i = 0; i < this.records.size(); i++) {
      final Record record1 = this.records.get(i);
      for (int j = this.records.size() - 1; j > i; j--) {
        final Record record2 = this.records.get(j);
        if (record2.equalValue(record1, SitePoint.CREATE_PARTNER_ORG)) {
          if (record1.equalValue(record2, SitePoint.FULL_ADDRESS)) {
            if (counters.isCreatePartnerOrg(record1)) {
              if (SitePointMerger.setMergedPoint(record1, record2)) {
                counters.mergeUnit.add();
              } else {
                counters.duplicate.add();
              }
              this.records.remove(j);
            }

          }
        }
      }
    }
  }

  @Override
  public String toString() {
    return this.civicNumberSuffix;
  }
}
