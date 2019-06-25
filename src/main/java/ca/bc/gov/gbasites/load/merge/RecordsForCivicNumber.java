package ca.bc.gov.gbasites.load.merge;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import ca.bc.gov.gbasites.model.type.SitePoint;

import com.revolsys.record.Record;
import com.revolsys.util.Counter;

public class RecordsForCivicNumber {

  private final Map<String, RecordsForCivicNumberSuffix> recordsByCivicNumberSuffix = new TreeMap<>();

  private final Integer civicNumber;

  public RecordsForCivicNumber(final Integer civicNumber) {
    this.civicNumber = civicNumber;
  }

  public void addRecord(final Record record) {
    final String civicNumberSuffix = record.getString(SitePoint.CIVIC_NUMBER_SUFFIX, "");
    RecordsForCivicNumberSuffix records = this.recordsByCivicNumberSuffix.get(civicNumberSuffix);
    if (records == null) {
      records = new RecordsForCivicNumberSuffix(civicNumberSuffix);
      this.recordsByCivicNumberSuffix.put(civicNumberSuffix, records);
    }
    records.addRecord(record);
  }

  public void addRecord(final Record record, final RecordMergeCounters counters) {
    final String civicNumberSuffix = record.getString(SitePoint.CIVIC_NUMBER_SUFFIX, "");
    RecordsForCivicNumberSuffix records = this.recordsByCivicNumberSuffix.get(civicNumberSuffix);
    if (records == null) {
      records = new RecordsForCivicNumberSuffix(civicNumberSuffix);
      this.recordsByCivicNumberSuffix.put(civicNumberSuffix, records);
    }
    records.addRecord(record, counters);
  }

  public void deleteRecords(final Counter counter) {
    for (final RecordsForCivicNumberSuffix records : this.recordsByCivicNumberSuffix.values()) {
      records.deleteRecords(counter);
    }
  }

  public RecordsForCivicNumberSuffix getCivicNumberSuffix(final String civicNumberSuffix) {
    return this.recordsByCivicNumberSuffix.get(civicNumberSuffix);
  }

  public Set<String> getCivicNumberSuffixes() {
    return this.recordsByCivicNumberSuffix.keySet();
  }

  public RecordsForCivicNumberSuffix removeCivicNumberSuffix(final String civicNumberSuffix) {
    final RecordsForCivicNumberSuffix records = this.recordsByCivicNumberSuffix
      .remove(civicNumberSuffix);
    if (records == null) {
      return new RecordsForCivicNumberSuffix(civicNumberSuffix);
    } else {
      return records;
    }
  }

  @Override
  public String toString() {
    return Integer.toString(this.civicNumber);
  }
}
