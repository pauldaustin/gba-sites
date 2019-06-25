package ca.bc.gov.gbasites.load.merge;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import ca.bc.gov.gbasites.model.type.SitePoint;

import com.revolsys.record.Record;
import com.revolsys.util.Counter;

public class RecordsForStreetName {

  private final Map<Integer, RecordsForCivicNumber> recordsByCivicNumber = new TreeMap<>();

  private final String streetName;

  public RecordsForStreetName(final String streetName) {
    super();
    this.streetName = streetName;
  }

  public void addRecord(final Record record) {
    final Integer civicNumber = record.getInteger(SitePoint.CIVIC_NUMBER, -1);
    RecordsForCivicNumber records = this.recordsByCivicNumber.get(civicNumber);
    if (records == null) {
      records = new RecordsForCivicNumber(civicNumber);
      this.recordsByCivicNumber.put(civicNumber, records);
    }
    records.addRecord(record);
  }

  public void addRecord(final Record record, final RecordMergeCounters counters) {
    final Integer civicNumber = record.getInteger(SitePoint.CIVIC_NUMBER, -1);
    RecordsForCivicNumber records = this.recordsByCivicNumber.get(civicNumber);
    if (records == null) {
      records = new RecordsForCivicNumber(civicNumber);
      this.recordsByCivicNumber.put(civicNumber, records);
    }
    records.addRecord(record, counters);
  }

  public void deleteRecords(final Counter counter) {
    for (final RecordsForCivicNumber records : this.recordsByCivicNumber.values()) {
      records.deleteRecords(counter);
    }
  }

  public RecordsForCivicNumber getCivicNumber(final Integer civicNumber) {
    return this.recordsByCivicNumber.get(civicNumber);
  }

  public Set<Integer> getCivicNumbers() {
    return this.recordsByCivicNumber.keySet();
  }

  public RecordsForCivicNumber removeCivicNumber(final Integer civicNumber) {
    final RecordsForCivicNumber records = this.recordsByCivicNumber.remove(civicNumber);
    if (records == null) {
      return new RecordsForCivicNumber(civicNumber);
    } else {
      return records;
    }
  }

  @Override
  public String toString() {
    return this.streetName;
  }
}
