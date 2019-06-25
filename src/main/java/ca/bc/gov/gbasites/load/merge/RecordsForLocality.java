package ca.bc.gov.gbasites.load.merge;

import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import ca.bc.gov.gbasites.model.type.SitePoint;

import com.revolsys.record.Record;
import com.revolsys.record.io.RecordReader;
import com.revolsys.util.Cancellable;
import com.revolsys.util.Counter;

public class RecordsForLocality {

  private final Map<String, RecordsForStreetName> recordsByStreetName = new TreeMap<>();

  public void addRecord(final Record record) {
    final String streetName = record.getString(SitePoint.STREET_NAME, "");
    RecordsForStreetName records = this.recordsByStreetName.get(streetName);
    if (records == null) {
      records = new RecordsForStreetName(streetName);
      this.recordsByStreetName.put(streetName, records);
    }
    records.addRecord(record);
  }

  public void addRecord(final Record record, final RecordMergeCounters counters) {
    final String streetName = record.getString(SitePoint.STREET_NAME, "");
    RecordsForStreetName records = this.recordsByStreetName.get(streetName);
    if (records == null) {
      records = new RecordsForStreetName(streetName);
      this.recordsByStreetName.put(streetName, records);
    }
    records.addRecord(record, counters);
  }

  public void addRecords(final Cancellable cancellable, final Path file, final Counter counter) {
    try (
      RecordReader reader = RecordReader.newRecordReader(file)) {
      for (final Record record : cancellable.cancellable(reader)) {
        addRecord(record);
        counter.add();
      }
    }
  }

  public void addRecords(final Cancellable cancellable, final Path file,
    final RecordMergeCounters counters) {
    final Counter counter = counters.read;
    try (
      RecordReader reader = RecordReader.newRecordReader(file)) {
      for (final Record record : cancellable.cancellable(reader)) {
        addRecord(record, counters);
        counter.add();
      }
    }
  }

  public void deleteRecords(final Counter counter) {
    for (final RecordsForStreetName records : this.recordsByStreetName.values()) {
      records.deleteRecords(counter);
    }
  }

  public RecordsForStreetName getStreet(final String streetName) {
    return this.recordsByStreetName.get(streetName);
  }

  public Set<String> getStreetNames() {
    return this.recordsByStreetName.keySet();
  }

  public RecordsForStreetName removeStreet(final String streetName) {
    final RecordsForStreetName records = this.recordsByStreetName.remove(streetName);
    if (records == null) {
      return new RecordsForStreetName(streetName);
    } else {
      return records;
    }
  }

  @Override
  public String toString() {
    return "locality";
  }
}
