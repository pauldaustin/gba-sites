package ca.bc.gov.gbasites.load.convert;

import java.nio.file.Path;

import com.revolsys.geometry.model.Geometry;
import com.revolsys.geometry.model.Punctual;
import com.revolsys.io.BaseCloseable;
import com.revolsys.io.file.AtomicPathUpdator;
import com.revolsys.record.Record;
import com.revolsys.record.RecordLog;

public class ConvertAllRecordLog implements BaseCloseable {
  public static String[] FIELD_NAMES = {
    "PROVIDER", "LOCALITY_NAME"
  };

  private final Path directory;

  private final String fileSufix;

  private RecordLog errorLog;

  private RecordLog warningLog;

  private RecordLog ignoreLog;

  public ConvertAllRecordLog(final Path directory, final String fileSuffix) {
    this.directory = directory;
    this.fileSufix = fileSuffix;
  }

  @Override
  public void close() {
    if (this.errorLog != null) {
      this.errorLog.close();
    }
    if (this.warningLog != null) {
      this.warningLog.close();
    }
    if (this.ignoreLog != null) {
      this.ignoreLog.close();
    }
  }

  public void error(final String partnerOrganizationName, final String localityName,
    final Record record, final String message) {
    this.errorLog = log(this.errorLog, "ERROR", partnerOrganizationName, localityName, record,
      message);
  }

  public void ignore(final String partnerOrganizationName, final String localityName,
    final Record record, final String message) {
    this.ignoreLog = log(this.ignoreLog, "IGNORE", partnerOrganizationName, localityName, record,
      message);
  }

  private RecordLog log(RecordLog recordLog, final String filePrefix,
    final String partnerOrganizationName, final String localityName, final Record record,
    final String message) {
    Geometry geometry = record.getGeometry();
    if (!(geometry instanceof Punctual)) {
      geometry = geometry.getPointWithin();
    }
    if (recordLog == null) {
      final AtomicPathUpdator pathUpdator = new AtomicPathUpdator(this.directory,
        filePrefix + this.fileSufix + ".tsv");
      recordLog = new RecordLog(pathUpdator, record, FIELD_NAMES);
    }
    recordLog.log(message, record, geometry, partnerOrganizationName, localityName);
    return recordLog;
  }

  public void warning(final String partnerOrganizationName, final String localityName,
    final Record record, final String message) {
    this.warningLog = log(this.warningLog, "WARNING", partnerOrganizationName, localityName, record,
      message);
  }

}
