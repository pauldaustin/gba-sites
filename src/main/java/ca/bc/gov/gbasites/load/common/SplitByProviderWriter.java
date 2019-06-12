package ca.bc.gov.gbasites.load.common;

import java.io.Closeable;

import ca.bc.gov.gba.ui.StatisticsDialog;
import ca.bc.gov.gbasites.load.ImportSites;

import com.revolsys.io.file.AtomicPathUpdator;
import com.revolsys.record.Record;
import com.revolsys.record.io.RecordWriter;
import com.revolsys.record.schema.RecordDefinitionProxy;
import com.revolsys.util.Counter;

public class SplitByProviderWriter implements Closeable {

  private final RecordWriter recordWriter;

  private final Counter counter;

  private final String dataProvider;

  private final AtomicPathUpdator pathUpdator;

  public SplitByProviderWriter(final StatisticsDialog dialog, final String dataProvider,
    final Counter counter, final PartnerOrganizationFiles partnerOrganizationFiles,
    final RecordDefinitionProxy recordDefinition) {
    this.dataProvider = dataProvider;
    this.counter = counter;
    this.pathUpdator = partnerOrganizationFiles.newPathUpdator(ImportSites.SOURCE_BY_PROVIDER);
    this.recordWriter = RecordWriter.newRecordWriter(recordDefinition, this.pathUpdator.getPath());
    this.recordWriter.setProperty("useQuotes", false);
  }

  @Override
  public void close() {
    try {
      this.recordWriter.close();
    } finally {
      this.pathUpdator.close();
    }
  }

  public String getDataProvider() {
    return this.dataProvider;
  }

  public String getDataProviderUpper() {
    return this.dataProvider.toUpperCase();
  }

  @Override
  public String toString() {
    return this.dataProvider;
  }

  public void writeRecord(final Record record) {
    this.counter.add();
    this.recordWriter.write(record);
  }
}
