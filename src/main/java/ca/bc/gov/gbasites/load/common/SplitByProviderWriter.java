package ca.bc.gov.gbasites.load.common;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.jeometry.common.logging.Logs;

import ca.bc.gov.gba.model.type.code.PartnerOrganization;
import ca.bc.gov.gba.ui.BatchUpdateDialog;

import com.revolsys.io.file.Paths;
import com.revolsys.record.Record;
import com.revolsys.record.io.RecordWriter;
import com.revolsys.record.schema.RecordDefinitionProxy;
import com.revolsys.util.Counter;

public class SplitByProviderWriter implements Closeable {

  private final RecordWriter recordWriter;

  private int writeCount = 0;

  private final Path targetPath;

  private final Counter counter;

  private final String dataProvider;

  public SplitByProviderWriter(final String dataProvider, final Counter counter,
    final PartnerOrganization partnerOrganization, final RecordDefinitionProxy recordDefinition,
    final Path directory, final String fileNameSuffix) {
    this.dataProvider = dataProvider;
    this.counter = counter;
    final String shortName = partnerOrganization.getPartnerOrganizationShortName();
    final String baseName = BatchUpdateDialog.toFileName(shortName) + fileNameSuffix;
    final String fileName = baseName + ".tsv";
    this.targetPath = directory.resolve(fileName);
    this.recordWriter = RecordWriter.newRecordWriter(recordDefinition, this.targetPath);
    this.recordWriter.setProperty("useQuotes", false);
    Paths.deleteDirectories(directory.resolve("_" + baseName + ".prj"));
  }

  @Override
  public void close() {
    this.recordWriter.close();
    if (this.writeCount == 0) {
      try {
        Files.deleteIfExists(this.targetPath);
      } catch (final IOException e) {
        Logs.error(this, "Unable to delete: " + this.targetPath, e);
      }
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
    this.writeCount++;
  }
}
