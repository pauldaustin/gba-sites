package ca.bc.gov.gbasites.load.provider.geobc;

import java.nio.file.Path;

import org.jeometry.common.io.PathName;
import org.jeometry.common.logging.Logs;

import ca.bc.gov.gba.ui.StatisticsDialog;
import ca.bc.gov.gbasites.load.provider.addressbc.AddressBcSplitByProvider;

import com.revolsys.gis.esri.gdb.file.FileGdbRecordStore;
import com.revolsys.gis.esri.gdb.file.FileGdbRecordStoreFactory;
import com.revolsys.record.Record;
import com.revolsys.record.io.RecordReader;
import com.revolsys.record.schema.RecordDefinitionImpl;

public class GeoBcSplitByProvider extends AddressBcSplitByProvider {

  private FileGdbRecordStore recordStore;

  public GeoBcSplitByProvider(final StatisticsDialog dialog) {
    super(dialog, GeoBC.DIRECTORY);
    this.statisticsName = GeoBC.COUNT_PREFIX + "Source";
    this.fileSuffix = GeoBC.FILE_SUFFIX;
    this.orgFileName = "GEOBC_PARTNER_ORGANIZATION.xlsx";
  }

  @Override
  protected void addFields(final RecordDefinitionImpl recordDefinitionImpl) {
  }

  @Override
  protected RecordReader newRecordReader() {
    return this.recordStore.getRecords(PathName.newPathName("/SUPPLEMENTAL_ADDRESS"));
  }

  @Override
  protected void postRun() {
  }

  @Override
  protected void preRun() {
  }

  @Override
  protected void preWriteRecord(final Record sourceRecord) {
  }

  @Override
  public void run() {
    final Path file = this.sourceDirectory.resolve("Supplemental_Address.gdb");
    try (
      FileGdbRecordStore recordStore = FileGdbRecordStoreFactory.newRecordStoreInitialized(file)) {
      this.recordStore = recordStore;
      super.run();
    } catch (final Exception e) {
      Logs.error(this, "Error processing: " + file, e);
    }
  }

}
