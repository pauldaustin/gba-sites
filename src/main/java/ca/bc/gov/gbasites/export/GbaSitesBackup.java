package ca.bc.gov.gbasites.export;

import java.nio.file.Path;

import org.jeometry.common.io.PathName;

import ca.bc.gov.gba.controller.GbaConfig;
import ca.bc.gov.gba.model.GbaTables;
import ca.bc.gov.gba.ui.BatchUpdateDialog;
import ca.bc.gov.gba.ui.StatisticsDialog;
import ca.bc.gov.gbasites.controller.GbaSiteDatabase;
import ca.bc.gov.gbasites.model.type.SiteTables;
import ca.bc.gov.gbasites.model.type.code.CommunityPoly;

import com.revolsys.io.file.Paths;
import com.revolsys.parallel.process.ProcessNetwork;
import com.revolsys.record.Record;
import com.revolsys.record.io.RecordReader;
import com.revolsys.record.io.RecordWriter;
import com.revolsys.record.query.Query;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.record.schema.RecordStore;
import com.revolsys.transaction.Transaction;

public class GbaSitesBackup implements GbaTables {
  public static void main(final String[] args) {
    final GbaSitesBackup process = new GbaSitesBackup();
    BatchUpdateDialog.start(process::batchUpdate, "GBA Sites Backup", BatchUpdateDialog.READ,
      BatchUpdateDialog.WRITE);
  }

  private final Path backupDirectory = Paths.getPath(GbaConfig.getDataDirectory(),
    "exports/BACKUP/gba_sites_export/GBA/");

  private BatchUpdateDialog dialog;

  private final RecordStore gbaRecordStore = GbaSiteDatabase.getRecordStore();

  private void backupRecords(final PathName... typePaths) {
    final ProcessNetwork processNetwork = new ProcessNetwork();
    for (final PathName typePath : typePaths) {
      processNetwork.addProcess(() -> backupRecords(typePath));
    }
    processNetwork.startAndWait();
  }

  private void backupRecords(final PathName typePath) {
    final RecordDefinition recordDefinition = this.gbaRecordStore.getRecordDefinition(typePath);
    if (recordDefinition != null) {
      final Query query = new Query(recordDefinition) //
        .addOrderById();

      final Path tsvPath = Paths.getPath(this.backupDirectory, typePath.getName() + ".tsv");
      try (
        RecordReader gbaReader = this.gbaRecordStore.getRecords(query);
        RecordWriter tsvWriter = RecordWriter.newRecordWriter(recordDefinition, tsvPath);) {
        for (final Record record : this.dialog.cancellable(gbaReader)) {
          this.dialog.addLabelCount(StatisticsDialog.COUNTS, typePath, BatchUpdateDialog.READ);
          this.dialog.addLabelCount(StatisticsDialog.COUNTS, typePath, BatchUpdateDialog.WRITE);
          tsvWriter.write(record);
        }
      }
    }
  }

  private boolean batchUpdate(final BatchUpdateDialog dialog, final Transaction transaction) {
    this.dialog = dialog;
    Paths.deleteDirectories(this.backupDirectory);
    Paths.createDirectories(this.backupDirectory);
    backupRecords( //
      INTEGRATION_SESSION_POLY, //

      DATA_CAPTURE_METHOD_CODE, //
      SiteTables.FEATURE_STATUS_CODE, //

      PARTNER_ORGANIZATION, //

      CommunityPoly.COMMUNITY_POLY, //

      LOCALITY_POLY, //

      REGIONAL_DISTRICT_POLY, //

      NAME_DESCRIPTOR_CODE, //
      NAME_DIRECTION_CODE, //
      NAME_PREFIX_CODE, //
      NAME_SUFFIX_CODE, //
      STRUCTURED_NAME, //

      // SITE_POINT
      SiteTables.SITE_LOCATION_CODE, //
      SiteTables.SITE_TYPE_CODE, //
      SiteTables.SITE_POINT //
    );
    return true;
  }
}
