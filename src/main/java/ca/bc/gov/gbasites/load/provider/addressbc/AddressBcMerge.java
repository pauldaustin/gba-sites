package ca.bc.gov.gbasites.load.provider.addressbc;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import javax.swing.SortOrder;
import javax.swing.SwingUtilities;

import org.jeometry.common.data.type.DataType;
import org.jeometry.common.data.type.DataTypes;
import org.jeometry.common.logging.Logs;

import ca.bc.gov.gba.model.type.code.PartnerOrganization;
import ca.bc.gov.gba.model.type.code.PartnerOrganizations;
import ca.bc.gov.gba.ui.BatchUpdateDialog;
import ca.bc.gov.gba.ui.StatisticsDialog;
import ca.bc.gov.gbasites.controller.GbaSiteDatabase;
import ca.bc.gov.gbasites.load.ImportSites;
import ca.bc.gov.gbasites.model.type.SitePoint;
import ca.bc.gov.gbasites.model.type.SiteTables;

import com.revolsys.geometry.model.GeometryDataTypes;
import com.revolsys.io.file.Paths;
import com.revolsys.jdbc.io.JdbcRecordStore;
import com.revolsys.parallel.process.ProcessNetwork;
import com.revolsys.record.io.RecordWriter;
import com.revolsys.record.schema.FieldDefinition;
import com.revolsys.record.schema.RecordDefinitionImpl;
import com.revolsys.swing.table.counts.LabelCountMapTableModel;
import com.revolsys.util.Cancellable;
import com.revolsys.util.CaseConverter;

public class AddressBcMerge implements SitePoint, Cancellable {
  public static final String MOVED = "Moved";

  public static final String MATCHED = "Matched";

  public static final String PROVIDER_READ = "Provider Read";

  public static final String GBA_READ = "GBA Read";

  private final JdbcRecordStore recordStore = GbaSiteDatabase.getRecordStore();

  private final StatisticsDialog dialog;

  RecordWriter changedRecordWriter;

  private final Path directory;

  public AddressBcMerge(final StatisticsDialog importSites, final Path directory) {
    this.dialog = importSites;
    this.directory = directory;
  }

  public void deleteTempFiles(final Path directory) {
    try {
      Files.list(directory).forEach(path -> {
        final String fileName = Paths.getFileName(path);
        if (fileName.startsWith("_")) {
          if (!Paths.deleteDirectories(path)) {
            Logs.error(this, "Unable to remove temporary files from: " + path);
          }
        }
      });
    } catch (final IOException e) {
      Logs.error(this, "Unable to remove temporary files from: " + directory, e);
    }
  }

  @Override
  public boolean isCancelled() {
    return this.dialog.isCancelled();
  }

  private void mergeSites() {
    try (
      RecordWriter changedRecordWriter = newChangeLogWriter()) {

      final String suffix = "_SITE_POINT_ABC";
      final List<Path> providerFiles = ImportSites.SITE_POINT_BY_PROVIDER
        .listFiles(AddressBc.ADDRESS_BC_DIRECTORY, "_ABC");

      if (!providerFiles.isEmpty()) {
        final ProcessNetwork processNetwork = new ProcessNetwork();
        for (int i = 0; i < 8; i++) {
          processNetwork.addProcess(() -> {
            while (!isCancelled()) {
              Path siteFile;
              synchronized (providerFiles) {
                if (providerFiles.isEmpty()) {
                  return;
                }
                siteFile = providerFiles.remove(0);
              }
              final String baseName = Paths.getBaseName(siteFile);
              final String providerShortName = baseName.replace(suffix, "");
              final PartnerOrganization partnerOrganization = PartnerOrganizations
                .newPartnerOrganization(CaseConverter.toCapitalizedWords(providerShortName));

              new AddressBcMergeForProvider(this, this.dialog, partnerOrganization, siteFile)//
                .run();
            }
          });
        }
        processNetwork.startAndWait();
      }
    }
  }

  private RecordWriter newChangeLogWriter() {
    final RecordDefinitionImpl changeRecordDefinition = new RecordDefinitionImpl();
    changeRecordDefinition.addField("CHANGE_INDEX", DataTypes.INT);
    changeRecordDefinition.addField("PARTNER_ORGANIZATION", DataTypes.STRING);
    changeRecordDefinition.addField("CHANGE_TYPE", DataTypes.STRING);
    changeRecordDefinition.addField("CHANGE_VALUES", DataTypes.STRING);
    for (final FieldDefinition fieldDefinition : this.recordStore
      .getRecordDefinition(SiteTables.SITE_POINT)
      .getFields()) {
      final String fieldName = fieldDefinition.getName();
      final DataType fieldType = fieldDefinition.getDataType();
      if (GEOMETRY.equals(fieldName)) {
        changeRecordDefinition.addField("POINT", GeometryDataTypes.POINT);
      } else {
        changeRecordDefinition.addField(fieldName, fieldType);
      }
    }
    final Path changeRecordFile = this.directory.resolve("ADDRESS_BC_SITE_POINT_CHANGES.tsv");
    this.changedRecordWriter = RecordWriter.newRecordWriter(changeRecordDefinition,
      changeRecordFile);
    return this.changedRecordWriter;
  }

  public void run() {
    final LabelCountMapTableModel counts = this.dialog.newLabelCountTableModel("Merge", "Provider",
      PROVIDER_READ, GBA_READ, MATCHED, BatchUpdateDialog.INSERTED, BatchUpdateDialog.UPDATED,
      MOVED, "Delete");
    SwingUtilities.invokeLater(() -> {

      counts.getTable().setSortOrder(0, SortOrder.ASCENDING);

      this.dialog.setSelectedTab("Merge");
    });

    mergeSites();
  }

}
