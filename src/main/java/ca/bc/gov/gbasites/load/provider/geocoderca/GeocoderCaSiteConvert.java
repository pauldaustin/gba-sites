package ca.bc.gov.gbasites.load.provider.geocoderca;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import javax.swing.SortOrder;
import javax.swing.SwingUtilities;

import org.jeometry.common.data.type.DataTypes;

import ca.bc.gov.gba.controller.GbaController;
import ca.bc.gov.gba.model.type.code.PartnerOrganization;
import ca.bc.gov.gba.model.type.code.PartnerOrganizations;
import ca.bc.gov.gba.model.type.code.StructuredNames;
import ca.bc.gov.gbasites.load.convert.AbstractSiteConverter;

import com.revolsys.io.file.Paths;
import com.revolsys.parallel.process.ProcessNetwork;
import com.revolsys.record.RecordLog;
import com.revolsys.record.io.RecordReader;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.record.schema.RecordDefinitionImpl;
import com.revolsys.swing.table.counts.LabelCountMapTableModel;
import com.revolsys.util.Cancellable;

public class GeocoderCaSiteConvert implements Cancellable {

  private final GeocoderCaImportSites importSites;

  private final Path inputByProviderDirectory;

  LabelCountMapTableModel counts;

  RecordDefinition recordDefinition;

  private final Path sitePointByProviderDirectory;

  private PartnerOrganization partnerOrganization;

  private final String processLocality = //
      // "VANCOUVER"//
      // "WEST_VANCOUVER"//
      null//
  ;

  public GeocoderCaSiteConvert(final GeocoderCaImportSites importSites) {
    this.importSites = importSites;
    this.inputByProviderDirectory = importSites.getInputByProviderDirectory();
    this.sitePointByProviderDirectory = importSites.getSitePointByProviderDirectory();
    Paths.deleteDirectories(this.sitePointByProviderDirectory);
    Paths.createDirectories(this.sitePointByProviderDirectory);
  }

  private void destroy() {
    if (!isCancelled()) {
      final Path providerCountsPath = this.importSites.getDirectory()
        .resolve("GEOCODER_CA_PROVIDER_COUNTS.xlsx");
      this.counts.writeCounts(providerCountsPath);
    }
    Paths.deleteFiles(this.sitePointByProviderDirectory, "*.prj");
  }

  public PartnerOrganization getPartnerOrganization() {
    return this.partnerOrganization;
  }

  private void init() {
    final StructuredNames structuredNames = GbaController.getStructuredNames();
    structuredNames.setLoadAll(true);
    structuredNames.setLoadMissingCodes(false);
    structuredNames.refresh();

    AbstractSiteConverter.init();
  }

  @Override
  public boolean isCancelled() {
    return this.importSites.isCancelled();
  }

  public RecordLog newAllRecordLog(final List<Path> providerFiles, final String suffix) {
    final Path allErrorFile = this.importSites.getDirectory()
      .resolve("GEOCODER_CA_CONVERT_" + suffix + ".tsv");

    try (
      RecordReader reader = RecordReader.newRecordReader(providerFiles.get(0))) {
      this.recordDefinition = reader.getRecordDefinition();
      final RecordDefinitionImpl recordDefinition = (RecordDefinitionImpl)this.recordDefinition;
      recordDefinition.addField("FULL_ADDRESS", DataTypes.STRING);
      recordDefinition.addField("FULL_ADDRESS_PARTS", DataTypes.STRING);
    }
    return new RecordLog(allErrorFile, this.recordDefinition, true);
  }

  public void run() {
    this.partnerOrganization = PartnerOrganizations.newPartnerOrganization("Geocoder.ca");
    this.counts = this.importSites.newLabelCountTableModel("Convert", "Provider", "Read", "Error",
      "Ignored", "Warning", "Write", "Merged UD", "Duplicate");
    SwingUtilities.invokeLater(() -> {

      this.counts.getTable().setSortOrder(0, SortOrder.ASCENDING);

      this.importSites.setSelectedTab("Convert");
    });

    final List<Path> providerFiles = Paths.listFiles(this.inputByProviderDirectory,
      "[^_].*_GEOCODER_CA.tsv");
    Collections.sort(providerFiles);

    if (!providerFiles.isEmpty()) {
      try (
        RecordLog allErrorLog = newAllRecordLog(providerFiles, "ERROR");
        RecordLog allWarningLog = newAllRecordLog(providerFiles, "WARNING");
        RecordLog allIgnoreLog = newAllRecordLog(providerFiles, "IGNORE");) {
        init();

        final ProcessNetwork processNetwork = new ProcessNetwork();
        for (int i = 0; i < 8; i++) {
          processNetwork.addProcess(() -> {
            while (!isCancelled()) {
              Path path;
              synchronized (providerFiles) {
                if (providerFiles.isEmpty()) {
                  return;
                }
                path = providerFiles.remove(0);
              }
              if (this.processLocality == null || this.processLocality
                .equals(Paths.getBaseName(path).replace("_GEOCODER_CA", ""))) {
                new GeocoderCaSiteConverter(this, this.importSites, path, allErrorLog,
                  allWarningLog, allIgnoreLog).run();
              }
            }
          });
        }
        processNetwork.startAndWait();
      } finally {
        destroy();
      }
    }
  }
}
