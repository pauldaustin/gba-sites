package ca.bc.gov.gbasites.load.provider.addressbc;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import javax.swing.SortOrder;
import javax.swing.SwingUtilities;

import ca.bc.gov.gba.controller.GbaController;
import ca.bc.gov.gba.model.type.code.StructuredNames;
import ca.bc.gov.gba.ui.StatisticsDialog;
import ca.bc.gov.gbasites.load.converter.AbstractSiteConverter;
import ca.bc.gov.gbasites.model.type.SitePoint;

import com.revolsys.io.file.Paths;
import com.revolsys.parallel.process.ProcessNetwork;
import com.revolsys.record.RecordLog;
import com.revolsys.record.io.RecordReader;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.swing.table.counts.LabelCountMapTableModel;
import com.revolsys.util.Cancellable;

public class AddressBcConvert implements Cancellable, Runnable {

  private final StatisticsDialog dialog;

  private final Path inputByProviderDirectory;

  LabelCountMapTableModel counts;

  RecordDefinition recordDefinition;

  private final Path sitePointByProviderDirectory;

  private final Path directory;

  public AddressBcConvert(final StatisticsDialog dialog) {
    this.dialog = dialog;

    this.inputByProviderDirectory = SitePoint.SITES_DIRECTORY //
      .resolve("AddressBc")//
      .resolve("InputByProvider") //
    ;
    this.sitePointByProviderDirectory = SitePoint.SITES_DIRECTORY //
      .resolve("AddressBc")//
      .resolve("SitePointByProvider") //
    ;

    this.directory = this.sitePointByProviderDirectory.getParent();
    Paths.deleteDirectories(this.sitePointByProviderDirectory);
    Paths.createDirectories(this.sitePointByProviderDirectory);
  }

  public AddressBcConvert(final StatisticsDialog dialog, final Path directory) {
    this.dialog = dialog;
    this.directory = directory;
    this.inputByProviderDirectory = directory.resolve("InputByProvider");
    this.sitePointByProviderDirectory = directory.resolve("SitePointByProvider");
    Paths.deleteDirectories(this.sitePointByProviderDirectory);
    Paths.createDirectories(this.sitePointByProviderDirectory);
  }

  private void destroy() {
    if (!isCancelled()) {
      final Path providerCountsPath = this.directory.resolve("ADDRESS_BC_PROVIDER_COUNTS.xlsx");
      this.counts.writeCounts(providerCountsPath);
    }
    Paths.deleteFiles(this.sitePointByProviderDirectory, "*.prj");
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
    return this.dialog.isCancelled();
  }

  public RecordLog newAllRecordLog(final List<Path> providerFiles, final String suffix) {
    final Path allErrorFile = this.directory.resolve("ADDRESS_BC_CONVERT_" + suffix + ".tsv");

    try (
      RecordReader reader = RecordReader.newRecordReader(providerFiles.get(0))) {
      this.recordDefinition = reader.getRecordDefinition();
    }
    return new RecordLog(allErrorFile, this.recordDefinition, true);
  }

  @Override
  public void run() {
    this.counts = this.dialog.newLabelCountTableModel("Convert", "Provider", "Read", "Error",
      "Ignored", "Warning", "Write", "Merged UD", "Duplicate");
    SwingUtilities.invokeLater(() -> {

      this.counts.getTable().setSortOrder(0, SortOrder.ASCENDING);

    });

    final List<Path> providerFiles = Paths.listFiles(this.inputByProviderDirectory,
      "[^_].*_ADDRESS_BC.tsv");
    Collections.sort(providerFiles);

    if (!providerFiles.isEmpty()) {
      try (
        RecordLog allErrorLog = newAllRecordLog(providerFiles, "ERROR");
        RecordLog allWarningLog = newAllRecordLog(providerFiles, "WARNING");) {
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
              new AddressBcSiteConverter(this, this.dialog, this.sitePointByProviderDirectory, path,
                allErrorLog, allWarningLog).run();
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
