package ca.bc.gov.gbasites.load.provider.geocoderca;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import javax.swing.SortOrder;
import javax.swing.SwingUtilities;

import ca.bc.gov.gba.core.model.CountNames;

import com.revolsys.io.file.Paths;
import com.revolsys.parallel.process.ProcessNetwork;
import com.revolsys.swing.table.counts.LabelCountMapTableModel;
import com.revolsys.util.Cancellable;

public class GeocoderCaUpdateGbaPostalCodes implements Cancellable {

  private final GeocoderCaImportSites importSites;

  LabelCountMapTableModel counts;

  private final Path sitePointByProviderDirectory;

  private final String processLocality = //
      // "VANCOUVER"//
      // "WEST_VANCOUVER"//
      null//
  ;

  public GeocoderCaUpdateGbaPostalCodes(final GeocoderCaImportSites importSites) {
    this.importSites = importSites;
    this.sitePointByProviderDirectory = importSites.getSitePointByProviderDirectory();
  }

  @Override
  public boolean isCancelled() {
    return this.importSites.isCancelled();
  }

  public void run() {
    this.counts = this.importSites.labelCounts("Post Code Update", "Locality",
      CountNames.READ, "GBA Read", CountNames.UPDATED);
    SwingUtilities.invokeLater(() -> {

      this.counts.getTable().setSortOrder(0, SortOrder.ASCENDING);

      this.importSites.setSelectedTab("Post Code Update");
    });

    final List<Path> providerFiles = Paths.listFiles(this.sitePointByProviderDirectory,
      "[^_].*_SITE_POINT_GEOCODER_CA.tsv");
    Collections.sort(providerFiles);

    if (!providerFiles.isEmpty()) {
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
              .equals(Paths.getBaseName(path).replace("_SITE_POINT_GEOCODER_CA", ""))) {
              new GeocoderCaUpdateGbaPostalCodesLocality(this, this.importSites, path).run();
            }
          }
        });
      }
      processNetwork.startAndWait();
    }
  }
}
