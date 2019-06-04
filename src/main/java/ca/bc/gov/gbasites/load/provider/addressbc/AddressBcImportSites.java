package ca.bc.gov.gbasites.load.provider.addressbc;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import javax.swing.SortOrder;

import org.jeometry.common.date.Dates;
import org.jeometry.common.logging.Logs;

import ca.bc.gov.gba.controller.GbaController;
import ca.bc.gov.gba.ui.BatchUpdateDialog;
import ca.bc.gov.gbasites.controller.SitePointInit;
import ca.bc.gov.gbasites.model.type.SitePoint;

import com.revolsys.io.ZipUtil;
import com.revolsys.io.file.Paths;
import com.revolsys.jdbc.io.JdbcRecordStore;
import com.revolsys.spring.resource.UrlResource;
import com.revolsys.transaction.Transaction;

public class AddressBcImportSites extends BatchUpdateDialog implements SitePoint {

  private static final long serialVersionUID = 1L;

  public static void deleteTempFiles(final Path directory) {
    try {
      Files.list(directory).forEach(path -> {
        final String fileName = Paths.getFileName(path);
        if (fileName.startsWith("_")) {
          if (!Paths.deleteDirectories(path)) {
            Logs.error(GbaController.class, "Unable to remove temporary files from: " + path);
          }
        }
      });
    } catch (final IOException e) {
      Logs.error(GbaController.class, "Unable to remove temporary files from: " + directory, e);
    }
  }

  public static void main(final String[] args) {
    start(AddressBcImportSites.class);
  }

  private final boolean action1Download = true;

  private final boolean action2Split = true;

  private final boolean action3Convert = true;

  private final boolean action4MergeDb = true;

  public boolean action4UpdateDb = true;

  private final JdbcRecordStore recordStore = GbaController.getGbaRecordStore();

  public String dateDirectory = //
      // "2018-09-11" //
      // "2018-10-09"//
      "2018-10-24"//
  ;

  public AddressBcImportSites() {
    super("Address BC Site Import");
    removeLabelCountTableModel(COUNTS);
    setLockName(null);
    setLogChanges(false);
    // this.action4UpdateDb = "2018-09-11".equals(this.dateDirectory);
  }

  private void action1Download() {
    final String url = "ftp://geoshare.icisociety.ca/Addresses/ABC.csv.zip";
    try {
      final String user = GbaController.getProperty("addressBcFtpUser");
      final String password = GbaController.getProperty("addressBcFtpPassword");
      final UrlResource resource = new UrlResource(url, user, password);
      this.dateDirectory = Dates.format("yyyy-MM-dd");
      final Path inputDirectory = getInputDirectory();
      Paths.createDirectories(inputDirectory);
      ZipUtil.unzipFile(resource, inputDirectory);
    } catch (final Exception e) {
      Logs.error(this, "Error downloading: " + url, e);
    }
  }

  @Override
  protected boolean batchUpdate(final Transaction transaction) {
    SitePointInit.init(this.recordStore);
    if (!isCancelled() && this.action1Download) {
      action1Download();
    }
    if (this.dateDirectory == null) {

    }
    if (!isCancelled() && (this.action2Split || !Paths.exists(getInputByProviderDirectory()))) {
      new AddressBcSplitByProvider(this).run();
    }
    if (!isCancelled() && this.action3Convert || !Paths.exists(getSitePointByProviderDirectory())) {
      new AddressBcConvert(this).run();
    }
    if (!isCancelled() && this.action4MergeDb) {
      new AddressBcMerge(this).run();
    }
    return !isCancelled();
  }

  public Path getDirectory() {
    return AddressBc.DIRECTORY //
      .resolve(this.dateDirectory);
  }

  public Path getInputByProviderDirectory() {
    return getDirectory() //
      .resolve("InputByProvider");
  }

  public Path getInputDirectory() {
    return getDirectory().resolve("Input");
  }

  public Path getSitePointByProviderDirectory() {
    return getDirectory().resolve("SitePointByProvider");
  }

  @Override
  protected void initDialog() {
    super.initDialog();
    getLabelCountTableModel(COUNTS).getTable().setSortOrder(0, SortOrder.ASCENDING);
  }

}
