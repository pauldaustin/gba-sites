package ca.bc.gov.gbasites.load.provider.addressbc;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import javax.swing.SortOrder;

import org.jeometry.common.logging.Logs;

import ca.bc.gov.gba.controller.GbaController;
import ca.bc.gov.gba.ui.BatchUpdateDialog;
import ca.bc.gov.gbasites.controller.GbaSiteDatabase;
import ca.bc.gov.gbasites.controller.SitePointInit;
import ca.bc.gov.gbasites.model.type.SitePoint;

import com.revolsys.io.file.Paths;
import com.revolsys.jdbc.io.JdbcRecordStore;
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

  private final boolean action1Download = false;

  private final boolean action2Split = true;

  private final boolean action3Convert = true;

  private final boolean action4MergeDb = true;

  public boolean action4UpdateDb = true;

  private final JdbcRecordStore recordStore = GbaSiteDatabase.getRecordStore();

  public String dateDirectory = //
      // "2018-09-11" //
      // "2018-10-09"//
      "2019-06-04"//
  ;

  public AddressBcImportSites() {
    super("Address BC Site Import");
    removeLabelCountTableModel(COUNTS);
    setLockName(null);
    setLogChanges(false);
    // this.action4UpdateDb = "2018-09-11".equals(this.dateDirectory);
  }

  @Override
  protected boolean batchUpdate(final Transaction transaction) {
    SitePointInit.init(this.recordStore);
    final Path directory = AddressBcSplitByProvider.split(this, this.action1Download);
    if (directory != null) {
      if (!isCancelled() && this.action3Convert
        || !Paths.exists(directory.resolve("SitePointByProvider"))) {
        new AddressBcConvert(this, directory).run();
      }
      if (!isCancelled() && this.action4MergeDb) {
        new AddressBcMerge(this, directory).run();
      }
    }
    return !isCancelled();
  }

  @Override
  protected void initDialog() {
    super.initDialog();
    getLabelCountTableModel(COUNTS).getTable().setSortOrder(0, SortOrder.ASCENDING);
  }

}
