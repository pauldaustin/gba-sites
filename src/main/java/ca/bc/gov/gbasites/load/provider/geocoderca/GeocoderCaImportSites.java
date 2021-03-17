package ca.bc.gov.gbasites.load.provider.geocoderca;

import java.nio.file.Path;

import ca.bc.gov.gba.core.model.CountNames;
import ca.bc.gov.gba.itn.model.code.StructuredName;
import ca.bc.gov.gba.ui.BatchUpdateDialog;
import ca.bc.gov.gbasites.model.type.SitePoint;

import com.revolsys.transaction.Transaction;

public class GeocoderCaImportSites extends BatchUpdateDialog
  implements SitePoint, StructuredName, CountNames {

  private static final long serialVersionUID = 1L;

  public static final String LOCALITY_NAME = "LOCALITY_NAME";

  public static void main(final String[] args) {
    start(GeocoderCaImportSites.class);
  }

  private final boolean action1ExtractBc = false;

  private final boolean action2SplitByLocality = false;

  private final boolean action3ConvertByLocality = false;

  private final boolean action4UpdatePostalCodeByLocality = true;

  public GeocoderCaImportSites() {
    super("Geocoder.ca Import");
    removeLabelCountTableModel(COUNTS);
  }

  @Override
  protected boolean batchUpdate(final Transaction transaction) {
    if (this.action1ExtractBc) {
      new GeocoderCaFilterBC(this)//
        .run();
    }
    if (this.action2SplitByLocality) {
      new GeocoderCaSplitByLocality(this)//
        .run();
    }
    if (this.action3ConvertByLocality) {
      new GeocoderCaSiteConvert(this)//
        .run();
    }
    if (this.action4UpdatePostalCodeByLocality) {
      new GeocoderCaUpdateGbaPostalCodes(this)//
        .run();
    }
    return true;
  }

  public Path getDirectory() {
    return GeocoderCa.DIRECTORY;
  }

  public Path getInputByProviderDirectory() {
    return getDirectory() //
      .resolve("InputByLocality");
  }

  public Path getInputDirectory() {
    return getDirectory().resolve("Input");
  }

  public Path getSitePointByProviderDirectory() {
    return getDirectory() //
      .resolve("SitePointByLocality");
  }

}
