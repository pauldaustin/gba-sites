package ca.bc.gov.gbasites.load.common;

import java.nio.file.Path;

import ca.bc.gov.gba.model.type.code.PartnerOrganization;
import ca.bc.gov.gba.model.type.code.PartnerOrganizationProxy;
import ca.bc.gov.gba.ui.StatisticsDialog;

import com.revolsys.io.file.AtomicPathUpdator;

public class PartnerOrganizationFiles implements CharSequence, PartnerOrganizationProxy {

  private final Path baseDirectory;

  private final PartnerOrganization partnerOrganization;

  private final String providerSuffix;

  private final StatisticsDialog dialog;

  public PartnerOrganizationFiles(final StatisticsDialog dialog,
    final PartnerOrganization partnerOrganization, final Path baseDirectory,
    final String providerSuffix) {
    this.dialog = dialog;
    this.partnerOrganization = partnerOrganization;
    this.baseDirectory = baseDirectory;
    this.providerSuffix = providerSuffix;
  }

  @Override
  public char charAt(final int index) {
    return this.partnerOrganization.charAt(index);
  }

  public Path getBaseDirectory() {
    return this.baseDirectory;
  }

  public String getFileName(final DirectorySuffixAndExtension dirAndSuffix) {
    return dirAndSuffix.getFileName(this.partnerOrganization, this.providerSuffix);
  }

  public Path getFilePath(final DirectorySuffixAndExtension dirAndSuffix) {
    return dirAndSuffix.getFilePath(this.baseDirectory, this.partnerOrganization,
      this.providerSuffix);
  }

  @Override
  public PartnerOrganization getPartnerOrganization() {
    return this.partnerOrganization;
  }

  public String getProviderSuffix() {
    return this.providerSuffix;
  }

  @Override
  public int length() {
    return this.partnerOrganization.length();
  }

  public AtomicPathUpdator newPathUpdator(final DirectorySuffixAndExtension dirAndSuffix) {
    return dirAndSuffix.newPathUpdator(this.dialog, this.baseDirectory, this.partnerOrganization,
      this.providerSuffix);
  }

  @Override
  public CharSequence subSequence(final int start, final int end) {
    return this.partnerOrganization.subSequence(start, end);
  }

  @Override
  public String toString() {
    return this.partnerOrganization.getPartnerOrganizationName();
  }
}
