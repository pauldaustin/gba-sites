package ca.bc.gov.gbasites.load.common;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import ca.bc.gov.gba.model.type.code.PartnerOrganization;
import ca.bc.gov.gba.ui.BatchUpdateDialog;
import ca.bc.gov.gbasites.controller.GbaSiteDatabase;

import com.revolsys.io.file.AtomicPathUpdator;
import com.revolsys.io.file.Paths;
import com.revolsys.util.Cancellable;
import com.revolsys.util.CaseConverter;

public class DirectorySuffixAndExtension {

  private final String directory;

  private final String suffix;

  private final String extension;

  public DirectorySuffixAndExtension(final String directory, final String suffix,
    final String extension) {
    this.directory = directory;
    this.suffix = suffix;
    this.extension = extension;
  }

  public void createDirectory(final Path baseDirectory) {
    final Path directory = baseDirectory.resolve(this.directory);
    Paths.createDirectories(directory);
  }

  public String getDirectory() {
    return this.directory;
  }

  public String getExtension() {
    return this.extension;
  }

  public String getFileName(final PartnerOrganization partnerOrganization,
    final String providerSuffix) {
    final String partnerOrganizationFileName = partnerOrganization.getPartnerOrganizationFileName();
    return getFileName(partnerOrganizationFileName, providerSuffix);
  }

  private String getFileName(final String prefix) {
    return prefix + this.suffix + this.extension;
  }

  private String getFileName(final String prefix, final String providerSuffix) {
    return prefix + this.suffix + providerSuffix + this.extension;
  }

  public Path getFilePath(final Path baseDirectory, final PartnerOrganization partnerOrganization,
    final String providerSuffix) {
    final Path directory = newDirectoryPath(baseDirectory);
    final String fileName = getFileName(partnerOrganization, providerSuffix);
    return directory.resolve(fileName);
  }

  public Path getLocalityFilePath(final Path baseDirectory, final String localityName) {
    final Path directory = newDirectoryPath(baseDirectory);
    final String fileName = getFileName(localityName);
    return directory.resolve(fileName);
  }

  public String getSuffix() {
    return this.suffix;
  }

  public List<Path> listFiles(final Path baseDirectory, String providerSuffix) {
    if (providerSuffix == null) {
      providerSuffix = "";
    }
    final List<Path> files = Paths.listFiles(newDirectoryPath(baseDirectory),
      "[^_].*" + this.suffix + providerSuffix + this.extension);
    Collections.sort(files);
    return files;
  }

  public List<Path> listLocalityFiles(final Path baseDirectory, final String localityName) {
    final String localityFileName = BatchUpdateDialog.toFileName(localityName);
    final String pattern = localityFileName + this.suffix + ".*" + this.extension;
    final List<Path> files = Paths.listFiles(newDirectoryPath(baseDirectory), pattern);
    Collections.sort(files);
    return files;
  }

  public List<PartnerOrganization> listPartnerOrganizations(final Path baseDirectory,
    String providerSuffix) {
    if (providerSuffix == null) {
      providerSuffix = "";
    }
    final String fullSuffix = this.suffix + providerSuffix + this.extension;
    final List<Path> files = Paths.listFiles(newDirectoryPath(baseDirectory),
      "[^_].*" + fullSuffix);

    final List<PartnerOrganization> partnerOrganizations = new ArrayList<>();
    for (final Path file : files) {
      final String fileName = Paths.getFileName(file);
      final String providerShortName = fileName.substring(0,
        fileName.length() - fullSuffix.length());
      final PartnerOrganization partnerOrganization = GbaSiteDatabase
        .newPartnerOrganization(CaseConverter.toCapitalizedWords(providerShortName));
      partnerOrganizations.add(partnerOrganization);
    }
    Collections.sort(partnerOrganizations);

    return partnerOrganizations;
  }

  public Path newDirectoryPath(final Path baseDirectory) {
    return baseDirectory.resolve(this.directory);
  }

  public AtomicPathUpdator newLocalityPathUpdator(final Cancellable cancellable,
    final Path baseDirectory, final String localityName) {
    final Path directory = newDirectoryPath(baseDirectory);

    final String prefixFileName = BatchUpdateDialog.toFileName(localityName);
    final String fileName = getFileName(prefixFileName);

    return new AtomicPathUpdator(cancellable, directory, fileName);
  }

  public AtomicPathUpdator newLocalityPathUpdator(final Cancellable cancellable,
    final Path baseDirectory, final String localityName,
    final PartnerOrganization partnerOrganization, String providerSuffix) {
    final Path directory = newDirectoryPath(baseDirectory);

    final String prefixFileName = BatchUpdateDialog.toFileName(localityName);
    final String partnerOrganizationFileName = partnerOrganization.getPartnerOrganizationFileName();
    providerSuffix = "_" + partnerOrganizationFileName + providerSuffix;
    final String fileName = getFileName(prefixFileName, providerSuffix);

    return new AtomicPathUpdator(cancellable, directory, fileName);
  }

  public AtomicPathUpdator newPathUpdator(final Cancellable cancellable, final Path baseDirectory,
    final PartnerOrganization partnerOrganization, final String providerSuffix) {
    final Path directory = newDirectoryPath(baseDirectory);
    final String fileName = getFileName(partnerOrganization, providerSuffix);
    return new AtomicPathUpdator(cancellable, directory, fileName);
  }
}
