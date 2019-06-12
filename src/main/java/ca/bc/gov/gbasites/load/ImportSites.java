package ca.bc.gov.gbasites.load;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;

import javax.swing.BorderFactory;

import org.jeometry.common.data.identifier.Identifier;
import org.jeometry.common.logging.Logs;

import ca.bc.gov.gba.controller.GbaController;
import ca.bc.gov.gba.process.AbstractTaskByLocality;
import ca.bc.gov.gba.ui.StatisticsDialog;
import ca.bc.gov.gbasites.load.common.DirectorySuffixAndExtension;
import ca.bc.gov.gbasites.load.common.LoadEmergencyManagementSites;
import ca.bc.gov.gbasites.load.common.LoadProviderSitesIntoGba;
import ca.bc.gov.gbasites.load.common.ProviderSitePointConverter;
import ca.bc.gov.gbasites.load.common.SitePointProviderRecord;
import ca.bc.gov.gbasites.load.convert.SiteConverterAddress;
import ca.bc.gov.gbasites.load.convert.SiteConverterCKRD;
import ca.bc.gov.gbasites.load.convert.SiteConverterParts;
import ca.bc.gov.gbasites.load.provider.addressbc.AddressBcSiteConverter;
import ca.bc.gov.gbasites.load.provider.addressbc.AddressBcSplitByProvider;
import ca.bc.gov.gbasites.load.readsource.SourceReaderArcGis;
import ca.bc.gov.gbasites.load.readsource.SourceReaderFile;
import ca.bc.gov.gbasites.load.readsource.SourceReaderFileGdb;
import ca.bc.gov.gbasites.load.readsource.SourceReaderJoin;
import ca.bc.gov.gbasites.load.readsource.SourceReaderMapGuide;
import ca.bc.gov.gbasites.model.type.SitePoint;
import ca.bc.gov.gbasites.model.type.SiteTables;

import com.revolsys.collection.list.Lists;
import com.revolsys.collection.range.RangeSet;
import com.revolsys.geometry.index.PointRecordMap;
import com.revolsys.geometry.model.Geometry;
import com.revolsys.geometry.model.Point;
import com.revolsys.io.Reader;
import com.revolsys.io.file.Paths;
import com.revolsys.io.map.MapObjectFactoryRegistry;
import com.revolsys.parallel.process.ProcessNetwork;
import com.revolsys.record.Record;
import com.revolsys.record.Records;
import com.revolsys.record.code.CodeTable;
import com.revolsys.record.io.RecordReader;
import com.revolsys.record.schema.RecordStore;
import com.revolsys.spring.resource.ClassPathResource;
import com.revolsys.swing.SwingUtil;
import com.revolsys.swing.component.BasePanel;
import com.revolsys.swing.field.CheckBox;
import com.revolsys.swing.field.ComboBox;
import com.revolsys.swing.layout.GroupLayouts;
import com.revolsys.swing.table.counts.LabelCountMapTableModel;
import com.revolsys.transaction.Propagation;
import com.revolsys.transaction.Transaction;
import com.revolsys.util.Property;

public class ImportSites extends AbstractTaskByLocality implements SitePoint {
  private static final Comparator<Record> COMPARATOR_CUSTODIAN_SITE_ID = Record
    .newComparatorIdentifier(CUSTODIAN_SITE_ID);

  public static final ThreadLocal<Identifier> custodianPartnerOrgIdForThread = new ThreadLocal<>();

  public static StatisticsDialog dialog;

  public static final DirectorySuffixAndExtension ERROR_BY_PROVIDER = new DirectorySuffixAndExtension(
    "ErrorByProvider", "_ERROR", ".tsv");

  private static final List<String> FIELD_NAMES_CUSTODIAN_SITE_ID = Collections
    .singletonList(CUSTODIAN_SITE_ID);

  public static final Path LOCALITY_DIRECTORY = GbaController.getDataDirectory("Sites/Locality");

  public static final DirectorySuffixAndExtension NAME_ERROR_BY_PROVIDER = new DirectorySuffixAndExtension(
    "NameErrorByProvider", "_NAME_ERROR", ".xlsx");

  private static final long serialVersionUID = 1L;

  public static final DirectorySuffixAndExtension SITE_POINT_BY_PROVIDER = new DirectorySuffixAndExtension(
    "SitePointByProvider", "_SITE_POINT", ".tsv");

  public static final DirectorySuffixAndExtension SITE_POINT_BY_LOCALITY = new DirectorySuffixAndExtension(
    "SitePointByLocality", "_SITE_POINT", ".tsv");

  public static final Path SITES_DIRECTORY = GbaController.getDataDirectory("Sites");

  public static final Map<String, String> siteTypeByBuildingType = new LinkedHashMap<>();

  public static final DirectorySuffixAndExtension SOURCE_BY_PROVIDER = new DirectorySuffixAndExtension(
    "SourceByProvider", "_SOURCE", ".tsv");

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

  public static void initializeService() {

    MapObjectFactoryRegistry.newFactory("gbaSiteConverterAddress",
      SiteConverterAddress::newFactory);
    MapObjectFactoryRegistry.newFactory("gbaSiteConverterCKRD", SiteConverterCKRD::newFactory);
    MapObjectFactoryRegistry.newFactory("gbaSiteConverterParts", SiteConverterParts::newFactory);

    MapObjectFactoryRegistry.newFactory("gbaSiteLoader", ProviderSitePointConverter::new);

    MapObjectFactoryRegistry.newFactory("sourceReaderArcGis", SourceReaderArcGis::newFactory);
    MapObjectFactoryRegistry.newFactory("sourceReaderFile", SourceReaderFile::newFactory);
    MapObjectFactoryRegistry.newFactory("sourceReaderFileGdb", SourceReaderFileGdb::newFactory);
    MapObjectFactoryRegistry.newFactory("sourceReaderJoin", SourceReaderJoin::newFactory);
    MapObjectFactoryRegistry.newFactory("sourceReaderMapGuide", SourceReaderMapGuide::newFactory);
  }

  public static void main(final String[] args) {
    initializeService();
    start(ImportSites.class);
  }

  private final CheckBox addressBcConvertCheckbox = new CheckBox("addressBcConvert", true);

  private final CheckBox addressBcDownloadCheckbox = new CheckBox("addressBcDownload", true);

  private final CheckBox addressBcSplitCheckbox = new CheckBox("addressBcSplit", true);

  private final List<ProviderSitePointConverter> dataProvidersToProcess = Collections
    .synchronizedList(new LinkedList<>());

  private CheckBox loadSitesGdb;

  private final Map<String, Identifier> partnerOrganizationIdByShortName = new HashMap<>();

  private ComboBox<ProviderSitePointConverter> providerComboBox;

  private final CheckBox providerConvertCheckbox = new CheckBox("providerConvertCheckbox", true);

  private final CheckBox providerDownloadCheckbox = new CheckBox("providerDownload", true);

  private final int threadCount = 10;

  public ImportSites() {
    super("Import Sites");
    dialog = this;
    setLockName(null);
    setUseTotalCounts(false);
    setLogChanges(false);

    ProviderSitePointConverter.init();
  }

  private void action1Download() {
    final Consumer<ProviderSitePointConverter> providerAction = converter -> {
      final boolean downloadData = this.providerDownloadCheckbox.isSelected();
      converter.downloadData(this, downloadData);
    };

    final Runnable addressBcRunnable = () -> {
      final boolean download = this.addressBcDownloadCheckbox.isSelected();
      final boolean split = this.addressBcSplitCheckbox.isSelected();
      AddressBcSplitByProvider.split(this, download, split);
    };

    newConverterProcessNetwork(providerAction, "Download and Split Address BC", addressBcRunnable);
  }

  private void action2Convert() {
    final Consumer<ProviderSitePointConverter> providerAction = converter -> {
      final boolean convert = this.providerConvertCheckbox.isSelected();
      converter.convertData(this, convert);
    };

    final Runnable addressBcRunnable = () -> {
      final boolean convert = this.addressBcConvertCheckbox.isSelected();
      AddressBcSiteConverter.convertAll(this, convert);
    };

    try {
      ProviderSitePointConverter.preProcess();
      newConverterProcessNetwork(providerAction, "Convert Address BC", addressBcRunnable);
    } finally {
      ProviderSitePointConverter.postProcess(this.dataProvidersToProcess);
    }
  }

  private void addConverterProcesses(final ProcessNetwork processes,
    final Consumer<ProviderSitePointConverter> action) {
    if (!this.dataProvidersToProcess.isEmpty()) {
      final List<ProviderSitePointConverter> converters = Lists.toList(LinkedList::new,
        this.dataProvidersToProcess);
      for (int i = 0; i < this.threadCount; i++) {
        processes.addProcess("Provider " + i, () -> {
          while (!isCancelled()) {
            try {
              final ProviderSitePointConverter converter = converters.remove(0);
              action.accept(converter);
            } catch (final Throwable e) {
              return;
            }
          }
        });
      }
    }
  }

  @Override
  protected boolean batchUpdate(final Transaction transaction) {
    loadFeatureStatusCodes();
    loadSiteLocationCodes();
    loadSiteTypeCodes();

    action1Download();
    action2Convert();

    initPartnerOrganizationShortNames();
    if (!isCancelled()) {
      {
        final Path providerCountsPath = SITES_DIRECTORY.resolve("PROVIDER_COUNTS.xlsx");
        final LabelCountMapTableModel providerCounts = this.getLabelCountTableModel("Provider");
        providerCounts.writeCounts(providerCountsPath);
      }

      if (isHasLocalitiesToProcess()) {
        // setSelectedTab(ProviderSitePointConverter.LOCALITY);
        super.batchUpdate(transaction);
        if (isProcessAllLocalities()) {
          LoadProviderSitesIntoGba.writeNoSites();
        }
      }
    }
    if (!isCancelled() && this.loadSitesGdb.isSelected()) {
      final LoadEmergencyManagementSites loadEmergencyManagementSites = new LoadEmergencyManagementSites(
        this);
      loadEmergencyManagementSites.run();
    }
    return !isCancelled();
  }

  public Identifier getPartnerOrganizationByShortName(final String shortName) {
    return this.partnerOrganizationIdByShortName.get(shortName);
  }

  private void initPartnerOrganizationShortNames() {
    final CodeTable partnerOrganizations = GbaController.getPartnerOrganizations();
    for (final Identifier partnerOrganizationId : partnerOrganizations.getIdentifiers()) {
      final String partnerOrganizationName = partnerOrganizations.getValue(partnerOrganizationId);
      String shortName = null;
      if (partnerOrganizationName.startsWith("Locality - ")) {
        shortName = partnerOrganizationName.substring(11);
      } else if (partnerOrganizationName.startsWith("Regional District - ")) {
        shortName = partnerOrganizationName.substring(20);
      } else if (partnerOrganizationName.startsWith("Provider - ")) {
        shortName = partnerOrganizationName.substring(11);
      } else {
        shortName = partnerOrganizationName;
      }
      if (shortName != null) {
        shortName = shortName.replace(' ', '_').toUpperCase();
        this.partnerOrganizationIdByShortName.put(shortName, partnerOrganizationId);
      }
    }
  }

  private void loadFeatureStatusCodes() {
    final RecordStore gbaRecordStore = GbaController.getUserRecordStore();
    try (
      Transaction transaction = gbaRecordStore.newTransaction(Propagation.REQUIRES_NEW);
      Reader<Record> reader = RecordReader.newRecordReader(
        new ClassPathResource("/ca/bc/gov/gba/schema/codes/FEATURE_STATUS_CODE.tsv"))) {
      for (final Record record : reader) {
        final String code = record.getString("FEATURE_STATUS_CODE");
        final String description = record.getString("DESCRIPTION");
        if (gbaRecordStore.getRecord(SiteTables.FEATURE_STATUS_CODE, code) == null) {
          final Record codeRecord = gbaRecordStore.newRecord(SiteTables.FEATURE_STATUS_CODE);
          codeRecord.setIdentifier(Identifier.newIdentifier(code));
          codeRecord.setValue("DESCRIPTION", description);
          final Identifier integrationSessionId = getIntegrationSessionId();
          codeRecord.setValue(CREATE_INTEGRATION_SESSION_ID, integrationSessionId);
          codeRecord.setValue(MODIFY_INTEGRATION_SESSION_ID, integrationSessionId);
          gbaRecordStore.insertRecord(codeRecord);
        }
      }
    }
  }

  private void loadSiteLocationCodes() {
    final RecordStore gbaRecordStore = GbaController.getUserRecordStore();
    try (
      Transaction transaction = gbaRecordStore.newTransaction(Propagation.REQUIRES_NEW);
      Reader<Record> reader = RecordReader.newRecordReader(
        new ClassPathResource("/ca/bc/gov/gba/schema/codes/SITE_LOCATION_CODE.tsv"))) {
      for (final Record record : reader) {
        final String code = record.getString("SITE_LOCATION_CODE");
        final String description = record.getString("DESCRIPTION");
        if (gbaRecordStore.getRecord(SiteTables.SITE_LOCATION_CODE, code) == null) {
          final Record codeRecord = gbaRecordStore.newRecord(SiteTables.SITE_LOCATION_CODE);
          codeRecord.setIdentifier(Identifier.newIdentifier(code));
          codeRecord.setValue("DESCRIPTION", description);
          final Identifier integrationSessionId = getIntegrationSessionId();
          codeRecord.setValue(CREATE_INTEGRATION_SESSION_ID, integrationSessionId);
          codeRecord.setValue(MODIFY_INTEGRATION_SESSION_ID, integrationSessionId);
          gbaRecordStore.insertRecord(codeRecord);
        }
      }
    }
  }

  private void loadSiteTypeCodes() {
    final RecordStore gbaRecordStore = GbaController.getUserRecordStore();
    try (
      Transaction transaction = gbaRecordStore.newTransaction(Propagation.REQUIRES_NEW);
      Reader<Record> reader = RecordReader
        .newRecordReader(new ClassPathResource("/ca/bc/gov/gba/schema/codes/SITE_TYPE_CODE.tsv"))) {
      for (final Record record : reader) {
        final String code = record.getString(SITE_TYPE_CODE);
        final String description = record.getString("DESCRIPTION");
        if (gbaRecordStore.getRecord(SiteTables.SITE_TYPE_CODE, code) == null) {
          final Record codeRecord = gbaRecordStore.newRecord(SiteTables.SITE_TYPE_CODE);
          codeRecord.setIdentifier(Identifier.newIdentifier(code));
          codeRecord.setValue("DESCRIPTION", description);
          final Identifier integrationSessionId = getIntegrationSessionId();
          codeRecord.setValue(CREATE_INTEGRATION_SESSION_ID, integrationSessionId);
          codeRecord.setValue(MODIFY_INTEGRATION_SESSION_ID, integrationSessionId);
          gbaRecordStore.insertRecord(codeRecord);
        }
        final String plGroup = record.getString("PL_GROUP");
        if (Property.hasValue(plGroup)) {
          final String plType = record.getString("PL_TYPE");
          for (final String type : plType.split(",")) {
            siteTypeByBuildingType.put(plGroup + "-" + type, code);
          }
        }
      }
    }
  }

  private void mergeDuplicates() {

    final Comparator<SitePointProviderRecord> SUFFIX_UNIT_COMPARATOR = (a, b) -> {
      final String suffix1 = a.getString(CIVIC_NUMBER_SUFFIX, "");
      final String suffix2 = b.getString(CIVIC_NUMBER_SUFFIX, "");
      int compare = suffix1.compareTo(suffix2);
      if (compare == 0) {
        final RangeSet descriptor1 = a.getUnitDescriptorRanges();
        final RangeSet descriptor2 = b.getUnitDescriptorRanges();
        compare = descriptor1.compareTo(descriptor2);
        if (compare == 0) {
          final Point point1 = a.getGeometry();
          final Point point2 = b.getGeometry();
          compare = point1.compareTo(point2);
        }
      }
      return compare;
    };
    final Map<String, Map<Integer, List<SitePointProviderRecord>>> sitesByStreetAddress = new TreeMap<>();
    for (final Map<Integer, List<SitePointProviderRecord>> sitesByCivicNumber : cancellable(
      sitesByStreetAddress.values())) {
      for (final List<SitePointProviderRecord> sites : cancellable(sitesByCivicNumber.values())) {
        sites.sort(SUFFIX_UNIT_COMPARATOR);
        mergeDuplicates(sites);
        // for (final Record record : sites) {
        // writeSitePoint(record);
        // }
      }
    }
  }

  private void mergeDuplicates(final List<SitePointProviderRecord> sites) {
    // final int recordCount = sites.size();
    // if (recordCount > 1) {
    // for (int i = 0; i < sites.size() - 1; i++) {
    // final SitePointProviderRecord site1 = sites.get(i);
    // for (int j = sites.size() - 1; j > i; j--) {
    // String custodianAddress1 = site1.getString(CUSTODIAN_FULL_ADDRESS);
    // final Point point1 = site1.getGeometry();
    // final String suffix1 = site1.getString(CIVIC_NUMBER_SUFFIX, "");
    // final RangeSet range1 = site1.getUnitDescriptorRanges();
    //
    // final SitePointProviderRecord site2 = sites.get(j);
    // String custodianAddress2 = site2.getString(CUSTODIAN_FULL_ADDRESS);
    // final Point point2 = site2.getGeometry();
    // final String suffix2 = site2.getString(CIVIC_NUMBER_SUFFIX, "");
    // final RangeSet range2 = site2.getUnitDescriptorRanges();
    // if (point1.isWithinDistance(point2, 1)) {
    // if (site1.equalValuesExclude(site2,
    // Arrays.asList(CUSTODIAN_FULL_ADDRESS, GEOMETRY, POSTAL_CODE))) {
    // final SitePointProviderRecord duplicateSite = sites.remove(j);
    // addWarning(duplicateSite, "Duplicate");
    // if (!site1.hasValue(POSTAL_CODE)) {
    // site1.setValue(site2, POSTAL_CODE);
    // }
    // } else {
    // boolean mergeSites = false;
    // if (suffix1.equals(suffix2)) {
    // if (range1.equals(range2)) {
    // Debug.noOp();
    // } else if (range1.isEmpty()) {
    // if (custodianAddress2.endsWith(custodianAddress1)) {
    // site1.setValue(CUSTODIAN_FULL_ADDRESS, custodianAddress2);
    // } else {
    // addError(site1, "Merge CUSTODIAN_FULL_ADDRESS different");
    // addError(site2, "Merge CUSTODIAN_FULL_ADDRESS different");
    // }
    // site1.setUnitDescriptor(range2);
    // mergeSites = true;
    // } else if (range2.isEmpty()) {
    // Logs.error(this, "Not expecting range2 to be empty");
    // } else {
    // final RangeSet newRange = new RangeSet();
    // newRange.addRanges(range1);
    // newRange.addRanges(range2);
    // site1.setUnitDescriptor(newRange);
    // custodianAddress1 = removeUnitFromAddress(custodianAddress1, range1);
    // custodianAddress2 = removeUnitFromAddress(custodianAddress2, range2);
    // if (custodianAddress1.equals(custodianAddress2)) {
    // final String custodianAddress =
    // SitePoint.getSimplifiedUnitDescriptor(newRange)
    // + " " + custodianAddress1;
    // site1.setValue(CUSTODIAN_FULL_ADDRESS, custodianAddress);
    // } else {
    // addError(site1, "Merge CUSTODIAN_FULL_ADDRESS different");
    // addError(site2, "Merge CUSTODIAN_FULL_ADDRESS different");
    // }
    // mergeSites = true;
    // }
    // } else {
    // Debug.noOp();
    // }
    // if (mergeSites) {
    // SitePoint.updateFullAddress(site1);
    // sites.remove(j);
    // addWarning(site2, "Merged UNIT_DESCRIPTOR");
    // if (!site1.hasValue(POSTAL_CODE)) {
    // site1.setValue(site2, POSTAL_CODE);
    // }
    // }
    // }
    // }
    // }
    // }
    // }
  }

  private void newConverterProcessNetwork(final Consumer<ProviderSitePointConverter> action,
    final String addressBcProcessName, final Runnable addressBcRunnable) {
    final ProcessNetwork processes = new ProcessNetwork();
    addConverterProcesses(processes, action);
    processes.addProcess(addressBcProcessName, addressBcRunnable);
    setSelectedTab("Provider");
    processes.startAndWait();
  }

  @Override
  protected Consumer<Identifier> newLocalityHandler() {
    return new LoadProviderSitesIntoGba(this);
  }

  @Override
  protected BasePanel newPanelOptions() {
    return new BasePanel(//
      newPanelOptionsAddressBc(), //
      newPanelOptionsProvider(), //
      newPanelOptionsLocalities() //
    );
  }

  private BasePanel newPanelOptionsAddressBc() {

    final BasePanel downloadPanel = new BasePanel(//
      SwingUtil.newLabel("Download"), //
      this.addressBcDownloadCheckbox, //
      SwingUtil.newLabel("Split"), //
      this.addressBcSplitCheckbox, //
      SwingUtil.newLabel("Convert"), //
      this.addressBcConvertCheckbox //
    );
    downloadPanel.setBorder(BorderFactory.createTitledBorder("Address BC"));
    GroupLayouts.makeColumns(downloadPanel, 6, true);
    return downloadPanel;
  }

  private BasePanel newPanelOptionsLocalities() {
    this.loadSitesGdb = new CheckBox("loadSitesGdb", false);

    final BasePanel optionsPanel = super.newPanelOptions();
    optionsPanel.add(SwingUtil.newLabel("Load sites.gdb"));
    optionsPanel.add(this.loadSitesGdb);
    GroupLayouts.makeColumns(optionsPanel, 2, true);
    optionsPanel.setBorder(BorderFactory.createTitledBorder("Import Sites into GBA Database"));
    return optionsPanel;
  }

  private BasePanel newPanelOptionsProvider() {
    final List<ProviderSitePointConverter> dataProviders = new ArrayList<>();
    dataProviders.add(null);
    dataProviders.addAll(ProviderSitePointConverter.getLoaders());
    this.providerComboBox = ComboBox.newComboBox("dataProvider", dataProviders);

    final BasePanel downloadPanel = new BasePanel(//
      SwingUtil.newLabel("Data Provider"), //
      this.providerComboBox, //
      SwingUtil.newLabel("Download"), //
      this.providerDownloadCheckbox, //
      SwingUtil.newLabel("Convert"), //
      this.providerConvertCheckbox //
    );
    downloadPanel.setBorder(BorderFactory.createTitledBorder("Provider: Download and Convert"));
    GroupLayouts.makeColumns(downloadPanel, 2, true);
    return downloadPanel;
  }

  @Override
  protected void preUpdateRecord(final Record record) {
    final Identifier custodianPartnerOrgId = custodianPartnerOrgIdForThread.get();
    if (custodianPartnerOrgId != null) {
      if (record.equalValue(CUSTODIAN_PARTNER_ORG_ID, custodianPartnerOrgId)) {
        record.setValue(CUSTODIAN_SESSION_ID, getIntegrationSessionId());
      }
    }
  }

  @Override
  protected void setOptions(final BasePanel optionsPanel) {
    super.setOptions(optionsPanel);
    final ProviderSitePointConverter dataProviderToProcess = this.providerComboBox
      .getSelectedItem();

    if (dataProviderToProcess == null) {
      this.dataProvidersToProcess.addAll(ProviderSitePointConverter.getLoaders());
    } else {
      this.dataProvidersToProcess.add(dataProviderToProcess);
    }
    final boolean hasProviders = !this.dataProvidersToProcess.isEmpty();
    newLabelCountTableModel("Provider", //
      "Data Provider", //
      "P Source", //
      "ABC Source", //
      "P Converted", //
      "ABC Converted", //
      "P Ignored", //
      "ABC Ignored", //
      "P Error", //
      "ABC Error", //
      "P Warning", //
      "ABC Warning" //
    );
    if (hasProviders) {
      if (isHasLocalitiesToProcess()) {
        newLabelCountTableModel(ProviderSitePointConverter.LOCALITY, //
          ProviderSitePointConverter.LOCALITY, //
          "P Convert", //
          GBA_READ, //
          LoadProviderSitesIntoGba.PROVIDER_READ, //
          LoadProviderSitesIntoGba.MATCHED, //
          INSERTED, //
          UPDATED, //
          LoadProviderSitesIntoGba.TO_DELETE //
        );
      } else {
        newLabelCountTableModel(ProviderSitePointConverter.LOCALITY, //
          ProviderSitePointConverter.LOCALITY, //
          "P Convert" //
        );
      }
      for (final String localityName : getLocalityNamesToProcess()) {
        newLabelCount(ProviderSitePointConverter.LOCALITY, localityName, "P Convert");
      }
    } else if (isHasLocalitiesToProcess()) {
      newLabelCountTableModel(ProviderSitePointConverter.LOCALITY, //
        ProviderSitePointConverter.LOCALITY, //
        GBA_READ, //
        LoadProviderSitesIntoGba.PROVIDER_READ, //
        LoadProviderSitesIntoGba.MATCHED, //
        INSERTED, //
        UPDATED, //
        LoadProviderSitesIntoGba.TO_DELETE //
      );
      for (final String localityName : getLocalityNamesToProcess()) {
        newLabelCount(ProviderSitePointConverter.LOCALITY, localityName,
          LoadProviderSitesIntoGba.PROVIDER_READ);
      }
    }
    if (this.loadSitesGdb.isSelected()) {
      newLabelCountTableModel(LoadEmergencyManagementSites.EM_SITES, "Type Name", GBA_READ,
        LoadProviderSitesIntoGba.PROVIDER_READ, LoadEmergencyManagementSites.IGNORE_XCOVER,
        INSERTED, UPDATED);
    }
    if (hasProviders) {
      newLabelCountTableModel(ERROR, //
        "Message", //
        ERROR//
      );

      newLabelCountTableModel(ProviderSitePointConverter.NAME_ERRORS, //
        "Message", //
        ERROR//
      );

      newLabelCountTableModel(ProviderSitePointConverter.WARNING, "Message", //
        ProviderSitePointConverter.WARNING);
    }
  }

  private void tagDuplicatesWithSameFullAddress() {
    final Map<String, List<Record>> sitesByFullAddress = new HashMap<>();
    for (final List<Record> sitesWithSameFullAddress : cancellable(sitesByFullAddress.values())) {
      sitesWithSameFullAddress.sort(COMPARATOR_CUSTODIAN_SITE_ID);
      // Remove duplicate records with the same point location
      if (sitesWithSameFullAddress.size() > 1) {
        final PointRecordMap sitesByPoint = new PointRecordMap(sitesWithSameFullAddress);
        for (final Point point : sitesByPoint.getKeys()) {
          final List<Record> sitesWithSamePoint = sitesByPoint.getRecords(point);
          sitesWithSamePoint.sort(COMPARATOR_CUSTODIAN_SITE_ID);
          if (sitesWithSamePoint.size() > 1) {
            for (int i = 0; i < sitesWithSamePoint.size(); i++) {
              final Record site1 = sitesWithSamePoint.get(i);
              for (int j = i + 1; j < sitesWithSamePoint.size();) {
                final Record site2 = sitesWithSamePoint.get(j);
                final List<String> differentFieldNames = site1.getDifferentFieldNames(site2);
                boolean remove = false;
                if (differentFieldNames.isEmpty()) {
                  remove = true;
                } else {
                  if (differentFieldNames.equals(FIELD_NAMES_CUSTODIAN_SITE_ID)) {
                    // Keep the lowest ID, values already sorted.
                    remove = true;
                  }
                }
                if (remove) {
                  sitesWithSamePoint.remove(j);
                  sitesWithSameFullAddress.remove(site2);
                  // addError(site2, "Ignore duplicate FULL_ADDRESS and point");
                } else {
                  j++;
                }
              }
            }
          }
        }
      }

      Record closetRecord = null;
      double closestDistance = Double.MAX_VALUE;
      if (sitesWithSameFullAddress.size() > 1) {
        final Geometry allGeometries = Records.getGeometry(sitesWithSameFullAddress);
        final Point centroid = allGeometries.getCentroid();
        for (final Record record : cancellable(sitesWithSameFullAddress)) {
          final Point point = record.getGeometry();
          final double distance = point.distancePoint(centroid);
          if (distance < closestDistance) {
            closetRecord = record;
            closestDistance = distance;
          }
        }
      }

      for (final Record site : cancellable(sitesWithSameFullAddress)) {
        if (!(site == closetRecord || closetRecord == null)) {
          site.setValue(USE_IN_ADDRESS_RANGE_IND, "N");
        }
      }

      // localityWriter.write(site);

    }
  }

}
