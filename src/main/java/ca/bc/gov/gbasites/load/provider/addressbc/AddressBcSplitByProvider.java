package ca.bc.gov.gbasites.load.provider.addressbc;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jeometry.common.data.type.DataTypes;
import org.jeometry.common.exception.Exceptions;
import org.jeometry.common.logging.Logs;

import ca.bc.gov.gba.controller.GbaConfig;
import ca.bc.gov.gba.controller.GbaController;
import ca.bc.gov.gba.model.Gba;
import ca.bc.gov.gba.model.type.code.PartnerOrganization;
import ca.bc.gov.gba.ui.StatisticsDialog;
import ca.bc.gov.gbasites.controller.GbaSiteDatabase;
import ca.bc.gov.gbasites.load.ImportSites;
import ca.bc.gov.gbasites.load.common.PartnerOrganizationFiles;
import ca.bc.gov.gbasites.load.common.ProviderSitePointConverter;
import ca.bc.gov.gbasites.load.common.SplitByProviderWriter;
import ca.bc.gov.gbasites.model.type.SitePoint;

import com.revolsys.collection.map.LinkedHashMapEx;
import com.revolsys.collection.map.MapEx;
import com.revolsys.collection.range.RangeSet;
import com.revolsys.collection.set.Sets;
import com.revolsys.io.Reader;
import com.revolsys.io.ZipUtil;
import com.revolsys.io.file.Paths;
import com.revolsys.record.Record;
import com.revolsys.record.io.RecordReader;
import com.revolsys.record.io.format.tsv.Tsv;
import com.revolsys.record.io.format.tsv.TsvWriter;
import com.revolsys.record.schema.RecordDefinitionImpl;
import com.revolsys.spring.resource.UrlResource;
import com.revolsys.util.Cancellable;
import com.revolsys.util.CaseConverter;
import com.revolsys.util.Counter;
import com.revolsys.util.Property;

public class AddressBcSplitByProvider implements Cancellable, SitePoint {

  private static final String STATISTICS_NAME = AddressBC.COUNT_PREFIX + "Source";

  public static void downloadAddressBc(final Path sourceDirectory) {
    final String url = "ftp://geoshare.icisociety.ca/Addresses/ABC.csv.zip";
    try {
      final String user = GbaConfig.getProperty("addressBcFtpUser");
      final String password = GbaConfig.getProperty("addressBcFtpPassword");
      final UrlResource resource = new UrlResource(url, user, password);

      Paths.createDirectories(sourceDirectory);
      ZipUtil.unzipFile(resource, sourceDirectory);
    } catch (final Exception e) {
      throw Exceptions.wrap("Error downloading: " + url, e);
    }
  }

  public static void split(final StatisticsDialog dialog, final boolean download,
    final boolean split) {
    final Path sourceDirectory = AddressBC.DIRECTORY //
      .resolve("Source") //
    ;

    if (!dialog.isCancelled() && (download || !Files.exists(sourceDirectory))) {
      downloadAddressBc(sourceDirectory);
    }
    if (split) {
      new AddressBcSplitByProvider(dialog, AddressBC.DIRECTORY).run();
    }
  }

  protected final Path sourceDirectory;

  private final Map<String, RangeSet> unitDescriptorsById = new HashMap<>();

  private final Map<String, String> buildingNameById = new HashMap<>();

  private final Map<String, String> buildingTypeById = new HashMap<>();

  private final Map<String, String> postalCodeById = new HashMap<>();

  private final StatisticsDialog dialog;

  private final Map<String, SplitByProviderWriter> writerByProvider = new HashMap<>();

  private final List<SplitByProviderWriter> writers = new ArrayList<>();

  private TsvWriter extraDataWriter;

  private RecordDefinitionImpl recordDefinition;

  private final Path directory;

  protected String statisticsName = STATISTICS_NAME;

  private final Path baseDirectory;

  protected String fileSuffix = AddressBC.FILE_SUFFIX;

  protected String orgFileName = "ADDRESS_BC_PARTNER_ORGANIZATION.xlsx";

  public AddressBcSplitByProvider(final StatisticsDialog dialog, final Path baseDirectory) {
    this.dialog = dialog;
    this.baseDirectory = baseDirectory;
    this.directory = ImportSites.SOURCE_BY_PROVIDER.newDirectoryPath(baseDirectory);
    this.sourceDirectory = baseDirectory.resolve("Source");
  }

  protected void addFields(final RecordDefinitionImpl recordDefinitionImpl) {
    recordDefinitionImpl.addField(UNIT_DESCRIPTOR, DataTypes.STRING);
    recordDefinitionImpl.addField(AddressBC.BUILDING_NAME, DataTypes.STRING);
    recordDefinitionImpl.addField(AddressBC.BUILDING_TYPE, DataTypes.STRING);
    recordDefinitionImpl.addField(AddressBC.POSTAL_CODE, DataTypes.STRING);
  }

  private void addWriter(final String dataProvider, final SplitByProviderWriter providerWriter) {
    this.writerByProvider.put(dataProvider, providerWriter);
    this.writerByProvider.put(dataProvider.toUpperCase(), providerWriter);
  }

  private SplitByProviderWriter getWriter(final Record record, final String issuingAgency) {
    SplitByProviderWriter writer = this.writerByProvider.get(issuingAgency);
    if (writer == null) {
      writer = this.writerByProvider.get(issuingAgency.toUpperCase());
      if (writer == null) {
        final String dataProviderWords = CaseConverter.toCapitalizedWords(issuingAgency);
        final PartnerOrganization partnerOrganization = GbaSiteDatabase
          .newPartnerOrganization(dataProviderWords);
        final String partnerOrganizationName = partnerOrganization.getPartnerOrganizationName();

        final String dataProviderName = partnerOrganizationName
          .substring(partnerOrganizationName.indexOf('-') + 2);
        writer = newProviderWriter(dataProviderName);
        addWriter(issuingAgency, writer);
      } else {
        this.writerByProvider.put(issuingAgency.toUpperCase(), writer);
      }
    }
    return writer;
  }

  @Override
  public boolean isCancelled() {
    return this.dialog.isCancelled();
  }

  private void loadConfig() {
    final Path file = ProviderSitePointConverter.SITE_CONFIG_DIRECTORY.resolve(this.orgFileName);
    try (
      RecordReader reader = RecordReader.newRecordReader(file)) {
      for (final Record record : reader) {
        final String issuingAgency = record.getString("Issuing Agency");
        if (Property.hasValue(issuingAgency)) {
          final String locality = record.getString("Locality");
          final String regionalDistrict = record.getString("Regional District");
          final String provider = record.getString("Provider");
          final String other = record.getString("Other");
          String providerName = null;
          if (Property.hasValue(locality)) {
            if (Property.hasValue(regionalDistrict) || Property.hasValue(provider)) {
              Logs.error(AddressBcSplitByProvider.class,
                "Cannot have Locality, Regional District and Provider\n" + record);
            } else if (GbaController.getLocalities().getIdentifier(locality) == null) {
              Logs.error(AddressBcSplitByProvider.class, "Locality not found " + locality);
            } else {
              providerName = locality;
            }
          } else if (Property.hasValue(regionalDistrict)) {
            if (Property.hasValue(provider)) {
              Logs.error(AddressBcSplitByProvider.class,
                "Cannot have Regional District and  Provider\n" + record);
            } else if (GbaController.getRegionalDistricts()
              .getIdentifier(regionalDistrict) == null) {
              Logs.error(AddressBcSplitByProvider.class,
                "Regional District not found " + regionalDistrict);
            } else {
              providerName = regionalDistrict;
            }
          } else if (Property.hasValue(provider)) {
            providerName = provider;
          } else if (Property.hasValue(other)) {
            providerName = other;
          } else {
            Logs.error(AddressBcSplitByProvider.class,
              "Must have one of Locality, Regional District, Provider, or Other\n" + record);
          }
          if (providerName != null) {
            SplitByProviderWriter writer = this.writerByProvider.get(providerName.toUpperCase());
            if (writer == null) {
              writer = newProviderWriter(providerName);
            }
            if (!issuingAgency.equalsIgnoreCase(providerName)) {
              addWriter(issuingAgency, writer);
            }
          }
        }
      }
    }
  }

  private void loadExtendedAddress() {
    final Set<String> newBuildingTypes = new TreeSet<>();
    final MapEx buildingTypeMap = new LinkedHashMapEx() //
      .add("CHURCH", "Civic Chruch") //
      .add("COMMERCIAL", "Commercial Commercial") //
      .add("COMMUNITY", "Civic Community Hall") //
      .add("Commercial Commercial", "Commercial Commercial") //
      .add("FARM BUILD", "Farm Building") //
      .add("FIRE", "Fire Hall") //
      .add("GOLF", "Recreation Golf") //
      .add("GOVERNMENT", "Civic Government") //
      .add("Hospital Care", "Hospital Care") //
      .add("Hotel Hostel", "Hotel Hostel") //
      .add("INDUSTRIAL", "Commercial Industrial") //
      .add("LIBRARY", "Civic Library") //
      .add("MOSQUE", "Civic Mosque") //
      .add("MULTI FAMI", "Residential Condo") //
      .add("MUNICIPAL", "Government Municipal") //
      .add("MUSEUM", "Civic Museum") //
      .add("PARK BUILD", "Park Building") //
      .add("RECREATION", "Recreation Recreation") //
      .add("RECYCLING", "Civic Recycling") //
      .add("Residential Condo", "Residential Condo") //
      .add("SCHOOL", "School Standard") //
      .add("SINGLE FAM", "Residential Single Family") //
      .add("SKI", "Recreation Ski") //
      .add("STABLES", "Farm Stables") //
    ;
    final Set<String> ignoreBuildingTypes = Sets.newHash("MIXED USE", "OTHER");

    final Pattern postalCodePattern = Pattern.compile("[A-Z]\\d[A-Z] ?\\d[A-Z]\\d");

    final List<String> ignoreAddressNotes = Arrays.asList("LSCR", "PC NOT ON CANADAPOST YET",
      "CHECK POSTAL CODE", "CHECK PC", "CHECK PC - BOGUS", "<Null>",
      "CHECK POSTAL CODE - RESIDENTIAL", "CHECK POSTAL CODE - COMMERCIAL", "Bldg plan 127 suites",
      "97 unit rental building", "check pc b/c new units", "CHECK PC - commercial unit");

    final Path file = this.sourceDirectory.resolve("ABC_EXTENDED_ADDRESS.csv");
    try (
      Reader<Record> reader = RecordReader.newRecordReader(file)) {
      for (final Record record : reader) {
        final String civicId = record.getValue(AddressBC.CIVIC_ID);

        String postalCode = record.getValue(AddressBC.POSTAL_CODE);
        if (Property.hasValue(postalCode)) {
          postalCode = postalCode.toUpperCase();
          if (postalCodePattern.matcher(postalCode).matches()) {
            if (postalCode.length() == 6) {
              postalCode = postalCode.substring(0, 3) + " " + postalCode.substring(3);
            }
            this.postalCodeById.put(civicId, postalCode);
          } else {
            writeExtraDataWarning("ABC_EXTENDED_ADDRESS.csv", civicId, AddressBC.POSTAL_CODE,
              postalCode);
          }
        }

        String buildingType = record.getValue(AddressBC.BUILDING_TYPE);
        if (!Property.hasValue(buildingType)) {
          final String addressNotes = record.getValue("ADDRESS_NOTES");
          if (Property.hasValue(addressNotes)) {
            if (addressNotes.startsWith("Hostel")) {
              buildingType = "Hotel Hostel";
            } else if (addressNotes.equalsIgnoreCase("commercial unit")) {
              buildingType = "Commercial Commercial";
            } else if (addressNotes.equalsIgnoreCase("Commercial")) {
              buildingType = "Commercial Commercial";
            } else if (addressNotes.equalsIgnoreCase("residential unit")) {
              buildingType = "Residential Condo";
            } else if (addressNotes.equalsIgnoreCase("Care Facility")) {
              buildingType = "Hospital Care";
            } else if (!ignoreAddressNotes.contains(addressNotes)) {
              System.err.println(addressNotes);
            }
          }

        }
        if (buildingType != null && !ignoreBuildingTypes.contains(buildingType)) {
          String code = buildingTypeMap.getString(buildingType);
          if (code == null) {
            code = buildingTypeMap.getString(buildingType.toUpperCase().trim());
          }

          if (code == null) {
            newBuildingTypes.add(buildingType.toUpperCase());
          } else {
            this.buildingTypeById.put(civicId, buildingType);

          }
        }
        final String buildingName = record.getValue(AddressBC.BUILDING_NAME);
        if (Property.hasValue(buildingName)) {
          this.buildingNameById.put(civicId, buildingName);
        }

      }
    }
    for (final String buildingType : newBuildingTypes) {
      writeExtraDataWarning("ABC_EXTENDED_ADDRESS.csv", null, AddressBC.BUILDING_TYPE,
        buildingType);
    }
  }

  private void loadSubAddress() {
    final Pattern unitWithSuffixOrPrefixPattern = Pattern
      .compile("(?:\\d+[A-Z]|[A-Z]\\d+|LC\\d+|L \\d+|A-\\d)");
    final Pattern numberRangePattern = Pattern.compile("(\\d+) ?- ?(\\d+)");

    final Set<String> ignoreUnitDescriptors = Sets.newHash("DOCK");
    final Set<String> ignoreUnitNumberSuffixes = Sets.newHash("MH", "BH");
    final Path file = this.sourceDirectory.resolve("ABC_SUB_ADDRESS.csv");
    try (
      Reader<Record> reader = RecordReader.newRecordReader(file)) {
      for (final Record record : reader) {
        final String civicId = record.getValue(AddressBC.CIVIC_ID);
        final String unitNumber = record.getString(AddressBC.UNIT_NUMBER, "").trim();
        final String unitNumberSuffix = record.getString(AddressBC.UNIT_NUMBER_SUFFIX, "").trim();
        String unitDescriptor;
        if (ignoreUnitNumberSuffixes.contains(unitNumberSuffix) || unitNumberSuffix.length() == 0) {
          unitDescriptor = unitNumber;
        } else if (unitNumber.length() == 0) {
          unitDescriptor = unitNumberSuffix;
        } else {
          unitDescriptor = unitNumber + unitNumberSuffix;
        }

        if (Property.hasValue(unitDescriptor) && !ignoreUnitDescriptors.contains(unitDescriptor)) {
          if (unitDescriptor.startsWith("#")) {
            unitDescriptor = unitDescriptor.substring(1);
          }
          if (unitDescriptor.startsWith("-")) {
            unitDescriptor = unitDescriptor.substring(1);
          }
          if (unitDescriptor.endsWith("-")) {
            unitDescriptor = unitDescriptor.substring(0, unitDescriptor.length() - 1);
          }
          if (unitDescriptor.endsWith("?")) {
            unitDescriptor = unitDescriptor.substring(0, unitDescriptor.length() - 1);
          }
          if (unitDescriptor.endsWith("-FLR")) {
            unitDescriptor = unitDescriptor.substring(0, unitDescriptor.length() - 4);
          }
          if (unitDescriptor.equals("41BH")) {
            unitDescriptor = "41";
          }
          if (unitDescriptor.startsWith("BUILDING ")) {
            unitDescriptor = unitDescriptor.substring(9);
          }

          RangeSet range = this.unitDescriptorsById.get(civicId);
          if (range == null) {
            range = new RangeSet();
            this.unitDescriptorsById.put(civicId, range);
          }
          if (unitDescriptor.endsWith(" - 3994")) {
            range.add(Integer.parseInt(unitDescriptor.substring(0, unitDescriptor.length() - 7)));
          } else if ("17MN".equals(unitDescriptor)) {
            range.add(17);
          } else if ("6TO".equals(unitDescriptor)) {
            range.add(6);
          } else if ("14QR".equals(unitDescriptor)) {
            range.add(14);
          } else if ("1TO".equals(unitDescriptor)) {
            range.add(1);
          } else if ("19KL".equals(unitDescriptor)) {
            range.add(19);
          } else if ("ABC".equals(unitDescriptor)) {
            range.addRange('A', 'C');
          } else if ("A/B".equals(unitDescriptor)) {
            range.addRange('A', 'B');
          } else if ("A&B".equals(unitDescriptor)) {
            range.addRange('A', 'B');
          } else {
            try {
              final int number = Integer.parseInt(unitDescriptor);
              range.add(number);
            } catch (final Exception e) {
              if (unitDescriptor.length() == 1) {
                final char character = unitDescriptor.charAt(0);
                range.add(character);
              } else {
                final Matcher rangeMatcher = numberRangePattern.matcher(unitDescriptor);
                if (rangeMatcher.matches()) {
                  final int from = Integer.parseInt(rangeMatcher.group(1));
                  final int to = Integer.parseInt(rangeMatcher.group(2));
                  range.addRange(from, to);
                } else {
                  final Matcher matcher = unitWithSuffixOrPrefixPattern.matcher(unitDescriptor);
                  if (matcher.matches()) {
                    range.add(unitDescriptor.replaceAll("[ \\-]", ""));
                  } else {
                    range.add(unitDescriptor);
                    writeExtraDataWarning("ABC_SUB_ADDRESS.csv", civicId, AddressBC.UNIT_NUMBER,
                      unitDescriptor);
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  private SplitByProviderWriter newProviderWriter(final String dataProvider) {
    SplitByProviderWriter providerWriter;
    final PartnerOrganization partnerOrganization = GbaSiteDatabase
      .newPartnerOrganization(dataProvider);
    final PartnerOrganizationFiles partnerOrganizationFiles = new PartnerOrganizationFiles(
      this.dialog, partnerOrganization, this.baseDirectory, this.fileSuffix);
    final Counter counter = this.dialog.getCounter("Provider",
      partnerOrganization.getPartnerOrganizationName(), this.statisticsName);

    providerWriter = new SplitByProviderWriter(this.dialog, dataProvider, counter,
      partnerOrganizationFiles, this.recordDefinition);
    this.writers.add(providerWriter);
    addWriter(dataProvider, providerWriter);
    return providerWriter;
  }

  protected RecordReader newRecordReader() {
    final MapEx readerProperties = new LinkedHashMapEx();
    readerProperties.put("pointXFieldName", "X_COORD");
    readerProperties.put("pointYFieldName", "Y_COORD");
    readerProperties.put("geometryFactory", Gba.GEOMETRY_FACTORY_2D);
    final Path sourceFile = this.sourceDirectory.resolve("ABC_CIVIC_ADDRESS.csv");
    return RecordReader.newRecordReader(sourceFile, readerProperties);
  }

  protected void postRun() {
    if (this.extraDataWriter != null) {
      this.extraDataWriter.close();
    }
  }

  protected void preRun() {
    loadExtendedAddress();
    loadSubAddress();
  }

  protected void preWriteRecord(final Record sourceRecord) {
    setUnitNumber(sourceRecord);

    final Object civicId = sourceRecord.get(AddressBC.CIVIC_ID);
    final String buildingName = this.buildingNameById.get(civicId);
    if (buildingName != null) {
      sourceRecord.put(AddressBC.BUILDING_NAME, buildingName);
    }
    final String buildingType = this.buildingTypeById.get(civicId);
    if (buildingType != null) {
      sourceRecord.put(AddressBC.BUILDING_TYPE, buildingType);
    }
    final String postalCode = this.postalCodeById.remove(civicId);
    if (postalCode != null) {
      sourceRecord.put(AddressBC.POSTAL_CODE, postalCode);
    }
  }

  public void run() {
    Paths.createDirectories(this.directory);
    if (!this.dialog.isCancelled()) {

      preRun();

      splitRecordsByProvider();

      postRun();
    }
  }

  private void setUnitNumber(final Record sourceRecord) {
    final String civicId = sourceRecord.getString(AddressBC.CIVIC_ID);
    final RangeSet range = this.unitDescriptorsById.get(civicId);
    if (range != null) {
      sourceRecord.setValue(UNIT_DESCRIPTOR, range.toString());
    }
  }

  private void splitRecordsByProvider() {
    try {
      try (
        RecordReader sourceReader = newRecordReader()) {

        final RecordDefinitionImpl recordDefinitionImpl = (RecordDefinitionImpl)sourceReader
          .getRecordDefinition();
        addFields(recordDefinitionImpl);
        this.recordDefinition = recordDefinitionImpl;
        loadConfig();
        if (!isCancelled()) {

          for (final Record sourceRecord : cancellable(sourceReader)) {

            preWriteRecord(sourceRecord);
            writeRecord(sourceRecord);
          }
        }
      }
    } catch (final Throwable e) {
      Logs.error(this, e);
    } finally {
      for (final SplitByProviderWriter writer : this.writers) {
        writer.close();
      }
    }
  }

  private synchronized void writeExtraDataWarning(final String fileName, final String civicId,
    final String fieldName, final String fieldValue) {
    if (this.extraDataWriter == null) {
      this.extraDataWriter = Tsv
        .plainWriter(this.directory.getParent().resolve("EXTENDED_FIELD_WARNING.tsv"));
      this.extraDataWriter.write("FILE_NAME", AddressBC.CIVIC_ID, "FIELD_NAME", "FIELD_VALUE");
    }
    this.extraDataWriter.write(fileName, civicId, fieldName, fieldValue);
  }

  private void writeRecord(final Record record) {
    String issuingAgency = record.getString(AddressBC.ISSUING_AGENCY);
    if (issuingAgency == null
      || "FRASER-FORT GEORGE REGIONAL DISTRICT".equalsIgnoreCase(issuingAgency)
      || "CENTRAL OKANAGAN REGIONAL DISTRICT".equalsIgnoreCase(issuingAgency)
      || "FROM FIRST NATION".equalsIgnoreCase(issuingAgency)) {
      issuingAgency = record.getString(AddressBC.LOCALITY);
      if (issuingAgency == null) {
        issuingAgency = "Unknown";
      }
    }
    final SplitByProviderWriter writer = getWriter(record, issuingAgency);
    writer.writeRecord(record);

  }
}
