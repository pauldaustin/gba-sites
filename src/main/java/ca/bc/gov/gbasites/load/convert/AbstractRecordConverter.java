package ca.bc.gov.gbasites.load.convert;

import java.nio.file.Path;

import org.jeometry.common.data.identifier.Identifier;
import org.jeometry.common.logging.Logs;

import ca.bc.gov.gba.model.type.code.PartnerOrganization;
import ca.bc.gov.gba.model.type.code.PartnerOrganizationProxy;
import ca.bc.gov.gba.ui.BatchUpdateDialog;
import ca.bc.gov.gba.ui.StatisticsDialog;
import ca.bc.gov.gbasites.load.ImportSites;
import ca.bc.gov.gbasites.load.common.IgnoreSiteException;
import ca.bc.gov.gbasites.load.common.PartnerOrganizationFiles;
import ca.bc.gov.gbasites.load.common.ProviderSitePointConverter;

import com.revolsys.geometry.model.Geometry;
import com.revolsys.io.file.AtomicPathUpdator;
import com.revolsys.io.map.MapSerializer;
import com.revolsys.properties.BaseObjectWithProperties;
import com.revolsys.record.Record;
import com.revolsys.record.RecordLog;
import com.revolsys.record.code.CodeTable;
import com.revolsys.record.io.RecordReader;
import com.revolsys.record.io.RecordWriter;
import com.revolsys.record.schema.FieldDefinition;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.util.Cancellable;
import com.revolsys.util.Counter;
import com.revolsys.util.Debug;

public abstract class AbstractRecordConverter<R extends Record> extends BaseObjectWithProperties
  implements Cancellable, MapSerializer, PartnerOrganizationProxy {

  protected PartnerOrganizationFiles partnerOrganizationFiles;

  protected Counter convertCounter;

  protected Counter ignoreCounter;

  protected String countPrefix = "";

  protected StatisticsDialog dialog;

  private Counter errorCounter;

  private Counter warningCounter;

  protected RecordLog errorLog;

  protected AtomicPathUpdator errorPathUpdator;

  protected String fileSuffix = "";

  protected Path baseDirectory;

  protected String localityName;

  protected PartnerOrganization createModifyPartnerOrganization;

  private Record sourceRecord;

  protected Identifier localityId;

  public void addError(final Record record, final String message) {
    addErrorCount(message);

    if (this.errorLog == null) {
      this.errorPathUpdator = this.partnerOrganizationFiles
        .newPathUpdator(ImportSites.ERROR_BY_PROVIDER);
      final Path dataProviderErrorFile = this.errorPathUpdator.getPath();
      this.errorLog = new RecordLog(true);
      final RecordDefinition logRecordDefinition = this.errorLog.getLogRecordDefinition(record);
      final RecordWriter errorWriter = RecordWriter.newRecordWriter(logRecordDefinition,
        dataProviderErrorFile);
      this.errorLog.setWriter(errorWriter);
    }
    Geometry geometry = record.getGeometry();
    if (geometry != null) {
      geometry = geometry.getPointWithin();
    }
    this.errorLog.error(this.localityName, message, record, geometry);
  }

  protected void addErrorCount(final String message) {
    this.errorCounter.add();
    this.dialog.addLabelCount(BatchUpdateDialog.ERROR, message, BatchUpdateDialog.ERROR);
  }

  protected void addIgnoreCount() {
    this.ignoreCounter.add();
  }

  public void addWarning(final Record record, final String message) {
    this.warningCounter.add();
    this.dialog.addLabelCount(ProviderSitePointConverter.WARNING, message,
      ProviderSitePointConverter.WARNING);
  }

  public void addWarning(final String message) {
    addWarning(this.sourceRecord, message);
  }

  protected R convertRecord(final Record sourceRecord) {
    this.localityName = null;
    this.sourceRecord = sourceRecord;
    R record = null;
    try {
      record = convertRecordDo(sourceRecord);
      if (record == null) {
        Debug.noOp();
      }
    } catch (final IgnoreSiteException e) {
      final String countName = e.getCountName();
      if (e.isError()) {
        addError(sourceRecord, countName);
      } else {
        addWarning(sourceRecord, countName);
      }
    } catch (final NullPointerException e) {
      Logs.error(this, "Null pointer", e);
      addError(sourceRecord, "Null Pointer");
    } catch (final Exception e) {
      addError(sourceRecord, e.getMessage());
    }
    if (record == null) {
      this.ignoreCounter.add();
    } else {
      this.convertCounter.add();
    }
    return record;
  }

  protected abstract R convertRecordDo(Record sourceRecord);

  public void convertSourceRecords() {
    final RecordDefinition providerRecordDefinition = null;
    try (
      RecordReader sourceReader = newSourceRecordReader(providerRecordDefinition)) {

      for (final Record sourceRecord : cancellable(sourceReader)) {
        final R convertedRecord = convertRecord(sourceRecord);
        if (convertedRecord != null) {
          postConvertRecord(convertedRecord);
        }
      }
      if (!isCancelled()) {
        postConvertRecords();
      }
    } catch (final Exception e) {
      Logs.error(this, e);
    } finally {
      close();
    }
  }

  private Counter getCounter(final String countName) {
    return this.dialog.getCounter("Provider", this.partnerOrganizationFiles,
      this.countPrefix + countName);
  }

  protected AtomicPathUpdator getDataProviderPathUpdator(final Path directory,
    final String suffix) {
    final String baseName = getPartnerOrganizationFileName();
    final String fileName = baseName + suffix;
    return new AtomicPathUpdator(this, directory, fileName);
  }

  protected StatisticsDialog getDialog() {
    return this.dialog;
  }

  @Override
  public PartnerOrganization getPartnerOrganization() {
    return this.partnerOrganizationFiles.getPartnerOrganization();
  }

  @Override
  public boolean isCancelled() {
    return this.dialog.isCancelled();
  }

  protected RecordReader newSourceRecordReader(final RecordDefinition providerRecordDefinition) {
    final Path sourceFile = this.partnerOrganizationFiles
      .getFilePath(ImportSites.SOURCE_BY_PROVIDER);

    RecordReader reader;
    reader = RecordReader.newRecordReader(sourceFile);
    if (reader != null && providerRecordDefinition != null) {
      final RecordDefinition sitesRecordDefinition = reader.getRecordDefinition();
      for (final FieldDefinition field : sitesRecordDefinition.getFields()) {
        final String name = field.getName();
        final CodeTable codeTable = providerRecordDefinition.getCodeTableByFieldName(name);
        if (codeTable != null) {
          field.setCodeTable(codeTable);
        }
      }
    }
    return reader;
  }

  protected void postConvertRecord(final R convertedRecord) {
  }

  protected abstract void postConvertRecords();

  public void setBaseDirectory(final Path baseDirectory) {
    this.baseDirectory = baseDirectory;
  }

  public void setCountPrefix(final String countPrefix) {
    this.countPrefix = countPrefix;
  }

  public void setCreateModifyPartnerOrganization(
    final PartnerOrganization createModifyPartnerOrganization) {
    this.createModifyPartnerOrganization = createModifyPartnerOrganization;
  }

  public void setDialog(final StatisticsDialog dialog) {
    this.dialog = dialog;
    this.convertCounter = getCounter("Converted");
    this.errorCounter = getCounter("Error");
    this.warningCounter = getCounter("Warning");
    this.ignoreCounter = getCounter("Ignored");
  }

  public void setFileSuffix(final String fileSuffix) {
    this.fileSuffix = fileSuffix;
  }

  public void setPartnerOrganizationFiles(final PartnerOrganizationFiles partnerOrganizationFiles) {
    this.partnerOrganizationFiles = partnerOrganizationFiles;
    setFileSuffix(partnerOrganizationFiles.getProviderSuffix());
    setBaseDirectory(partnerOrganizationFiles.getBaseDirectory());
    if (this.createModifyPartnerOrganization == null) {
      this.createModifyPartnerOrganization = partnerOrganizationFiles.getPartnerOrganization();
    }
  }

  @Override
  public String toString() {
    return this.partnerOrganizationFiles.toString();
  }
}
