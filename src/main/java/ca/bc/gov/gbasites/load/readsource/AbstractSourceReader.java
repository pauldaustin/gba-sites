package ca.bc.gov.gbasites.load.readsource;

import java.nio.file.Path;

import org.jeometry.common.data.type.DataType;
import org.jeometry.common.io.PathName;
import org.jeometry.common.logging.Logs;

import ca.bc.gov.gba.model.type.code.PartnerOrganization;
import ca.bc.gov.gba.model.type.code.PartnerOrganizationProxy;
import ca.bc.gov.gba.ui.StatisticsDialog;
import ca.bc.gov.gbasites.load.ImportSites;
import ca.bc.gov.gbasites.load.common.PartnerOrganizationFiles;
import ca.bc.gov.gbasites.load.common.ProviderSitePointConverter;

import com.revolsys.collection.map.MapEx;
import com.revolsys.geometry.model.Geometry;
import com.revolsys.geometry.model.GeometryFactory;
import com.revolsys.io.file.AtomicPathUpdator;
import com.revolsys.properties.BaseObjectWithProperties;
import com.revolsys.record.Record;
import com.revolsys.record.io.RecordWriter;
import com.revolsys.record.schema.FieldDefinition;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.record.schema.RecordDefinitionImpl;
import com.revolsys.util.Counter;

public abstract class AbstractSourceReader extends BaseObjectWithProperties
  implements PartnerOrganizationProxy {

  private GeometryFactory geometryFactory;

  protected int expectedRecordCount = -1;

  private StatisticsDialog dialog;

  private RecordWriter writer;

  private int recordCount = 0;

  protected Path baseDirectory;

  private PartnerOrganizationFiles partnerOrganizationFiles;

  private Counter counter;

  private String countPrefix = "";

  public AbstractSourceReader(final MapEx properties) {
    setProperties(properties);
  }

  protected void checkExpectedCount() {
    if (this.expectedRecordCount != -1) {
      if (this.expectedRecordCount != this.recordCount) {
        Logs.error(ProviderSitePointConverter.class,
          "Expecting site record count=" + this.expectedRecordCount + " not " + this.recordCount
            + " for " + getPartnerOrganizationName());
      }
    }
  }

  @Override
  public void close() {
    super.close();
  }

  public void downloadData(final boolean downloadData) {
    try (
      AtomicPathUpdator pathUpdator = this.partnerOrganizationFiles
        .newPathUpdator(ImportSites.SOURCE_BY_PROVIDER)) {
      if (downloadData || !pathUpdator.isTargetExists()) {

        final RecordDefinition sourceWriterRecordDefinition = getSourceRecordDefinition();
        if (sourceWriterRecordDefinition != null) {
          final Path sourceOutputPath = pathUpdator.getPath();
          try (
            RecordWriter sourceRecordWriter = RecordWriter
              .newRecordWriter(sourceWriterRecordDefinition, sourceOutputPath)) {
            this.writer = sourceRecordWriter;
            writeRecords();
          }
        }
        checkExpectedCount();
      }
    }
  }

  private Counter getCounter(final String countName) {
    return this.dialog.getCounter("Provider", getPartnerOrganizationName(),
      this.countPrefix + countName);
  }

  public GeometryFactory getGeometryFactory() {
    return this.geometryFactory;
  }

  @Override
  public PartnerOrganization getPartnerOrganization() {
    return this.partnerOrganizationFiles.getPartnerOrganization();
  }

  protected RecordDefinition getSourceRecordDefinition() {
    final GeometryFactory forceGeometryFactory = this.geometryFactory;

    final RecordDefinition sourceRecordDefinition = getSourceRecordDefinitionDo();

    final PathName pathName = sourceRecordDefinition.getPathName();
    final RecordDefinitionImpl sourceWriterRecordDefinition = new RecordDefinitionImpl(pathName);
    final String geometryFieldName = sourceRecordDefinition.getGeometryFieldName();
    for (final FieldDefinition fieldDefinition : sourceRecordDefinition.getFields()) {
      String fieldName = fieldDefinition.getName();
      final DataType fieldType = fieldDefinition.getDataType();
      if (geometryFieldName.equals(fieldName)) {
        fieldName = "GEOMETRY";
      }
      sourceWriterRecordDefinition.addField(fieldName, fieldType);
    }
    GeometryFactory sourceGeometryFactory = sourceRecordDefinition.getGeometryFactory();
    if (sourceGeometryFactory == null
      || sourceGeometryFactory.getHorizontalCoordinateSystemId() == 0) {
      if (forceGeometryFactory != null) {
        sourceGeometryFactory = forceGeometryFactory;
      }
    }
    sourceWriterRecordDefinition.setGeometryFactory(sourceGeometryFactory);
    return sourceWriterRecordDefinition;
  }

  protected abstract RecordDefinition getSourceRecordDefinitionDo();

  public void setBaseDirectory(final Path baseDirectory) {
    this.baseDirectory = baseDirectory;
  }

  public void setCounter(final Counter counter) {
    this.counter = counter;
  }

  public void setCountPrefix(final String countPrefix) {
    this.countPrefix = countPrefix;
  }

  public void setDialog(final StatisticsDialog dialog) {
    this.dialog = dialog;
    this.counter = getCounter("Source");
  }

  public void setGeometryFactory(final GeometryFactory geometryFactory) {
    this.geometryFactory = geometryFactory;
  }

  public void setPartnerOrganizationFiles(final PartnerOrganizationFiles partnerOrganizationFiles) {
    this.partnerOrganizationFiles = partnerOrganizationFiles;
  }

  @Override
  public String toString() {
    return getPartnerOrganizationName();
  }

  protected abstract void writeRecords();

  protected void writeRecords(final Iterable<Record> records) {
    for (final Record sourceRecord : this.dialog.cancellable(records)) {
      writeSourceRecord(sourceRecord);
    }
  }

  protected void writeSourceRecord(final Record sourceRecord) {
    this.recordCount++;
    final Record writeRecord = this.writer.newRecord(sourceRecord);
    final Geometry geometry = sourceRecord.getGeometry();
    if (geometry != null) {
      final Geometry geometry2d = geometry.newGeometry(2);
      writeRecord.setGeometryValue(geometry2d);
    }
    this.writer.write(writeRecord);
    if (this.counter != null) {
      this.counter.add();
    }
  }
}
