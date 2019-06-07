package ca.bc.gov.gbasites.load.sourcereader;

import java.nio.file.Path;

import org.jeometry.common.data.type.DataType;
import org.jeometry.common.io.PathName;
import org.jeometry.common.logging.Logs;

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
import com.revolsys.util.Cancellable;
import com.revolsys.util.Counter;

public abstract class AbstractSourceReader extends BaseObjectWithProperties {

  private GeometryFactory geometryFactory;

  protected int expectedRecordCount = -1;

  private Cancellable cancellable;

  protected String dataProvider;

  private RecordWriter writer;

  private int recordCount = 0;

  protected Path baseDirectory;

  private Counter counter;

  public AbstractSourceReader(final MapEx properties) {
    setProperties(properties);
  }

  protected void checkExpectedCount() {
    if (this.expectedRecordCount != -1) {
      if (this.expectedRecordCount != this.recordCount) {
        Logs.error(ProviderSitePointConverter.class, "Expecting site record count="
          + this.expectedRecordCount + " not " + this.recordCount + " for " + this.dataProvider);
      }
    }
  }

  @Override
  public void close() {
    super.close();
  }

  public void downloadData(final AtomicPathUpdator pathUpdator) {
    final RecordDefinition sourceWriterRecordDefinition = getSourceRecordDefinition();
    if (sourceWriterRecordDefinition != null) {
      final Path sourceOutputPath = pathUpdator.getPath();
      try (
        RecordWriter sourceRecordWriter = RecordWriter.newRecordWriter(sourceWriterRecordDefinition,
          sourceOutputPath)) {
        this.writer = sourceRecordWriter;
        writeRecords();
      }
    }
    checkExpectedCount();
    pathUpdator.setCancelled(this.cancellable.isCancelled());
  }

  public GeometryFactory getGeometryFactory() {
    return this.geometryFactory;
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

  public void setCancellable(final Cancellable cancellable) {
    this.cancellable = cancellable;
  }

  public void setCounter(final Counter counter) {
    this.counter = counter;
  }

  public void setDataProvider(final String dataProvider) {
    this.dataProvider = dataProvider;
  }

  public void setGeometryFactory(final GeometryFactory geometryFactory) {
    this.geometryFactory = geometryFactory;
  }

  protected abstract void writeRecords();

  protected void writeRecords(final Iterable<Record> records) {
    for (final Record sourceRecord : this.cancellable.cancellable(records)) {
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
