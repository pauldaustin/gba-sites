package ca.bc.gov.gbasites.load.readsource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import com.revolsys.collection.map.MapEx;
import com.revolsys.geometry.model.Geometry;
import com.revolsys.geometry.model.GeometryDataTypes;
import com.revolsys.geometry.model.GeometryFactory;
import com.revolsys.record.Record;
import com.revolsys.record.io.ListRecordReader;
import com.revolsys.record.io.RecordReader;
import com.revolsys.record.schema.RecordDefinitionImpl;

public class SourceReaderJoin extends AbstractRecordReaderSourceReader {

  public static Function<MapEx, SourceReaderJoin> newFactory(
    final Map<String, ? extends Object> config) {
    return properties -> new SourceReaderJoin(properties.addAll(config));
  }

  private String geometryRecordFieldName;

  private String fieldRecordFieldName;

  private Supplier<RecordReader> fieldReaderFactory;

  private Supplier<RecordReader> geometryReaderFactory;

  public SourceReaderJoin(final MapEx properties) {
    super(properties);
  }

  @Override
  protected RecordReader newRecordReader() {

    GeometryFactory geometryFactory;
    final Map<String, Geometry> geometryById = new HashMap<>();
    try (
      RecordReader geometryReader = this.geometryReaderFactory.get()) {
      geometryFactory = geometryReader.getGeometryFactory();
      for (final Record geometryRecord : geometryReader) {
        final String id = geometryRecord.getString(this.geometryRecordFieldName);
        final Geometry geometry = geometryRecord.getGeometry();
        final Geometry geometry2d = geometry.newGeometry(2);
        geometryById.put(id, geometry2d);
      }
    }
    final List<Record> sourceSites = new ArrayList<>();
    final RecordDefinitionImpl recordDefinition;
    try (
      RecordReader fieldReader = this.fieldReaderFactory.get()) {
      recordDefinition = (RecordDefinitionImpl)fieldReader.getRecordDefinition();
      recordDefinition.addField("geometry", GeometryDataTypes.GEOMETRY);
      recordDefinition.setGeometryFactory(geometryFactory);

      for (final Record fieldRecord : fieldReader) {
        final String id = fieldRecord.getString(this.fieldRecordFieldName);
        final Geometry geometry = geometryById.get(id);
        if (geometry != null) {
          fieldRecord.setGeometryValue(geometry);
        }
        sourceSites.add(fieldRecord);
      }
    }
    return new ListRecordReader(recordDefinition, sourceSites);

  }

  public void setFieldReaderFactory(final Supplier<RecordReader> fieldReaderFactory) {
    this.fieldReaderFactory = fieldReaderFactory;
  }

  public void setFieldRecordFieldName(final String fieldRecordFieldName) {
    this.fieldRecordFieldName = fieldRecordFieldName;
  }

  public void setGeometryReaderFactory(final Supplier<RecordReader> geometryReaderFactory) {
    this.geometryReaderFactory = geometryReaderFactory;
  }

  public void setGeometryRecordFieldName(final String geometryRecordFieldName) {
    this.geometryRecordFieldName = geometryRecordFieldName;
  }
}
