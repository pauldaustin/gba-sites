package ca.bc.gov.gbasites.load.readsource;

import java.util.Map;
import java.util.function.Function;

import org.jeometry.common.logging.Logs;

import com.revolsys.collection.map.MapEx;
import com.revolsys.geometry.model.GeometryFactory;
import com.revolsys.record.ArrayRecord;
import com.revolsys.record.io.RecordReader;
import com.revolsys.record.query.Query;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.webservice.WebServiceFeatureLayer;

public class SourceReaderMapGuide extends AbstractRecordReaderSourceReader {

  public static Function<MapEx, SourceReaderMapGuide> newFactory(
    final Map<String, ? extends Object> config) {
    return properties -> new SourceReaderMapGuide(properties.addAll(config));
  }

  public SourceReaderMapGuide(final MapEx properties) {
    super(properties);
  }

  @Override
  protected RecordReader newRecordReader() {
    final String serverUrl = getProperty("serverUrl");
    final String path = getProperty("path");

    final GeometryFactory forceGeometryFactory = getGeometryFactory();

    final WebServiceFeatureLayer layer = com.revolsys.record.io.format.mapguide.FeatureLayer
      .getFeatureLayer(serverUrl, path);
    if (layer == null) {
      Logs.error(this, "Cannot find layer: " + path + " on " + serverUrl);
      return null;
    } else {
      final RecordDefinition layerRecordDefinition = layer.getRecordDefinition();
      if (forceGeometryFactory != null) {
        layerRecordDefinition.setGeometryFactory(forceGeometryFactory);
      }

      return layer.newRecordReader(ArrayRecord.FACTORY, (Query)null);
    }
  }
}
