package ca.bc.gov.gbasites.load.readsource;

import java.util.Map;
import java.util.function.Function;

import org.jeometry.common.logging.Logs;

import com.revolsys.collection.map.MapEx;
import com.revolsys.record.ArrayRecord;
import com.revolsys.record.io.RecordReader;
import com.revolsys.record.io.format.esri.rest.map.FeatureLayer;
import com.revolsys.record.query.Query;

public class SourceReaderArcGis extends AbstractRecordReaderSourceReader {

  public static Function<MapEx, SourceReaderArcGis> newFactory(
    final Map<String, ? extends Object> config) {
    return properties -> new SourceReaderArcGis(properties.addAll(config));
  }

  private String serverUrl;

  private boolean loadByObjectId;

  private String path;

  public SourceReaderArcGis(final MapEx properties) {
    super(properties);
  }

  @Override
  protected RecordReader newRecordReader() {
    try {
      final FeatureLayer layer = FeatureLayer.getRecordLayerDescription(this.serverUrl, this.path);
      if (layer == null) {
        Logs.error(this, getPartnerOrganizationName() + ": using cached files. Cannot find layer: "
          + this.path + " on " + this.serverUrl);
      } else {
        this.expectedRecordCount = layer.getRecordCount((Query)null);
        if (this.loadByObjectId || !layer.isSupportsPagination()
          || layer.getCurrentVersion() < 10.3) {
          return layer.newRecordReader((Query)null, true);
        } else {
          return layer.newRecordReader(ArrayRecord.FACTORY, (Query)null);
        }
      }
    } catch (final Throwable e) {
      Logs.error(this, getPartnerOrganizationName()
        + ": using cached files. Cannot connect to server: " + this.serverUrl, e);
    }
    return null;
  }

  public void setLoadByObjectId(final boolean loadByObjectId) {
    this.loadByObjectId = loadByObjectId;
  }

  public void setPath(final String path) {
    this.path = path;
  }

  public void setServerUrl(final String serverUrl) {
    this.serverUrl = serverUrl;
  }
}
