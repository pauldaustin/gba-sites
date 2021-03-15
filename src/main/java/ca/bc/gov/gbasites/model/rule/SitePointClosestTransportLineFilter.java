package ca.bc.gov.gbasites.model.rule;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;

import org.jeometry.common.data.identifier.Identifier;

import ca.bc.gov.gba.itn.model.TransportLine;
import ca.bc.gov.gba.model.type.TransportLines;

import com.revolsys.collection.CollectionUtil;
import com.revolsys.geometry.model.LineString;
import com.revolsys.geometry.model.Point;
import com.revolsys.record.Record;
import com.revolsys.record.Records;

public class SitePointClosestTransportLineFilter implements Predicate<Record> {

  private final double maxDistance;

  private final Point point;

  private final Collection<Identifier> structuredNameIds;

  public SitePointClosestTransportLineFilter(final Point point, final double maxDistance,
    final Collection<Identifier> structuredNameIds) {
    this.point = point;
    this.maxDistance = maxDistance;
    this.structuredNameIds = structuredNameIds;
  }

  public SitePointClosestTransportLineFilter(final Point point, final double maxDistance,
    final Identifier structuredNameId) {
    this(point, maxDistance, Arrays.asList(structuredNameId));
  }

  @Override
  public boolean test(final Record record) {
    if (TransportLines.isDemographic(record)) {
      final List<Identifier> roadStructuredNameIds = Records.getIdentifiers(record,
        TransportLine.STRUCTURED_NAME_FIELD_NAMES);
      // Only attach to road if it shares one structured name with the
      // site
      if (CollectionUtil.containsAny(roadStructuredNameIds, this.structuredNameIds)) {
        final LineString line = record.getGeometry();
        if (line.isWithinDistance(this.point, this.maxDistance)) {
          return true;
        }
      } else {
        // final Set<WmsIdentifier> directionalAliases =
        // SitePointRule.STRUCTURED_NAMES.getDirectionalAliases(structuredNameId);
        // if (!directionalAliases.isEmpty()) {
        // for (final WmsIdentifier identifier : directionalAliases) {
        // if (!identifier.equals(structuredNameIds)) {
        // Debug.println(SitePointRule.STRUCTURED_NAMES.getValue(identifier));
        // }
        // }
        // }
      }
    }
    return false;
  }
}
