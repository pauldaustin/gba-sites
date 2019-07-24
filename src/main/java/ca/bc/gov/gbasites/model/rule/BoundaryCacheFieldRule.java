package ca.bc.gov.gbasites.model.rule;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jeometry.common.data.identifier.Identifier;
import org.jeometry.common.logging.Logs;

import ca.bc.gov.gba.controller.GbaController;
import ca.bc.gov.gba.model.BoundaryCache;
import ca.bc.gov.gba.model.message.QaMessageDescription;
import ca.bc.gov.gba.rule.AbstractRecordRule;
import ca.bc.gov.gba.rule.fix.SetToMessageData;

import com.revolsys.collection.list.Lists;
import com.revolsys.collection.map.Maps;
import com.revolsys.geometry.model.Point;
import com.revolsys.record.Record;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.record.schema.RecordStore;

public class BoundaryCacheFieldRule extends AbstractRecordRule implements Cloneable {

  private static final QaMessageDescription MESSAGE_DIFFERENT_FROM_CONTAINED = new QaMessageDescription(
    "BNDY_DIFCON", "Boundary Field Differs From Contained",
    "The value for a boundary field should be the same as the one calculated spatially. Either change the field value, move the point location, or add a rule exclusion.",
    "Fb({fieldTitle})=Vb({boundaryName}) not same as contained Vb({containedBoundaryName}).", true) //
      .addQuickFix(new SetToMessageData("containedBoundaryName"));

  private static final QaMessageDescription MESSAGE_NOT_CONTAINED = new QaMessageDescription(
    "BNDY_NTCON", "Boundary Field Not Contained",
    "The value for a boundary field should be only set if the record is spatially contained in a boundary. Either set field value = null, move the point location, or add a rule exclusion. NOTE: setting to null may result in a required error.",
    "Fb({fieldTitle})=Vb({boundaryName}) not contained in any boundary.", true) //
      .addQuickFix(FIX_SET_NULL);

  private final Map<String, BoundaryCache> boundaryCacheByFieldName = new HashMap<>();

  private List<String> boundaryFieldNames;

  public BoundaryCacheFieldRule() {
    this.boundaryFieldNames = new ArrayList<>();
  }

  public BoundaryCacheFieldRule(final String... boundaryFieldNames) {
    this.boundaryFieldNames = Lists.newArray(boundaryFieldNames);
  }

  public List<String> getBoundaryFieldNames() {
    return this.boundaryFieldNames;
  }

  @Override
  protected void init(final RecordDefinition recordDefinition) {
    final RecordStore recordStore = GbaController.getRecordStore();
    for (final String fieldName : this.boundaryFieldNames) {
      final BoundaryCache boundaryCache = (BoundaryCache)recordStore
        .getCodeTableByFieldName(fieldName);
      if (boundaryCache == null) {
        Logs.error(this, "Cannot find code table for " + fieldName);
      } else {
        this.boundaryCacheByFieldName.put(fieldName, boundaryCache);
      }
    }
    setFieldNames(this.boundaryFieldNames);
    addFieldNames(recordDefinition.getGeometryFieldName());
  }

  public void setBoundaryFieldNames(final List<String> boundaryFieldNames) {
    this.boundaryFieldNames = boundaryFieldNames;
  }

  @Override
  protected boolean validateRecordDo(final Record record) {
    boolean valid = true;
    final Point point = record.getGeometry();
    for (final String fieldName : this.boundaryFieldNames) {
      final BoundaryCache boundaryCache = this.boundaryCacheByFieldName.get(fieldName);
      if (boundaryCache != null) {
        final Identifier boundaryId = record.getIdentifier(fieldName);
        final Identifier containedBoundaryId = boundaryCache.getBoundaryId(point);
        if (boundaryId == null) {
          if (containedBoundaryId != null) {
            if (record.setValue(fieldName, containedBoundaryId)) {
              addCount("Fixed", "Boundary - Set " + fieldName);
            }
          }
        } else if (containedBoundaryId == null) {
          final Map<String, Object> data = Maps.newLinkedHash("boundaryName",
            boundaryCache.getValue(boundaryId));
          if (record.getRecordDefinition().isFieldRequired(fieldName)) {
            valid &= addMessage(record, MESSAGE_NOT_CONTAINED, data, fieldName);
          } else {
            valid &= addMessage(record, MESSAGE_NOT_CONTAINED, data, fieldName, () -> {
              if (record.setValue(fieldName, null)) {
                addCount("Fixed", "Boundary - Set " + fieldName + "=null");
              }
            });
          }
        } else if (!containedBoundaryId.equals(boundaryId)) {
          final Map<String, Object> data = Maps.newLinkedHash("boundaryName",
            boundaryCache.getValue(boundaryId));
          data.put("containedBoundaryName", boundaryCache.getValue(containedBoundaryId));
          valid &= addMessage(record, MESSAGE_NOT_CONTAINED, data, fieldName, () -> {
            if (record.setValue(fieldName, containedBoundaryId)) {
              addCount("Fixed", "Boundary - Set " + fieldName);
            }
          });
        } else {
          removeMessages(record, MESSAGE_DIFFERENT_FROM_CONTAINED, fieldName);
          removeMessages(record, MESSAGE_NOT_CONTAINED, fieldName);
        }
      }
    }
    return valid;
  }
}
