package ca.bc.gov.gbasites.controller;

import ca.bc.gov.gba.core.model.qa.rule.RecordRuleProperty;
import ca.bc.gov.gbasites.model.rule.BoundaryCacheFieldRule;
import ca.bc.gov.gbasites.model.rule.SitePointFieldValueRule;
import ca.bc.gov.gbasites.model.rule.SitePointRule;
import ca.bc.gov.gbasites.model.type.SitePoint;
import ca.bc.gov.gbasites.model.type.SiteTables;

import com.revolsys.collection.map.LinkedHashMapEx;
import com.revolsys.collection.map.MapEx;
import com.revolsys.record.property.ValueRecordDefinitionProperty;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.record.schema.RecordStore;

public class SitePointInit implements SitePoint {
  public static void init(final RecordStore recordStore) {
    final RecordDefinition recordDefinition = recordStore
      .getRecordDefinition(SiteTables.SITE_POINT);
    if (recordDefinition != null) {
      initDefaultValues(recordDefinition);
      initRules(recordDefinition);
    }
  }

  private static void initDefaultValues(final RecordDefinition recordDefinition) {
    final MapEx defaultValues = new LinkedHashMapEx() //
      .add(EMERGENCY_MANAGEMENT_SITE_IND, "N") //
      .add(DATA_CAPTURE_METHOD_CODE, "unknown") //
      .add(FEATURE_STATUS_CODE, "A") //
      .add(SITE_LOCATION_CODE, "P") //
      .add(USE_IN_ADDRESS_RANGE_IND, "Y") //
      .add(USE_SITE_NAME_IN_ADDRESS_IND, "N") //
    ;
    ValueRecordDefinitionProperty.setProperty(recordDefinition, "defaultValues", defaultValues);
  }

  private static void initRules(final RecordDefinition recordDefinition) {
    final RecordRuleProperty rules = new RecordRuleProperty();
    rules.setRules( //
      new BoundaryCacheFieldRule("COMMUNITY_ID", "LOCALITY_ID", "REGIONAL_DISTRICT_ID"), //
      new SitePointFieldValueRule(), //
      new SitePointRule() //
    );
    rules.setRecordDefinition(recordDefinition);
  }
}
