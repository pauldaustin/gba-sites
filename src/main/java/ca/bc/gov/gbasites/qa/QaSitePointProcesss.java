package ca.bc.gov.gbasites.qa;

import java.util.List;

import ca.bc.gov.gba.itn.model.GbaItnTables;
import ca.bc.gov.gba.model.Gba;
import ca.bc.gov.gba.process.qa.AbstractTaskByLocalityProcess;
import ca.bc.gov.gba.rule.RecordRuleThreadProperties;
import ca.bc.gov.gbasites.model.rule.SitePointRule;
import ca.bc.gov.gbasites.model.type.SiteTables;

import com.revolsys.geometry.index.RecordSpatialIndex;
import com.revolsys.record.Record;
import com.revolsys.transaction.Transaction;

public class QaSitePointProcesss extends AbstractTaskByLocalityProcess {

  public QaSitePointProcesss(final QaSitePoint qaDialog) {
    super(qaDialog);
    setRules(SiteTables.SITE_POINT);
  }

  @Override
  public boolean processLocality() {
    boolean valid = true;
    // Make sure the transport line index is initialized
    SitePointRule.getTransportLines();
    final List<Record> sites = SitePointRule.getSites();
    final RecordSpatialIndex<Record> index = RecordSpatialIndex.quadTree(Gba.GEOMETRY_FACTORY_2D)
      .addRecords(sites);
    RecordRuleThreadProperties.setSpatialIndex(SiteTables.SITE_POINT, index);

    valid &= validateRecords(sites);

    valid &= validateLocality(SiteTables.SITE_POINT);

    try (
      Transaction transaction = gbaRecordStore.newTransaction()) {
      // TODO update records with exclusions
      saveChanges("Site", sites);
      final List<Record> allTransportLines = RecordRuleThreadProperties.getRecords(null,
        this.localityId, GbaItnTables.TRANSPORT_LINE, false);
      saveChanges("Transport Line", allTransportLines);
    }
    return valid;
  }

}
