package ca.bc.gov.gbasites.load.merge;

import ca.bc.gov.gba.model.type.GbaType;
import ca.bc.gov.gba.model.type.code.PartnerOrganization;
import ca.bc.gov.gba.ui.StatisticsDialog;
import ca.bc.gov.gbasites.load.common.ProviderSitePointConverter;
import ca.bc.gov.gbasites.load.provider.addressbc.AddressBC;
import ca.bc.gov.gbasites.load.provider.geobc.GeoBC;

import com.revolsys.record.Record;
import com.revolsys.swing.table.counts.LabelCountMapTableModel;
import com.revolsys.util.Counter;
import com.revolsys.util.count.TotalLabelCounters;

public class RecordMergeCounters {

  private static final String DUPLICATE = "Duplicate";

  private static final String MATCH_ADDRESS = "Match Address";

  private static final String MATCH_POINT = "Match Point";

  private static final String MATCH_UNIT = "Match Unit";

  private static final String MERGE_POINT = "Merge Point";

  private static final String MERGE_UNIT = "Merge Unit";

  private static final String READ = "Read";

  public static final String USED = "Used";

  public static LabelCountMapTableModel addProviderCounts(final StatisticsDialog dialog,
    final String categoryName) {
    final LabelCountMapTableModel counters = dialog.newLabelCountTableModel(categoryName, //
      ProviderSitePointConverter.LOCALITY, //

      READ, //
      DUPLICATE, //
      MERGE_POINT, //
      MERGE_UNIT);

    if (AddressBC.NAME.equals(categoryName)) {
      counters.addCountNameColumns(MATCH_ADDRESS, MATCH_POINT, MATCH_UNIT);
    }

    final TotalLabelCounters totalColumn = counters.addTotalColumn(RecordMergeCounters.USED, READ) //
      .subtractCounters(counters.getLabelCounters(DUPLICATE)) //
      .subtractCounters(counters.getLabelCounters(MERGE_POINT)) //
      .subtractCounters(counters.getLabelCounters(MERGE_UNIT)) //
    ;
    if (AddressBC.NAME.equals(categoryName)) {
      totalColumn //
        .subtractCounters(counters.getLabelCounters(MATCH_ADDRESS))//
        .subtractCounters(counters.getLabelCounters(MATCH_POINT))//
        .subtractCounters(counters.getLabelCounters(MATCH_UNIT))//
      ;
    }
    return counters;
  }

  public LabelCountMapTableModel counters;

  public Counter duplicate;

  private final PartnerOrganization partnerOrganization;

  public Counter matchAddress;

  public Counter matchPoint;

  public Counter matchUnit;

  public Counter mergePoint;

  public Counter mergeUnit;

  public Counter read;

  public RecordMergeCounters(final LabelCountMapTableModel counters,
    final PartnerOrganization partnerOrganization) {
    super();
    this.counters = counters;
    this.partnerOrganization = partnerOrganization;
  }

  public PartnerOrganization getPartnerOrganization() {
    return this.partnerOrganization;
  }

  public void init(final String localityName) {
    this.read = this.counters.getCounter(localityName, READ);
    this.duplicate = this.counters.getCounter(localityName, DUPLICATE);
    this.mergePoint = this.counters.getCounter(localityName, MERGE_POINT);
    this.mergeUnit = this.counters.getCounter(localityName, MERGE_UNIT);
    if (this.partnerOrganization == AddressBC.PARTNER_ORGANIZATION) {
      this.matchAddress = this.counters.getCounter(localityName, MATCH_ADDRESS);
      this.matchPoint = this.counters.getCounter(localityName, MATCH_POINT);
      this.matchUnit = this.counters.getCounter(localityName, MATCH_UNIT);
    }
  }

  public boolean isCreatePartnerOrg(final Record record) {
    final String organizationName = record.getString(GbaType.CREATE_PARTNER_ORG);
    if (this.partnerOrganization == null) {
      return !(organizationName.equals(GeoBC.PARTNER_ORGANIZATION_NAME)
        || organizationName.equals(AddressBC.PROVIDER_ICI_SOCIETY));
    } else {
      return this.partnerOrganization.equalsName(organizationName);
    }
  }
}
