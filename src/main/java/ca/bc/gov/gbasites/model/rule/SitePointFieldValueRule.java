package ca.bc.gov.gbasites.model.rule;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jeometry.common.data.type.DataType;

import ca.bc.gov.gba.model.GbaTables;
import ca.bc.gov.gba.model.message.QaMessageDescription;
import ca.bc.gov.gba.rule.AbstractRecordRule;
import ca.bc.gov.gba.rule.fix.SetValue;
import ca.bc.gov.gba.rule.impl.FieldValueRule;
import ca.bc.gov.gba.rule.impl.SessionField;
import ca.bc.gov.gbasites.model.type.SitePoint;
import ca.bc.gov.gbasites.model.type.SiteTables;
import ca.bc.gov.gbasites.model.type.code.SiteType;

import com.revolsys.collection.list.Lists;
import com.revolsys.collection.map.Maps;
import com.revolsys.collection.range.RangeInvalidException;
import com.revolsys.record.Record;
import com.revolsys.record.Records;
import com.revolsys.util.Property;
import com.revolsys.util.Strings;

public class SitePointFieldValueRule extends FieldValueRule implements SitePoint {
  public static final QaMessageDescription CIVIC_NUMBER_0_NOT_VIRTUAL = new QaMessageDescription(
    "SP_VRTCN0", "Virtual Sites Civic Number 0",
    "Fb=(SITE_TYPE_CODE) must be Vb(Virtual Block From) for Fb=(CIVIC_NUMBER)=Vb(0).",
    "Fb=(SITE_TYPE_CODE) must be Vb(Virtual Block From) for Fb=(CIVIC_NUMBER)=Vb(0)", false) //
      .addQuickFix(new SetValue(SITE_TYPE_CODE, SiteType.VIRTUAL_BLOCK_FROM));

  public static final QaMessageDescription NO_FULL_ADDRESS = new QaMessageDescription("SP_NOFA",
    "Cannot calculate Full Address",
    "The full address is calculated from other fields. Combinations include: CIVIC_NUMBER and STRUCTURED_NAME_1_ID; UNIT_DESCRIPTOR; SITE_NAME_1 and USE_SITE_NAME_IN_ADDRESS_IND",
    "Cannot calculate Full Address", false);

  public static final QaMessageDescription UNIT_DESCRIPTOR_INVALID = new QaMessageDescription(
    "SP_UDINV", "Unit Descriptor not a Valid Range",
    "The Fb(UNIT_DESCRIPTOR) must be a valid range.", "Invalid Range: Vb({message})", false);

  public static final QaMessageDescription VIRTUAL_FIELD_VALUE_INVALID = new QaMessageDescription(
    "SP_VRTFVI", "Virtual Site Field Not Allowed", "Virtual Site Field Not Allowed",
    "Virtual site value must Fb({fieldName})=Vb({expectedValue}) [Vb({fieldValue})]", false) //
      .addQuickFix(new SetValue("{expectedValue}"));

  public static final QaMessageDescription VIRTUAL_FIELD_VALUE_NOT_ALLOWED = new QaMessageDescription(
    "SP_VRTFVNA", "Virtual Site Field Not Allowed", "Virtual Site Field Not Allowed",
    "Virtual site value must be null for Fb({fieldName})=Vb({fieldValue})", false) //
      .addQuickFix(FIX_SET_NULL);

  private static void addFullAddressLines(final List<String> lines, final Record site) {
    final int size = lines.size();
    // Added in reverse order so that they will appear at the top of the list.
    // This is important for sub-sites.
    final String streetAddress = SitePoint.getFullAddress(site);
    Lists.addNotContains(lines, 0, streetAddress);

    final String siteName1 = site.getString(SITE_NAME_1);
    if (Records.getBoolean(site, USE_SITE_NAME_IN_ADDRESS_IND)) {
      Lists.addNotContains(lines, 0, siteName1);
    }

    if (size == lines.size()) {
      Lists.addNotContains(lines, 0, siteName1);
    }
  }

  public static boolean setFullAddress(final AbstractRecordRule rule, final Record site) {
    final List<String> lines = new ArrayList<>();
    final Set<Record> sites = new LinkedHashSet<>();
    sites.add(site);
    for (Record parentSite = rule.loadRecord(site,
      PARENT_SITE_ID); parentSite != null; parentSite = rule.loadRecord(parentSite,
        PARENT_SITE_ID)) {
      if (sites.add(parentSite)) {
        addFullAddressLines(lines, parentSite);
      } else {
        parentSite = null;
      }
    }
    addFullAddressLines(lines, site);
    final String fullAddress = Strings.toString("\n", lines);
    if (Property.hasValue(fullAddress)) {
      if (site.setValue(FULL_ADDRESS, fullAddress)) {
        rule.addCount("Fixed", "FULL_ADDRESS updated");
      }
      return true;
    } else {
      return false;
    }
  }

  public SitePointFieldValueRule() {
  }

  @Override
  protected boolean isIgnoreFieldValueRule(final String fieldName) {
    if (TRANSPORT_LINE_ID.equals(fieldName)) {
      return true;
    } else if (PARENT_SITE_ID.equals(fieldName)) {
      return true;
    } else {
      return false;
    }
  }

  @Override
  public boolean isRequired(final Record site, final SessionField field) {
    final String fieldName = field.getName();
    if (super.isRequired(site, field)) {
      return true;
    } else if (FULL_ADDRESS.equals(fieldName)) {
      return false;
    } else if (CIVIC_NUMBER.equals(fieldName)) {
      if (site.hasValue(CIVIC_NUMBER_SUFFIX)) {
        return true;
      } else if (SitePoint.isUseInAddressRange(site)) {
        return true;
      } else if (SitePoint.isVirtual(site)) {
        return true;
      } else {
        return false;
      }
    } else if (SITE_NAME_1.equals(fieldName)) {
      if (Records.getBoolean(site, USE_SITE_NAME_IN_ADDRESS_IND)) {
        return true;
      } else if (Records.getBoolean(site, EMERGENCY_MANAGEMENT_SITE_IND)) {
        return true;
      } else {
        return false;
      }
    } else if (SITE_TYPE_CODE.equals(fieldName)) {
      if (Records.getBoolean(site, EMERGENCY_MANAGEMENT_SITE_IND)) {
        return true;
      } else {
        return false;
      }
    } else if (STREET_NAME_ID.equals(fieldName)) {
      if (site.hasValuesAny(UNIT_DESCRIPTOR, CIVIC_NUMBER, CIVIC_NUMBER_SUFFIX,
        STREET_NAME_ALIAS_1_ID)) {
        return true;
      } else if (SitePoint.isUseInAddressRange(site)) {
        return true;
      } else {
        return false;
      }
    } else {
      return false;
    }
  }

  private boolean validateCivicNumber(final Record site) {
    if (site.equalValue(CIVIC_NUMBER, 0)) {
      final String siteTypeCode = site.getValue(SITE_TYPE_CODE);
      if (SiteType.VIRTUAL_BLOCK_FROM.equals(siteTypeCode)) {
        if (SitePoint.isVirtual(site)) {
          site.setValue(SITE_TYPE_CODE, SiteType.VIRTUAL_BLOCK_FROM);
        } else {
          final Map<String, Object> data = Maps.newLinkedHash(SITE_TYPE_CODE, siteTypeCode);
          addMessage(site, CIVIC_NUMBER_0_NOT_VIRTUAL, data, CIVIC_NUMBER);
          return false;
        }
      }
    }
    return true;
  }

  private boolean validateFullAddress(final Record site) {
    final boolean valid = setFullAddress(this, site);
    if (!valid) {
      addMessage(site, NO_FULL_ADDRESS, FULL_ADDRESS);
    }
    return valid;
  }

  @Override
  protected boolean validateRecordDo(final Record site) {
    boolean valid = super.validateRecordDo(site);
    valid &= validateParentForeignKey(SiteTables.SITE_POINT, site, PARENT_SITE_ID);
    valid &= validateForeignKey(GbaTables.TRANSPORT_LINE, site, TRANSPORT_LINE_ID);

    valid &= validateUnitDescriptor(site);
    valid &= validateFullAddress(site);
    valid &= validateCivicNumber(site);

    validateVirtualSite(site);
    return valid;
  }

  private boolean validateUnitDescriptor(final Record site) {
    try {
      SitePoint.getUnitDescriptorRanges(site);
      return true;
    } catch (final RangeInvalidException e) {
      final Map<String, Object> data = Maps.newLinkedHash("message", e.getMessage());
      addMessage(site, UNIT_DESCRIPTOR_INVALID, data, UNIT_DESCRIPTOR);
      return false;
    }
  }

  private void validateVirtualSite(final Record site) {
    if (SitePoint.isVirtual(site)) {
      final Integer civicNumber = site.getInteger(CIVIC_NUMBER);
      final String siteTypeCode = SiteType.getVirtualSiteTypeCode(civicNumber);
      if (siteTypeCode != null && site.setValue(SITE_TYPE_CODE, siteTypeCode)) {
        addCount("Fixed", "Set virtual SITE_TYPE_CODE=" + siteTypeCode);
      }
      final List<String> VIRTUAL_NULL_FIELDS = Arrays.asList(PARENT_SITE_ID, STREET_NAME_ALIAS_1_ID,
        CIVIC_NUMBER_SUFFIX, ADDRESS_COMMENT, SITE_NAME_1, SITE_NAME_2, SITE_NAME_3);
      for (final String fieldName : VIRTUAL_NULL_FIELDS) {
        final Object value = site.getValue(fieldName);
        if (Property.hasValue(value)) {
          final Map<String, Object> data = Maps.newLinkedHash("fieldValue", value);
          addMessage(site, VIRTUAL_FIELD_VALUE_NOT_ALLOWED, data, fieldName);
        }
      }
      validateVirtualSiteFieldValid(site, SITE_LOCATION_CODE, "V");
      validateVirtualSiteFieldValid(site, USE_IN_ADDRESS_RANGE_IND, "Y");
      validateVirtualSiteFieldValid(site, USE_SITE_NAME_IN_ADDRESS_IND, "N");
      validateVirtualSiteFieldValid(site, EMERGENCY_MANAGEMENT_SITE_IND, "N");
      validateVirtualSiteFieldValid(site, FEATURE_STATUS_CODE, "A");
    }
  }

  private void validateVirtualSiteFieldValid(final Record site, final String fieldName,
    final Object expectedValue) {
    final Object value = site.getValue(fieldName);
    if (value == null) {
      site.setValue(fieldName, expectedValue);
    } else if (!DataType.equal(expectedValue, value)) {
      final Map<String, Object> data = Maps.newLinkedHash("fieldValue", value);
      data.put("expectedValue", expectedValue);
      addMessage(site, VIRTUAL_FIELD_VALUE_INVALID, data, fieldName);
    }
  }
}
