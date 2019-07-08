package ca.bc.gov.gbasites.load.convert;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import ca.bc.gov.gbasites.load.common.IgnoreSiteException;
import ca.bc.gov.gbasites.load.common.SitePointProviderRecord;

import com.revolsys.collection.map.MapEx;
import com.revolsys.collection.set.Sets;
import com.revolsys.geometry.model.Point;
import com.revolsys.record.Record;
import com.revolsys.util.Property;

public class SiteConverterAddress extends AbstractSiteConverter {
  private static final Pattern UNIT_RANGE_PATTERN = Pattern
    .compile("(?:#|EVEN |ODD )?(\\d+-\\d+) (.+)");

  private static final Set<String> IGNORE_UNIT_DESCRIPTORS_WITHOUT_CIVIC_NUMBER = Sets
    .newHash("LANE", "BCH&P", "BC");

  public static Function<MapEx, SiteConverterAddress> newFactory(
    final Map<String, ? extends Object> config) {
    return properties -> new SiteConverterAddress(properties.addAll(config));
  }

  public SiteConverterAddress() {
  }

  public SiteConverterAddress(final Map<String, ? extends Object> properties) {
    setProperties(properties);
  }

  @Override
  public SitePointProviderRecord convertRecordSite(final Record sourceRecord, final Point point) {
    final String addressFieldName = getAddressFieldName();
    final String fullAddress = getUpperString(sourceRecord, addressFieldName);
    if (Property.hasValue(fullAddress)) {
      final SitePointProviderRecord sitePoint = newSitePoint(this, point);
      setFeatureStatusCodeByFullAddress(sitePoint, fullAddress);
      String unitDescriptor = "";
      Integer civicNumber = null;
      final String civicNumberSuffix = "";
      String originalStretName = "";
      if (Character.isDigit(fullAddress.charAt(0))) {
        final int index = fullAddress.indexOf(' ');
        final String streetNumber = fullAddress.substring(0, index);
        try {
          civicNumber = Integer.valueOf(streetNumber);
          originalStretName = fullAddress.substring(index + 1);
          final Matcher matcher = UNIT_RANGE_PATTERN.matcher(originalStretName);
          if (matcher.matches()) {
            unitDescriptor = matcher.group(1);
            originalStretName = matcher.group(2);
          }
        } catch (final NumberFormatException e) {
          originalStretName = fullAddress;
        }
      } else {
        originalStretName = fullAddress;
      }
      String structuredName = originalStretName;
      structuredName = structuredName.replace(" (ACC RTE)", "");
      if (Property.isEmpty(structuredName)) {
        throw IgnoreSiteException.warning(AbstractSiteConverter.IGNORE_STREET_NAME_NOT_SPECIFIED);
      } else if ((civicNumber == null || civicNumber == 0) && (!Property.hasValue(unitDescriptor)
        || IGNORE_UNIT_DESCRIPTORS_WITHOUT_CIVIC_NUMBER.contains(unitDescriptor))) {
        final String message = "Ignore CIVIC_NUMBER not specified";
        throw IgnoreSiteException.warning(message);
      }
      sitePoint.setValue(CUSTODIAN_FULL_ADDRESS, fullAddress);
      sitePoint.setValue(UNIT_DESCRIPTOR, unitDescriptor);
      sitePoint.setValue(CIVIC_NUMBER, civicNumber);
      sitePoint.setValue(CIVIC_NUMBER_SUFFIX, civicNumberSuffix);

      if (!setStructuredName(sourceRecord, sitePoint, 0, structuredName, structuredName)) {
        throw IgnoreSiteException.warning("STRUCTURED_NAME ignored");
      }
      setCustodianSiteId(sitePoint, sourceRecord);
      return sitePoint;
    } else {
      throw IgnoreSiteException.warning("Ignore FULL_ADDRESS not specified");
    }
  }

}
