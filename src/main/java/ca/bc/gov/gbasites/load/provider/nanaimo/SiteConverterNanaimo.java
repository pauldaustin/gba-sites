package ca.bc.gov.gbasites.load.provider.nanaimo;

import java.util.Map;
import java.util.regex.Pattern;

import ca.bc.gov.gbasites.load.common.SitePointProviderRecord;
import ca.bc.gov.gbasites.load.converter.SiteConverterParts;

import com.revolsys.geometry.model.Point;
import com.revolsys.record.Record;
import com.revolsys.util.Strings;

public class SiteConverterNanaimo extends SiteConverterParts {
  private static final Pattern NANAIMO_PATTERN = Pattern
    .compile(" (NANAIMO$|NANAIMO BC$|NANAIMO BC.*)");

  public SiteConverterNanaimo() {
  }

  public SiteConverterNanaimo(final Map<String, ? extends Object> properties) {
    setProperties(properties);
  }

  @Override
  public SitePointProviderRecord convert(final Record sourceRecord, final Point point) {
    final String addressFieldName = getAddressFieldName();
    String fullAddress = sourceRecord.getString(addressFieldName);
    if (fullAddress != null) {
      fullAddress = Strings.replaceAll(fullAddress, NANAIMO_PATTERN, "");
      sourceRecord.setValue(addressFieldName, fullAddress);
    }
    return super.convert(sourceRecord, point);
  }
}
