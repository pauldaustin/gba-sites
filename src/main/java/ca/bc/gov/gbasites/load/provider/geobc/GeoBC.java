package ca.bc.gov.gbasites.load.provider.geobc;

import java.nio.file.Path;

import org.jeometry.common.data.identifier.Identifier;

import ca.bc.gov.gba.model.type.code.PartnerOrganization;
import ca.bc.gov.gbasites.model.type.SitePoint;

public interface GeoBC {

  public static final String NAME = "GeoBC";

  public static final String COUNT_PREFIX = "G ";

  Path DIRECTORY = SitePoint.SITES_DIRECTORY //
    .resolve(NAME);

  String FILE_SUFFIX = "_GEOBC";

  public static final String PARTNER_ORGANIZATION_NAME = NAME;

  public static final PartnerOrganization PARTNER_ORGANIZATION = new PartnerOrganization(
    Identifier.newIdentifier(3), PARTNER_ORGANIZATION_NAME, PARTNER_ORGANIZATION_NAME);
}
