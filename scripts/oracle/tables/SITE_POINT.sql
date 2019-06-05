CREATE TABLE  "SITE_POINT"
(
	"SITE_ID" NUMBER(10) NOT NULL,
	"PARENT_SITE_ID" NUMBER(10),
	"FULL_ADDRESS" VARCHAR2(350) NOT NULL,
	"SITE_TYPE_CODE" VARCHAR2(7),
	"SITE_LOCATION_CODE" VARCHAR2(1) NOT NULL,
	"TRANSPORT_LINE_ID" NUMBER(10),
	"LOCALITY_ID" NUMBER(5) NOT NULL,
	"REGIONAL_DISTRICT_ID" VARCHAR2(5) NOT NULL,
	"COMMUNITY_ID" NUMBER(10),
	"UNIT_DESCRIPTOR" VARCHAR2(4000),
	"CIVIC_NUMBER" NUMBER(10),
	"CIVIC_NUMBER_SUFFIX" VARCHAR2(10),
	"CIVIC_NUMBER_RANGE" VARCHAR2(50),
	"STREET_NAME_ID" NUMBER(10),
	"STREET_NAME_ALIAS_1_ID" NUMBER(10),
	"ADDRESS_COMMENT" VARCHAR2(4000),
	"USE_IN_ADDRESS_RANGE_IND" VARCHAR2(1) NOT NULL,
	"FEATURE_STATUS_CODE" VARCHAR2(1) NOT NULL,
	"EMERGENCY_MANAGEMENT_SITE_IND" VARCHAR2(1) NOT NULL,
	"USE_SITE_NAME_IN_ADDRESS_IND" VARCHAR2(1) NOT NULL,
	"SITE_NAME_1" VARCHAR2(100),
	"SITE_NAME_2" VARCHAR2(100),
	"SITE_NAME_3" VARCHAR2(100),
	"CREATE_PARTNER_ORG_ID" NUMBER(5) NOT NULL,
	"MODIFY_PARTNER_ORG_ID" NUMBER(5) NOT NULL,
	"CUSTODIAN_PARTNER_ORG_ID" NUMBER(5),
	"OPEN_DATA_IND" VARCHAR2(1) NOT NULL,
	"CAPTURE_DATE" TIMESTAMP(6),
	"EXTENDED_DATA" VARCHAR2(4000),
	"EXCLUDED_RULES" VARCHAR2(4000),
	"CUSTODIAN_SESSION_ID" NUMBER(10),
	"DATA_CAPTURE_METHOD_CODE" VARCHAR2(30) NOT NULL,
	"GEOMETRY" MDSYS.SDO_GEOMETRY NOT NULL,
	"CREATE_INTEGRATION_SESSION_ID" NUMBER(10) NOT NULL,
	"MODIFY_INTEGRATION_SESSION_ID" NUMBER(10) NOT NULL,
	CONSTRAINT "GBA_SP_PK" PRIMARY KEY ("SITE_ID"),
	CONSTRAINT "GBA_SP_C_GBA_ISP_FK" FOREIGN KEY ("CREATE_INTEGRATION_SESSION_ID") REFERENCES  "INTEGRATION_SESSION_POLY" ("INTEGRATION_SESSION_POLY_ID"),
	CONSTRAINT "GBA_SP_M_GBA_ISP_FK" FOREIGN KEY ("MODIFY_INTEGRATION_SESSION_ID") REFERENCES  "INTEGRATION_SESSION_POLY" ("INTEGRATION_SESSION_POLY_ID"),
	CONSTRAINT "GBA_SP_GBA_DCMC_FK" FOREIGN KEY ("DATA_CAPTURE_METHOD_CODE") REFERENCES  "DATA_CAPTURE_METHOD_CODE" ("DATA_CAPTURE_METHOD_CODE"),
	CONSTRAINT "GBA_SP_GBA_FSC_FK" FOREIGN KEY ("FEATURE_STATUS_CODE") REFERENCES  "FEATURE_STATUS_CODE" ("FEATURE_STATUS_CODE"),
	CONSTRAINT "GBA_SP_C_GBA_PO_FK" FOREIGN KEY ("CREATE_PARTNER_ORG_ID") REFERENCES  "PARTNER_ORGANIZATION" ("PARTNER_ORGANIZATION_ID"),
	CONSTRAINT "GBA_SP_M_GBA_PO_FK" FOREIGN KEY ("MODIFY_PARTNER_ORG_ID") REFERENCES  "PARTNER_ORGANIZATION" ("PARTNER_ORGANIZATION_ID"),
	CONSTRAINT "GBA_SP_U_GBA_PO_FK" FOREIGN KEY ("CUSTODIAN_PARTNER_ORG_ID") REFERENCES  "PARTNER_ORGANIZATION" ("PARTNER_ORGANIZATION_ID"),
	CONSTRAINT "GBA_SP_GBA_SLC_FK" FOREIGN KEY ("SITE_LOCATION_CODE") REFERENCES  "SITE_LOCATION_CODE" ("SITE_LOCATION_CODE"),
	CONSTRAINT "GBA_SP_GBA_STC_FK" FOREIGN KEY ("SITE_TYPE_CODE") REFERENCES  "SITE_TYPE_CODE" ("SITE_TYPE_CODE"),
	CONSTRAINT "GBA_SP_GBA_SN_FK" FOREIGN KEY ("STREET_NAME_ID") REFERENCES  "STRUCTURED_NAME" ("STRUCTURED_NAME_ID"),
	CONSTRAINT "GBA_SP_1_GBA_SN_FK" FOREIGN KEY ("STREET_NAME_ALIAS_1_ID") REFERENCES  "STRUCTURED_NAME" ("STRUCTURED_NAME_ID"),
	CONSTRAINT "GBA_SP_GBA_TL_FK" FOREIGN KEY ("TRANSPORT_LINE_ID") REFERENCES  "TRANSPORT_LINE" ("TRANSPORT_LINE_ID"),
	CONSTRAINT "GBA_SP_GBA_CP_FK" FOREIGN KEY ("COMMUNITY_ID") REFERENCES  "COMMUNITY_POLY" ("COMMUNITY_ID"),
	CONSTRAINT "GBA_SP_U_GBA_ISP_FK" FOREIGN KEY ("CUSTODIAN_SESSION_ID") REFERENCES  "INTEGRATION_SESSION_POLY" ("INTEGRATION_SESSION_POLY_ID"),
	CONSTRAINT "GBA_SP_GBA_LP_FK" FOREIGN KEY ("LOCALITY_ID") REFERENCES  "LOCALITY_POLY" ("LOCALITY_ID"),
	CONSTRAINT "GBA_SP_GBA_RDP_FK" FOREIGN KEY ("REGIONAL_DISTRICT_ID") REFERENCES  "REGIONAL_DISTRICT_POLY" ("REGIONAL_DISTRICT_ID"),
	CONSTRAINT "GBA_SP_GBA_SP_FK" FOREIGN KEY ("PARENT_SITE_ID") REFERENCES  "SITE_POINT" ("SITE_ID")
)
;
CREATE INDEX "GBA_SP_C_GBA_ISP_FK_I"
 ON  "SITE_POINT" ("CREATE_INTEGRATION_SESSION_ID") 
TABLESPACE	GBA_INDEXES
;
CREATE INDEX "GBA_SP_M_GBA_ISP_FK_I"
 ON  "SITE_POINT" ("MODIFY_INTEGRATION_SESSION_ID") 
TABLESPACE	GBA_INDEXES
;

COMMENT ON TABLE  "SITE_POINT" IS 'The spatial layer SITE POINT is a non multi-part point feature that represents the point location of a geographic site. Sites can be nested using the PARENT SITE ID.'
;

COMMENT ON COLUMN  "SITE_POINT"."SITE_ID" IS 'The SITE ID is a unique identifier for the SITE_POINT. This will not change unless the site is deleted and replaced with a new site.'
;

COMMENT ON COLUMN  "SITE_POINT"."PARENT_SITE_ID" IS 'The PARENT_SITE_ID is the SITE_ID of the parent site. For example; a unit within a strata complex would have a parent site for the whole strata complex.'
;

COMMENT ON COLUMN  "SITE_POINT"."FULL_ADDRESS" IS 'The FULL ADDRESS is the full address of this site including the UNIT DESCRIPTOR, CIVIC NUMBER, [STREET_NAME_ID.FULL_NAME] and any FULL ADDRESS from the parent. It does not include the city, province and postal code.'
;

COMMENT ON COLUMN  "SITE_POINT"."SITE_TYPE_CODE" IS 'The SITE TYPE CODE is a unique code that indicates the type of site. For example; AIR_HLI=Air Heliport, AMB_STN=Ambulance Station, COM_IND=Commercial Industrial, PRL_MNI=Park Municipal, RES_SFH=Residential Single Family, TRL_TRH=Trail Head.'
;

COMMENT ON COLUMN  "SITE_POINT"."SITE_LOCATION_CODE" IS 'The SITE LOCATION CODE is a unique code that indicates the conceptual location of the point geometry in relation to the site. For example; A=Access, B=Back Door, F=Front Door, P=Parcel, R=Rooftop.'
;

COMMENT ON COLUMN  "SITE_POINT"."TRANSPORT_LINE_ID" IS 'The TRANSPORT LINE ID is a unique surrogate identifier for the object TRANSPORT LINE that the UNIT DESCRIPTOR or CIVIC NUMBER is used for the house number ranges. Allowed only if USE IN ADDRESS RANGE IND=Y.'
;

COMMENT ON COLUMN  "SITE_POINT"."LOCALITY_ID" IS 'The LOCALITY ID is a unique surrogate identifier for the object LOCALITY POLY.'
;

COMMENT ON COLUMN  "SITE_POINT"."REGIONAL_DISTRICT_ID" IS 'The REGIONAL DISTRICT ID is a unique surrogate identifier for the object REGIONALDISTRICT POLY.'
;

COMMENT ON COLUMN  "SITE_POINT"."COMMUNITY_ID" IS 'The COMMUNITY ID is a unique surrogate identifier for the object COMMUNITY POLY.'
;

COMMENT ON COLUMN  "SITE_POINT"."UNIT_DESCRIPTOR" IS 'The UNIT DESCRIPTOR is a single unit number of an apartment or unit in a multi-tenant site. It can also be a list of units separated by a comma (e.g. 1,2) or a range separated by a ~ (e.g. 1~10, A~F).'
;

COMMENT ON COLUMN  "SITE_POINT"."CIVIC_NUMBER" IS 'The CIVIC NUMBER is the numeric street address given to a house, building or lot.'
;

COMMENT ON COLUMN  "SITE_POINT"."CIVIC_NUMBER_SUFFIX" IS 'The CIVIC NUMBER SUFFIX is a suffix applied to the CIVIC NUMBER. For example; A, B, or 1/2.'
;

COMMENT ON COLUMN  "SITE_POINT"."CIVIC_NUMBER_RANGE" IS 'The Civic Number Range is used where the site represents a range of Civic Numbers. For example; 1000~1002. '
;

COMMENT ON COLUMN  "SITE_POINT"."STREET_NAME_ID" IS 'The STREET NAME ID is the identifier STRUCTURED NAME that is the primary street name used for the addressing of this site.'
;

COMMENT ON COLUMN  "SITE_POINT"."STREET_NAME_ALIAS_1_ID" IS 'The STREET NAME ALIAS 1 ID is the identifier STRUCTURED NAME that is the first alias street name used for the addressing of this site.'
;

COMMENT ON COLUMN  "SITE_POINT"."ADDRESS_COMMENT" IS 'The ADDRESS COMMENT is a free form field to provide an additional comment about the addressing on the site. This should only be used if there is something odd about the addressing.'
;

COMMENT ON COLUMN  "SITE_POINT"."USE_IN_ADDRESS_RANGE_IND" IS 'The USE IN ADDRESS RANGE IND is the true (Y), false (N) indicator that the CIVIC NUMBER or UNIT DESCRIPTOR should be used in the house number range for a TRANSPORT LINE.'
;

COMMENT ON COLUMN  "SITE_POINT"."FEATURE_STATUS_CODE" IS 'The FEATURE STATUS CODE is a unique code that indicates the status of a spatial feature (record). For example; A=Active, P=Planned, R=Retired.'
;

COMMENT ON COLUMN  "SITE_POINT"."EMERGENCY_MANAGEMENT_SITE_IND" IS 'The EMERGENCY MANAGEMENT SITE IND is the true (Y), false (N) indicator that the site is to be included in the emergency management site export.'
;

COMMENT ON COLUMN  "SITE_POINT"."USE_SITE_NAME_IN_ADDRESS_IND" IS 'The USE_SITE_NAME_IN_ADDRESS_IND is the true (Y), false (N) indicator that the SITE NAME 1 is to be used in the FULL ADDRESS. For example; this could be to include a building name.'
;

COMMENT ON COLUMN  "SITE_POINT"."SITE_NAME_1" IS 'The SITE NAME 1 is the first name for the site. For example; a building name or police station name.'
;

COMMENT ON COLUMN  "SITE_POINT"."SITE_NAME_2" IS 'The SITE NAME 2 is the second name for the site.'
;

COMMENT ON COLUMN  "SITE_POINT"."SITE_NAME_3" IS 'The SITE NAME 3 is the third name for the site.'
;

COMMENT ON COLUMN  "SITE_POINT"."CREATE_PARTNER_ORG_ID" IS 'The PARTNER ORGANIZATION ID is a unique surrogate identifier for the object PARTNER ORGANIZATION.'
;

COMMENT ON COLUMN  "SITE_POINT"."MODIFY_PARTNER_ORG_ID" IS 'The PARTNER ORGANIZATION ID is a unique surrogate identifier for the object PARTNER ORGANIZATION.'
;

COMMENT ON COLUMN  "SITE_POINT"."CUSTODIAN_PARTNER_ORG_ID" IS 'The PARTNER ORGANIZATION ID is a unique surrogate identifier for the object PARTNER ORGANIZATION.'
;

COMMENT ON COLUMN  "SITE_POINT"."OPEN_DATA_IND" IS 'The OPEN DATA IND is the true (Y), false (N) indicator that the provider for the site provides the data under an open data license.'
;

COMMENT ON COLUMN  "SITE_POINT"."CAPTURE_DATE" IS 'The CAPTURE DATE is the date the geometry was originally captured in the field (e.g. GPS date).'
;

COMMENT ON COLUMN  "SITE_POINT"."EXTENDED_DATA" IS 'The EXTENDED DATA is a JSON encoded object or key=value pairs for any additional data describing the site. This provides an extension mechanism for the model.'
;

COMMENT ON COLUMN  "SITE_POINT"."EXCLUDED_RULES" IS 'The EXCLUDED RULES is the list of rules and parameters for those rules that are excluded for this record. This allows overriding rules in specific cases.'
;

COMMENT ON COLUMN  "SITE_POINT"."CUSTODIAN_SESSION_ID" IS 'The CUSTODIAN SESSION ID is the identifier of the INTEGRATION_SESSION_POLY where the custodian last modified the site fields.'
;

COMMENT ON COLUMN  "SITE_POINT"."DATA_CAPTURE_METHOD_CODE" IS 'The DATA CAPTURE METHOD CODE is a code that indicates the method used to capture the geometry of the feature. For example photogrammetric=Photogrammetric, differentialGPS=Differential GPS, coordinateGeometryWithControl=Coordinate Geometry With Control, orthoPhotography=Ortho Photography, monoRestitution=Mono Restitution, satelliteImagery=Satellite Imagery, tabletDigitizing=Tablet Digitizing, scanning=Scanning, sketchMap=Sketch Map, nondifferentialGPS=Non-differential GPS, rubberSheeting=Rubber Sheeting, unknown=Unknown, geodeticSurvey=Geodetic Survey, tightChainTraverse=Tight Chain Traverse, variable=Variable, cadastre=Cadastre.'
;

COMMENT ON COLUMN  "SITE_POINT"."GEOMETRY" IS 'The GEOMETRY is the spatial point location of the feature.'
;

COMMENT ON COLUMN  "SITE_POINT"."CREATE_INTEGRATION_SESSION_ID" IS 'CREATE_INTEGRATION_SESSION_ID is the INTEGRATION SESSION POLY where the record was first created.'
;

COMMENT ON COLUMN  "SITE_POINT"."MODIFY_INTEGRATION_SESSION_ID" IS 'MODIFY_INTEGRATION_SESSION_ID is the INTEGRATION SESSION POLY where the record was last modified.'
;

	
	
CREATE SEQUENCE "SITE_POINT_SEQ" 
	INCREMENT BY 1 
	START WITH 1 
	NOMAXVALUE 
	MINVALUE  1 
	NOCYCLE 
	NOCACHE 
	NOORDER
;

GRANT SELECT ON "SITE_POINT_SEQ" TO GBA_USER
;

GRANT DELETE,INSERT,SELECT,UPDATE ON  "SITE_POINT" TO gba_user
;

GRANT SELECT ON  "SITE_POINT" TO gba_viewer
;