CREATE TABLE  "COMMUNITY_POLY"
(
	"COMMUNITY_ID" NUMBER(10) NOT NULL,
	"NAME" VARCHAR2(50) NOT NULL,
	"GEOMETRY" MDSYS.SDO_GEOMETRY NOT NULL,
	"CREATE_INTEGRATION_SESSION_ID" NUMBER(10) NOT NULL,
	"MODIFY_INTEGRATION_SESSION_ID" NUMBER(10) NOT NULL,
	CONSTRAINT "GBA_CP_PK" PRIMARY KEY ("COMMUNITY_ID"),
	CONSTRAINT "GBA_CP_C_GBA_ISP_FK" FOREIGN KEY ("CREATE_INTEGRATION_SESSION_ID") REFERENCES  "INTEGRATION_SESSION_POLY" ("INTEGRATION_SESSION_POLY_ID"),
	CONSTRAINT "GBA_CP_M_GBA_ISP_FK" FOREIGN KEY ("MODIFY_INTEGRATION_SESSION_ID") REFERENCES  "INTEGRATION_SESSION_POLY" ("INTEGRATION_SESSION_POLY_ID")
)
;
CREATE INDEX "GBA_CP_C_GBA_ISP_FK_I"
 ON  "COMMUNITY_POLY" ("CREATE_INTEGRATION_SESSION_ID") 
TABLESPACE	GBA_INDEXES
;
CREATE INDEX "GBA_CP_M_GBA_ISP_FK_I"
 ON  "COMMUNITY_POLY" ("MODIFY_INTEGRATION_SESSION_ID") 
TABLESPACE	GBA_INDEXES
;

COMMENT ON TABLE  "COMMUNITY_POLY" IS 'The spatial layer COMMUNITY POLY is a multi-part polygon feature that represents the boundaries of the communities within localities (e.g. Langford, Westlynn). These are used for an alternate name from the LOCALITY POLY for a SITE POINT.'
;

COMMENT ON COLUMN  "COMMUNITY_POLY"."COMMUNITY_ID" IS 'The COMMUNITY POLY ID is a unique surrogate identifier for the object COMMUNITY POLY.'
;

COMMENT ON COLUMN  "COMMUNITY_POLY"."NAME" IS 'The NAME is the name of a COMMUNITY POLY (e.g. Langford, Westlynn).'
;

COMMENT ON COLUMN  "COMMUNITY_POLY"."GEOMETRY" IS 'The GEOMETRY is the Oracle SDO_GEOMETRY containing the spatial multi-polygon location of the feature.'
;

COMMENT ON COLUMN  "COMMUNITY_POLY"."CREATE_INTEGRATION_SESSION_ID" IS 'CREATE_INTEGRATION_SESSION_ID is the INTEGRATION SESSION POLY where the record was first created.'
;

COMMENT ON COLUMN  "COMMUNITY_POLY"."MODIFY_INTEGRATION_SESSION_ID" IS 'MODIFY_INTEGRATION_SESSION_ID is the INTEGRATION SESSION POLY where the record was last modified.'
;

	
	
CREATE SEQUENCE "COMMUNITY_POLY_SEQ" 
	INCREMENT BY 1 
	START WITH 1 
	NOMAXVALUE 
	MINVALUE  1 
	NOCYCLE 
	NOCACHE 
	NOORDER
;

GRANT SELECT ON "COMMUNITY_POLY_SEQ" TO GBA_USER
;

GRANT DELETE,INSERT,SELECT,UPDATE ON  "COMMUNITY_POLY" TO gba_user
;

GRANT SELECT ON  "COMMUNITY_POLY" TO gba_viewer
;