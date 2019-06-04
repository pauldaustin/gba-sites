CREATE INDEX GBA_TL_GBA_FSC_FK_I ON TRANSPORT_LINE(FEATURE_STATUS_CODE) TABLESPACE GBA_INDEXES;

ALTER TABLE TRANSPORT_LINE_NODE_POINT DROP INTERSECTION_REQUIRED_IND

"FEATURE_STATUS_CODE" VARCHAR2(1) NOT NULL,
    "DEMOGRAPHIC_IND" VARCHAR2(1) NOT NULL,

COMMENT ON COLUMN  "TRANSPORT_LINE"."RESOURCE_ROAD_FILE_ID" IS 'File identification assigned to Provincial Forest Use files. Assigned file number. Usually the Licence, Tenure or Private Mark number.'
;

COMMENT ON COLUMN  "TRANSPORT_LINE"."RESOURCE_ROAD_SECTION_ID" IS 'Resource Road Section Id is the identifier of a section of resource road within a permit (Resource Road File Id).'
;

COMMENT ON COLUMN  "TRANSPORT_LINE"."RESOURCE_ROAD_NAME" IS 'The name (if applicable) given to the road. (e.g Morice River F.S.R.)'
;

COMMENT ON COLUMN  "TRANSPORT_LINE"."FROM_TRANSPORT_NODE_POINT_ID" IS 'From Transport Line Node Point Id is the identifier of the Transport Line Node Point that is at the start of the Transport Line geometry.'
;

COMMENT ON COLUMN  "TRANSPORT_LINE"."TO_TRANSPORT_NODE_POINT_ID" IS 'To Transport Line Node Point Id is the identifier of the Transport Line Node Point that is at the end of the Transport Line geometry.'
;

COMMENT ON COLUMN  "TRANSPORT_LINE"."FEATURE_STATUS_CODE" IS 'The FEATURE STATUS CODE is a unique code that indicates the status of a spatial feature (record). For example; A=Active, P=Planned, R=Retired.'
;
,
  CONSTRAINT "GBA_TL_GBA_FSC_FK" FOREIGN KEY ("FEATURE_STATUS_CODE") REFERENCES  "FEATURE_STATUS_CODE" ("FEATURE_STATUS_CODE")
  
    "FROM_TRANSPORT_NODE_POINT_ID" NUMBER(10) NOT NULL,
  "TO_TRANSPORT_NODE_POINT_ID" NUMBER(10) NOT NULL,
  CONSTRAINT "GBA_TL_F_GBA_TDC_FK" FOREIGN KEY ("FROM_TRANSPORT_NODE_POINT_ID") REFERENCES  "TRANSPORT_LINE_NODE_POINT" ("TRANSPORT_LINE_NODE_POINT_ID"),
  CONSTRAINT "GBA_TL_T_GBA_TDC_FK" FOREIGN KEY ("TO_TRANSPORT_NODE_POINT_ID") REFERENCES  "TRANSPORT_LINE_NODE_POINT" ("TRANSPORT_LINE_NODE_POINT_ID"),
