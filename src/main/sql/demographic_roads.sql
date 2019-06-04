select
  *
from
  gba.transport_line
where
  transport_line_type_code in (select transport_line_type_code from gba.transport_line_type_code where demographic_ind = 'Y')