select
  transport_line_id,
  l.name as "locality_name",
  s.full_name,
  tt.description as "road_type",
  single_house_number,
  left_house_num_scheme_code,
  from_left_house_number,
  to_left_house_number,
  right_house_num_scheme_code,
  from_right_house_number,
  to_right_house_number
from
  gba.transport_line t
    join gba.locality_poly l on l.locality_id = t.left_locality_id 
    join gba.structured_name s on s.structured_name_id = t.structured_name_1_id
    join gba.transport_line_type_code tt on tt.transport_line_type_code = t.transport_line_type_code
where
  not t.transport_line_type_code  in ('R', 'RST', 'RRT', 'T', 'TS') and
  single_house_number is not null
order by
  locality_name,
  full_name,
  transport_line_id