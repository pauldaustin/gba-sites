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
  t.left_locality_id  is not null and (
    (left_house_num_scheme_code <> 'N' and (from_left_house_number is null or to_left_house_number is null)) or
    (right_house_num_scheme_code <> 'N' and (from_right_house_number is null or to_right_house_number is null)) or
    left_house_num_scheme_code IS NULL or
    right_house_num_scheme_code IS NULL
  )
order by
  locality_name,
  full_name,
  transport_line_id