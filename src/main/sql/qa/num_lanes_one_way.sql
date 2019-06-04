select
  transport_line_id,
  l.name as "locality_name",
  s.full_name,
  tt.description as "road_type",
  travel_direction_code,
  left_number_of_lanes,
  right_number_of_lanes,
  total_number_of_lanes
from
  gba.transport_line t
    join gba.locality_poly l on l.locality_id = t.left_locality_id 
    join gba.structured_name s on s.structured_name_id = t.structured_name_1_id
    join gba.transport_line_type_code tt on tt.transport_line_type_code = t.transport_line_type_code
where
  travel_direction_code <> 'B' and
  left_number_of_lanes is not null and
  right_number_of_lanes is not null 
order by
  locality_name,
  full_name,
  transport_line_id
