


select count(*) FROM segments as s1
LEFT JOIN segments as s2 on s1.node1_source_id = s2.node2_source_id;

select count(*) FROM segments as s1
LEFT JOIN segments as s2 ON s1.node1_source_id = s2.node1_source_id
WHERE s1.segments_id != s2.segments_id
AND s1.road_source_id = s2.road_source_id
AND s1.street = s2.street AND s1.street_dir = s2.street_dir;

CREATE VIEW segments_link_to_bad_address AS
select s2.segments_id, 
s1.lnumber AS lnumber1, s1.hnumber AS hnumber1, 
s2.lnumber AS lnumber2, s2.hnumber AS hnumber2,
s1.street_dir,  s1.street
FROM segments as s1, segments as s2
WHERE s1.node2_source_id = s2.node1_source_id
AND s1.segments_id != s2.segments_id
AND s1.road_source_id = s2.road_source_id
AND s1.has_addresses AND s2.has_addresses
AND s1.street = s2.street AND (s1.street_dir == s2.street_dir OR 
                            (s1.street_dir IS NULL AND s2.street_dir IS NULL))
AND s2.lnumber = 0;



select s2.segments_id, s2.road_source_id,
s1.lnumber AS lnumber1, s1.hnumber AS hnumber1, 
s2.lnumber AS lnumber2, s2.hnumber AS hnumber2,
s1.street_dir,  s1.street
FROM segments as s1, segments as s2
WHERE s1.node1_source_id = s2.node1_source_id
AND s1.segments_id != s2.segments_id
AND s1.road_source_id = s2.road_source_id
AND s1.has_addresses AND s2.has_addresses
AND s1.hnumber < s2.hnumber
AND s2.lnumber = 0;


--- For s1 where lnumber is zero, find all of the road segments that have
--- a lower high number. The first of these should be one less than the 
--- missing low number in s1

SELECT *, max(hnumber2)
FROM (
    SELECT s1.segments_id, s2.road_source_id,
    s2.lnumber AS lnumber2, s2.hnumber AS hnumber2,
    s1.lnumber AS lnumber1, s1.hnumber AS hnumber1, 
    s1.street_dir,  s1.street
    FROM segments as s1, segments as s2
    WHERE s1.road_source_id = s2.road_source_id
    AND s1.lnumber = 0
    AND s2.hnumber != 0
    AND s2.hnumber < s1.hnumber
    ORDER BY s1.road_source_id, s2.hnumber DESC 
) 
GROUP BY road_source_id;

select s2.segments_id, s2.road_source_id,
s1.lnumber AS lnumber1, s1.hnumber AS hnumber1, 
s2.lnumber AS lnumber2, s2.hnumber AS hnumber2,
s1.street_dir,  s1.street, s2.street, s1.lcity, s1.rcity, s2.lcity, s2.rcity
FROM segments as s1, segments as s2
WHERE s1.node1_source_id = s2.node2_source_id
AND s1.segments_id != s2.segments_id
AND s1.has_addresses AND s2.has_addresses
AND s1.hnumber < s2.hnumber
AND s1.lcity = s2.lcity AND s1.rcity = s2.rcity
AND s2.lnumber = 0;






