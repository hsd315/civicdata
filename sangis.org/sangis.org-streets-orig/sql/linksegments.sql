ATTACH DATABASE 'build/sangis.org/streets-orig-429e-r1/roads.db' as r;
ATTACH DATABASE 'build/sangis.org/streets-orig-429e-r1/addresses.db' as a;
ATTACH DATABASE 'build/sangis.org/streets-orig-429e-r1/intersections.db' as i;
.headers on
.mode columns

# Remove road segments that can't have addresses
-- AND  segclass NOT IN ('7','A','1','Z','9','P','8','H','2','W')

-- Roads that are missing a to node
SELECT "--- Roads that are missing a To node";
SELECT rt.objectid, rt.roadsegid, rt.rd20name, inter.interid
FROM r.roads AS rt
LEFT JOIN i.intersections AS inter ON rt.tnode = inter.interid
WHERE inter.objectid IS NULL;

-- Roads that are missing to from node
SELECT "--- Roads that are missing a From node";
SELECT rf.objectid, rf.roadsegid, rf.rd20name, inter.interid
FROM r.roads AS rf
LEFT JOIN i.intersections AS inter ON rf.fnode = inter.interid
WHERE inter.objectid IS NULL;

-- Roads that aren't connected to anything
-- These can be errors, or they can be the end of the road. 
SELECT rt.objectid, rt.rd20name, inter.interid, rf.objectid, rf.rd20name
FROM r.roads AS rt
LEFT JOIN i.intersections AS inter ON rt.tnode = inter.interid
LEFT JOIN r.roads AS rf ON rf.fnode = inter.interid
WHERE rf.objectid IS NULL;



-- Similar to above, by skip the intersection and connect the from to the to
SELECT rt.objectid, rt.rd20name, rf.objectid, rf.rd20name
FROM r.roads AS rt
LEFT JOIN r.roads AS rf ON rf.fnode = rt.tnode
WHERE rf.objectid IS NOT NULL;

SELECT rt.abhiaddr, rf.abloaddr, rt.objectid, rt.rd20name, inter.interid, rf.objectid, rf.rd20name
FROM r.roads AS rt
LEFT JOIN i.intersections AS inter ON rt.tnode = inter.interid
LEFT JOIN r.roads AS rf ON rf.fnode = inter.interid
WHERE rf.objectid IS NOT NULL
AND rf.rd20full = rt.rd20full
AND rf.abloaddr = 0 AND rt.abhiaddr != 0;

-- Skip the intersection
SELECT count(*)
FROM r.roads AS rt, r.roads AS rf 
WHERE rt.tnode = rf.fnode
AND rf.rd20full = rt.rd20full
AND rf.lpsjur = rt.lpsjur
AND rt.segclass NOT IN ('7','A','1','Z','9','P','8','H','2','4','W')
AND rf.segclass NOT IN ('7','A','1','Z','9','P','8','H','2','4','W')
AND rf.abloaddr = 0 AND rt.abhiaddr != 0;

-- Count using the intersection links
select count(*)
FROM r.roads AS rt
LEFT JOIN i.intersections AS inter ON rt.tnode = inter.interid
LEFT JOIN r.roads AS rf ON rf.fnode = inter.interid
WHERE rf.objectid IS NOT NULL
AND rf.rd20full = rt.rd20full
AND rf.abloaddr != 0 AND rt.abhiaddr = 0;


-- Skip the intersection
SELECT count(*)
FROM r.roads AS rt, r.roads AS rf 
WHERE rt.tnode = rf.fnode
AND rf.rd20full = rt.rd20full
AND rt.segclass NOT IN ('7','A','1','Z','9','P','8','H','2','4','W')
AND rf.segclass NOT IN ('7','A','1','Z','9','P','8','H','2','4','W')
AND rf.abloaddr != 0 AND rt.abhiaddr = 0;

select distinct(rd20name) from roads where abhiaddr = 0 and abloaddr = 0 AND rd20name != 'ALLEY' and rd20name != 'PRIVATE' and rd20name not like 'I-%' and rd20name not like 'SR-%' not like 'UNNAMED%';




