select '12',ST_asewkt('GEOMETRYCOLLECTION(POINT( 1 2 ))'::GEOMETRY);
select '13',ST_asewkt('GEOMETRYCOLLECTION(POINT( 1 2 3))'::GEOMETRY);
select '14',ST_asewkt('GEOMETRYCOLLECTION(LINESTRING( 0 0, 1 1, 2 2, 3 3 , 4 4))'::GEOMETRY);
select '15',ST_asewkt('GEOMETRYCOLLECTION(LINESTRING( 1 2 3 , 4 5 6 , 7 8 9 , 10 11 12, 13 14 15))'::GEOMETRY);
select '16',ST_asewkt('GEOMETRYCOLLECTION(POLYGON( (0 0 1, 10 0 1, 10 10 1, 0 10 1, 0 0 1),(5 5 1, 7 5 1, 7 7 1 , 5 7 1, 5 5 1) ))'::GEOMETRY);
select '17',ST_asewkt('GEOMETRYCOLLECTION(POINT( 1 2 0),POINT( 1 2 3) )'::GEOMETRY);
select '18',ST_asewkt('GEOMETRYCOLLECTION(LINESTRING( 0 0 0, 1 1 0, 2 2 0, 3 3 0, 4 4 0),POINT( 1 2 3) )'::GEOMETRY);
select '19',ST_asewkt('GEOMETRYCOLLECTION(POINT( 1 2 ),LINESTRING( 0 0, 1 1, 2 2, 3 3 , 4 4) )'::GEOMETRY);
select '20',ST_asewkt('GEOMETRYCOLLECTION(POINT( 1 2 0 ),POINT( 1 2 3),LINESTRING( 1 2 3 , 4 5 6 , 7 8 9 , 10 11 12, 13 14 15) )'::GEOMETRY);
select '21',ST_asewkt('GEOMETRYCOLLECTION(POINT( 1 2 0 ),POINT( 1 2 3),LINESTRING( 1 2 3 , 4 5 6 , 7 8 9 , 10 11 12, 13 14 15),POLYGON( (0 0 0, 10 0 0, 10 10 0, 0 10 0, 0 0 0) ) )'::GEOMETRY);
select '22',ST_asewkt('GEOMETRYCOLLECTION(POINT( 1 2 0),POINT( 1 2 3),POLYGON( (0 0 1, 10 0 1, 10 10 1, 0 10 1, 0 0 1),(5 5 1, 7 5 1, 7 7 1, 5 7 1, 5 5 1),(1 1 1,2 1 1, 2 2 1, 1 2 1, 1 1 1) ) )'::GEOMETRY);

select '36',ST_asewkt('GEOMETRYCOLLECTION(MULTIPOINT( 1 2))'::GEOMETRY);
select '37',ST_asewkt('GEOMETRYCOLLECTION(MULTIPOINT( 1 2 3))'::GEOMETRY);
select '38',ST_asewkt('GEOMETRYCOLLECTION(MULTIPOINT( 1 2 3, 5 6 7, 8 9 10, 11 12 13))'::GEOMETRY);
select '39',ST_asewkt('GEOMETRYCOLLECTION(MULTILINESTRING( (0 0, 1 1, 2 2, 3 3 , 4 4) ))'::GEOMETRY);
select '40',ST_asewkt('GEOMETRYCOLLECTION(MULTILINESTRING( (1 2 3 , 4 5 6 , 7 8 9 , 10 11 12, 13 14 15),(0 0 0, 1 1 0, 2 2 0, 3 3 0, 4 4 0),(0 0 0, 1 1 0, 2 2 0, 3 3 0, 4 4 0)))'::GEOMETRY);
select '41',ST_asewkt('GEOMETRYCOLLECTION(MULTIPOLYGON( ((0 0 0, 10 0 0, 10 10 0, 0 10 0, 0 0 0)),( (0 0 0, 10 0 0, 10 10 0, 0 10 0, 0 0 0),(5 5 0, 7 5 0, 7 7 0, 5 7 0, 5 5 0) ) ,( (0 0 1, 10 0 1, 10 10 1, 0 10 1, 0 0 1),(5 5 1, 7 5 1, 7 7 1, 5 7 1, 5 5 1),(1 1 1,2 1 1, 2 2 1, 1 2 1, 1 1 1) ) ))'::GEOMETRY);
select '42',ST_asewkt('GEOMETRYCOLLECTION(POINT( 1 2 0),MULTIPOINT( 1 2 3))'::GEOMETRY);
select '43',ST_asewkt('GEOMETRYCOLLECTION(MULTIPOINT( 1 2 0, 3 4 0, 5 6 0),POINT( 1 2 3))'::GEOMETRY);
select '44',ST_asewkt('GEOMETRYCOLLECTION(POINT( 1 2 3),MULTILINESTRING( (0 0 0, 1 1 0, 2 2 0, 3 3 0 , 4 4 0) ))'::GEOMETRY);
select '45',ST_asewkt('GEOMETRYCOLLECTION(MULTILINESTRING( (0 0 0, 1 1 0, 2 2 0, 3 3 0 , 4 4 0) ),POINT( 1 2 3))'::GEOMETRY);
select '46',ST_asewkt('GEOMETRYCOLLECTION(POINT( 1 2 3), MULTIPOLYGON( ((0 0 0, 10 0 0, 10 10 0, 0 10 0, 0 0 0)),( (0 0 0, 10 0 0, 10 10 0, 0 10 0, 0 0 0),(5 5 0, 7 5 0, 7 7 0, 5 7 0, 5 5 0) ) ,( (0 0 1, 10 0 1, 10 10 1, 0 10 1, 0 0 1),(5 5 1, 7 5 1, 7 7 1, 5 7 1, 5 5 1),(1 1 1,2 1 1, 2 2 1, 1 2 1, 1 1 1) ) ))'::GEOMETRY);
select '47',ST_asewkt('GEOMETRYCOLLECTION(MULTIPOLYGON( ((0 0 0, 10 0 0, 10 10 0, 0 10 0, 0 0 0)),( (0 0 0, 10 0 0, 10 10 0, 0 10 0, 0 0 0),(5 5 0, 7 5 0, 7 7 0, 5 7 0, 5 5 0) ) ,( (0 0 1, 10 0 1, 10 10 1, 0 10 1, 0 0 1),(5 5 1, 7 5 1, 7 7 1, 5 7 1, 5 5 1),(1 1 1,2 1 1, 2 2 1, 1 2 1, 1 1 1) ) ),MULTILINESTRING( (0 0 0, 1 1 0, 2 2 0, 3 3 0, 4 4 0),(0 0 0, 1 1 0, 2 2 0, 3 3 0, 4 4 0),(1 2 3 , 4 5 6 , 7 8 9 , 10 11 12, 13 14 15) ),MULTIPOINT( 1 2 3, 5 6 7, 8 9 10, 11 12 13))'::GEOMETRY);

select '49', ST_asewkt('GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(POINT(1 1) ))'::GEOMETRY) as geom;

