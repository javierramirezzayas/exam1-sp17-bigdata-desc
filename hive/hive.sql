-- Table definitions in Hive
create table studentspr(school_region string, school_district string, school_id int, school_name string, school_level string, student_gender char(1), student_id int) row format delimited fields terminated by ',';

create table schoolspr(school_region string, school_district string, city string, school_id int, school_name string, school_level string, college_board_id int) row format delimited fields terminated by ',';


-- 1) Find the total number of students per region and city
select A.school_region, B.city, count(*)
from studentspr as A, schoolspr as B
where A.school_id = B.school_id
group by A.school_region, B.city;

-- 2) Find the total number of schools per city and level
select A.city, A.school_level, count(*)
from schoolspr as A
group by A.city, A.school_level;

-- 3) Find the total number of female students from Ponce that go to a 'Superior' level school
select count(*), B.city
from studentspr as A, schoolspr as B
where A.school_id = B.school_id
and A.student_gender = 'F' 
and B.city = 'Ponce'
and B.school_level = 'Superior';

-- 4) Find the total number of male students per region, district and city.
select A.school_region, A.school_district, B.city, count(*)
from studentspr as A, schoolspr as B
where A.school_id = B.school_id
and A.student_gender = 'M'
group by A.school_region, A.school_district, B.city;
