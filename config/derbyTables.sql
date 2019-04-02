create table Street(street_name varchar(35) primary key, between_street_1 varchar(35), between_street_2 varchar(35), area varchar(20))
create table ParkingBay(street_marker varchar(6) primary key, sign_details   varchar(50), street_id int, side_of_street int, street_name varchar(35) references Street (street_name))
create table ParkingTime(device_id int not null, arrival_time timestamp not null, departure_time timestamp, duration bigint, in_violation boolean, street_marker  varchar(6) references ParkingBay (street_marker), primary key (device_id, arrival_time))