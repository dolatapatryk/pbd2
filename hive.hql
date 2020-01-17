drop database if exists traffic cascade;

create database traffic;

use traffic;

create table weathers(id int, weather string) STORED AS ORC;

create table vehicle_types(type_id int, type_name string) STORED AS ORC;

create table dates (id int, year int, month int, day int) STORED AS ORC;

create table authorities (local_authority_ons_code string, local_authority_name string, region_ons_code string, region_name string) STORED AS ORC;

create table facts (local_authority_ons_code string, weather_id int, date_id int, hour int, vehicle_type_id int, count int) STORED AS ORC;
