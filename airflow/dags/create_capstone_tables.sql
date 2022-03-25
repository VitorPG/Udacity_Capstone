CREATE TABLE IF NOT EXISTS stage_i94
		( 
        	cicid float,
            i94yr float,
            i94mon float,
            i94cit float,
            i94res float,
            i94port varchar,
            arrdate float,
            i94mode float,
            i94addr varchar,
            depdate float,
            i94bir float,
            i94visa float,
            count float,
            dtadfile varchar,
            visapost varchar,
            occup varchar,
            entdepa varchar,
            entdepd varchar,
            entdepu varchar,
            matflag varchar,
            biryear float,
            dtaddto varchar,
            gender varchar,
            insnum varchar,
            airline varchar,
            admnum float,
            fltno varchar,
            visatype varchar
            );

CREATE TABLE IF NOT EXISTS stage_us_demo 
		( city varchar NOT NULL,
        	state varchar NOT NULL,
            median_age float,
            male_pop float,
            female_pop float, 
            total_pop float,
            number_veterans float,
            foreign_born float,
            avg_household_size float,
            state_code varchar, 
            race varchar,
            count float
            );
            
CREATE TABLE IF NOT EXISTS stage_global_temp 
		(dt datetime,
        avg_temp float,
        avg_temp_uncer float,
        city varchar NOT NULL,
        country varchar NOT NULL,
        latitude varchar,
        longitude varchar
        );

CREATE TABLE IF NOT EXISTS stage_port_label
		( code varchar NOT NULL,
        	city varchar,
            state varchar
            );

CREATE TABLE IF NOT EXISTS stage_country_label
		( country_code float,
        country_name varchar
        );
        
CREATE TABLE IF NOT EXISTS stage_airport_codes
		( ident varchar NOT NULL,
        	type varchar,
            name varchar,
            elevation float,
            continent varchar,
            iso_country varchar,
            iso_region varchar,
            municipality varchar,
            gps_code varchar,
            iata_code varchar,
            local_code varchar,
            coord varchar
            );
            
CREATE TABLE IF NOT EXISTS imigration_table
		(admnum float,
        	i94yr float,
            i94mon float,
            cntry_residence float,
            destination varchar,
            i94port varchar,
            i94mode varchar,
            arrdate datetime,
            deptdate datetime,
            duration float,
            i94birth float,
            visatype varchar,
            gender varchar
            );
 
 CREATE TABLE IF NOT EXISTS land_temp_table 
 		(year float,
        	city varchar,
            country_code float,
            avg_temp float
            );
            
 CREATE TABLE IF NOT EXISTS airport_table
 		(ident varchar,
        	type varchar,
            name varchar,
            city varchar, 
            country varchar,
            airport varchar
            );
 
 CREATE TABLE IF NOT EXISTS us_city_demo
 		(city_name varchar NOT NULL,
        	state varchar,
            state_code varchar,
            median_age float,
            male_pop float,
            female_pop float,
            total_pop float,
            foreign_born float
            );
            
 