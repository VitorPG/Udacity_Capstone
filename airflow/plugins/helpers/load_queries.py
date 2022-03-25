class loadQuery:
    load_imigration_table = (""" 
                                SELECT DISTINCT admnum as entry_id,
                                        i94yr,
                                        i94mon,
                                        i94res as residence,
                                        i94addr as destination,
                                        i94port,
                                        i94mode,
                                        DATEADD(day, cast(arrdate as int),'1960-01-01') as arriv_date,  
                                        DATEADD(day, cast(depdate  as int),'1960-01-01') as dept_date,
                                        ( depdate - arrdate ) as stay_duration,
                                        i94bir as age,
                                        visatype,
                                        gender
                                FROM stage_i94
                                WHERE admnum IS NOT NULL
                                """)
    
    load_temperature_table = (""" 
                                SELECT EXTRACT(year FROM gt.dt) as c_year, gt.city, cl.country_code, AVG(gt.avg_temp) as avg_temp
                                FROM stage_global_temp gt
                                JOIN stage_country_label cl 
                                ON UPPER(gt.country) = cl.country_name 
                                WHERE c_year = (
                                                    SELECT EXTRACT(year FROM dt) 
                                                    FROM stage_global_temp 
                                                    ORDER BY 1 DESC 
                                                    LIMIT 1
                                                )
                                GROUP BY 1,3,2 
                                """)
    
    load_airport_table = ("""
                            SELECT ac.ident, ac.type, ac.name, ac.municipality as city, ac.iso_country, i94.i94port 
                            FROM stage_airport_codes ac
                            LEFT JOIN stage_port_label pl
                            ON pl.city=ac.municipality
                            JOIN stage_i94 as i94
                            ON i94.i94port=pl.code
                            """)
    
    load_us_city_demo = ("""
                            SELECT city, state, state_code, median_age, male_pop, female_pop, total_pop, foreign_born as immigrants
                            FROM stage_us_demo 
                            """)
    #load_country_label =("""  """)
    #load_port_label = (""" """) 