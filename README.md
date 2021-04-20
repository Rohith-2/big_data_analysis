# Big_Data_Analysis
### 19AIE214-Big Data Analysis End Project   
Authors:  
Anirudh Vadakedath     
Rohith Ramakrishnan   
<hr style=\"border:0.5px solid gray\"> </hr>

## Data
The data consists of flight arrival and departure details for all commercial flights within the USA, from October 1987 to April 2008. This is a large dataset: there are nearly 120 million records in total, and takes up 4 gigabytes of space compressed and 12 gigabytes when uncompressed.  
Link : <a>http://stat-computing.org/dataexpo/2009/</a>
<hr style=\"border:0.5px solid gray\"> </hr>

### Data Description
| Sr.No |	Name |	Description |
|-------|------|--------------|
|1  | Year	|1987-2008
|2	|Month	|1-12
|3	|DayofMonth	|1-31
|4	|DayOfWeek	|1 (Monday) - 7 (Sunday)
|5	|DepTime	|actual departure time (local, hhmm)
|6	|CRSDepTime	|scheduled departure time (local, hhmm)
|7	|ArrTime	|actual arrival time (local, hhmm)
|8	|CRSArrTime	|scheduled arrival time (local, hhmm)
|9	|UniqueCarrier	|unique carrier code
|10	|FlightNum	|flight number
|11	|TailNum	|plane tail number
|12	|ActualElapsedTime	|in minutes
|13	|CRSElapsedTime	|in minutes
|14	|AirTime	|in minutes
|15	|ArrDelay	|arrival delay, in minutes
|16	|DepDelay	|departure delay, in minutes
|17	|Origin	|origin IATA airport code
|18	|Dest	|destination IATA airport code
|19	|Distance	|in miles
|20	|TaxiIn	|taxi in time, in minutes
|21	|TaxiOut	|taxi out time in minutes
|22	|Cancelled|	was the flight cancelled?
|23	|CancellationCode	|reason for cancellation (A = carrier, B = weather, C = NAS, D = security)
|24	|Diverted	|1 = yes, 0 = no
|25	|CarrierDelay	|in minutes
|26	|WeatherDelay	|in minutes
|27	|NASDelay	|in minutes
|28	|SecurityDelay	|in minutes
|29	|LateAircraftDelay	|in minutes
<hr style=\"border:0.5px solid gray\"> </hr>

## Challenges
The aim of the data expo is to provide a graphical summary of important features of the data set. This is intentionally vague in order to allow different entries to focus on different aspects of the data, but here are a few ideas to get you started:  

  When is the best time of day/day of week/time of year to fly to minimise delays?  
  Do older planes suffer more delays?  
  How well does weather predict plane delays?  
<hr style=\"border:0.5px solid gray\"> </hr>

## Dashboard
![Analysis_Dashboard](https://user-images.githubusercontent.com/55501708/115195544-83eda080-a10c-11eb-8ec6-7497406bb17d.jpg)
<hr style=\"border:0.5px solid gray\"> </hr>

## Instructions
To run the Entire Analysis:  
On Terminal / CMD :  
```
spark-shell  
:load report.scala
```  
The output of the exisiting report in saved in <i>out.scala</i> 
