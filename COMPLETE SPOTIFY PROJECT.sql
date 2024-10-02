/*======================================================================================================================================
														
                                                        --COMPLETE PROJECT--

======================================================================================================================================
*/



-- create table
create Database shubham_1;
use shubham_1;
DROP TABLE IF EXISTS spotify;
CREATE TABLE spotify (
    artist VARCHAR(255),
    track VARCHAR(255),
    album VARCHAR(255),
    album_type VARCHAR(50),
    danceability FLOAT,
    energy FLOAT,
    loudness FLOAT,
    speechiness FLOAT,
    acousticness FLOAT,
    instrumentalness FLOAT,
    liveness FLOAT,
    valence FLOAT,
    tempo FLOAT,
    duration_min FLOAT,
    title VARCHAR(255),
    channel VARCHAR(255),
    views FLOAT,
    likes BIGINT,
    comments BIGINT,
    licensed BOOLEAN,
    official_video BOOLEAN,
    stream BIGINT,
    energy_liveness FLOAT,
    most_played_on VARCHAR(50)
);

select * from spotify
limit 1000;
-- 'C:\ProgramData\MySQL\MySQL Server 8.0\Uploads\cleaned_dataset.csv'

LOAD DATA INFILE 
'E:\cleaned_dataset.csv'
INTO TABLE spotify
FIELDS TERMINATED BY ',' 
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
ignore 1 rows
;

SHOW VARIABLES LIKE 'local_infile';
SET GLOBAL local_infile = 1;

SET GLOBAL local_infile = 1;

ALTER TABLE spotify MODIFY COLUMN energy_liveness FLOAT;


ALTER TABLE spotify
MODIFY licensed VARCHAR(25);

ALTER TABLE spotify
MODIFY official_video VARCHAR(25);

ALTER TABLE spotify
MODIFY duration_min FLOAT;

describe spotify;

with platformStream as
(
select artist,track,most_played_on as Platform,sum(stream) as total_stream
,row_number() over(partition by most_played_on  order by sum(stream) desc ) as ranks
  from spotify
group by artist,track,Platform
)
select * from platformStream
where 
ranks<=10
order by  Platform, Total_Stream desc
; 


select distinct(most_played_on) from spotify;
select * from spotify
limit 1000;




select Track,views,likes,comments,concat(round((comments+likes)/views,2)*100 , "%") as engagment_ratio_percentage from spotify
where views > 0
order by engagment_ratio_percentage desc
; 

/* Task 3
Task Title 3: Seasonal Popularity of Tracks
Problem Statement: Analyze the trends in track popularity by calculating 
month-over-month growth in views and streams to identify tracks with seasonal spikes in popularity.
*/
/*
Task Title: Correlating Energy Levels with Popularity
Problem Statement: Investigate whether there is a correlation between the "Energy" score of a track and 
its total number of streams, helping identify trends in listener preferences.*/

select artist,Track,album,energy from spotify;


/*Task 5 
No Data ,But Great Question To solve
Task Title: Predicting Churn for Licensed Tracks
Problem Statement: Identify tracks that are likely to lose popularity by comparing the trend in views and streams
 for licensed and unlicensed tracks over the past year.
*/

/*
Task Title: Identifying the Top Artists by Revenue Potential
Problem Statement: Based on track views, likes, and streams, identify 
which artists have the highest revenue potential across multiple platforms.*/
-- Revenue Potential=(Track Views×Revenue per View)+(Likes×Revenue per Like)+(Streams×Revenue per Stream)

-- Assuming:
-- revenue_per_view = 0.01 (1 cent per view)
-- revenue_per_like = 0.05 (5 cents per like)
-- revenue_per_stream = 0.003 (0.3 cets per stream)
set @revenue_per_view = 0.01 ;
set @revenue_per_like = 0.05 ;
set @revenue_per_stream = 0.003 ;
with Revenue as 
(
select 
artist,
Track,
album,
likes, 
views,
stream
-- ,(Views*revenue_per_view)+(Likes*revenue_per_like)+(Stream*revenue_per_stream) as revenue
,round((Views*0.01)+(Likes*0.05)+(Stream*0.003)) as revenue_Doller
 from spotify
order by revenue_Doller desc 
)
select * from revenue 
where revenue_Doller >=(
					select avg(revenue_Doller) from revenue
);

/*
Task Title: Detecting Outliers in Track Performance
Problem Statement: Use advanced filtering techniques to detect outliers in track performance, 
such as tracks that have abnormally high views but low likes or streams.*/

-- By  Standard Deviation Method

select * from spotify;
with Z_Score_Calculations as
(
select 
artist,
track,
album,
views,
likes,
stream,
avg(views) over() as avg_views,
avg(likes) over() as avg_likes,
avg(stream) over() as avg_stream,
stddev(views) over() as stdd_Views,
stddev(likes) over() as stdd_likes,
stddev(stream) over() as stdd_stream,
-- Z-Score ===>> x=observe value , m=mean of sample,s=standerd deviation of sample
-- z=(x-m)/s

(views-avg(views) over()/nullif(stddev(views) over(),0)) as Z_Score_Views,
(likes-avg(likes) over()/nullif(stddev(likes) over(),0)) as Z_Score_likes,
(stream-avg(stream) over()/nullif(stddev(stream) over(),0)) as Z_Score_Stream


from spotify
)
select * from  Z_Score_Calculations
where 
(abs(Z_score_views)>3 and Z_Score_Views <0) or
(ABS(z_score_views) > 3 AND z_score_stream < 0)
order by views  desc;

/*
What the Query Identifies:
Tracks with a disproportionate number of views compared to likes and streams.
These tracks might be the result of:
Viral trends where users watch but don't interact.
Artificial inflation of views (e.g., bots), where genuine user engagement (likes/streams) is lacking.
When to Use:
Content Quality Analysis: You want to identify tracks that may have gone viral but don't have matching audience engagement 
(e.g., people watch but don't like or stream further).
Fraud Detection: If you suspect artificial boosting of views (e.g., bot traffic),
 this query can help detect tracks that have abnormally high views but low genuine engagement.*/
 
 /*
 Task Title: Calculating the Lifetime Value of a Track
Problem Statement: Calculate the estimated lifetime value of a track based on historical stream and view data
*/
select * from spotify;

select Track,sum(views) Total_views,
			 sum(stream) Total_Stram_Views,
			 sum(views)*0.001 as Revenue_Views, -- Revenure for 1 View ==> 0.001
             sum(stream)*0.005 as Revenue_Stream,  -- -- Revenure for 1 Stream ==> 0.005
             (sum(stream)*0.005+sum(views)*0.001) as life_time_Value
from spotify
group by Track
order by life_time_Value desc;

/*
Task Title: Analyzing Platform-Specific Performance
Problem Statement: Compare the performance of tracks on Spotify vs. YouTube by analyzing views, likes, and streams 
over time to determine which platform offers more consistent growth.*/

select * from spotify;
select album,
		most_played_on,
        views,likes,
        stream,
        (likes/stream)*100 as engegment_rate_percent from spotify
where likes > 0 and  stream >0;

with rank_Track_100 as (
select 
	Track,
    danceability,
    energy,
    loudness,
    stream,
    rank() over(order by stream desc) rank_Dance 
from spotify
	where stream > 0
	limit  100)
select * from rank_Track_100;
/*
Task Title: Analyzing Viewer Retention by Track Length
Problem Statement: Investigate whether longer or shorter tracks tend to have
 higher viewership and stream numbers, based on the "Duration_min" field.

*/
with Track_Length_Categories as (
select artist,track,views,duration_min,stream,
case when duration_min <3 then "Short"
	when duration_min between 3 and 5 then "Medium"
    else "Long"
    end as track_length_category 
from spotify
)
select track_length_category,count(track),avg(views)
,avg(stream) from Track_Length_Categories
group by track_length_category 
order by track_length_category ;






select * from spotify;
SELECT 
    Track,views,likes,comments,
    CONCAT(ROUND((comments + likes) / views, 2) * 100,
            '%') AS engagment_ratio_percentage
FROM
    spotify
WHERE
    views > 1000000 AND likes > 50000
        AND comments > 50000
ORDER BY engagment_ratio_percentage DESC
; 




with Revenue as 
(
select 
artist,Track,album,likes, views,stream
-- ,(Views*revenue_per_view)+(Likes*revenue_per_like)+(Stream*revenue_per_stream) as revenue
,round((Views*0.01)+(Likes*0.05)+(Stream*0.003)) as revenue_Doller
 from spotify
order by revenue_Doller desc 
)
select * from revenue 
where revenue_Doller >=(
					select avg(revenue_Doller) from revenue);
                    
                    
                    
with platformStream as
(
select  artist,track,most_played_on as Platform,
		sum(stream) as total_stream,
        row_number() over(partition by most_played_on  order by sum(stream) desc ) as ranks
from spotify
		group by artist,track,Platform
)
select * from platformStream
		where ranks<=10
		order by  Platform, Total_Stream desc; 
/*
						-- Easy 
Retrieve the names of all tracks that have more than 1 billion streams.
List all albums along with their respective artists.
Get the total number of comments for tracks where licensed = TRUE.
Find all tracks that belong to the album type single.
Count the total number of tracks by each artist.
*/


/*
						-- Medium Level
Calculate the average danceability of tracks in each album.
Find the top 5 tracks with the highest energy values.
List all tracks along with their views and likes where official_video = TRUE.
For each album, calculate the total views of all associated tracks.
Retrieve the track names that have been streamed on Spotify more than YouTube.

*/

/*
						-- Advanced Level
Find the top 3 most-viewed tracks for each artist using window functions.
Write a query to find tracks where the liveness score is above the average.
Use a WITH clause to calculate the difference between the highest and lowest energy values for tracks in each album.
Find tracks where the energy-to-liveness ratio is greater than 1.2.
Calculate the cumulative sum of likes for tracks ordered by the number of views, using window functions.
*/
with CTE as
(
select album,
		min(energy) as lowest_energy,
        max(energy) as Highest_energy
from spotify
group by 1)
select album, round((Highest_energy-lowest_energy),4) as energy_diff from CTE
order by energy_diff desc ;






/*
======================================================================================================================================
														--ADVANCE  LEVEL

======================================================================================================================================
*/


/*
Advanced MySQL Case Study Tasks
Task Title: Identifying Most Popular Tracks by Platform
Problem Statement: Identify the top 10 tracks by total streams on different platforms
 (e.g., Spotify, YouTube) to understand which platform drives the most engagement for each track.

Task Title: Calculating Engagement Rate by Track
Problem Statement: Calculate the engagement rate for each track, defined as the ratio of likes to views. 
Rank the tracks based on engagement to understand which ones resonate most with the audience.

Task Title: Seasonal Popularity of Tracks
Problem Statement: Analyze the trends in track popularity by calculating month-over-month growth in views 
and streams to identify tracks with seasonal spikes in popularity.

Task Title: Correlating Energy Levels with Popularity
Problem Statement: Investigate whether there is a correlation between the "Energy" score of a track 
and its total number of streams, helping identify trends in listener preferences.

Task Title: Predicting Churn for Licensed Tracks
Problem Statement: Identify tracks that are likely to lose popularity by comparing the trend in views and streams for licensed and unlicensed tracks over the past year.

Task Title: Identifying the Top Artists by Revenue Potential
Problem Statement: Based on track views, likes, and streams, 
identify which artists have the highest revenue potential across multiple platforms.

Task Title: Detecting Outliers in Track Performance
Problem Statement: Use advanced filtering techniques to detect outliers in track performance,
such as tracks that have abnormally high views but low likes or streams.

Task Title: Calculating the Lifetime Value of a Track
Problem Statement: Calculate the estimated lifetime value of a track based on historical stream and view data.

Task Title: Analyzing Platform-Specific Performance
Problem Statement: Compare the performance of tracks on Spotify vs. YouTube by analyzing views, likes, and streams over time to determine which platform offers more consistent growth.

Task Title: Ranking Tracks by Acoustic Features
Problem Statement: Rank tracks by their acoustic features (e.g., danceability, energy, loudness) to identify 
the characteristics of the most successful tracks in terms of streams.

Task Title: Measuring the Impact of Official Videos
Problem Statement: Investigate the impact of having an "official video" on a track’s performance by comparing the total streams
 and views of tracks with and without official videos.

Task Title: Creating a Heatmap of Popularity by Region
Problem Statement: Use SQL to generate a heatmap showing the distribution of track popularity (streams and views) by region, 
helping to identify key markets.

Task Title: Analyzing Viewer Retention by Track Length
Problem Statement: Investigate whether longer or shorter tracks tend to have higher viewership and stream numbers, 
based on the "Duration_min" field.

Task Title: Optimizing Queries for Real-Time Analytics
Problem Statement: Refactor a complex query that pulls real-time analytics from a large dataset
 to optimize for performance using indexes and partitioning.

Task Title: Grouping Tracks by Genre or Album Type
Problem Statement: Group tracks by their "Album_type" (album, single, etc.) and analyze the differences in
 performance metrics like streams, views, and likes.

Task Title: Calculating the Pareto Principle for Streams
Problem Statement: Apply the Pareto Principle (80/20 rule) to identify the top 20% of tracks 
that generate 80% of the total streams across platforms.

Task Title: Implementing Recursive CTE for Artist Hierarchies
Problem Statement: Use recursive Common Table Expressions (CTE) to model hierarchical relationships 
between artists, albums, and tracks, and calculate aggregated statistics for each level.

Task Title: Evaluating the Impact of Collaborations
Problem Statement: Analyze whether collaboration tracks (featuring multiple artists) 
outperform solo tracks in terms of streams and views.

Task Title: Trend Detection Using Window Functions
Problem Statement: Use advanced window functions to detect long-term trends in streams and views 
for each track, helping to forecast future performance.

Task Title: Detecting Declining Tracks
Problem Statement: Identify tracks that are experiencing a decline in streams and views over the 
past six months and predict potential causes.
*/