libname mylib '/home/u60689626/sasuser.v94/BAN110/Project/Dataset';
filename f_nov "/home/u60689626/sasuser.v94/BAN110/Project/Input Files/202111-capitalbikeshare-tripdata.csv";
filename f_dec "/home/u60689626/sasuser.v94/BAN110/Project/Input Files/202112-capitalbikeshare-tripdata.csv";

/* Importing the Dataset */
PROC IMPORT DATAFILE=f_nov DBMS=CSV OUT=mylib.nov_data;
	GETNAMES=YES;
RUN;

PROC IMPORT DATAFILE=f_dec DBMS=CSV OUT=mylib.dec_data;
	GETNAMES=YES;
RUN;

/* Merging data for November and December */
data mylib.bike_ride_master;
	set mylib.dec_data mylib.nov_data;
run;

/* Check variables and their data type */
proc contents data=mylib.bike_ride_master varnum;
run;

/* Check missing values */
/* Categorical variable  */
proc freq data=mylib.bike_ride_master;
	tables rideable_type start_station_name end_station_name member_casual;
run;

/* Numerical variable */
proc means data=mylib.bike_ride_master n mean median stddev min max nmiss 
		missing;
run;

/* Creating temporary dataset for working */
data mylib.temp_ride_master;
	set mylib.dec_data mylib.nov_data;
run;

/* Clean values for columns 'ridable_type' , 'start_station_name' , 'end_station_name' */
/* 'start_station_name' and 'end_station_name' records missing records need to be deleted as using mode
is not justifiable */
data mylib.delete;
	set mylib.temp_ride_master;

	if rideable_type="electric_bik" then
		rideable_type="electric_bike";

	if rideable_type="" then
		delete;

	if start_station_name="" then
		delete;

	if end_station_name="" then
		delete;
run;

data mylib.temp_ride_master;
	set mylib.delete;
run;

/*  */
proc format;
	value missingnum
		.='missing' other='not missing';
	value $missingchar ' '='missing' other='not missing';
run;

/* Verify missing values have been either corrected or removed */
proc freq data=mylib.bike_ride_master;
	tables rideable_type start_station_name end_station_name member_casual;
	format rideable_type start_station_name end_station_name 
		member_casual $missingchar.;
run;

/* Create new custom derived categorical variable */
data mylib.derived;
	set mylib.temp_ride_master;
	trip_info=catx(" , " , start_station_name, end_station_name);
run;

/* Get the top 3 most taken routes */
%let TopN = 3;

proc freq data=mylib.derived order=freq;
	table trip_info/ maxlevels=&TopN;
run;

/* Create new flag most_visited based on the top 3 most taken routes */
data mylib.derived;
	set mylib.derived;

	if trip_info in ('1st & M St NE , New Jersey Ave & F St NW', 'Smithsonian-National Mall / Jefferson , Smithsonian-National Mall / Jefferson Dr &', 
		'Jefferson Dr & 14th St SW , Jefferson Dr & 14th St SW') then
			most_visited=1;
	else
		most_visited=0;
run;

proc print data=mylib.derived(obs=5);
run;

data mylib.temp_ride_master;
	set mylib.derived;
run;

/* Create custom derived numerical variable 'ride_time' to calculate duration of the trip */
data mylib.derived_var;
	set mylib.temp_ride_master;
	ride_time=intck("minute", started_at, ended_at);
	label ride_time="Total_ride_time_in_minutes";
run;

data mylib.temp_ride_master;
	set mylib.derived_var;
run;

/* Create custom derived numerical variable 'distance' to calculate distance travelled during the trip */
data mylib.dist;
	set mylib.temp_ride_master;
	format start_lng start_lng end_lat end_lng best16.;
	distance=geodist(start_lat, start_lng, end_lat, end_lng);
	put "Distance = " distance "kilometers";
run;

data mylib.temp_ride_master;
	set mylib.dist;
run;

proc print data=mylib.temp_ride_master(obs=5);
	var ride_id trip_info ride_time distance most_visited;
run;

proc means data=mylib.temp_ride_master n mean median stddev min max nmiss 
		missing;
run;

title 'Checking Distribution shape using SG Plot for ride_time';

proc sgplot data=mylib.temp_ride_master;
	/*   histogram ride_time; */
	xaxis values=(-50000 to 50000 by 10000);
	density ride_time;
run;

/* Plot suggests the data for ride_time is normally distributed as it follows the bell curve */

proc univariate data=mylib.temp_ride_master nextrobs=20;
	var ride_time;
	histogram ride_time/normal;
	qqplot ride_time/normal;
run;

/* Check and delete if ride_time is negative for any rows */

data mylib.ride_time_positive_only;
	set mylib.temp_ride_master;

	if ride_time < 0 then
		delete;
run;

data _null_;
	set mylib.ride_time_positive_only;

	if ride_time < 0 then
		count_negative+1;

	if _n_=397593 then
		do;

			if count_negative=0 then
				put 'Custom Message: No Negative Values';
		end;
run;

title 'Identifying 'ride_id' Outliers Based on Interquartile Range';
proc means data=mylib.ride_time_positive_only noprint;
	var ride_time;
	output out=Tmp Q1=Q3=QRange= / autoname;
run;

data _null_;
	file print;
	set mylib.ride_time_positive_only(keep=ride_id ride_time);

	if _n_=1 then
		set Tmp;

	if (ride_time le ride_time_Q1 - 3*ride_time_QRange and not missing(ride_time)) 
		or (ride_time ge ride_time_Q3 + 3*ride_time_QRange) then
			put "Possible Outlier for ride_id: " ride_id ", value of ride_time is: " 
			ride_time;
run;

title 'Deleting 'ride_id' Outliers Based on Interquartile Range';
proc means data=mylib.ride_time_positive_only noprint;
	var ride_time;
	output out=Tmp Q1=Q3=QRange= / autoname;
run;

data mylib.ride_time_outliers_Removed;
	file print;
	set mylib.ride_time_positive_only;

	if _n_=1 then
		set Tmp;

	if (ride_time le ride_time_Q1 - 3*ride_time_QRange and not missing(ride_time)) 
		or (ride_time ge ride_time_Q3 + 3*ride_time_QRange) then
			delete;
run;

proc print data=mylib.ride_time_outliers_Removed(obs=10);
	var ride_id ride_time _TYPE_ _FREQ_ ride_time_Q1 ride_time_Q3 ride_time_QRange;
run;

data mylib.temp_ride_master;
	set mylib.ride_time_outliers_Removed(drop=_TYPE_ _FREQ_ ride_time_Q1 
		ride_time_Q3 ride_time_QRange);
run;

title 'Plot for "distance"';
proc univariate data=mylib.temp_ride_master;
	var distance;
	histogram distance/normal;
	qqplot distance/normal;
run;

title 'Identifying 'distance' Outliers Based on Interquartile Range';
proc means data=mylib.temp_ride_master noprint;
	var distance;
	output out=Tmp Q1=Q3=QRange= / autoname;
run;

data _null_;
	file print;
	set mylib.temp_ride_master(keep=ride_id distance);

	if _n_=1 then
		set Tmp;

	if (distance le distance_Q1 - 3*distance_QRange and not missing(distance)) 
		or (distance ge distance_Q3 + 3*distance_QRange) then
			put "Possible Outlier for ride_id: " ride_id ", value of distance is: " 
			distance;
run;

title 'Deleting "distance" Outliers Based on Interquartile Range';
proc means data=mylib.temp_ride_master noprint;
	var distance;
	output out=Tmp Q1=Q3=QRange= / autoname;
run;

data mylib.distance_outliers_removed;
	/* 	file print; */
	set mylib.temp_ride_master;

	if _n_=1 then
		set Tmp;

	if (distance le distance_Q1 - 3*distance_QRange and not missing(distance)) 
		or (distance ge distance_Q3 + 3*distance_QRange) then
			delete;
run;

title 'Testing normality';
proc univariate data=mylib.temp_ride_master plot normaltest;
	var distance ride_time;
run;
/* Looking at the value for Kolmogorov-Smirnov,
 we can reject the null hypothesis that the distribution is normally distributed.  */

/* We transform the variables ‘distance’ and ‘ride_time’ using 
logartithmic transformation to try and get them to as normalized as possible. */

title ' Logarithmic Transformation';
data mylib.bike_transformation;
	set mylib.temp_ride_master;
	ride_time_log=log(ride_time);
	distance_log=log(distance);
run;

title 'Testing after transformation';
proc univariate data=mylib.bike_transformation;
	var ride_time_log distance_log;
	qqplot;
run;

proc univariate data=mylib.bike_transformation;
	var ride_time_log distance_log;
	histogram;

	/* Checking for normality */
	probplot ride_time_log distance_log / normal (mu=est sigma=est);
run;

/* For "ride_time_log", we can say it is closer to normal distribution than before but still not normally distributed, 
a significant umber of points do not lie on the normal line of the probability curve */

/* For "distance_log", we can say it is closer to normal distribution as per shape but still not normally distributed as 
a significant umber of points do not lie on the normal line of the probability curve */