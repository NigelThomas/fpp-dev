create or replace schema "utils";
set schema '"utils"';


create or replace view serverstats as
select stream 
       1 as jkey
      , s.* 
from stream(sys_boot.mgmt.getServerInfoForever(60)) s;

create or replace view streamstats as 
select stream 
  * 
, 1 as jkey,name_in_query_plan as stream_name
from stream(sys_boot.mgmt.getStreamOperatorInfoForever(0,60)) s
where name_in_query_plan like '[LOCAL%';

create or replace view streamstats_agg as
select stream jkey
     , measured_at
     , max(case when name_in_query_plan like '%transactions%' then net_input_rows else null end) as input_rows
     , max(case when name_in_query_plan like '%transactions%' then net_input_row_rate else null end) as input_row_rate
     , max(case when name_in_query_plan like '%transactions%' then input_rowtime_clock else null end) as input_rowtime
     , max(case when name_in_query_plan like '%fe_pipeline_out%' then net_output_rows else null end) as output_rows
     , max(case when name_in_query_plan like '%fe_pipeline_out%' then output_rowtime_clock else null end) as output_rowtime
     , max(case when name_in_query_plan like '%transactions%' then net_output_row_rate else null end) as output_row_rate
from streamstats s
group by s.rowtime, s.measured_at, jkey;

create or replace view fpp_monitor as
select stream s.measured_at,s.net_memory_bytes,s.max_memory_bytes
    , a.input_rows, a.input_row_rate, a.input_rowtime
    , a.output_rows, a.output_row_rate, a.output_rowtime
from   streamstats_agg a
join   serverstats over (range interval '30' second preceding) s on s.jkey = a.jkey;


create or replace view fpp_monitor_15 as
select stream * from fpp_monitor s
where  step(s.rowtime by interval '1' minute) = step(s.rowtime by interval '2' minute)
;

