create or replace schema "utils";
set schema '"utils"';


create or replace view serverstats as
select stream s.measured_at as ROWTIME
      ,1 as jkey
      , s.* 
from stream(sys_boot.mgmt.getServerInfoForever(600)) s;

create or replace view streamstats as 
select stream s.measured_at as ROWTIME
, * 
, 1 as jkey,name_in_query_plan as stream_name
from stream(sys_boot.mgmt.getStreamOperatorInfoForever(0,600)) s
where name_in_query_plan like '[LOCAL%';

create or replace view streamstats_agg as
select stream jkey
     , max(case when name_in_query_plan like '%transactions%' then net_input_rows else null end) as input_rows
     , max(case when name_in_query_plan like '%transactions%' then input_rowtime_clock else null end) as input_rowtime
     , max(case when name_in_query_plan like '%fe_pipeline_out%' then net_output_rows else null end) as output_rows
     , max(case when name_in_query_plan like '%fe_pipeline_out%' then output_rowtime_clock else null end) as output_rowtime
from streamstats s
group by s.rowtime, jkey;

create or replace view fpp_monitor as
select stream s.measured_at,s.net_memory_bytes,s.max_memory_bytes
    , a.input_rows, a.output_rows
    , a.input_rowtime, a.output_rowtime
from   serverstats s
join   streamstats_agg a on s.jkey = a.jkey;


