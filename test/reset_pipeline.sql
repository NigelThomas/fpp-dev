-- reset_pipeline.sql

!set force on
alter pump "interface".* stop;
alter stream "interface"."transactions" reset;
!set force off

