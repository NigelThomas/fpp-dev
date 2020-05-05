!run sql/guavus/interface.sql
!run sql/guavus/normalization_data.sql
!run sql/fpp-prepare.sql
!run sql/guavus/feature_engineering.sql
!run sql/fpp-score.sql

ALTER PUMP "interface".*, "feature_engineering".*, "fpp-test-score-0.1_v2".* START;

