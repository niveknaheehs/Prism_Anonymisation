create or replace PACKAGE const is

  ------------------------------------------
  -- Metadata generation for subsetting
  ------------------------------------------
   k_sleep_seconds          constant number := 0.1;
   k_max_parallel_jobs      constant number := 8;
   k_monitor_job_interval   constant number := 60; --seconds
   k_job_monitor            constant varchar2(20) := 'JOB_MONITOR';
       
end const;
/