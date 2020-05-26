create or replace package gp AS 
  col_mask_row_threshold      number;        
  obfus_run_id                number;        
  execution_id                number;    
  src_prefix                  varchar2(128); 
  tgt_prefix                  varchar2(128); 
  run_env                     varchar2(128); 
  anon_version                varchar2(128);
  
  procedure set_col_mask_row_threshold  (p_col_mask_row_threshold   number);      
  procedure set_obfus_run_id(p_obfus_run_id number);       
  procedure set_execution_id(p_execution_id number);    
  procedure set_src_prefix(p_src_prefix varchar2 );
  procedure set_tgt_prefix(p_tgt_prefix varchar2) ;
  procedure set_run_env(p_run_env varchar2);
  procedure set_anon_version(p_anon_version varchar2);
  procedure init_gp(p_src_prefix               varchar2  DEFAULT NULL,
                    p_tgt_prefix               varchar2  DEFAULT NULL,
                    p_run_env                  varchar2  DEFAULT NULL,
                    p_anon_version             varchar2  DEFAULT NULL,
                    p_obfus_run_id             number    DEFAULT NULL,
                    p_execution_id             number    DEFAULT NULL,
                    p_col_mask_row_threshold   number    DEFAULT NULL);  
END gp;
/