create or replace package body gp as 

  procedure set_col_mask_row_threshold  (p_col_mask_row_threshold   number) is
  begin
    col_mask_row_threshold := p_col_mask_row_threshold;
  end;        
  procedure set_obfus_run_id(p_obfus_run_id number) is
  begin
    obfus_run_id := p_obfus_run_id;
  end;        
  procedure set_execution_id(p_execution_id number) is
  begin
    execution_id := p_execution_id;
  end;    
  procedure set_src_prefix(p_src_prefix varchar2 )  is
  begin
    src_prefix := p_src_prefix;
  end; 
  procedure set_tgt_prefix(p_tgt_prefix varchar2)  is
  begin
    tgt_prefix := p_tgt_prefix;
  end; 
  procedure set_run_env(p_run_env varchar2) is
  begin
    run_env := p_run_env;
  end; 
  procedure set_anon_version(p_anon_version varchar2)  is
  begin
      anon_version := p_anon_version;
  end;

  procedure init_gp(p_src_prefix               varchar2  DEFAULT NULL,
                    p_tgt_prefix               varchar2  DEFAULT NULL,
                    p_run_env                  varchar2  DEFAULT NULL,
                    p_anon_version             varchar2  DEFAULT NULL,
                    p_obfus_run_id             number    DEFAULT NULL,
                    p_execution_id             number    DEFAULT NULL,
                    p_col_mask_row_threshold   number    DEFAULT NULL) is
  begin
  
     if p_src_prefix is not null then
        set_src_prefix(p_src_prefix);
     end if;

     if p_tgt_prefix is not null then
        set_tgt_prefix(p_tgt_prefix); 
     end if;    

     if p_run_env is not null then
        set_run_env(p_run_env);
     end if;

     if p_anon_version is not null then
        set_anon_version(p_anon_version);
     end if;
    
     if p_obfus_run_id is not null then
        set_obfus_run_id(p_obfus_run_id);
     end if;

     if p_execution_id is not null then
        set_execution_id(p_execution_id);
     end if;

     if p_col_mask_row_threshold is not null then
        set_col_mask_row_threshold(p_col_mask_row_threshold);
     end if;
     
  end init_gp;                     
end gp;
/