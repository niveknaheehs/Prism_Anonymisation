create or replace PACKAGE ss_reporting AUTHID CURRENT_USER is

  procedure gen_obj_count_diff_report;

  procedure gen_obj_counts(p_src_or_tgt varchar2);  

  procedure gen_missing_object_report;

  procedure gen_tab_col_diff_report;

  procedure gen_missing_constraints_report;

  procedure gen_missing_indexes_report;

  procedure missing_privs_report;
  
  procedure gen_missing_table_report;
  
  procedure gen_missing_syn_report;

  procedure invalid_objects_report;

  procedure invalid_objects_delta_report;
  
  -- DATA REPORTS
  procedure gen_ptn_count_diff_report;  

  procedure gen_load_all_count_diff_report;
  
  procedure gen_load_other_report;
  
  procedure gen_missing_parts_report;

end ss_reporting;
/  