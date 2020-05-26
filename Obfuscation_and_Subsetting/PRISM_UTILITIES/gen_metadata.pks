create or replace PACKAGE gen_metadata  AUTHID CURRENT_USER
AS

  TYPE string_list_4000 IS TABLE OF VARCHAR2(4000) INDEX BY BINARY_INTEGER;
  g_db_version varchar2(11);

  --procedure resolve_ref_part_seq(p_run_id number,p_src_schema varchar2);

  procedure resolve_ref_part_seq;

  procedure add_partition_clause ( p_src_schema varchar2 );

  procedure get_table_metadata(p_src_schema varchar2,p_part_list varchar2);

  procedure load_metadata(p_regenerate boolean default FALSE, p_comp_list varchar2);
  
  procedure add_companies(p_comp_list varchar2);

  procedure pop_obj_metadata_xmlddl ( p_schema_prefix   varchar2,
                                      p_object_type     varchar2 );


  procedure pop_user_obj_metadata_xmlddl ( p_src_schema      varchar2,
                                           p_object_type     varchar2 );

  procedure pop_obj_metadata_getddl ( p_schema_prefix   varchar2,
                                      p_object_type     varchar2);

  function pop_user_obj_metadata_getddl ( p_src_schema      varchar2,
                                         p_object_type     varchar2,
                                         p_object_name     varchar2 default null,
                                         p_commit          boolean default true) return boolean;


  procedure pop_trigger_ddl ( p_src_schema varchar2, p_object_name varchar2 default null );

  procedure add_local_index_ddl( p_src_schema varchar2, p_object_name varchar2 default null );

  procedure pop_index_ddl ( p_src_schema varchar2, p_object_name varchar2 default null );

  procedure pop_object_grant_ddl ( p_src_schema varchar2);

  procedure pop_user_granted_ddl ( p_owner_prefix varchar2);

  procedure pop_ref_constraints ( p_schema_prefix  varchar2 );

  procedure pop_user_ref_constraints ( p_src_schema varchar2 );

  procedure pop_dependent_xml ( p_schema_prefix  varchar2,
                                p_object_type    varchar2 default 'REF_CONSTRAINT' );

  procedure pop_user_dependent_xml ( p_src_schema    varchar2,
                                     p_object_type   varchar2 default 'REF_CONSTRAINT' );

  procedure get_table_ref_xml ( p_schema_prefix  varchar2,
                                p_object_type    varchar2 default 'TABLE' );

  procedure get_user_table_ref_xml ( p_src_schema    varchar2,
                                     p_object_type   varchar2 default 'TABLE' );

  function insert_metadata_ddl_parts ( p_metadata_ddl_id number )  return number;

  function fn_view_exists(p_view_owner  varchar2, p_view_name  varchar2) return boolean;

  procedure create_partition_view(p_src_schema varchar2, p_table_name varchar2, p_part_list varchar2);

  procedure build_all_load_views(p_part_list varchar2);

  procedure build_load_views(p_src_schema varchar2,p_part_list varchar2);

  procedure load_src_schema_list(p_src_prefix varchar2);

  procedure get_table_splits(p_src_schema varchar2);
  
  procedure get_dp_table_metadata;
  
  procedure load_ddl_exclusions;

  procedure pop_rel_level;

  procedure remove_md_sys_c_cons;
 
 end gen_metadata;
 /