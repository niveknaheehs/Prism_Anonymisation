create or replace PACKAGE ut AUTHID CURRENT_USER is

   procedure drop_unused_cols (p_tgt_prefix in varchar2);

   function EXCLUDE (astring varchar2)  return varchar2;
 
   function GET_FORMATTED_SK (pi_name in varchar2,pi_holder_type varchar2,pi_holder_designation varchar2) return varchar2;

   function MAN (astring varchar2)  return varchar2;
  
   function TDC (astring varchar2) return varchar2;
  
   function TDC2 (p_string varchar2) return varchar2;
  
   function gen_rnd_non_alphanum ( p_not_char IN VARCHAR DEFAULT NULL ) return varchar2;  
  
   function TDC2_UNIQUE (p_str_for_unique_obfus in varchar2) return varchar2; 
 
   function OFE (astring varchar2)  return varchar2;
  
   function OFT (astring varchar2)  return varchar2;
  
   function OFW (astring varchar2)  return varchar2;
  
   function RANDOMISE_DATE_30 (p_date date) return date;
  
   function RD30 (p_date date) return date;
  
   function RN (note varchar2) return varchar2;
  
   function RND_DIGIT_WO_RESEL (anumber number) return number;
  
  -- function RND_NOTE return varchar2;
  
  -- procedure DISABLE_R_CONS (p_table_owner varchar2 ,p_table_name varchar2);
  
  -- procedure ENABLE_R_CONS (p_table_owner varchar2 ,p_table_name varchar2);
  
   function fn_date_mask (p_date in date) return date;
  
   function fn_char_mask ( p_string varchar2 ) return varchar2;

   function fn_number_mask ( p_number number ) return number;
  
   procedure truncate_table_new (p_table_owner varchar2, p_table_name varchar2, p_temporary varchar2 DEFAULT 'N');
  
   procedure rebuild_indexes (p_table_owner varchar2 ,p_table_name varchar2);
  
   function AN (p_account_name varchar2) return varchar2;

   procedure disable_triggers;

   procedure enable_triggers;

end ut;  
/