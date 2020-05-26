create or replace PACKAGE BODY  anonymisation_process
AS

  procedure obfus_log(p_log_msg VARCHAR2,p_code NUMBER,p_errm varchar2,p_module varchar2) is
  
     pragma autonomous_transaction;
    
  begin 
     insert into obfuscation_log (log_id,log_msg,src_prefix,anon_version,tgt_prefix,err_code ,errm ,module,run_date,mod_timestamp)
     values (obfuscation_log_seq.nextval ,p_log_msg ,g_src_prefix,g_anon_version,g_tgt_prefix,p_code,p_errm,p_module,g_run_date,systimestamp);
     commit;
     
     g_errm := null;
     g_code := null;
  end;  
  
  procedure reset_schema 
  as
  begin
     begin
        g_run_date := sysdate;
        g_module := 'reset_schema';       
        -- Reset  the working tables      

        --execute immediate 'truncate table manual_transform';
        execute immediate 'truncate table purge_transform';
          
     exception when others then 
        g_code := SQLCODE; 
        g_errm := SUBSTR(SQLERRM, 1 , 4000);
        obfus_log('reset_schema'||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),g_code,g_errm,g_module);
            
     end;   
  end reset_schema;
  
  procedure  set_globals as
  begin
      select count(rnd_words) into g_max_rnd_note  from randomised_notes;
      select max(key_ns) into g_max_rnd_address_line_1_seq from  address_line_1_rnd;
      select max(key_ns) into g_max_rnd_address_line_2_seq from  address_line_2_rnd;
      select max(key_ns) into g_max_rnd_address_line_3_seq from  address_line_3_rnd;
      select max(key_ns) into g_max_rnd_postcode_seq from  postcode_rnd;    
      select max(surname_seq) into g_max_rnd_surname_seq from  surname_list;
      select max(key_ns) into g_max_rnd_forename_seq from  forename_list;
  end set_globals;
  
  procedure anonymise 
   as
     x_obfus_not_ready EXCEPTION;
     PRAGMA exception_init (x_obfus_not_ready, -20001);    
   begin
      begin
   
          g_module := 'anonymise';
          obfus_log('anonymise '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,g_module);           
          obfuscation_control.check_obfus_ready(g_obfus_run_id,g_src_prefix,g_tgt_prefix,g_run_env,g_anon_version);
          
          if ( g_obfus_run_id is null or g_src_prefix is null or g_tgt_prefix is null or g_run_env is null or g_anon_version is null )
          then
              RAISE x_obfus_not_ready;
          else
             obfuscation_control.update_obfus_control(g_obfus_run_id, g_src_prefix, g_tgt_prefix, g_run_env, g_anon_version, p_obfus_status => 'RUNNING');
          end if;
 
          obfus_log('reset_schema '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,'anonymise');                  
          reset_schema;
                
          obfus_log('gen_rnd_notes '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,'anonymise');
          gen_rnd_notes;
  
          obfus_log('gen_rnd_addresses '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,'anonymise');
          gen_rnd_addresses;
            
          obfus_log('gen_rnd_names '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,'anonymise');
          gen_rnd_names;
  
          obfus_log('set_globals '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,'anonymise'); 
          set_globals;
  
          obfus_log('process_privacy_catalog '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,'anonymise');
          process_privacy_catalog(g_src_prefix);
    
          obfus_log('process_manual_obfus '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,'anonymise');
          process_manual_obfus;
            
          obfus_log('merge_to_target '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,'anonymise');
          merge_to_target (g_tgt_prefix);
         
          obfus_log('run_purge_data '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,'anonymise');
          run_purge_data(g_tgt_prefix);

          obfus_log('apply_temp_patches '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,'anonymise');  
          apply_temp_patches;
  
          obfus_log('generate_stats '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,'anonymise');
          generate_stats(1);
        
          obfus_log('merge_fix_anonalies '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,'anonymise');
          merge_fix_anonalies;
  
          obfus_log('generate_stats '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,'anonymise');
          generate_stats(2);
      
          obfus_log('generate_qa_reports '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,'anonymise');
          generate_qa_reports;
        
          obfuscation_control.update_obfus_control(g_obfus_run_id, g_src_prefix, g_tgt_prefix, g_run_env, g_anon_version, p_obfus_status => 'COMPLETED');
          obfus_log('Finish'||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,g_module);
        
     exception
       when x_obfus_not_ready then
          g_errm := SUBSTR(SQLERRM, 1 , 4000);  
          obfus_log('Obfuscation not ready to run: check obfus_control table. ' ||
                    'The PRE, ANON and POST schema prefixes must be configured and PENDING to run with setup and checked flags Y',SQLCODE,SQLERRM,g_module); 
          RAISE_APPLICATION_ERROR(-20001,'Obfuscation not ready to run: check obfus_control table.');          
       when others then
          g_code := SQLCODE;
          g_errm := SUBSTR(SQLERRM, 1 , 4000);     
          obfuscation_control.update_obfus_control(g_obfus_run_id, g_src_prefix, g_tgt_prefix, g_run_env, g_anon_version, p_obfus_status => 'FAILED');
          obfus_log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),g_code,g_errm,g_module);
     end;
  end anonymise;

  procedure process_manual_obfus
  is
  begin
     begin
        g_module := 'process_manual_obfus';

        obfus_log('anon_bank_accounts '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,'process_manual_obfus');  
        anon_bank_accounts;

        obfus_log('anon_holder_names '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,'process_manual_obfus');    
        anon_holder_names;
         
        obfus_log('anon_holder_addresses '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,'process_manual_obfus');         
        anon_holder_addresses;

        obfus_log('anon_holder '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,'process_manual_obfus');       
        anon_holder;

        obfus_log('anon_holder_labels '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,'process_manual_obfus');          
        anon_holder_labels;
  
        obfus_log('anon_holder_mandates '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,'process_manual_obfus');  
        anon_holder_mandates;

        obfus_log('anon_disc_exer_spouse_dtls '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,'process_manual_obfus');    
        anon_disc_exer_spouse_dtls;

        obfus_log('anon_disc_exer_req_mandates '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,'process_manual_obfus');           
        anon_disc_exer_req_mandates;
  
        obfus_log('anon_mifid_entities '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,'process_manual_obfus');          
        anon_mifid_entities;

        obfus_log('anon_mifid_integration '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,'process_manual_obfus');                     
        anon_mifid_integration; 
 
        obfus_log('anon_cash_ivc_class_copies '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,'process_manual_obfus');          
        anon_cash_ivc_class_copies;
  
        obfus_log('anon_comp_payee_mandates '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,'process_manual_obfus');
        anon_comp_payee_mandates;
       
        obfus_log('anon_payments '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,'process_manual_obfus');
        anon_payments;
  
        obfus_log('gen_sortkey '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,'process_manual_obfus');
        gen_sortkey ; 
     end; 
  end process_manual_obfus;

procedure anon_disc_exer_req_mandates is 
begin
  begin

    g_module := 'anon_disc_exer_req_mandates';
    
    begin
      execute immediate 'drop table rnd_derm_adresss_lu';    
      exception when others then null;
    end;
    
    --dbms_output.put_line(
    execute immediate 'create table  rnd_derm_adresss_lu (disc_exer_req_mandate_id number,address_line1_key number,
    address_line2_key  number,address_line3_key  number)';
        
    insert into rnd_derm_adresss_lu(disc_exer_req_mandate_id,address_line1_key,address_line2_key,address_line3_key) 
    select disc_exer_req_mandate_id,
           mod(abs(dbms_random.random), g_max_rnd_address_line_1_seq ) +1 address_line1_key ,
           mod(abs(dbms_random.random), g_max_rnd_address_line_2_seq ) +1 address_line2_key,
           mod(abs(dbms_random.random), g_max_rnd_address_line_3_seq ) +1 address_line3_key    
    from disc_exer_req_mandates ;
    
    begin
      execute immediate 'drop index rnd_derm_adresss_lu_idx1';    
      exception when others then null;
    end;
    
    execute immediate 'create index rnd_derm_adresss_lu_idx1 on rnd_derm_adresss_lu(disc_exer_req_mandate_id)';

    begin
      execute immediate 'drop sequence ns1';        
      exception when others then null;
    end;
    
    begin
      execute immediate 'drop sequence ns2';        
      exception when others then null;
    end;
    
    execute immediate 'create sequence ns1 start with 1';
    execute immediate 'create sequence ns2 start with 1';

    execute immediate 'truncate table disc_exer_req_mandates_tmp1';

    insert into disc_exer_req_mandates_tmp1 ( disc_exer_req_mandate_id,discret_exercise_req_id ,  
    mandate_type_id,use_for_sales_proceeds_yn,use_for_div_pays_yn ,bank_acc_no,society_acc_roll_no ,               
    bank_sort_code,bank_name ,bank_id_code_bic,int_bank_acc_no_iban,int_acc_no ,address_line_1,                     
    address_line_2 ,address_line_3 ,address_line_6 ,country_code ,created_by ,created_date ,modified_by,mod_timestamp)
    select  derm.disc_exer_req_mandate_id,derm.discret_exercise_req_id ,  
    derm.mandate_type_id,derm.use_for_sales_proceeds_yn,derm.use_for_div_pays_yn , 
    case when bank_acc_no is not null then lpad(ns1.nextval, 10, 0) else null end bank_acc_no,
    tdc(derm.society_acc_roll_no),               
    derm.bank_sort_code,bank_name,tdc(derm.bank_id_code_bic),
    tdc(derm.int_bank_acc_no_iban),
    case when derm.int_acc_no is not null then lpad(ns2.nextval, 10, 0) else null end int_acc_no,
    addr1.address_line_1,                     
    addr2.address_line_2 ,addr3.address_line_3 ,null address_line_6 , 'GB' country_code,
    derm.created_by ,derm.created_date ,derm.modified_by,derm.mod_timestamp                   
    from disc_exer_req_mandates derm
    left join rnd_derm_adresss_lu rn_addr on rn_addr.disc_exer_req_mandate_id = derm.DISCRET_EXERCISE_REQ_ID
    left join address_line_1_rnd addr1 on addr1.key_ns  = rn_addr.address_line1_key
    left join address_line_2_rnd addr2 on addr2.key_ns  = rn_addr.address_line2_key
    left join address_line_3_rnd addr3 on addr3.key_ns  = rn_addr.address_line3_key;
  

--    insert into manual_transform (owner,table_name, actual_col,trans_function,technique)   
--    select 'PRISM_CORE' ,'DISC_EXER_SPOUSE_DTLS','BANK_ACC_NO','bespoke(bank_acc_no)','PADDED2_SEQ' from dual union all
--    select 'PRISM_CORE' ,'DISC_EXER_SPOUSE_DTLS','SOCIETY_ACC_ROLL_NO','tdc(society_acc_roll_no)', 'SUBS_DIGITS_TRANSFORM_CAPS_CHAR' from dual union all
--    select 'PRISM_CORE' ,'DISC_EXER_SPOUSE_DTLS','BANK_ID_CODE_BIC','tdc(bank_id_code_bic)', 'SUBS_DIGITS_TRANSFORM_CAPS_CHAR' from dual union all
--    select 'PRISM_CORE' ,'DISC_EXER_SPOUSE_DTLS','INT_BANK_ACC_NO_IBAN','tdc(int_bank_acc_no_iban)','SUBS_DIGITS_TRANSFORM_CAPS_CHAR' from dual union all
--    select 'PRISM_CORE' ,'DISC_EXER_SPOUSE_DTLS','INT_ACC_NO','bespoke(int_bank_acc_no_iban)','PADDED2_SEQ' from dual union all
--    select 'PRISM_CORE' ,'DISC_EXER_SPOUSE_DTLS','ADDRESS_LINE_1','bespoke(adress_line_1)', 'RND_NAME' from dual union all
--    select 'PRISM_CORE' ,'DISC_EXER_SPOUSE_DTLS','ADDRESS_LINE_2','bespoke(adress_line_2)' ,'RND_NAME' from dual union all
--    select 'PRISM_CORE' ,'DISC_EXER_SPOUSE_DTLS','ADDRESS_LINE_3','bespoke(adress_line_3)','RND_NAME' from dual union all
--    select 'PRISM_CORE' ,'DISC_EXER_SPOUSE_DTLS','ADDRESS_LINE_6','bespoke(adress_line_6)','RND_NAME' from dual union all
--    select 'PRISM_CORE' ,'DISC_EXER_SPOUSE_DTLS','COUNTRY_CODE','bespoke(country_code)'  , 'DEFAULT_TO_GB' from dual;

    commit; 
    
    exception
       when others then
          g_code := SQLCODE;
          g_errm := SUBSTR(SQLERRM, 1 , 4000);     
          obfus_log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),g_code,g_errm,g_module);
          RAISE; -- Error will propogate back to anonymise setting p_obfus_status => 'FAILED' 
  end;
end anon_disc_exer_req_mandates;

procedure anon_disc_exer_spouse_dtls is 
begin

  begin

    g_module := 'anon_disc_exer_spouse_dtls';
    
    begin
      execute immediate 'drop table rnd_adesd_name_lookup';        
      exception when others then null;
    end;
        
    execute immediate 'create table  rnd_adesd_name_lookup (discret_exercise_req_id number,surname_key number,forename_key number)';
        

    insert into rnd_adesd_name_lookup (discret_exercise_req_id,surname_key ,forename_key ) 
    select discret_exercise_req_id, mod(abs(dbms_random.random), g_max_rnd_surname_seq ) +1 surname_key, mod(abs(dbms_random.random), g_max_rnd_forename_seq ) +1 forename_key
    from disc_exer_spouse_dtls;

    begin
      execute immediate 'drop index rnd_adesd_name_idx1';    
      exception when others then null;
    end;
    
    execute immediate 'create index rnd_adesd_name_idx1 on rnd_adesd_name_lookup(discret_exercise_req_id)';
--    Match on address_type 4 if no match get from seed_address					

    begin
      execute immediate 'drop table rnd_adesd_adresss_lu';        
      exception when others then null;
    end;
        
    execute immediate 'create table  rnd_adesd_adresss_lu (discret_exercise_req_id number,address_line1_key number,
    address_line2_key  number,address_line3_key  number,post_code_key number)';
        

    insert into rnd_adesd_adresss_lu(discret_exercise_req_id,address_line1_key,address_line2_key,address_line3_key,post_code_key) 
    select discret_exercise_req_id,
           mod(abs(dbms_random.random), g_max_rnd_address_line_1_seq ) +1 address_line1_key ,
           mod(abs(dbms_random.random), g_max_rnd_address_line_2_seq ) +1 address_line2_key,
           mod(abs(dbms_random.random), g_max_rnd_address_line_3_seq ) +1 address_line3_key,   
           mod(abs(dbms_random.random), g_max_rnd_postcode_seq ) +1 post_code_key       
    from disc_exer_spouse_dtls ;
    
    begin
      execute immediate 'drop index rnd_adesd_adresss_lu_idx1';    
      exception when others then null;
    end;
    
    execute immediate 'create index rnd_adesd_adresss_lu_idx1 on rnd_adesd_adresss_lu(discret_exercise_req_id)';

    execute immediate 'truncate table disc_exer_spouse_dtls_tmp1';


    insert into disc_exer_spouse_dtls_tmp1 ( discret_exercise_req_id,surname,forenames , title,address_line_1,
    address_line_2,address_line_3, address_line_4, address_line_5,  address_line_6, 
    post_code_left , post_code_right ,
    country_code,created_by,created_date,modified_by,mod_timestamp)
    select desd.discret_exercise_req_id,hn_surname.surname,fl_forename.name forenames,case when fl_forename.gender = 'M' then 'MR' else 'MRS' end title,addr1.address_line_1,
    addr2.address_line_2,addr3.address_line_3,null address_line_4,null address_line_5, null address_line_6, 
    postcode.postcode_left post_code_left ,postcode.postcode_right post_code_right ,
    'GB' country_code,desd.created_by,desd.created_date,desd.modified_by,desd.mod_timestamp  
    from disc_exer_spouse_dtls desd
    left join rnd_adesd_adresss_lu rn_addr on rn_addr.discret_exercise_req_id = desd.discret_exercise_req_id
    left join address_line_1_rnd addr1 on addr1.key_ns  = rn_addr.address_line1_key
    left join address_line_2_rnd addr2 on addr2.key_ns  = rn_addr.address_line2_key
    left join address_line_3_rnd addr3 on addr3.key_ns  = rn_addr.address_line3_key
    left join postcode_rnd postcode on postcode.key_ns  = rn_addr.post_code_key
    left join  rnd_adesd_name_lookup rn_name on rn_name.discret_exercise_req_id = desd.discret_exercise_req_id
    left join  surname_list fl_surname on fl_surname.surname_seq = rn_name.surname_key 
    left join holder_names_tmp1 hn_surname on hn_surname.holder_name_id = fl_surname.holder_name_id and hn_surname.holder_seq = 1
    left join  forename_list fl_forename on fl_forename.key_ns = rn_name.forename_key ;

--    insert into manual_transform (owner,table_name, actual_col,trans_function,technique)   
--    select 'PRISM_CORE' ,'DISC_EXER_SPOUSE_DTLS','SURNAME','rnd_name(surname)','RND_NAME' from dual union all
--    select 'PRISM_CORE' ,'DISC_EXER_SPOUSE_DTLS','FORENAMES','rnd_name(forenames)', 'RND_NAME' from dual union all
--    select 'PRISM_CORE' ,'DISC_EXER_SPOUSE_DTLS','TITLE','rnd_name(title)', 'RND_NAME' from dual union all
--    select 'PRISM_CORE' ,'DISC_EXER_SPOUSE_DTLS','ADDRESS_LINE_1','rnd_address(adress_line_1)','RND_NAME' from dual union all
--    select 'PRISM_CORE' ,'DISC_EXER_SPOUSE_DTLS','ADDRESS_LINE_2','rnd_address(adress_line_2)', 'RND_NAME' from dual union all
--    select 'PRISM_CORE' ,'DISC_EXER_SPOUSE_DTLS','ADDRESS_LINE_3','rnd_address(adress_line_3)'  , 'RND_NAME' from dual union all
--    select 'PRISM_CORE' ,'DISC_EXER_SPOUSE_DTLS','POST_CODE_LEFT','rnd_address(post_code_left)'  , 'RND_NAME' from dual union all
--    select 'PRISM_CORE' ,'DISC_EXER_SPOUSE_DTLS','POST_CODE_RIGHT','rnd_address(post_code_right)'  , 'RND_NAME' from dual;


    commit; 
      
    exception
       when others then
          g_code := SQLCODE;
          g_errm := SUBSTR(SQLERRM, 1 , 4000);     
          obfus_log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),g_code,g_errm,g_module);
          RAISE; -- Error will propogate back to anonymise setting p_obfus_status => 'FAILED'
  end;

end anon_disc_exer_spouse_dtls;

procedure anon_cash_ivc_class_copies  is  
begin

    g_module := 'anon_cash_ivc_class_copies';
    insert into cash_ivc_class_copies_tmp1 (cash_ivc_class_copy_id,ivc_class_copy_id,residual_currency_code,currency_code,
    payment_method_code,comp_code,class_code,event_code,reinvestment_id,rights_type_code,
    holding,residual_repay_amt,cash_acc_id,cash_acc_grp_id,payee_name,invalid_yn,
    retained_residual_amt,note,charity_residual_amt,company_residual_amt,withhold_pay_yn,
    consolidation_no,cash_calc_complete_yn,
    bank_name,sort_code,country,account_no,building_society_acc_no,
    bic,international_acc_no,branch_name,international_branch_id,iban,society_acc_roll_no,
    address_line_1,address_line_2,address_line_3,address_line_4,address_line_5,address_line_6,
    post_code_left,post_code_right,irish_distribution_code,
    
    country_code,created_by,created_date,
    modified_by,mod_timestamp,mandate_type_id,branch_address,thld_ineligible_yn,ips_currency_code,
    cfwd_rein_fract_rule_type_code,bfwd_residual_amt,cfwd_residual_amt,reinvest_ivc_class_copy_id,
    cash_ivc_cls_copy_type_code)
    
    select cicc.cash_ivc_class_copy_id,cicc.ivc_class_copy_id,cicc.residual_currency_code,cicc.currency_code,
    cicc.payment_method_code,cicc.comp_code,cicc.class_code,cicc.event_code,cicc.reinvestment_id,cicc.rights_type_code,
    cicc.holding,cicc.residual_repay_amt,cicc.cash_acc_id,cicc.cash_acc_grp_id,cicc.payee_name,cicc.invalid_yn,
    cicc.retained_residual_amt,cicc.note,cicc.charity_residual_amt,cicc.company_residual_amt,cicc.withhold_pay_yn,
    cicc.consolidation_no,cicc.cash_calc_complete_yn,
    
    

    hm.bank_name,cicc.sort_code,hm.country_code country,hm.bank_account_number,bsb.building_society_account_no,
    hm.bic_code,hm.international_account_num,cicc.branch_name,cicc.international_branch_id,hm.iban_number,hm.society_acc_roll_number ,
    hm.address_line1,hm.address_line2,hm.address_line3,hm.address_line4,hm.address_line5,hm.address_line6,
    hm.postcode_left,hm.postcode_right,hm.irish_distribution_code,
    
    
    cicc.country_code,cicc.created_by,cicc.created_date,
    cicc.modified_by,cicc.mod_timestamp,cicc.mandate_type_id,cicc.branch_address,cicc.thld_ineligible_yn,cicc.ips_currency_code,
    cicc.cfwd_rein_fract_rule_type_code,cicc.bfwd_residual_amt,cicc.cfwd_residual_amt,cicc.reinvest_ivc_class_copy_id,
    cicc.cash_ivc_cls_copy_type_code
    cash_ivc_cls_copy_type_code
    from cash_ivc_class_copies cicc
    left outer join cash_accounts ca on ca.CASH_ACCOUNT_ID  = cicc.CASH_ACC_ID
    left outer join holder_mandates hm on ca.comp_code = hm.comp_code and hm.ivc_code = ca.ivc_code  and nvl(hm.class_code,-1) = nvl(cicc.class_code,-1)  and hm.mandate_type_id = cicc.mandate_type_id 
    left outer join building_society_branches bsb on bsb.building_society_branch_id = hm.building_society_branch_id;
  
exception
   when others then
      g_code := SQLCODE;
      g_errm := SUBSTR(SQLERRM, 1 , 4000);     
      obfus_log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),g_code,g_errm,g_module);
      RAISE; -- Error will propogate back to anonymise setting p_obfus_status => 'FAILED'          
end anon_cash_ivc_class_copies;

procedure anon_comp_payee_mandates  is  
begin
  begin

    g_module := 'anon_comp_payee_mandates';

    begin
      execute immediate 'drop table rnd_cpm_adresss_lu';    
      exception when others then null;
    end;
    
    --dbms_output.put_line(
    execute immediate 'create table  rnd_cpm_adresss_lu (comp_payee_id number,address_line1_key number,
    address_line2_key  number,address_line3_key  number,post_code_key number)';
        
    insert into rnd_cpm_adresss_lu(comp_payee_id,address_line1_key,address_line2_key,address_line3_key,post_code_key) 
    select comp_payee_id,
           mod(abs(dbms_random.random), g_max_rnd_address_line_1_seq ) +1 address_line1_key ,
           mod(abs(dbms_random.random), g_max_rnd_address_line_2_seq ) +1 address_line2_key,
           mod(abs(dbms_random.random), g_max_rnd_address_line_3_seq ) +1 address_line3_key ,
           mod(abs(dbms_random.random), g_max_rnd_postcode_seq ) +1 post_code_key
    from comp_payee_mandates ;
    
    begin
      execute immediate 'drop index rnd_cpm_adresss_lu_idx1';    
      exception when others then null;
    end;
    
    execute immediate 'create index rnd_cpm_adresss_lu_idx1 on rnd_cpm_adresss_lu(comp_payee_id)';

    begin
      execute immediate 'drop sequence ns1';        
      exception when others then null;
    end;
    
    begin
      execute immediate 'drop sequence ns2';        
      exception when others then null;
    end;
    
    execute immediate 'create sequence ns1 start with 1';
    execute immediate 'create sequence ns2 start with 1';

    execute immediate 'truncate table comp_payee_mandates_tmp1';

    insert into comp_payee_mandates_tmp1 (comp_payee_id,mandate_type_id,country_code,currency_code,
                                          bank_sort_code,building_society_branch_id,bank_account_number,
                                          international_account_num,account_reference,society_acc_roll_number,
                                          bic_code,iban_number,bank_name,address_line1,address_line2,address_line3,
                                          address_line4,address_line5,address_line6,post_code_left,post_code_right,
                                          irish_distribution_code,created_by,created_date,modified_by,mod_timestamp)

    select  cpm.comp_payee_id,cpm.mandate_type_id,cpm.country_code,cpm.currency_code,
                                          cpm.bank_sort_code,cpm.building_society_branch_id,cpm.bank_account_number,
                                          cpm.international_account_num,cpm.account_reference,cpm.society_acc_roll_number,
                                          cpm.bic_code,cpm.iban_number,cpm.bank_name,addr1.address_line_1,addr2.address_line_2,addr3.address_line_3,
                                          null address_line4,null address_line5,null address_line6,postcode.postcode_left,postcode.postcode_right,
                                          null irish_distribution_code,cpm.created_by,cpm.created_date,cpm.modified_by,cpm.mod_timestamp
    
    
    from comp_payee_mandates cpm
    left join rnd_cpm_adresss_lu rn_addr on rn_addr.comp_payee_id = cpm.comp_payee_id
    left join address_line_1_rnd addr1 on addr1.key_ns  = rn_addr.address_line1_key
    left join address_line_2_rnd addr2 on addr2.key_ns  = rn_addr.address_line2_key
    left join address_line_3_rnd addr3 on addr3.key_ns  = rn_addr.address_line3_key
    left join postcode_rnd postcode on postcode.key_ns  = rn_addr.post_code_key;
  
  

    commit; 
     
    exception
       when others then
          g_code := SQLCODE;
          g_errm := SUBSTR(SQLERRM, 1 , 4000);     
          obfus_log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),g_code,g_errm,g_module);
          RAISE; -- Error will propogate back to anonymise setting p_obfus_status => 'FAILED'
  end;
end anon_comp_payee_mandates;
   
procedure anon_mifid_entities is 
begin

  begin

    g_module := 'anon_mifid_entities';
   
    begin
      execute immediate 'drop table rnd_me_name_lookup';        
      exception when others then null;
    end;
        
    execute immediate 'create table  rnd_me_name_lookup (mifid_entity_id number,surname_key number,forename_key number)';
        

    insert into rnd_me_name_lookup (mifid_entity_id,surname_key ,forename_key ) 
    select discret_exercise_req_id, mod(abs(dbms_random.random), g_max_rnd_surname_seq ) +1 surname_key, mod(abs(dbms_random.random), g_max_rnd_forename_seq ) +1 forename_key
    from disc_exer_spouse_dtls;

    begin
      execute immediate 'drop index rnd_me_name_idx1';    
      exception when others then null;
    end;
    
    execute immediate 'create index rnd_me_name_idx1 on rnd_me_name_lookup(mifid_entity_id)';
--    Match on address_type 4 if no match get from seed_address					

    execute immediate 'truncate table mifid_entities_tmp1';

    insert into mifid_entities_tmp1 ( mifid_entity_id	,derived_nat_country_code	,confirmation_type_code	,
    mifid_entity_cat_type_code,mifid_identifier_type_id,mifid_identifier_type_value,natural_person_yn,
    mifid_entity_type_code,comp_code,entity_sequence,forenames,surname,date_of_birth,register_forenames,
    register_surname,name	,ack_1st_prty_not_issued_yn,ack_2nd_prty_not_issued_yn,deletion_date_nt,
    last_confirmed_date,last_confirmed_by,created_by,created_date,modified_by,mod_timestamp)
    select me.mifid_entity_id	,me.derived_nat_country_code	,me.confirmation_type_code	,
    me.mifid_entity_cat_type_code,me.mifid_identifier_type_id,me.mifid_identifier_type_value,me.natural_person_yn,
    me.mifid_entity_type_code,me.comp_code,me.entity_sequence,fl_forename.name,hn_surname.surname,me.date_of_birth,
    fl_forename.name register_forenames,hn_surname.surname,me.name,me.ack_1st_prty_not_issued_yn,me.ack_2nd_prty_not_issued_yn,
    me.deletion_date_nt,me.last_confirmed_date,me.last_confirmed_by,me.created_by,me.created_date,me.modified_by,me.mod_timestamp 
    from mifid_entities me
    left join  rnd_me_name_lookup rn_name on rn_name.mifid_entity_id = me.mifid_entity_id
    left join  surname_list fl_surname on fl_surname.surname_seq = rn_name.surname_key 
    left join holder_names_tmp1 hn_surname on hn_surname.holder_name_id = fl_surname.holder_name_id and hn_surname.holder_seq = 1
    left join  forename_list fl_forename on fl_forename.key_ns = rn_name.forename_key;

--    insert into manual_transform (owner,table_name, actual_col,trans_function,technique)   
--    select 'EGIS' ,'MIFID_ENTITIES','SURNAME','rnd_name(surname)','RND_NAME' from dual union all
--    select 'EGIS' ,'MIFID_ENTITIES','FORENAMES','rnd_name(forenames)', 'RND_NAME' from dual union all
--    select 'EGIS' ,'MIFID_ENTITIES','REGISTER_SURNAME','rnd_name(register_surname)', 'RND_NAME' from dual union all
--    select 'EGIS' ,'MIFID_ENTITIES','REGISTER_FORENAMES','rnd_name(register_forenames)','RND_NAME' from dual;
    
    commit; 
    
    exception
       when others then
          g_code := SQLCODE;
          g_errm := SUBSTR(SQLERRM, 1 , 4000);     
          obfus_log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),g_code,g_errm,g_module);
          RAISE; -- Error will propogate back to anonymise setting p_obfus_status => 'FAILED'          
  end;

end anon_mifid_entities;

procedure anon_mifid_integration is 
begin

  begin

    g_module := 'anon_mifid_integration';
   
    begin
      execute immediate 'drop table rnd_mi_name_lookup';        
      exception when others then null;
    end;
        
    execute immediate 'create table  rnd_mi_name_lookup (transaction_detail_id number,bulk_trade_id number,surname_key number)';
          
    insert into rnd_mi_name_lookup (transaction_detail_id,bulk_trade_id,surname_key)   
    select transaction_detail_id,bulk_trade_id,mod(abs(dbms_random.random), g_max_rnd_surname_seq ) +1 surname_key
    from mifid_transaction_details mbt
    left join holder_names hn on hn.comp_code =  mbt.company_code and hn.ivc_code =  substr('0000000'||mbt.ivc_code,1,11) 
    where hn.ivc_code is null;
    
    begin
      execute immediate 'drop index rnd_mi_name_idx1';    
      exception when others then null;
    end;
    
    execute immediate 'create index rnd_mi_name_idx1 on rnd_mi_name_lookup(transaction_detail_id)';
    
    execute immediate 'create index rnd_mi_name_idx2 on rnd_mi_name_lookup(bulk_trade_id)';
    		
    execute immediate 'truncate table mifid_transaction_details_tmp1';
        
    execute immediate 'truncate table mifid_bulk_trades_tmp1';
    
    insert into mifid_transaction_details_tmp1 (transaction_detail_id,job_id,bulk_trade_id,extract_date,trade_date,
    reference,trade_time,trade_reason,bulk_trade_reference,ticker,company_name,company_code,
    isin,ivc_code,holder_surname,
    trade_type,quantity,currency_code,share_price,commission,
    consideration,settlement_date,dealer_name,counterparty,broker_code,execution_venue,
    comments,stock_received,checked_by,checked_on,check_status,data_source_location,
    created_by,created_date,modified_by,mod_timestamp )
    
    select mtd.transaction_detail_id,mtd.job_id,mtd.bulk_trade_id,mtd.extract_date,mtd.trade_date,
    mtd.reference,mtd.trade_time,mtd.trade_reason,mtd.bulk_trade_reference,mtd.ticker,mtd.company_name,
    mtd.company_code,mtd.isin,mtd.ivc_code,
    case when hn.surname is null then hn_surname.surname else hn.surname end holder_surname,
    mtd.trade_type,mtd.quantity,mtd.currency_code,mtd.share_price,mtd.commission,
    mtd.consideration,mtd.settlement_date,mtd.dealer_name,mtd.counterparty,mtd.broker_code,mtd.execution_venue,
    mtd.comments,mtd.stock_received,mtd.checked_by,mtd.checked_on,mtd.check_status,mtd.data_source_location,
    mtd.created_by,mtd.created_date,mtd.modified_by,mtd.mod_timestamp    
    from mifid_transaction_details mtd
    left join holder_names hn on hn.comp_code =  mtd.company_code and hn.ivc_code =  substr('0000000'||mtd.ivc_code,1,11) and hn.holder_seq = 1
    left join  rnd_mi_name_lookup rn_name on rn_name.transaction_detail_id = mtd.transaction_detail_id
    left join  surname_list fl_surname on fl_surname.surname_seq = rn_name.surname_key 
    left join holder_names_tmp1 hn_surname on hn_surname.holder_name_id = fl_surname.holder_name_id;


    insert into mifid_bulk_trades_tmp1(bulk_trade_id,job_id,trade_date,reference,
    trade_time,trade_reason,bulk_trade_reference,ticker,company_name,company_code,
    isin,ivc_code,holder_surname,trade_type,quantity,currency_code,share_price,commission,
    settlement_date,consideration,counterparty,broker_code,execution_venue,dealer_name,
    comments,checker_authority,checked_by,checked_on,check_status,data_source_location,
    created_by,created_date,modified_by,mod_timestamp)
    select mbt.bulk_trade_id,mbt.job_id,trade_date,mbt.reference,
    mbt.trade_time,mbt.trade_reason,mbt.bulk_trade_reference,mbt.ticker,mbt.company_name,mbt.company_code,
    mbt.isin,ivc_code,case when trans_surname.holder_surname is null then 'N/A' else trans_surname.holder_surname end holder_surname,
    mbt.trade_type,mbt.quantity,mbt.currency_code,mbt.share_price,
    mbt.commission,mbt.settlement_date,mbt.consideration,mbt.counterparty,mbt.broker_code,mbt.execution_venue,
    mbt.dealer_name,mbt.comments,mbt.checker_authority,mbt.checked_by,mbt.checked_on,mbt.check_status,
    mbt.data_source_location,mbt.created_by,mbt.created_date,mbt.modified_by,mbt.mod_timestamp
    from mifid_bulk_trades mbt     
    left join (select bulk_trade_id,no_in_bulk,holder_surname from 
                (
                    select bulk_trade_id, count(*) over (partition by bulk_trade_id) no_in_bulk ,
                    first_value(holder_surname) over (partition by bulk_trade_id order by 1) holder_surname
                    from mifid_transaction_details mtd 
                ) where no_in_bulk = 1
              ) trans_surname on  mbt.bulk_trade_id = trans_surname.bulk_trade_id;


--    insert into manual_transform (owner,table_name, actual_col,trans_function,technique)   
--    select 'INTEGRATION' ,'mifid_bulk_trades','HOLDER_SURNAME','rnd_name(holder_surname)','RND_NAME' from dual union all
--    select 'INTEGRATION' ,'mifid_bulk_trades','HOLDER_SURNAME','rnd_name(holder_surname)', 'RND_NAME' from dual;

    commit; 
    
    exception
       when others then
          g_code := SQLCODE;
          g_errm := SUBSTR(SQLERRM, 1 , 4000);     
          obfus_log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),g_code,g_errm,g_module);      
          RAISE; -- Error will propogate back to anonymise setting p_obfus_status => 'FAILED'
  end;

end anon_mifid_integration;


procedure gen_rnd_addresses is
   l_counter number;
begin

    g_module := 'gen_rnd_addresses';
    
    begin execute immediate 'drop index idx_address_line_1_rnd1'; exception when others then null; end;
    begin execute immediate 'drop index idx_address_line_2_rnd1';  exception when others then null; end;
    begin execute immediate 'drop index idx_address_line_3_rnd1';  exception when others then null; end;
    begin execute immediate 'drop index idx_postcode_rnd';         exception when others then null; end;

    begin
      execute immediate 'drop sequence ns';   
      exception when others then null;
    end;

    execute immediate 'create sequence ns start with 1';

    execute immediate 'truncate table address_line_1_rnd';
    
    insert into  address_line_1_rnd( key_ns,address_line_1)
    select  ns.nextval as key_ns ,name1  as address_line_1 from places
    where local_type in ('Section Of Named Road','Named Road');
    
    execute immediate 'drop sequence ns';
    execute immediate 'create sequence  ns start with 1';
    
    execute immediate 'truncate table address_line_2_rnd';
    
    insert into  address_line_2_rnd( key_ns,address_line_2)
    select  ns.nextval as key_ns,name1 as address_line_2 from places
    where local_type in ('Other Settlement','Suburban Area','Hamlet');
    
    execute immediate 'drop sequence ns';
    execute immediate 'create sequence  ns start with 1';
 
    execute immediate 'truncate table address_line_3_rnd';
     
    insert into  address_line_3_rnd( key_ns,address_line_3)
    select  ns.nextval as key_ns,name1 as address_line_3 from places
    where local_type in ('Town','City');
    
    execute immediate 'drop sequence ns';
    execute immediate 'create sequence  ns start with 1';
  
    execute immediate 'truncate table postcode_rnd';    
    insert into  postcode_rnd( key_ns,postcode_left,postcode_right)
    select ns.nextval as key_ns,substr (name1,1,instr(name1,' ')-1) postcode_left,substr (name1,instr(name1,' ')+1) as postcode_right from places
    where local_type in ('Postcode');
    
    execute immediate 'create index idx_address_line_1_rnd1 on address_line_1_rnd(key_ns)';
    execute immediate 'create index idx_address_line_2_rnd1 on address_line_2_rnd(key_ns)';
    execute immediate 'create index idx_address_line_3_rnd1 on address_line_3_rnd(key_ns)';
    execute immediate 'create index idx_postcode_rnd on postcode_rnd(key_ns)';

    commit; 
exception
   when others then
      g_code := SQLCODE;
      g_errm := SUBSTR(SQLERRM, 1 , 4000);     
      obfus_log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),g_code,g_errm,g_module);       
      RAISE; -- Error will propogate back to anonymise setting p_obfus_status => 'FAILED'
end gen_rnd_addresses;

procedure gen_rnd_names is
   l_counter number;
begin

   g_module := 'gen_rnd_names';

   begin
      execute immediate 'drop sequence surname_seq';   
      exception when others then null;
   end;
        
   begin
     execute immediate 'drop sequence forename_seq';        
   exception when others then null;
   end;
        
   execute immediate 'create sequence surname_seq start with 1';
        
   execute immediate 'create sequence forename_seq start with 1';

   begin execute immediate 'drop index sl_idx1'; exception when others then null; end;
   begin execute immediate 'drop index fl_idx1'; exception when others then null; end;

   execute immediate 'truncate table surname_list';   
        
   execute immediate 'truncate table forename_list';  
        
   insert into  surname_list(surname_seq,holder_name_id) select surname_seq.nextval surname_seq,holder_name_id from holder_names
   where surname is not null and holder_type_code = 'I';
      
   insert into  forename_list(key_ns,year,name,percent,gender) 
   select forename_seq.nextval key_ns ,year,name,percent,gender from forename_seed;
        
   execute immediate 'create index sl_idx1 on surname_list(surname_seq)';
   execute immediate 'create index fl_idx1 on forename_list(key_ns)';

   commit; 
exception
   when others then
      g_code := SQLCODE;
      g_errm := SUBSTR(SQLERRM, 1 , 4000);     
      obfus_log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),g_code,g_errm,g_module);    
      RAISE; -- Error will propogate back to anonymise setting p_obfus_status => 'FAILED'
end gen_rnd_names;
  
procedure gen_rnd_notes is
  l_counter number;
begin
  begin

    g_module := 'gen_rnd_notes'; 
    begin    
      execute immediate 'truncate table randomised_notes';
    exception when others then null; 
    end;
    
    begin
      execute immediate 'drop sequence notes_seq';
    exception when others then null; 
    end;
    execute immediate 'create sequence notes_seq start with 1 increment by 1';
    
    l_counter := 0;
    loop
      insert into  randomised_notes(key_ns,rnd_words) 
      select notes_seq.nextval,rnd_words from
      (
            select  LISTAGG(word, ' ') WITHIN GROUP(order by rnd) rnd_words
            from 
            (
              select key_ns,word,rnd,mod(abs(dbms_random.random), 1000 ) +1 word_group
              from (select key_ns,word,dbms_random.random  rnd from word_list )
              order by rnd
            ) 
            group by word_group
     );
     l_counter := l_counter + 1;
     if l_counter = 100 then
      exit;
     end if;
     commit;
      --exit;
    end loop;
  
    commit; 
    
    exception
       when others then
          g_code := SQLCODE;
          g_errm := SUBSTR(SQLERRM, 1 , 4000);     
          obfus_log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),g_code,g_errm,g_module);
          RAISE; -- Error will propogate back to anonymise setting p_obfus_status => 'FAILED'
    end;
end gen_rnd_notes;
     
   
procedure merge_fix_anonalies
is
       cursor c_get_stmt is  
       
      with merge_parts as
      (select --*
      g_src_prefix||'_'||owner owner,table_name table_name, 'merge into tgt_'||substr(table_name,1,26)  ||' tgt using ' as merge_txt ,
          'tdc(TGT1.'||column_name||') '||column_name  select_cols,
          --listagg('tdc('||column_name||') '||column_name, ', ') within group (ORDER BY column_name) as select_cols,
          --substr(table_name,1,26)||'_tmp) src' as select_txt2,
           'TGT_'||substr(table_name,1,26)||' TGT1 ,'||table_name||' SRC ' as select_table,
          ' when matched then update set ' ||'tgt.'||column_name ||'= SRC.'||column_name set_txt,
          ' where  ' ||'SRC.'||column_name ||'=' ||'TGT1.'||column_name where_txt1
      from (select owner,table_name,column_name from stats_results_tmp where stat_type = 'equals' and val > 0 and trans_function  not in ('EXCLUDE' ) ) ) ,
      pk as (select 
          ac.owner,ac.table_name,' on ('||listagg('tgt.'||acc.column_name ||'=' ||'src.'||acc.column_name, ' and ') within group (ORDER BY column_name)||')'  as on_txt,
          listagg('TGT1.'||acc.column_name,',') within group (ORDER BY column_name) as pk_cols,
          ' and ('||listagg('SRC.'||acc.column_name ||'=' ||'TGT1.'||acc.column_name, ' and ') within group (ORDER BY column_name)||')'  as  where_txt2
          from all_constraints ac  join all_cons_columns  acc on ac.constraint_name = acc.constraint_name and ac.owner = acc.owner
          where ac.constraint_type = 'P' and  ac.owner like g_src_prefix||'_'||'%'
          group by ac.owner,ac.table_name)
          
        select merge_parts.merge_txt||'( select '||merge_parts.select_cols||','||pk.pk_cols||' from '||merge_parts.select_table||merge_parts.where_txt1||pk.where_txt2 ||') src'||pk.on_txt||merge_parts.set_txt as stmt from merge_parts,pk
        where merge_parts.owner = pk.owner and merge_parts.table_name = pk.table_name;      
   
    v_nCounter number;
    begin 
      begin
    
        g_module := 'merge_fix_anonalies';       
        v_nCounter := 0;
        
        dbms_session.set_identifier ('adcfs\ksheehan1' || ':' || '1');
        
        begin  
          insert into tgt_audit_events(EVENT_ID,COMP_CODE  )  VALUES(1,'');
          exception when others then null;
        end;
        
        loop
            for get_stmt_rec in c_get_stmt loop
                  
              begin
                  execute immediate get_stmt_rec.stmt;
                  commit;
                exception when others then  
                  dbms_output.put_line(get_stmt_rec.stmt);
              end;
          
            end loop;
            
            v_nCounter := v_nCounter + 1;
            if v_nCounter > 10 then
              exit;
            end if;
        end loop;
        commit;
        
      exception
        when others then
          g_code := SQLCODE;
          g_errm := SUBSTR(SQLERRM, 1 , 4000);     
          obfus_log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),g_code,g_errm,g_module);
          RAISE; -- Error will propogate back to anonymise setting p_obfus_status => 'FAILED'          
      end;

    end merge_fix_anonalies;


    procedure merge_privacy
    is
         cursor c_get_stmt is  
         
         with merge_parts as
        (select g_src_prefix||'_'||owner owner,table_name table_name, 'merge into tgt_'||substr(table_name,1,26)  ||' tgt using ' as merge_txt  ,
            listagg(column_name, ', ') within group (ORDER BY column_name) as select_cols,
            --substr(table_name,1,26)||'_tmp) src' as select_txt2,
             substr(table_name,1,26)||'_tmp' as select_table,
            ' when matched then update set ' || listagg('TGT.'||column_name ||'=' ||'SRC.'||column_name, ', ') within group (ORDER BY column_name)  as set_txt
        from pc_transform where column_name <> trans_function  group by owner,table_name),
        pk as (select 
            ac.owner,ac.table_name,' on ('||listagg('tgt.'||acc.column_name ||'=' ||'src.'||acc.column_name, ' and ') within group (ORDER BY column_name)||')'  as on_txt,
            listagg(acc.column_name,',') within group (ORDER BY column_name) as pk_cols
            from all_constraints ac  join all_cons_columns  acc on ac.constraint_name = acc.constraint_name and ac.owner = acc.owner
            where ac.constraint_type = 'P' and  ac.owner like g_src_prefix||'\_'||'%' escape '\'
            group by ac.owner,ac.table_name)
        select merge_parts.merge_txt||'( select '||merge_parts.select_cols||','||pk.pk_cols||' from '||merge_parts.select_table||') src'||pk.on_txt||merge_parts.set_txt as stmt from merge_parts,pk
        where merge_parts.owner = pk.owner and merge_parts.table_name = pk.table_name
        --and  merge_parts.table_name NOT IN ('HOLDERS','HOLDER_NAMES','HOLDER_ADDRESSES','HOLDER_LEBELS','HOLDER_MANDATES','CHEQUE_RANGES','BANK_ACCOUNTS','HOLDER_EMPLOYEE_DETAILS','FATCA_CLSF_EXTRACTS');      
        and  merge_parts.table_name NOT IN ('FATCA_CLSF_EXTRACTS','PAYMENTS','CREST_PARTICIPANTS') ;--and merge_parts.table_name  IN ('PAYMENTS');      
    begin  
      begin
        g_module := 'merge_privacy';

        for get_stmt_rec in c_get_stmt loop                 
          begin
            execute immediate get_stmt_rec.stmt;
            --obfus_log(substr(get_stmt_rec.stmt,1,4000),g_code,g_errm,g_module);
            commit;
            exception when others then  
              g_code := SQLCODE;
              g_errm := SUBSTR(SQLERRM, 1 , 4000);
              obfus_log(substr(get_stmt_rec.stmt,1,4000),g_code,g_errm,g_module);
              obfus_log(g_module||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),g_code,g_errm,g_module);
              g_code := NULL;
              g_errm := NULL;
              --raise;
          end;
        end loop;
        
        commit; 
        
     exception
        when others then
           g_code := SQLCODE;
           g_errm := SUBSTR(SQLERRM, 1 , 4000);     
           obfus_log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),g_code,g_errm,g_module);
           RAISE; -- Error will propogate back to anonymise setting p_obfus_status => 'FAILED'
     end; 
      
  end merge_privacy;


  procedure merge_to_target (pi_schemaprefix VARCHAR2)
  as
  begin
     begin
        g_module := 'merge_to_target'; 
        DBMS_SESSION.set_identifier ('adcfs\ksheehan1' || ':' || '1');
        begin  
          insert into tgt_audit_events select * from audit_events where event_id =1;
          exception when others then null;
        end;
        
        obfus_log('merge_privacy'||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,g_module);
        merge_privacy;
        obfus_log('merge_holder_names'||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,g_module);
        merge_holder_names;
        obfus_log('merge_holder_address'||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,g_module);
        merge_holder_address;
        execute immediate 'ALTER TRIGGER '||g_tgt_prefix||'_PRISM_CORE.HOLDERS_BRIUD disable';
        obfus_log('merge_holders'||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,g_module);
        merge_holders;
        obfus_log('merge_holder_labels'||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,g_module);
        merge_holder_labels;
        obfus_log('merge_holder_employee_details'||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,g_module);
        merge_holder_employee_details;
        obfus_log('merge_holder_mandate_details'||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,g_module);
        merge_holder_mandate_details;
        obfus_log('merge_bank_branches'||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,g_module);
        merge_bank_branches;
        obfus_log('merge_bank_accounts'||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,g_module);
        merge_bank_accounts;
        obfus_log('merge_holder_payments'||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,g_module);
        merge_payments;
        obfus_log('merge_cheque_ranges'||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,g_module);
        merge_cheque_ranges;
        obfus_log('merge_disc_exer_spouse_dtls'||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,g_module);
        merge_disc_exer_spouse_dtls;
        obfus_log('merge_disc_exer_req_mandates'||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,g_module);
        merge_disc_exer_req_mandates;
        obfus_log('merge_mifid_entities'||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,g_module);         
        merge_mifid_entities;
        obfus_log('merge_cash_ivc_class_copies'||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,g_module);      
        merge_cash_ivc_class_copies;
        obfus_log('merge_comp_payee_mandates'||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,g_module);   
        merge_comp_payee_mandates;
        obfus_log('merge_patches'||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,g_module);
        merge_patches;
        execute immediate 'ALTER TRIGGER '||g_tgt_prefix||'_PRISM_CORE.HOLDERS_BRIUD enable';
     exception
        when others then
          g_code := SQLCODE;
          g_errm := SUBSTR(SQLERRM, 1 , 4000);     
          obfus_log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),g_code,g_errm,g_module); 
     end;
  end merge_to_target;
   

   PROCEDURE run_purge_data(p_prefix varchar2)
   IS

      cursor purge_tables(p_prefix varchar2,p_escape varchar2)
      is
         select owner, table_name, single_column, num_rows
           from tables_to_truncate;

        ncount number;
        
   begin
      begin
         g_module := 'run_purge_data';  
         execute immediate 'truncate table purge_transform';
          
         for purge_tables_rec in purge_tables(p_prefix ,'\' )  loop
            begin
               
               execute immediate 'select count(*)   from '||purge_tables_rec.owner||'.'||purge_tables_rec.table_name||' where rownum = 1' into ncount;
               
               if ncount=1 then            
                  truncate_table_new (purge_tables_rec.owner,purge_tables_rec.table_name);
               end if;

               insert into purge_transform (owner,table_name, actual_col,trans_function,technique)
               values (replace(purge_tables_rec.owner,g_tgt_prefix||'_',''),purge_tables_rec.table_name,purge_tables_rec.single_column,'truncate('||purge_tables_rec.table_name||')','PURGE');
            
            exception when others then 
               g_code := SQLCODE;
               g_errm := SUBSTR(SQLERRM, 1 , 4000);
               obfus_log('Truncation of '||purge_tables_rec.owner||'.'||purge_tables_rec.table_name|| ' failed',g_code,g_errm,g_module);
            
            end;
         end loop;
    
    exception
       when others then
          g_code := SQLCODE;
          g_errm := SUBSTR(SQLERRM, 1 , 4000);     
          obfus_log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),g_code,g_errm,g_module); 
          RAISE; -- Error will propogate back to anonymise setting p_obfus_status => 'FAILED'       
    end;

  end run_purge_data;


  procedure generate_stats(prepost_anomolies number)  as
     
     cursor c_get_stmt  is select owner,table_name,column_name,stat_type,stmt,technique,trans_function ,stereo_type
                           from  stats_stmts 
                           order by table_name,column_name,stat_type;
     l_val number;
     l_table_name varchar2(30);
     l_type varchar2(20) ;
     
  begin
    begin
  
      g_module := 'generate_stats';   
      If prepost_anomolies = 1 then 
        
        execute immediate 'truncate table stats_stmts';
        execute immediate 'truncate table stats_results_tmp';
  
      
        insert into stats_stmts (owner,table_name,column_name,stat_type,technique,trans_function,stereo_type,stmt)
            with  pk as (select 
            ac.owner,ac.table_name,listagg('x.'||acc.column_name ||'=' ||'y.'||acc.column_name, ' and ') within group (ORDER BY column_name)  as pkcols
            from all_constraints ac  join all_cons_columns  acc on ac.constraint_name = acc.constraint_name and ac.owner = acc.owner
            where ac.constraint_type = 'P' and  ac.owner like g_src_prefix||'\_%' escape '\'
            group by ac.owner,ac.table_name)
            
            select distinct pt.owner,pt.table_name,pt.column_name, 'not_equals',pt.technique,pt.trans_function,pt.stereo_type,
            
            'select count(*) from '||g_src_prefix||'_'||pt.owner||'.'||pt.table_name||  ' x , '||'tgt_'|| substr(pt.table_name,1,26)||' y where  ' ||pk.pkcols ||' and '|| 'x.' ||pt.column_name || ' <> y.' ||pt.column_name
            
            from (select  owner , table_name,column_name ,trans_function,technique,stereo_type 
                  from pc_transform pt 
                  
                  --union all select * from manual_transform 
                  union all select * from purge_transform
                  
                  ) pt join pk pk on pk.table_name = pt.table_name and pk.owner = g_src_prefix||'_'||pt.owner 
            where pt.column_name <> pt.trans_function
            
            union
            
            select distinct pt.owner,pt.table_name,pt.column_name,'equals',pt.technique,pt.trans_function,pt.stereo_type,
            
            'select count(*) from ' ||g_src_prefix||'_'||pt.owner||'.'||pt.table_name||  ' x , '||'tgt_'|| substr(pt.table_name,1,26)||' y where  ' ||pk.pkcols ||' and '|| 'x.' ||pt.column_name || ' = y.' ||pt.column_name
            
            from (select owner , table_name,column_name ,trans_function,technique,stereo_type  from pc_transform pt 
            --union all select * from manual_transform
            union all select * from purge_transform ) pt join pk pk on pk.table_name = pt.table_name and pk.owner = ''||g_src_prefix||'_'||pt.owner 
            where pt.column_name <> pt.trans_function
            union
            
            select distinct pt.owner,pt.table_name,pt.column_name,'value_to_null',pt.technique,pt.trans_function,pt.stereo_type,
            
            'select count(*) from ' ||g_src_prefix||'_'||pt.owner||'.'||pt.table_name||  ' x , '||'tgt_'|| substr(pt.table_name,1,26)||' y where  ' ||pk.pkcols ||' and '|| 'x.' ||pt.column_name || ' is not null and y.' ||pt.column_name || ' is null '
            
            from (select owner , table_name,column_name ,trans_function,technique,stereo_type   from pc_transform pt 
            --union all select * from manual_transform
            union all select * from purge_transform ) pt join pk pk on pk.table_name = pt.table_name and pk.owner = ''||g_src_prefix||'_'||pt.owner 
            where pt.column_name <> pt.trans_function
            
            union
            
            select distinct pt.owner,pt.table_name,pt.column_name,'value_from_null',pt.technique,pt.trans_function,pt.stereo_type,
            
            'select count(*) from ' ||g_src_prefix||'_'||pt.owner||'.'||pt.table_name||  ' x , '||'tgt_'|| substr(pt.table_name,1,26)||' y where  ' ||pk.pkcols ||' and '|| 'x.' ||pt.column_name || ' is null and y.' ||pt.column_name || ' is not null '
            
            from (select owner , table_name,column_name ,trans_function,technique,stereo_type   from pc_transform pt 
            --union all select * from manual_transform
            union all select * from purge_transform) pt join pk pk on pk.table_name = pt.table_name and pk.owner = ''||g_src_prefix||'_'||pt.owner 
            where pt.column_name <> pt.trans_function
            
            union
              select distinct pt.owner,pt.table_name,pt.column_name,'avg_sim_dist',pt.technique,pt.trans_function,pt.stereo_type,
              'select round(avg(dist_sim),2) from ( select  utl_match.EDIT_DISTANCE_SIMILARITY(substr(x.' ||pt.column_name || ',1,20) , substr(y.' ||pt.column_name||',1,20))  dist_sim '||
              'from '||g_src_prefix||'_'||pt.owner||'.'||pt.table_name||  ' x , '||'tgt_'|| substr(pt.table_name,1,26)||' y '||
              ' where '|| pkcols|| 
              ' and x.' ||pt.column_name || ' is not null and y.' ||pt.column_name || ' is not null '||')'
            
             
            from (select owner , table_name,column_name ,trans_function,technique,stereo_type   from pc_transform pt 
            --union all select * from manual_transform
            union all select * from purge_transform) pt join pk pk on pk.table_name = pt.table_name and pk.owner = ''||g_src_prefix||'_'||pt.owner 
                  where pt.column_name <> pt.trans_function
           union
            
            select distinct pt.owner,pt.table_name,pt.column_name,'total_recs_src',pt.technique,pt.trans_function,pt.stereo_type,
            
            
            'select count(*) from '||g_src_prefix||'_'||pt.owner||'.'||pt.table_name||  ' x '  
        
            from (select owner , table_name,column_name ,trans_function,technique,stereo_type    from pc_transform pt 
            --union all select * from manual_transform 
            union all select * from purge_transform) pt
            where pt.column_name <> pt.trans_function
            
            union
            
            select distinct pt.owner,pt.table_name,pt.column_name,'total_recs_tgt',pt.technique,pt.trans_function,pt.stereo_type,
            'select count(*) from '||'tgt_'|| substr(pt.table_name,1,26)||'  x ' 
             
            from (select owner , table_name,column_name ,trans_function,technique,stereo_type    from pc_transform pt 
            --union all select * from manual_transform 
            union all select * from purge_transform) pt
            where pt.column_name <> pt.trans_function
            union
            
            select distinct pt.owner,pt.table_name,pt.column_name,'total_nulls_src',pt.technique,pt.trans_function,pt.stereo_type,
            
            'select count(*) from '||g_src_prefix||'_'||pt.owner||'.'||pt.table_name||  ' x ' || ' where '|| 'x.' ||pt.column_name || ' is null '  
            from (select owner , table_name,column_name ,trans_function,technique,stereo_type    from pc_transform pt 
            --union all select * from manual_transform 
            union all select * from purge_transform) pt
            where pt.column_name <> pt.trans_function
            union
            
            select distinct pt.owner,pt.table_name,pt.column_name,'total_nulls_tgt',pt.technique,trans_function,pt.stereo_type,
            'select count(*) from '|| 'tgt_'|| substr(pt.table_name,1,26)||'  x ' || ' where '|| 'x.' ||pt.column_name || ' is null'
            from (select owner , table_name,column_name ,trans_function,technique,stereo_type    from pc_transform pt 
            --union all select * from manual_transform 
            union all select * from purge_transform) pt
            where pt.column_name <> pt.trans_function;
           
        end if;                            
                           
        commit;
          
        execute immediate 'truncate table stats_results_tmp';
                        dbms_output.put_line('Start'); 
        
        for get_stmt_rec in c_get_stmt loop
           
              begin
                --dbms_output.put_line(substr(get_stmt_rec.stmt,1,4000)); 
                --if get_stmt_rec.a_order = 0 then
                  execute immediate get_stmt_rec.stmt INTO l_val;
                  insert into stats_results_tmp(owner,table_name,column_name,stat_type,val,technique,trans_function,stereo_type) 
                  values(get_stmt_rec.owner,get_stmt_rec.table_name,get_stmt_rec.column_name,
                  get_stmt_rec.stat_type, l_val,get_stmt_rec.technique,get_stmt_rec.trans_function,get_stmt_rec.stereo_type);
                --end if;
                commit;
                exception when others then  
                  g_code := SQLCODE;
                  g_errm := SUBSTR(SQLERRM, 1 , 4000);
                  obfus_log(g_module||':'||get_stmt_rec.owner ||'.'|| get_stmt_rec.table_name ||'.'|| get_stmt_rec.column_name,g_code,g_errm,g_module);
                  continue;                   
              end;
  --         
        end loop;
         
        If prepost_anomolies = 1 then   
                  
          execute immediate 'truncate table stats_results_pivot1';
        
           insert into  stats_results_pivot1(table_name ,column_name,technique, owner,trans_function,stereo_type,total_nulls_src,total_nulls_tgt,total_recs_src,total_recs_tgt ,equals,not_equals,avg_sim_dist,value_from_null,value_to_null)
           select * from stats_results_tmp
                pivot 
                (
                    sum(nvl(val,0)) as statistic
                    for stat_type in ('total_nulls_src' as total_nulls_src,
                                    'total_nulls_tgt' as total_nulls_tgt ,
                                    'total_recs_src' as total_recs_src,
                                    'total_recs_tgt' as total_recs_tgt,
                                    'equals' as equals,
                                    'not_equals' as not_equals ,
                                    'avg_sim_dist' as avg_sim_dist,
                                    'value_from_null' as value_from_null,
                                    'value_to_null' as value_to_null)
                );
                
            update stats_results_pivot1 set anon_version = g_anon_version, run_dttm = g_run_date ;
            
            update stats_results_pivot1 set equals = -1,not_equals = -1,avg_sim_dist = -1 where  technique IN ('PURGE_COLUMN','PURGE_INTEGRATION','PURGE_AUDIT');
        end if;       
  
        If prepost_anomolies = 2 then   
  
          execute immediate 'delete from stats_results_pivot2 where anon_version = '''||g_anon_version||'''';
        
           insert into  stats_results_pivot2(table_name ,column_name,technique,owner,trans_function,stereo_type,total_nulls_src,total_nulls_tgt,total_recs_src,total_recs_tgt ,equals,not_equals,avg_sim_dist,value_from_null,value_to_null)
           select * from stats_results_tmp
                pivot 
                (
                    sum(nvl(val,0)) as statistic
                    for stat_type in ('total_nulls_src' as total_nulls_src,
                                    'total_nulls_tgt' as total_nulls_tgt ,
                                    'total_recs_src' as total_recs_src,
                                    'total_recs_tgt' as total_recs_tgt,
                                    'equals' as equals,
                                    'not_equals' as not_equals ,
                                    'avg_sim_dist' as avg_sim_dist,
                                    'value_from_null' as value_from_null,
                                    'value_to_null' as value_to_null)

                );
                
  
            update stats_results_pivot2 set anon_version = g_anon_version, run_dttm = g_run_date where anon_version is null;
            commit;
            update stats_results_pivot2 set equals = -1,not_equals = -1,avg_sim_dist = -1 where  technique IN ('PURGE_COLUMN','PURGE_INTEGRATION','PURGE_AUDIT') and anon_version = g_anon_version ;
            commit;
        end if;     
        
        commit;
      
    exception
       when others then
          g_code := SQLCODE;
          g_errm := SUBSTR(SQLERRM, 1 , 4000);     
          obfus_log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),g_code,g_errm,g_module); 
    end;
  
  end generate_stats;
  
  procedure generate_qa_reports  as
     
     cursor c_get_stmt  is select owner,table_name,column_name,stat_type,stmt,technique,stereo_type
                        from  qa_stmts order by table_name,column_name,stat_type;
     l_val VARCHAR2(4000);
     l_table_name varchar2(30);
     l_type varchar2(20) ;
     
  begin
    begin
  
      g_module := 'generate_qa_reports';
      
      execute immediate 'truncate table qa_stmts';
      execute immediate 'truncate table qa_results_tmp';

      insert into qa_stmts (owner,table_name,column_name,stat_type,technique,stereo_type,stmt)
          with  pk as (select 
          ac.owner,ac.table_name,listagg('x.'||acc.column_name ||'=' ||'y.'||acc.column_name, ' and ') within group (ORDER BY column_name)  as pkcols
          from all_constraints ac  join all_cons_columns  acc on ac.constraint_name = acc.constraint_name and ac.owner = acc.owner
          where ac.constraint_type = 'P' and  ac.owner like g_src_prefix||'\_%' escape '\'
          group by ac.owner,ac.table_name)
      
       select distinct pt.owner,pt.table_name,pt.column_name, 'Eq_unique',pt.technique,pt.stereo_type,
       --''
        ' select listagg('||'X'||','','') within group (ORDER BY 1 ) 
from
( select ''<'' || x.'||pt.column_name||' || ''>'' X  from ' ||g_src_prefix||'_'||pt.owner||'.'||pt.table_name||  ' x , '||'tgt_'|| substr(pt.table_name,1,26)||' y where  ' ||pk.pkcols ||' and '|| 'x.' ||pt.column_name || ' = y.' ||pt.column_name ||' group by '||'x.' ||pt.column_name || ',y.' ||pt.column_name ||')'
        
        from (select owner , table_name,column_name,technique,stereo_type  from stats_results_pivot2 
              where anon_version = g_anon_version and  equals > 0 and trans_function <> 'EXCLUDE') pt 
              join pk pk on pk.table_name = pt.table_name and pk.owner = g_src_prefix||'_'||pt.owner 
        ;
           
      commit;

      execute immediate 'truncate table qa_results_tmp';
      
      for get_stmt_rec in c_get_stmt loop
         
        begin
            ----dbms_output.put_line(substr(get_stmt_rec.stmt,1,4000)); 
          --if get_stmt_rec.a_order = 0 then
            execute immediate get_stmt_rec.stmt INTO l_val;
           --end if;
          commit;
          exception when others then  
            l_val := 'N/A'; 
        end;     
        insert into qa_results_tmp(owner,table_name,column_name,stat_type,val,technique,stereo_type) 
        values(get_stmt_rec.owner,get_stmt_rec.table_name,get_stmt_rec.column_name,get_stmt_rec.stat_type, l_val,get_stmt_rec.technique,get_stmt_rec.stereo_type);

      end loop;
      
      -- Run QA Exceptions
      
      insert into qa_results_tmp(owner,table_name,column_name,stat_type,val,technique,stereo_type)
      
       select   owner,table_name,column_name ,'PC_Scope_Missing',NULL,NULL ,stereo_type
        from
        (
          select res.owner,res.table_name,res.column_name
          ,
          listagg(regexp_substr (pc.property_appliedstereotype, 'PII+[^\|]+', 1) , ',') within group (order by 1) over (partition by res.owner,res.table_name ,res.column_name) as stereo_type 
          from
          (select distinct owner,table_name,column_name from privacy_catalog
          minus
          select distinct owner,table_name,column_name from stats_results_pivot2 where anon_version = g_anon_version) res
          join privacy_catalog pc on res.owner = pc.owner and res.table_name = pc.table_name and res.column_name = pc.column_name
          group by res.owner,res.table_name,res.column_name,regexp_substr (pc.property_appliedstereotype, 'PII+[^\|]+', 1)
        );
      
      execute immediate 'truncate table qa_results_pivot';
      
      insert into  qa_results_pivot(table_name ,column_name,technique,stereo_type,owner,Eq_unique,PC_Scope_Missing)
      select * from qa_results_tmp
          pivot 
          (
            max(val)
            for stat_type in ('Eq_unique','PC_Scope_Missing')
          );
              

      update qa_results_pivot set anon_version = g_anon_version, run_dttm = g_run_date ;
          
      update qa_results_pivot set Eq_unique = 'N/A' where  technique IN ('PURGE_COLUMN','PURGE_INTEGRATION','PURGE_AUDIT');

      commit;
        
    exception
       when others then
          g_code := SQLCODE;
          g_errm := SUBSTR(SQLERRM, 1 , 4000);     
          obfus_log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),g_code,g_errm,g_module); 
    end;
  
  end generate_qa_reports;
  

procedure process_privacy_catalog(p_prefix varchar2) as
  
  start_time date;
  end_time date;
  
  v_prefix VARCHAR2(20);
  
  cursor getDistFuncs(p_funcs varchar2) is 
  select distinct regexp_substr(p_funcs,'[^,]+', 1, level) func
  from dual
  connect by regexp_substr(p_funcs, '[^,]+', 1, level) is not null; 
  
  cursor getItems(p_items varchar2) is 
  select  regexp_substr(p_items,'[^,]+', 1, level) item
  from dual
  connect by regexp_substr(p_items, '[^,]+', 1, level) is not null; 
      
  l_trans_function  varchar2(4000);
  l_needs_substr varchar2(10);
  l_length varchar2(10);
  
  begin
    begin
    
      g_module := 'process_privacy_catalog'; 
      v_prefix := substr(p_prefix,1,20);
--    execute immediate 'truncate table pc_transform'; 
--    
--    insert into  pc_transform (owner,table_name,column_name,trans_function,technique,char_length,no_trans_funcs,no_distinct_trans_funcs,stereo_type) 
--
--    select owner,table_name,column_name,trans_function,technique,char_length,no_trans_funcs,no_distinct_trans_funcs,stereo_type
--    from
--    (
--      select  distinct pc.owner,pc.table_name ,pc.column_name,
--      listagg(por.trans_function, ',') within group (order by 1) over (partition by pc.owner,pc.table_name ,pc.column_name) as trans_function,
--      listagg(por.execution_prior , ',') within group (order by 1) over (partition by pc.owner,pc.table_name ,pc.column_name) as execution_prior,
--      listagg(por.technique , ',') within group (order by 1) over (partition by pc.owner,pc.table_name ,pc.column_name) as technique,
--      listagg(atc.char_length , ',') within group (order by 1) over (partition by pc.owner,pc.table_name ,pc.column_name) as char_length,
--      listagg(atc.data_type , ',') within group (order by 1) over (partition by pc.owner,pc.table_name ,pc.column_name) as data_type,
--      listagg(regexp_substr (pc.property_appliedstereotype, 'PII+[^\|]+', 1) , ',') within group (order by 1) over (partition by pc.owner,pc.table_name ,pc.column_name) as stereo_type,  
--      count(por.trans_function) over (partition by pc.owner,pc.table_name,pc.column_name) no_trans_funcs, 
--      count(distinct por.trans_function) over (partition by pc.owner,pc.table_name,pc.column_name) no_distinct_trans_funcs
--      from PRIVACY_CATALOG pc
--      left join pc_obfuscatn_rules por on regexp_substr (pc.property_appliedstereotype, 'PII+[^\|]+', 1) = por.stereo_type
--      left join all_tab_columns atc on atc.owner = v_prefix||'_'||pc.owner and atc.table_name = pc.table_name  and atc.column_name = pc.column_name
--      where por.trans_function <> 'NA' --or property_appliedstereotype  is null
--      group by pc.owner,pc.table_name ,pc.column_name,por.trans_function,
--      por.execution_prior, por.technique ,regexp_substr (pc.property_appliedstereotype, 'PII+[^\|]+', 1),atc.char_length ,atc.data_type
--
--    );
--      
--    commit;  
      execute immediate ('truncate table pc_transform_2');
    
      for pc_rec in (select owner,table_name,column_name,trans_function,execution_prior,technique,char_length,data_type,
                      no_trans_funcs,no_distinct_trans_funcs,stereo_type from pc_transform) 
      loop 
        l_trans_function := '';
        l_needs_substr := 'false';
      
        for getDistFuncsRec in getDistFuncs(pc_rec.trans_function) loop
          if getDistFuncsRec.func in ('RN') then 
            l_needs_substr := 'true';
          end if;  
          l_trans_function := l_trans_function||getDistFuncsRec.func||'(';
        end loop; 
      
        l_trans_function := RPAD(l_trans_function||pc_rec.column_name,length(l_trans_function||pc_rec.column_name)+(pc_rec.no_distinct_trans_funcs),')');
      
        if l_needs_substr = 'true' then
          for getItemsRec in getItems(pc_rec.char_length) loop
            l_length:= getItemsRec.item;
          end loop;
          l_trans_function := 'substr('||l_trans_function||',1,'||l_length||')';
        end if;
        insert into pc_transform_2(owner,table_name ,column_name,trans_function,execution_prior,technique,char_length,data_type,no_trans_funcs,no_distinct_trans_funcs,stereo_type)
        values (pc_rec.owner,pc_rec.table_name ,pc_rec.column_name,l_trans_function,pc_rec.execution_prior,pc_rec.technique,pc_rec.char_length,
                pc_rec.data_type,pc_rec.no_trans_funcs,pc_rec.no_distinct_trans_funcs,pc_rec.stereo_type);        
      end loop;
    
      commit;   

      execute immediate 'truncate table pc_stmts'; 
      
      insert into pc_stmts(aorder,stmt) select distinct 0 as a_order , 'DROP TABLE '||substr(table_name,1,26)||'_tmp'  stmt from pc_transform_2
      union
      select distinct 1 as a_order ,'CREATE TABLE '||substr(src.table_name,1,26)||'_tmp  as SELECT * from '||v_prefix||'_'||src.owner||'.'||src.table_name||' where rownum < 1' stmt
      from pc_transform_2  src
         
      union                 
      (
         select 2,substr('insert into '||substr(src.table_name,1,26)||'_tmp(' ||src.acutal_cols||') select '||src.trans_functions||' from '||src.table_name,1,4000)
         from (
                select distinct src1.owner,src1.table_name ,
                --char_length,
                listagg(src1.column_name, ', ') within group (order by src1.column_name) over (partition by src1.owner,src1.table_name) as acutal_cols , 
                listagg(src1.trans_function, ', ') within group (order by src1.column_name) over (partition by src1.owner,src1.table_name) as trans_functions
                from   
                (
                  select v_prefix||'_'||pct.owner owner ,pct.table_name,pct.column_name,pct.trans_function 
                  from pc_transform_2 pct
                  where pct.table_name NOT IN ('FATCA_CLSF_EXTRACTS','PAYMENTS','CREST_PARTICIPANTS')
                  union
                  (
                    select atc.owner,atc.table_name,atc.column_name ,atc.column_name
                    from all_tab_columns atc
                    join pc_transform_2 pct on v_prefix||'_'||pct.owner = atc.owner and pct.table_name = atc.table_name
                    where pct.table_name NOT IN ('FATCA_CLSF_EXTRACTS','PAYMENTS','CREST_PARTICIPANTS')
                    minus
                    select v_prefix||'_'||pct.owner owner ,pct.table_name,pct.column_name,pct.column_name
                    from pc_transform_2 pct
                    where pct.table_name NOT IN ('FATCA_CLSF_EXTRACTS','PAYMENTS','CREST_PARTICIPANTS')
                 )
            ) src1                      
         ) src 
      );
                                   
      for get_stmt_rec in (select aorder,stmt from pc_stmts order by aorder)  loop    
        begin
          start_time := SYSDATE;
          execute immediate get_stmt_rec.stmt;
          end_time := SYSDATE;

          commit;
        exception when others then  
          if get_stmt_rec.aorder = 0 
            then null; 
          else  
            g_code := SQLCODE;
            g_errm := SUBSTR(SQLERRM, 1 , 4000);
            obfus_log(substr(get_stmt_rec.stmt,1,4000),g_code,g_errm,g_module); 
            g_code := NULL;
            g_errm := NULL;
          end if;
        end;        
      end loop;
         
      commit;  
         
    exception
       when others then
          g_code := SQLCODE;
          g_errm := SUBSTR(SQLERRM, 1 , 4000);     
          obfus_log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),g_code,g_errm,g_module); 
          RAISE; -- Error will propogate back to anonymise setting p_obfus_status => 'FAILED'
    end;
end process_privacy_catalog;


procedure gen_sortkey as
  
begin
  begin

    g_module := 'gen_sortkey';
  
    begin
      execute immediate 'ALTER TRIGGER '||g_tgt_prefix||'_PRISM_CORE.HOLDERS_BRIUD disable';
    end;
  
    begin
      execute immediate 'drop table sk_temp';
      exception when others then null;
    end;
       
    begin
      execute immediate 'create table sk_temp(comp_code varchar2(4),holder_seq number,ivc_code  varchar2(11),sk_gen    varchar2(4000))';
    end;
      
    begin
      execute immediate 'create index sk_temp_12345678 on sk_temp(comp_code,ivc_code)';
    end;
  
    insert into sk_temp(comp_code,ivc_code,holder_seq,sk_gen)
    
    select comp_code,ivc_code,holder_seq,get_formatted_sk(res1.sort_key_names,res1.holder_type_code,res1.designation_name) sk_gen 
    from
    
    (
        select res.comp_code,res.ivc_code,res.holder_seq,res.holder_type_code,res.surname,res.forename,res.company_name,res.trust_name,res.trustee_name,
        res.clean_company,instr(res.clean_company,'*',1),
             listagg(case when res.holder_type_code = 'I'  then res.clean_name 
                          when res.holder_type_code = 'C'  then replace(substr(res.clean_company,1,instr(res.clean_company,'*',1) -1 ),' ','') ||' '||substr(res.clean_company,instr(res.clean_company,'*',1) +1 )
                          when res.holder_type_code = 'T'  then res.clean_trust_name  end, ' ') 
            within group (order by res.holder_seq) over (partition by res.comp_code,res.ivc_code,res.holder_type_code) as sort_key_names,
            res.clean_name,
            res.sort_key,
            res.designation_name
        from 
        (
            select h.comp_code,h.ivc_code,hn.holder_seq,h.sort_key ,h.designation_name,
            hn.holder_name_id,hn.forename, hn.surname,hn.company_name,hn.trust_name,hn.trustee_name,
            hn.holder_type_code,
            case when instr(hn.surname,'-') > 0 then
               trim( replace(substr(hn.surname,instr(hn.surname,'-',1,1)+1),'-','')) ||' '||replace(hn.forename,'-',' ')||' '||trim(substr(hn.surname,1,instr(hn.surname,'-',1,1)-1))
            else
              hn.surname ||' '||replace(hn.forename,'-',' ') end clean_name,
            trim
            (
              regexp_replace(
                    replace(
                              regexp_replace(
                                  regexp_replace(hn.company_name,
                                  '(THE)([[:space:]]*)',' ',1,0,'i'),
                              '(LTD)(.{0,1})([[:space:]]*)','LIMITED',1,0,'i'),
                            ' & ',' AND '),
              '([[:alpha:]])([[:space:]])([[:alpha:]]{2,})','\1'||'*'||'\3',1,1)
            ) clean_company,
            replace (regexp_replace(hn.trust_name,'(LTD)(.{0,1})([[:space:]]*)','LIMITED',1,0,'i') ||' '||
                     REGEXP_REPLACE(hn.trustee_name,'(LTD)(.{0,1})([[:space:]]*)','LIMITED',1,0,'i') ,' & ',' ') clean_trust_name
            from holders_tmp1 h
            left join holder_names_tmp1 hn on h.comp_code = hn.comp_code and  h.ivc_code = hn.ivc_code
        ) res
    ) res1;
    
    commit;    
    
    merge into holders_tmp1 h
    using (select comp_code,ivc_code,sk_gen sort_key ,sk_gen previous_sortkey from sk_temp 
           where holder_seq = 1) ahn on (ahn.comp_code = h.comp_code and ahn.ivc_code = h.ivc_code  )
        when matched
        then
                 update set h.sort_key = ahn.sort_key,
                            h.previous_sortkey = ahn.previous_sortkey;
        commit;
    
      begin
      execute immediate 'ALTER TRIGGER '||g_tgt_prefix||'_PRISM_CORE.HOLDERS_BRIUD enable';
    end;
    
           
    exception
       when others then
          g_code := SQLCODE;
          g_errm := SUBSTR(SQLERRM, 1 , 4000);     
          obfus_log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),g_code,g_errm,g_module);         
          RAISE; -- Error will propogate back to anonymise setting p_obfus_status => 'FAILED'
  end;
  
end gen_sortkey;

  procedure anon_holder as
  begin
    begin
  
      g_module := 'anon_holder';    

      execute immediate 'truncate table holder_base';
                  
      insert into holder_base(key_ns,ni_key,comp_code,ivc_code,	amalgamated_ivc,sort_key,	participant_code,
                              share_master_holder_code ,reference_ivc_code,designation_name,country_code,date_of_death,	
                              gender,date_of_birth,national_insurance_no,personnel_number,payroll_number,
                              sort_key_anon,participant_code_anon,
                              share_master_holder_code_anon,reference_ivc_code_anon,designation_name_anon,country_code_anon,date_of_death_anon,	
                              gender_anon,date_of_birth_anon,national_insurance_no_anon,personnel_number_anon,payroll_number_anon)
  
      select res.key_ns key_ns, res.ni_key ni_key ,res.comp_code comp_code,res.ivc_code ivc_code,res.amalgamated_ivc amalgamated_ivc,res.sort_key sort_key,res.participant_code participant_code ,
                   res.share_master_holder_code,res.reference_ivc_code,res.designation_name,
                   res.country_code,res.date_of_death,res.gender,res.date_of_birth,
                   res.national_insurance_no,res.personnel_number,res.payroll_number,
                   res.sort_key as sort_key_anon,res.participant_code   as participant_code_anon,
                    res.share_master_holder_code as share_master_holder_code_anon,res.reference_ivc_code as reference_ivc_code_anon,
                    CASE WHEN substr(res.ivc_code,1,1) <> '7' then tdc(res.designation_name) else  res.designation_name end as designation_name_anon,res.country_code  as country_code_anon,
                    randomise_date_30(res.date_of_death) as date_of_death_anon, gender as gender_anon,
                    randomise_date_30 (res.date_of_birth) as date_of_birth_anon,
                    CASE WHEN res.national_insurance_no is not null THEN
                          CHR(mod(ni_key,25+1)+ 65)||CHR((mod(trunc(ni_key/(25+1)),26)+ 65))||lpad(ni_key,6,'0')||CHR((mod(trunc(ni_key/(675+1)),26)+ 65)) ELSE NULL END as a_national_insurance_no_anon,
                    decode (res.personnel_number,  null, null, 'PE' || res.ivc_code) as a_personnel_number_anon,
                    decode (res.payroll_number,  null, null, 'PR' || res.ivc_code) as a_payroll_number_anon
          from 
             (select row_number() over (order by hld.comp_code,hld.ivc_code) key_ns,
                      ora_hash(hld.comp_code||hld.ivc_code, 4294967295, 0) ni_key, 
                      hld.comp_code,hld.ivc_code,hld.amalgamated_ivc,hld.sort_key,hld.participant_code,
                         hld.share_master_holder_code,hld.reference_ivc_code,hld.designation_name,
                         hld.country_code,hld.date_of_death,hed.gender,hed.date_of_birth,
                         hed.national_insurance_no,hed.personnel_number,hed.payroll_number
                    from holders hld
                         left outer join holder_employee_details hed on  hld.comp_code = hed.comp_code and hld.ivc_code = hed.ivc_code) res;  
              
              
                    begin
          execute immediate 'drop table holders_tmp1';
          exception when others then null;
        end;
              
        execute immediate  'create table holders_tmp1 as select * from holders where rownum <1';
        
              insert into holders_tmp1(comp_code,ivc_code,amalgamated_ivc,last_changed_date,deleted_yn,member_type_employee_yn,member_type_registration_yn,sort_key,participant_code,
              share_master_holder_code,reference_ivc_code,designation_name,share_owner_start_date_nt,share_owner_end_date_nt,country_code,created_by,created_date,
              modified_by,mod_timestamp,number_of_holders_count,pledge_yn,court_protection_yn,uncertificated_yn,amalgamated_yn,closed_yn,control_yn,crest_yn,
              alert_yn,court_order_yn,posting_prism_task_id,posting_date,posting_batch_id,posting_transaction_id,contribution_period_type_id,expat_yn,
              previous_sortkey,discretion_residency_code,income_tax_rate_id,employer_ni_tax_rate_id,employee_ni_tax_rate_id,discret_election_type_code,stripped_yn,date_of_death)
              select h.comp_code,h.ivc_code,h.amalgamated_ivc,h.last_changed_date,h.deleted_yn,h.member_type_employee_yn,h.member_type_registration_yn,hb.sort_key_anon,hb.participant_code,
              hb.share_master_holder_code_anon,hb.reference_ivc_code_anon,hb.designation_name_anon,h.share_owner_start_date_nt,h.share_owner_end_date_nt,hb.country_code_anon,h.created_by,
              h.created_date,h.modified_by,h.mod_timestamp,h.number_of_holders_count,h.pledge_yn,h.court_protection_yn,h.uncertificated_yn,h.amalgamated_yn,h.closed_yn,h.control_yn,h.crest_yn,
              h.alert_yn,h.court_order_yn,h.posting_prism_task_id,h.posting_date,h.posting_batch_id,h.posting_transaction_id,h.contribution_period_type_id,h.expat_yn,h.previous_sortkey,
              h.discretion_residency_code,h.income_tax_rate_id,h.employer_ni_tax_rate_id,h.employee_ni_tax_rate_id,h.discret_election_type_code,h.stripped_yn,hb.date_of_death_anon
              from holders h join holder_base hb on h.comp_code = hb.comp_code and h.ivc_code = hb.ivc_code;
--              delete from manual_transform where table_name = 'HOLDERS';
              
--              insert into manual_transform (owner,table_name, actual_col,trans_function,technique)
--              select 'PRISM_CORE' ,'HOLDERS','SORT_KEY','randomise_date_30(sort_key)','SKGENERATE' from dual union all                     
--  --            select 'PRISM_CORE' ,'HOLDERS','SHARE_MASTER_HOLDER_CODE','randomise_date_30(share_master_holder_code)','phase1' from dual union all
--  --            select 'PRISM_CORE' ,'HOLDERS','REFERENCE_IVC_CODE','none(reference_ivc_code)', 'phase1' from dual union all
--              select 'PRISM_CORE' ,'HOLDERS','DESIGNATION_NAME','none(designation_name)'  , 'phase1' from dual union all
--              select 'PRISM_CORE' ,'HOLDERS','DATE_OF_DEATH','bespoke(date_of_death)'  , 'DATE30_DAYS' from dual ;
            
--              execute immediate 'create or replace synonym holders for holders_tmp1';
    
        begin
          execute immediate 'drop table holder_employee_details_tmp1';
          exception when others then null;
        end;
             
        execute immediate  'create table holder_employee_details_tmp1 as select * from holder_employee_details where rownum <1';
        
              insert into holder_employee_details_tmp1(holder_employee_detail_id , comp_code ,ivc_code ,comp_location_id ,
              division_id ,tax_detail_id ,payroll_id , gender, date_of_birth , national_insurance_no , personnel_number , 
              payroll_number , employment_start_date_nt , employer_comp_effect_date_nt , employer_div_effect_date_nt , employer_loc_effect_date_nt , 
              payroll_name_effect_date_nt ,created_by ,created_date ,modified_by , mod_timestamp)
              select hed.holder_employee_detail_id , hed.comp_code ,hed.ivc_code ,hed.comp_location_id ,hed.division_id ,
              hed.tax_detail_id ,hed.payroll_id , hb.gender_anon, hb.date_of_birth_anon , hb.national_insurance_no_anon , hb.personnel_number_anon , 
              hb.payroll_number_anon , hed.employment_start_date_nt ,hed.employer_comp_effect_date_nt ,hed.employer_div_effect_date_nt , 
              hed.employer_loc_effect_date_nt,hed.payroll_name_effect_date_nt ,hed.created_by ,hed.created_date ,hed.modified_by , 
              hed.mod_timestamp
              from holder_employee_details  hed join holder_base hb on hed.comp_code = hb.comp_code and hed.ivc_code = hb.ivc_code;
--              execute immediate 'create or replace synonym holder_employee_details for holder_employee_details_tmp1';
  
--              delete from manual_transform where table_name = 'HOLDER_EMPLOYEE_DETAILS';
--              
--              insert into manual_transform (owner,table_name, actual_col,trans_function,technique)           
--              select 'PRISM_CORE' ,'HOLDER_EMPLOYEE_DETAILS','DATE_OF_BIRTH','afn_randomise_date(date_of_birth)','DATE30_DAYS' from dual union all
--              select 'PRISM_CORE' ,'HOLDER_EMPLOYEE_DETAILS','NATIONAL_INSURANCE_NO','bespoke(national_insurance_no)', 'ANON_NI' from dual union all
--              select 'PRISM_CORE' ,'HOLDER_EMPLOYEE_DETAILS','PERSONNEL_NUMBER','bespoke(personnel_number)'  , 'ANON_PERSONELL' from dual union all
--              select 'PRISM_CORE' ,'HOLDER_EMPLOYEE_DETAILS','PAYROLL_NUMBER','bespoke(payroll_number)'  , 'ANON_PAYROLL' from dual ;
  
    exception
       when others then
          g_code := SQLCODE;
          g_errm := SUBSTR(SQLERRM, 1 , 4000);     
          obfus_log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),g_code,g_errm,g_module);  
          RAISE; -- Error will propogate back to anonymise setting p_obfus_status => 'FAILED'
    end;
  end anon_holder;

  procedure anon_bank_accounts
  is
  begin
    begin
      g_module := 'anon_bank_accounts'; 
   
      execute immediate 'truncate table bank_accounts_base';
      execute immediate 'truncate table bank_accounts_base_fin';
      
      insert into  bank_accounts_base(bank_account_id,key_ns,bank_name,branch_name,alias_name,
                account_name,chequeaddress_line1_text,chequeaddress_line2_text,chequeaddress_line3_text,chequeaddress_line4_text,
                chequeaddress_line5_text,chequeaddress_line6_text,bic, sortcode,account_no              ,
                iban,international_account_no , building_society_account_no,capita_reference, currency_code,
                bank_account_type_code,reg_status_type_code,status_type_code,country_code,second_sortcode, 
                bacs_code,account_no_anon,iban_anon,inter_account_no_anon,bs_account_no_anon,capita_reference_anon,bacs_code_anon,cheque_no_range_adj,
                alias_name_anon ,account_name_anon,branch_name_anon,chequeaddress_line1_text_anon,chequeaddress_line2_text_anon,                
                chequeaddress_line3_text_anon,chequeaddress_line4_text_anon,chequeaddress_line5_text_anon,                
                chequeaddress_line6_text_anon,international_branch_id_anon ,international_account_no_anon,fax_number_anon ,swift_code_anon  
      )
      select ba.bank_account_id,row_number() over (partition by 1 order by bank_account_id) key_ns,ba.bank_name, ba.branch_name,ba.alias_name,ba.account_name,
                ba.chequeaddress_line1_text,ba.chequeaddress_line2_text, ba.chequeaddress_line3_text,ba.chequeaddress_line4_text, ba.chequeaddress_line5_text,
                ba.chequeaddress_line6_text,ba.bic,ba.sortcode,ba.account_no, ba.iban,
                ba.international_account_no ,ba.building_society_account_no,ba.capita_reference, ba.currency_code,ba.bank_account_type_code,
                ba.reg_status_type_code,ba.status_type_code,ba.country_code,ba.second_sortcode,ba.bacs_code ,
                null, null,null,null,null,null,mod(abs(dbms_random.random), 1000 ) +1 cheque_no_range_adj,
                tdc(alias_name),tdc(account_name),tdc(branch_name),chequeaddress_line1_text,chequeaddress_line2_text,                
                chequeaddress_line3_text,chequeaddress_line4_text,chequeaddress_line5_text,chequeaddress_line6_text,   
                tdc(international_branch_id),tdc(international_account_no),tdc(fax_number),tdc(swift_code)  
                from bank_accounts ba;

      insert into  bank_accounts_base_fin(bank_account_id,key_ns,bank_name,branch_name,alias_name,
                account_name,chequeaddress_line1_text,chequeaddress_line2_text,chequeaddress_line3_text,chequeaddress_line4_text,
                chequeaddress_line5_text,chequeaddress_line6_text,bic, sortcode,account_no              ,
                iban,international_account_no , building_society_account_no,capita_reference, currency_code,
                bank_account_type_code,reg_status_type_code,status_type_code,country_code,second_sortcode,
                bacs_code,account_no_anon,iban_anon,  inter_account_no_anon,bs_account_no_anon,capita_reference_anon,bacs_code_anon,cheque_no_range_adj,
                alias_name_anon  ,account_name_anon  , branch_name_anon,chequeaddress_line1_text_anon,chequeaddress_line2_text_anon  ,
                chequeaddress_line3_text_anon ,chequeaddress_line4_text_anon,chequeaddress_line5_text_anon ,chequeaddress_line6_text_anon ,   
                international_branch_id_anon ,international_account_no_anon  ,fax_number_anon ,swift_code_anon)
      select ba.bank_account_id, key_ns,ba.bank_name, ba.branch_name,ba.alias_name,ba.account_name,
                ba.chequeaddress_line1_text,ba.chequeaddress_line2_text, ba.chequeaddress_line3_text,ba.chequeaddress_line4_text, ba.chequeaddress_line5_text,
                ba.chequeaddress_line6_text,ba.bic,ba.sortcode,ba.account_no, ba.iban,
                ba.international_account_no ,ba.building_society_account_no,tdc(ba.capita_reference), ba.currency_code,ba.bank_account_type_code,
                ba.reg_status_type_code,ba.status_type_code,ba.country_code,ba.second_sortcode,ba.bacs_code ,
                lpad(key_ns, 10, 0) account_no_anon,
                case when length (iban) > 0 then substr (iban, 1, 8)|| sortcode|| lpad (key_ns,10,0) end as iban_anon,
                null, null,  sortcode || lpad (key_ns,10,0) as  cla, lpad ( key_ns, 6, 2) bacs_code_anon,cheque_no_range_adj,
                alias_name_anon,account_name_anon,branch_name_anon,chequeaddress_line1_text_anon,chequeaddress_line2_text_anon  ,
                chequeaddress_line3_text_anon ,chequeaddress_line4_text_anon,chequeaddress_line5_text_anon,chequeaddress_line6_text_anon ,   
                international_branch_id_anon ,international_account_no_anon  ,fax_number_anon ,swift_code_anon
                from bank_accounts_base ba; 
                
      --Ensure that capita_reference is unique
      loop
        merge into bank_accounts_tmp1 bat
               using (select bank_account_id, capita_reference,num_dups
                      from
                      ( select bank_account_id,capita_reference,count(1)  over (partition by capita_reference) num_dups
                        from bank_accounts_tmp1
                      )
                      where num_dups > 1) res
                  on (bat.bank_account_id = res.bank_account_id)
        when matched
        then
           update set bat.capita_reference  =  tdc(res.capita_reference);  
  
      if sql%rowcount = 0 then
        exit;
      end if;
    end loop;

       begin
          execute immediate 'drop table bank_accounts_tmp1';
          exception when others then null;
       end;
           
      execute immediate  'create table bank_accounts_tmp1 as select * from bank_accounts where rownum <1';
            
      insert into bank_accounts_tmp1( bank_account_id,currency_code,bank_account_type_code,reg_status_type_code,
      status_type_code,country_code,second_sortcode,alias_name,bacs_code,blockpayment_yn,capita_reference,
      comment_text,isvirtual_yn,status_change_date,client_owned_bank_account_yn,account_no,account_name,bank_name,sortcode,
      international_branch_id,international_account_no,iban,fax_number,building_society_account_no,branch_name,
      bic,chequeaddress_line1_text,chequeaddress_line2_text,chequeaddress_line3_text,chequeaddress_line4_text,chequeaddress_line5_text,
      chequeaddress_line6_text,created_by,created_date,modified_by,mod_timestamp,bank_payment_cat_type_code,swift_code)
      
      select ba.bank_account_id,ba.currency_code,ba.bank_account_type_code,ba.reg_status_type_code,ba.status_type_code,
      ba.country_code,ba.second_sortcode,babf.alias_name_anon,babf.bacs_code_anon,ba.blockpayment_yn,babf.capita_reference_anon,ba.comment_text,
      ba.isvirtual_yn,ba.status_change_date,ba.client_owned_bank_account_yn,babf.account_no_anon,babf.account_name_anon,ba.bank_name,
      ba.sortcode,babf.international_branch_id_anon,babf.international_account_no_anon,babf.iban_anon,babf.fax_number_anon,babf.bs_account_no_anon,
      babf.branch_name_anon,ba.bic,babf.chequeaddress_line1_text_anon,babf.chequeaddress_line2_text_anon,babf.chequeaddress_line3_text_anon,
      babf.chequeaddress_line4_text_anon,babf.chequeaddress_line5_text_anon,babf.chequeaddress_line6_text_anon,ba.created_by,
      ba.created_date,ba.modified_by,ba.mod_timestamp,ba.bank_payment_cat_type_code,babf.swift_code_anon
      from bank_accounts ba join bank_accounts_base_fin babf on babf.bank_account_id = ba.bank_account_id;
--      execute immediate 'create or replace synonym bank_accounts for bank_accounts_tmp1';

--      delete from manual_transform where table_name = 'BANK_ACCOUNTS';
--
--      insert into manual_transform (owner,table_name, actual_col,trans_function,technique)
--   
--     select 'CASH_MANAGEMENT','BANK_ACCOUNTS','BACS_CODE','bespoke(bacs_code)', 'PADDED2_SEQ' from dual union all
--     select 'CASH_MANAGEMENT' ,'BANK_ACCOUNTS','CAPITA_REFERENCE','bespoke(capita_reference)', 'PADDED2_SEQ' from dual union all
--     select 'CASH_MANAGEMENT','BANK_ACCOUNTS','ACCOUNT_NO','bespoke(account_no)', 'PADDED2_SEQ' from dual union all
--     select 'CASH_MANAGEMENT','BANK_ACCOUNTS','INTERNATIONAL_ACCOUNT_NO','bespoke(international_account_no)', 'SUBS_DIGITS_TRANSFORM_CAPS_CHAR' from dual union all
--     select 'CASH_MANAGEMENT','BANK_ACCOUNTS','IBAN','bespoke(iban)', 'PADDED0_SEQ' from dual union all
--     select 'CASH_MANAGEMENT','BANK_ACCOUNTS','ACCOUNT_NAME','tdc(account_name)', 'SUBS_DIGITS_TRANSFORM_CAPS_CHAR' from dual union all 
--     select 'CASH_MANAGEMENT','BANK_ACCOUNTS','ALIAS_NAME','tdc(alias_name)', 'SUBS_DIGITS_TRANSFORM_CAPS_CHAR' from dual union all 
--     select 'CASH_MANAGEMENT','BANK_ACCOUNTS','BRANCH_NAME','tdc(branch_name)', 'SUBS_DIGITS_TRANSFORM_CAPS_CHAR' from dual union all 
--     select 'CASH_MANAGEMENT','BANK_ACCOUNTS','CHEQUEADDRESS_LINE1_TEXT','tdc(chequeaddress_line1_text)', 'SUBS_DIGITS_TRANSFORM_CAPS_CHAR' from dual union all 
--     select 'CASH_MANAGEMENT','BANK_ACCOUNTS','CHEQUEADDRESS_LINE2_TEXT','tdc(chequeaddress_line2_text)', 'SUBS_DIGITS_TRANSFORM_CAPS_CHAR' from dual union all 
--     select 'CASH_MANAGEMENT','BANK_ACCOUNTS','CHEQUEADDRESS_LINE3_TEXT','tdc(chequeaddress_line3_text)', 'SUBS_DIGITS_TRANSFORM_CAPS_CHAR' from dual union all 
--     select 'CASH_MANAGEMENT','BANK_ACCOUNTS','CHEQUEADDRESS_LINE4_TEXT','tdc(chequeaddress_line4_text)', 'SUBS_DIGITS_TRANSFORM_CAPS_CHAR' from dual union all 
--     select 'CASH_MANAGEMENT','BANK_ACCOUNTS','CHEQUEADDRESS_LINE5_TEXT','tdc(chequeaddress_line5_text)', 'SUBS_DIGITS_TRANSFORM_CAPS_CHAR' from dual union all 
--     select 'CASH_MANAGEMENT','BANK_ACCOUNTS','CHEQUEADDRESS_LINE6_TEXT','tdc(chequeaddress_line6_text)', 'SUBS_DIGITS_TRANSFORM_CAPS_CHAR' from dual union all 
--     select 'CASH_MANAGEMENT','BANK_ACCOUNTS','FAX_NUMBER','tdc(fax_number)', 'SUBS_DIGITS_TRANSFORM_CAPS_CHAR' from dual union all 
--     select 'CASH_MANAGEMENT','BANK_ACCOUNTS','INTERNATIONAL_BRANCH_ID','tdc(international_branch_id)', 'SUBS_DIGITS_TRANSFORM_CAPS_CHAR' from dual union all 
--     select 'CASH_MANAGEMENT','BANK_ACCOUNTS','SWIFT_CODE','tdc(swift_code)', 'SUBS_DIGITS_TRANSFORM_CAPS_CHAR' from dual union all 
--     select 'CASH_MANAGEMENT','BANK_ACCOUNTS','BUILDING_SOCIETY_ACCOUNT_NO','bespoke(building_society_account_no)', 'NONE' from dual;

    end;
    
    commit;
    
    exception
       when others then
          g_code := SQLCODE;
          g_errm := SUBSTR(SQLERRM, 1 , 4000);     
          obfus_log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),g_code,g_errm,g_module); 
          RAISE; -- Error will propogate back to anonymise setting p_obfus_status => 'FAILED'        
   end anon_bank_accounts;
   
   procedure anon_holder_names
   is
      v_ncount number;
      nMaxKey number;
    begin
    
    null;

      g_module := 'anon_holder_names';   
    
      execute immediate 'truncate table forename_shuffle1';
    
      insert into forename_shuffle1(HOLDER_NAME_ID,comp_code,ivc_code ,holder_seq,
      forename , forename_hash,working_gender,key_ns,forename_min_key_ns ,forename_max_key_ns,forename_shuffle_key_ns)
        select  res2.HOLDER_NAME_ID,res2.comp_code,res2.ivc_code ,res2.holder_seq,
        res2.forename , res2.forename_hash,res2.working_gender,res2.key_ns, 
        res2.forename_min_key_ns ,res2.forename_max_key_ns,
        mod(abs(dbms_random.random),(res2.forename_max_key_ns - res2.forename_min_key_ns) - 1  )  forename_shuffle_key_ns
        from
        (
          select   
          res1.HOLDER_NAME_ID,res1.comp_code,res1.ivc_code ,res1.holder_seq,
          res1.forename , res1.forename_hash,res1.working_gender,res1.key_ns,   
          first_value(res1.key_ns) over (partition by res1.working_gender order by res1.key_ns asc rows unbounded preceding ) forename_min_key_ns ,
          last_value(res1.key_ns) over (partition by res1.working_gender  order by res1.key_ns asc rows between  unbounded preceding and unbounded following ) forename_max_key_ns
          from
          (
              select res.HOLDER_NAME_ID,res.comp_code,res.ivc_code,res.holder_seq,
              res.forename , res.forename_hash,working_gender,row_number() over (partition by 1 order by res.working_gender) key_ns
              from 
              (
                select hn.HOLDER_NAME_ID,hn.comp_code comp_code,hn.ivc_code ivc_code,hn.holder_seq holder_seq,
                hn.forename ,ora_hash(hn.forename, 4294967295, 0) forename_hash,
                nvl(tt.gender,case when mod(abs(dbms_random.random),2) = 1 then 'F' else 'M' end) working_gender
                from holder_names hn 
                left outer join title_types tt on hn.title_type_code = tt.title_type_code
                where hn.forename  is not null and holder_type_code = 'I'
              ) res
          )res1
        ) res2;

       execute immediate 'truncate table forename_shuffle';
  
      insert into forename_shuffle (HOLDER_NAME_ID,comp_code,ivc_code ,holder_seq,
      forename , forename_hash,working_gender,key_ns,forename_min_key_ns ,forename_max_key_ns,forename_shuffle_key_ns,forename_shuffle_hash,
      resolved ,forename_shuffle)
    
      select res.HOLDER_NAME_ID,res.comp_code,res.ivc_code ,res.holder_seq,
      res.forename , res.forename_hash,res.working_gender,res.key_ns, 
      res.forename_min_key_ns ,res.forename_max_key_ns, res.forename_shuffle_key_ns, forename_shuffle_hash,
      case when res.forename_hash=res.forename_shuffle_hash then 'N' else 'Y' end   resolved,
      forename_shuffle
      from
      (
        select  fs1.HOLDER_NAME_ID,fs1.comp_code,fs1.ivc_code ,fs1.holder_seq,
        fs1.forename , fs1.forename_hash,fs1.working_gender,fs1.key_ns, 
        fs1.forename_min_key_ns ,fs1.forename_max_key_ns, fs1.forename_shuffle_key_ns,ora_hash(fs2.forename, 4294967295, 0) forename_shuffle_hash,
        fs2.forename forename_shuffle
        from forename_shuffle1 fs1 join forename_shuffle1 fs2 on fs1.forename_shuffle_key_ns = fs2.key_ns
      ) res;
      
      loop
      
        update forename_shuffle set forename_shuffle_key_ns = mod(abs(dbms_random.random),(forename_max_key_ns - forename_min_key_ns) - 1  ) 
        where resolved = 'N';
          
        if sql%rowcount = 0 then
          exit;
        end if;
        
        update forename_shuffle fs1 set (fs1.forename_shuffle,fs1.forename_shuffle_hash )= (select fs2.forename, ora_hash(fs2.forename, 4294967295, 0) from forename_shuffle fs2
                                                                     where fs1.forename_shuffle_key_ns = fs2.key_ns)
        where resolved = 'N'  ;                                                         
        
        update forename_shuffle fs1 set fs1.resolved = 'Y'
        where fs1.resolved = 'N' and fs1.forename_hash<>fs1.forename_shuffle_hash;
     end loop;
      
       execute immediate 'truncate table surname_shuffle1';

      insert into surname_shuffle1(HOLDER_NAME_ID,comp_code,ivc_code ,holder_seq,
      surname , surname_hash,working_gender,key_ns,surname_min_key_ns ,surname_max_key_ns,surname_shuffle_key_ns)
        select  res2.HOLDER_NAME_ID,res2.comp_code,res2.ivc_code ,res2.holder_seq,
        res2.surname , res2.surname_hash,res2.working_gender,res2.key_ns, 
        res2.surname_min_key_ns ,res2.surname_max_key_ns,
        mod(abs(dbms_random.random),(res2.surname_max_key_ns - res2.surname_min_key_ns) - 1  )  surname_shuffle_key_ns
        from
        (
          select   
          res1.HOLDER_NAME_ID,res1.comp_code,res1.ivc_code ,res1.holder_seq,
          res1.surname , res1.surname_hash,res1.working_gender,res1.key_ns,   
          first_value(res1.key_ns) over (partition by res1.working_gender order by res1.key_ns asc rows unbounded preceding ) surname_min_key_ns ,
          last_value(res1.key_ns) over (partition by res1.working_gender  order by res1.key_ns asc rows between  unbounded preceding and unbounded following ) surname_max_key_ns
          from
          (
              select res.HOLDER_NAME_ID,res.comp_code,res.ivc_code,res.holder_seq,
              res.surname , res.surname_hash,working_gender,row_number() over (partition by 1 order by res.working_gender) key_ns
              from 
              (
                select hn.HOLDER_NAME_ID,hn.comp_code comp_code,hn.ivc_code ivc_code,hn.holder_seq holder_seq,
                hn.surname ,ora_hash(hn.surname, 4294967295, 0) surname_hash,
                nvl(tt.gender,case when mod(abs(dbms_random.random),2) = 1 then 'F' else 'M' end) working_gender
                from holder_names hn 
                left outer join title_types tt on hn.title_type_code = tt.title_type_code
                where hn.surname  is not null and holder_type_code = 'I'
              ) res
          )res1
        ) res2;

       execute immediate 'truncate table surname_shuffle';
  
      insert into surname_shuffle (HOLDER_NAME_ID,comp_code,ivc_code ,holder_seq,
      surname , surname_hash,working_gender,key_ns,surname_min_key_ns ,surname_max_key_ns,surname_shuffle_key_ns,surname_shuffle_hash,
      resolved,surname_shuffle )
    
      select res.HOLDER_NAME_ID,res.comp_code,res.ivc_code ,res.holder_seq,
      res.surname , res.surname_hash,res.working_gender,res.key_ns, 
      res.surname_min_key_ns ,res.surname_max_key_ns, res.surname_shuffle_key_ns, surname_shuffle_hash,
      case when res.surname_hash=res.surname_shuffle_hash then 'N' else 'Y' end   resolved,
      surname_shuffle
      from
      (
        select  fs1.HOLDER_NAME_ID,fs1.comp_code,fs1.ivc_code ,fs1.holder_seq,
        fs1.surname , fs1.surname_hash,fs1.working_gender,fs1.key_ns, 
        fs1.surname_min_key_ns ,fs1.surname_max_key_ns, fs1.surname_shuffle_key_ns,ora_hash(fs2.surname, 4294967295, 0) surname_shuffle_hash,
        fs2.surname surname_shuffle
        from surname_shuffle1 fs1 join surname_shuffle1 fs2 on fs1.surname_shuffle_key_ns = fs2.key_ns
      ) res;
      
      loop
      
        update surname_shuffle set surname_shuffle_key_ns = mod(abs(dbms_random.random),(surname_max_key_ns - surname_min_key_ns) - 1  ) 
        where resolved = 'N';
          
        if sql%rowcount = 0 then
          exit;
        end if;
        
        update surname_shuffle fs1 set (fs1.surname_shuffle,fs1.surname_shuffle_hash) = (select fs2.surname,ora_hash(fs2.surname, 4294967295, 0) from surname_shuffle fs2
                                                                     where fs1.surname_shuffle_key_ns = fs2.key_ns)
        where resolved = 'N'  ;                                                         
        
        update surname_shuffle fs1 set fs1.resolved = 'Y'
        where fs1.resolved = 'N' and fs1.surname_hash<>fs1.surname_shuffle_hash;
     end loop;
     
    begin
      execute immediate 'drop table holder_names_tmp1';
      exception when others then null;
    end;
       
    execute immediate  'create table holder_names_tmp1 as select * from holder_names where rownum <1';
    
    insert into holder_names_tmp1(posting_date,posting_batch_id,posting_transaction_id,holder_name_id,
    comp_code,ivc_code,holder_seq,holder_type_code,title_type_code,surname,forename,suffix,salutation,
    other_title,preferred_name,trust_name,trustee_name,company_name,created_by,created_date,modified_by,mod_timestamp,posting_prism_task_id)
        
    select hn.posting_date,hn.posting_batch_id,hn.posting_transaction_id,hn.holder_name_id,hn.comp_code,
    hn.ivc_code,hn.holder_seq,hn.holder_type_code,hn.title_type_code,
    ss.surname_shuffle surname,fs.forename_shuffle forename,
    tdc(SUFFIX) SUFFIX_ANON,tdc(SALUTATION) SALUTATION_ANON,tdc(other_title) other_title,
    SUBSTR (NVL (SUBSTR (fs.forename_shuffle, 1, INSTR (fs.forename_shuffle, ' ')),fs.forename_shuffle),1,25) preferred_name,
    tdc(trust_name) trust_name,tdc(trustee_name) trustee_name,tdc(company_name) company_name,hn.created_by,hn.created_date,
    hn.modified_by,hn.mod_timestamp,hn.posting_prism_task_id   
    from holder_names hn 
    left join surname_shuffle ss on ss.holder_name_id = hn.holder_name_id
    left join forename_shuffle fs on fs.holder_name_id = hn.holder_name_id;
      
--    delete from manual_transform where table_name = 'HOLDER_NAMES';
--
--    insert into manual_transform (owner,table_name, actual_col,trans_function,technique)
--    select 'PRISM_CORE','HOLDER_NAMES','SURNAME','shuffle(surname)', 'SHUFFLE_GENDER' from dual union all
--    select 'PRISM_CORE','HOLDER_NAMES','FORENAME','shuffle(forename)', 'SHUFFLE_GENDER' from dual union all
--    select 'PRISM_CORE','HOLDER_NAMES','OTHER_TITLE','tdc(other_title)', 'SUBS_DIGITS_TRANSFORM_CAPS_CHAR' from dual union all
--    select 'PRISM_CORE','HOLDER_NAMES','SUFFIX','tdc(suffix)', 'SUBS_DIGITS_TRANSFORM_CAPS_CHAR' from dual union all
--    select 'PRISM_CORE','HOLDER_NAMES','COMPANY_NAME','tdc(company_name)', 'SUBS_DIGITS_TRANSFORM_CAPS_CHAR' from dual union all
--    select 'PRISM_CORE','HOLDER_NAMES','SALUTATION','tdc(salutation)', 'SUBS_DIGITS_TRANSFORM_CAPS_CHAR' from dual union all
--    select 'PRISM_CORE','HOLDER_NAMES','TRUSTEE_NAME','tdc(trustee_name)', 'SUBS_DIGITS_TRANSFORM_CAPS_CHAR' from dual union all
--    select 'PRISM_CORE','HOLDER_NAMES','PREFERRED_NAME','shuffle(preferred_name)', 'SHUFFLE_GENDER' from dual union all
--    select 'PRISM_CORE','HOLDER_NAMES','TRUST_NAME','tdc(trust_name)', 'SUBS_DIGITS_TRANSFORM_CAPS_CHAR' from dual ;
--    
    commit;
    
    exception
       when others then
          g_code := SQLCODE;
          g_errm := SUBSTR(SQLERRM, 1 , 4000);     
          obfus_log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),g_code,g_errm,g_module);   
          RAISE; -- Error will propogate back to anonymise setting p_obfus_status => 'FAILED'    
  end anon_holder_names;
  
  procedure anon_holder_labels
  is
  
  begin
      g_module := 'anon_holder_labels';
      execute immediate 'truncate table holder_labels_tmp3';    
      execute immediate 'truncate table holder_labels_tmp2';    
      execute immediate 'truncate table holder_labels_tmp1';
            
      insert into  holder_labels_tmp3 (holder_type_code  ,  other_title , holder_label_id , holder_label_type_code , comp_code , 
      ivc_code , forename , surname , title , initials , first_forename , tail_inits , 
      suffix , company_name , trustee_name , trust_name , trust_name_initials , 
      trust_name_first_name , trust_name_tail_inits , jh_surnames , jh_company_names , 
      jh_trustee_names , jh_forenames_surnames , jh_init_surnames , jh_first_forename_tail_init , designation_name ) 

      select -- hn.forename,hn.surname,
      hn.holder_type_code,hn.other_title,hl.holder_label_id,hl.holder_label_type_code,hl.comp_code,hl.ivc_code,hn.forename,hn.surname,
      case when htt.description = 'OTHER' then null else htt.description  end title,
      regexp_replace(initcap(regexp_replace(hn.forename,'([[:punct:]])')),'([[:lower:]])') initials,
      case when instr(hn.forename,' ') > 0 then substr(hn.forename,1,instr(hn.forename,' ')-1) else hn.forename end first_forename,
      case when instr(hn.forename,' ') > 0 then regexp_replace(initcap(regexp_replace(substr(hn.forename,instr(hn.forename,' ')+1) ,'([[:punct:]])')),'([[:lower:]])') else null end tail_inits,
      suffix,
      hn.company_name,hn.trustee_name,hn.trust_name,
      regexp_replace(initcap(regexp_replace(hn.trust_name,'([[:punct:]])')),'([[:lower:]])') trust_name_initials,
      case when instr(hn.trust_name,' ') > 0 then substr(hn.trust_name,1,instr(hn.trust_name,' ')-1) else hn.trust_name end trust_name_first_name,
      case when instr(hn.trust_name,' ') > 0 then regexp_replace(initcap(regexp_replace(substr(hn.trust_name,instr(hn.trust_name,' ')+1) ,'([[:punct:]])')),'([[:lower:]])') else null end  trust_name_tail_inits,
      case when jhn.surnames is not null then '+ '||jhn.surnames else null end jh_surnames,
      case when jhn.company_names is not null then '+ '||jhn.company_names else null end jh_company_names,
      case when jhn.trustee_names is not null then '+ '||jhn.trustee_names else null end jh_trustee_names,
      case when jhn.forenames_surnames is not null then '+ '||jhn.forenames_surnames else null end jh_forenames_surnames,
      case when jhn.init_surnames is not null then '+ '||jhn.init_surnames  else null end jh_init_surnames,
      case when jhn.first_forename_tail_init is not null then '+ '||jhn.first_forename_tail_init else null end jh_first_forename_tail_init,
      h.designation_name
      --select * 
      from holder_labels hl
      left join holder_names_tmp1 hn on  hl.comp_code = hn.comp_code and hl.ivc_code = hn.ivc_code
      left join holders_tmp1 h on  hl.comp_code = h.comp_code and hl.ivc_code = h.ivc_code
      left join 
              (select distinct comp_code,ivc_code ,
               listagg(regexp_replace(initcap(regexp_replace(forename,'([[:punct:]])')),'([[:lower:]])')||' '||surname , ' + ') within group (order by holder_seq) over (partition by comp_code,ivc_code) init_surnames,
               listagg(forename||' '||surname , ' + ') within group (order by holder_seq) over (partition by comp_code,ivc_code) forenames_surnames,                              
               listagg(surname , ' + ') within group (order by holder_seq) over (partition by comp_code,ivc_code)  surnames,
               listagg(substr(forename,1,instr(forename,' ')-1)||' '||regexp_replace(initcap(regexp_replace(substr(forename,instr(forename,' ')+1),'([[:punct:]])')),'([[:lower:]])'), ' + ')  within group (order by holder_seq) over (partition by comp_code,ivc_code) first_forename_tail_init,
               listagg(company_name, ' + ') within group (order by holder_seq) over (partition by comp_code,ivc_code) company_names,  
               listagg(trustee_name, ' + ') within group (order by holder_seq) over (partition by comp_code,ivc_code) trustee_names  
               from holder_names_tmp1 
               where holder_seq <> 1 and (forename is not null or surname is not null or company_name is not null or trustee_name is not null ) ) jhn on  hl.comp_code = jhn.comp_code and hl.ivc_code = jhn.ivc_code
      left join title_types htt on htt.title_type_code = hn.title_type_code
      where hn.holder_seq = 1
      and hn.holder_type_code IN ('I','C','T') and hl.holder_label_type_code in ('1','2','3','4','5','6','7','8','9') ;
 
 
      insert into holder_labels_tmp2 (holder_label_id  ,      
                   holder_type_code,  
                   holder_label_type_code, 
                   comp_code,
                   ivc_code,
                   main_holder,                     
                   joint_holders,                   
                   companies,                       
                   joint_companies,                 
                   trustees,                        
                   joint_trustees)         
      select holder_label_id,holder_type_code,holder_label_type_code,comp_code,ivc_code,
            REPLACE(RTRIM(LTRIM(case when to_char(holder_label_type_code) = '1' and holder_type_code = 'I' then other_title||' '||title||' '|| forename ||' '||surname ||' '||suffix ||
                 case when designation_name is not null then ' '||designation_name ||' ACCT' else null  end                  
            when to_char(holder_label_type_code) = '2'  and holder_type_code = 'I' then other_title||' '||title||' '|| forename||' '||surname  ||' '||suffix ||
                 case when designation_name is not null then ' '||designation_name ||' ACCT' else null   end                              
            when to_char(holder_label_type_code) = '3'  and holder_type_code = 'I' then other_title||' '||title||' '|| first_forename ||' '||tail_inits||' '||surname ||' '||suffix ||
                 case when designation_name is not null then ' '||designation_name ||' ACCT' else null end 
            when to_char(holder_label_type_code) = '4'  and holder_type_code = 'I' then other_title||' '||title||' '|| first_forename||' '||tail_inits||' '||surname ||' '||suffix ||
                 case when designation_name is not null then ' '||designation_name ||' ACCT' else null   end 
            when to_char(holder_label_type_code) = '5'  and holder_type_code = 'I' then other_title||' '||title||' '|| first_forename ||' '||tail_inits||' '||surname ||' '||suffix ||
                 case when designation_name is not null then ' '||designation_name ||' ACCT' else null  end 
            when to_char(holder_label_type_code) = '6'  and holder_type_code = 'I' then other_title||' '||title||' '|| forename||' '||surname  ||' '||suffix ||
                 case when designation_name is not null then ' '||designation_name ||' ACCT' else null   end 
            when to_char(holder_label_type_code) = '7'  and holder_type_code = 'I' then other_title||' '||title||' '|| forename ||' '||surname ||' '||suffix ||
                 case when designation_name is not null then ' '||designation_name ||' ACCT' else null   end 
            when to_char(holder_label_type_code) = '8'  and holder_type_code = 'I' then other_title||' '||title||' '|| forename||' '||surname ||' '||suffix ||
                 case when designation_name is not null then ' '||designation_name ||' ACCT' else null   end 
          when to_char(holder_label_type_code) = '9'  and holder_type_code = 'I' then other_title||' '||title||' '|| forename ||' '||surname ||' '||suffix ||
                 case when designation_name is not null then ' '||designation_name ||' ACCT' else null  end 
            end)),'  ',' ') main_holder,          
            RTRIM(LTRIM(case when to_char(holder_label_type_code) = '1'  and holder_type_code = 'I'  then 
                 case when jh_surnames is not null then jh_surnames else null end                
            when to_char(holder_label_type_code) = '2'  and holder_type_code = 'I' then  
                 case when jh_init_surnames is not null then jh_init_surnames else null end                              
            when to_char(holder_label_type_code) = '3'  and holder_type_code = 'I' then 
                 case when jh_surnames is not null then jh_surnames else null end
            when to_char(holder_label_type_code) = '4'  and holder_type_code = 'I' then 
                 case when jh_init_surnames is not null then jh_init_surnames else null end 
            when to_char(holder_label_type_code) = '5'  and holder_type_code = 'I' then 
                 case when jh_first_forename_tail_init is not null then jh_first_forename_tail_init else null end
            when to_char(holder_label_type_code) = '6'  and holder_type_code = 'I' then 
                 case when jh_surnames is not null then jh_surnames else null end
            when to_char(holder_label_type_code) = '7'  and holder_type_code = 'I'  then 
                 case when jh_init_surnames is not null then jh_init_surnames else null end
            when to_char(holder_label_type_code) = '8'  and holder_type_code = 'I' then 
                 case when jh_first_forename_tail_init is not null then jh_first_forename_tail_init else null end
          when to_char(holder_label_type_code) = '9'  and holder_type_code = 'I' then 
                 case when jh_init_surnames is not null then jh_init_surnames else null end 
          end)) joint_holders  ,
          case when holder_type_code = 'C' then  company_name ||
            case when designation_name is not null then ' '||designation_name ||' ACCT' else null end 
          end companies,
          case when holder_type_code = 'C'    then 
            case when jh_company_names is not null then jh_company_names else null end end joint_companies,
          case when to_char(holder_label_type_code) IN ('1','2') and holder_type_code = 'T' then  trust_name_initials||' '||trustee_name ||
            case when designation_name is not null then ' '||designation_name ||' ACCT' else null end 
          when to_char(holder_label_type_code) IN ('3','4','5') and holder_type_code = 'T' then  trust_name_first_name||' '||trust_name_tail_inits||' '|| trustee_name ||
            case when designation_name is not null then ' '||designation_name ||' ACCT' else null end 
          when to_char(holder_label_type_code) IN ('6','7','8','9') and holder_type_code = 'T' then  trustee_name ||
            case when designation_name is not null then ' '||designation_name ||' ACCT' else null end 
          end trustees,
         case when holder_type_code = 'T'    then 
            case when jh_trustee_names is not null then jh_trustee_names else null end end joint_trustees
          from holder_labels_tmp3;

  
    insert into holder_labels_tmp1(holder_label_id,holder_type_code,holder_label_type_code,comp_code,ivc_code,label_line_1,label_line_2,label_line_3,label_line_4) 
    select holder_label_id,holder_type_code,holder_label_type_code,comp_code,ivc_code,
    case when holder_type_code = 'I' and  length(main_holder)> 35 then substr(main_holder,1,instr(main_holder,' ',-1*(length(main_holder)-35),1)-1)  else main_holder end ||
    case when holder_type_code = 'C' and  length(companies)> 35 then substr(companies,1,instr(companies,' ',-1*(length(companies)-35),1)-1)  else companies  end ||
    case when holder_type_code = 'T' and  length(trustees)> 35 then substr(trustees,1,instr(trustees,' ',-1*(length(trustees)-35),1)-1)  else trustees  end label_line_1,
    case when holder_type_code = 'I' and length(main_holder)> 35 then lpad(substr(main_holder,instr(main_holder,' ',-1*(length(main_holder)-35),1)+1),35,' ') else null end||
    case when holder_type_code = 'C' and length(companies)> 35 then lpad(substr(companies,instr(companies,' ',-1*(length(companies)-35),1)+1),35,' ') else null end||
    case when holder_type_code = 'T' and length(trustees)> 35 then lpad(substr(trustees,instr(trustees,' ',-1*(length(trustees)-35),1)+1),35,' ') else null end label_line_2,
    case when holder_type_code = 'I' and length(joint_holders)> 35 then substr(joint_holders,1,instr(joint_holders,' ',-1*(length(joint_holders)-35),1)-1) else joint_holders end  ||
    case when holder_type_code = 'C' and length(joint_companies)> 35 then substr(joint_companies,1,instr(joint_companies,' ',-1*(length(joint_companies)-35),1)-1) else joint_companies end  ||
    case when holder_type_code = 'T' and length(joint_trustees)> 35 then substr(joint_trustees,1,instr(joint_trustees,' ',-1*(length(joint_trustees)-35),1)-1) else joint_trustees end label_line_3,
    case when holder_type_code = 'I' and length(joint_holders)> 35 then substr(joint_holders,instr(joint_holders,' ',-1*(length(joint_holders)-35),1)+1) else null end ||
    case when holder_type_code = 'C' and length(joint_companies)> 35 then substr(joint_companies,instr(joint_companies,' ',-1*(length(joint_companies)-35),1)+1) else null end ||
    case when length(joint_trustees)> 35 then substr(joint_trustees,instr(joint_trustees,' ',-1*(length(joint_trustees)-35),1)+1) else null end label_line_4
    from holder_labels_tmp2;
            
  
  
    merge into holder_labels_tmp1 hl
           using (select hlt.holder_label_id, tdc(hl.line1_text)  label_line_1
                    from  holder_labels_tmp1 hlt 
                    join holder_labels hl on hl.holder_label_id = hlt.holder_label_id
                    where hlt.label_line_1 is null) ahn
              on (hl.holder_label_id = ahn.holder_label_id)
      when matched
      then
         update set hl.label_line_1  =  ahn.label_line_1;  
  
--      delete from manual_transform where owner = 'PRISM_CORE' and table_name = 'HOLDER_LABELS';
--        
--      insert into manual_transform (owner,table_name, actual_col,trans_function,technique)
--      select 'PRISM_CORE','HOLDER_LABELS','LINE1_TEXT','local_rule(line1_text)', 'INHERIT_HN' from dual union all
--      select 'PRISM_CORE','HOLDER_LABELS','LINE2_TEXT','TO_NULL(line2_text)', 'INHERIT_HN' from dual union all
--      select 'PRISM_CORE','HOLDER_LABELS','LINE3_TEXT','TO_NULL(line3_text)', 'INHERIT_HN' from dual union all
--      select 'PRISM_CORE','HOLDER_LABELS','LINE4_TEXT','TO_NULL(line4_text)', 'INHERIT_HN' from dual;

      commit;

  exception
     when others then
        g_code := SQLCODE;
        g_errm := SUBSTR(SQLERRM, 1 , 4000);     
        obfus_log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),g_code,g_errm,g_module);         
        RAISE; -- Error will propogate back to anonymise setting p_obfus_status => 'FAILED'
  end anon_holder_labels;
   
  procedure address_line1_shuffle is
         nMaxKey number;
         nLoopCounter number;
  begin
  
      g_module := 'address_line1_shuffle';
      -- ADRESS LINE 1 
      
      execute immediate 'truncate table ADDRESS_LINE1_shuffle';
      execute immediate 'truncate table ADDRESS_LINE1_shuffle1';
            
      insert into ADDRESS_LINE1_shuffle1(HOLDER_ADDRESS_ID ,comp_code,ivc_code ,
      ADDRESS_LINE1 , ADDRESS_LINE1_hash,working_country_code,key_ns,ADDRESS_LINE1_min_key_ns ,ADDRESS_LINE1_max_key_ns,ADDRESS_LINE1_shuffle_key_ns)
        select  res2.HOLDER_ADDRESS_ID ,res2.comp_code,res2.ivc_code ,
        res2.ADDRESS_LINE1 , res2.ADDRESS_LINE1_hash,res2.working_country_code,res2.key_ns, 
        res2.ADDRESS_LINE1_min_key_ns ,res2.ADDRESS_LINE1_max_key_ns,
        mod(abs(dbms_random.random),(res2.ADDRESS_LINE1_max_key_ns - res2.ADDRESS_LINE1_min_key_ns) + 1  )  + res2.ADDRESS_LINE1_min_key_ns  ADDRESS_LINE1_shuffle_key_ns
        from
        (
          select   
          res1.HOLDER_ADDRESS_ID ,res1.comp_code,res1.ivc_code ,
          res1.ADDRESS_LINE1 , res1.ADDRESS_LINE1_hash,res1.working_country_code,res1.key_ns,   
          first_value(res1.key_ns) over (partition by res1.working_country_code order by res1.key_ns asc rows unbounded preceding ) ADDRESS_LINE1_min_key_ns ,
          last_value(res1.key_ns) over (partition by res1.working_country_code  order by res1.key_ns asc rows between  unbounded preceding and unbounded following ) ADDRESS_LINE1_max_key_ns
          from
          (
              select res.HOLDER_ADDRESS_ID ,res.comp_code,res.ivc_code,
              res.ADDRESS_LINE1 , res.ADDRESS_LINE1_hash,working_country_code,row_number() over (partition by 1 order by res.working_country_code) key_ns
              from 
              (
                select hn.HOLDER_ADDRESS_ID ,hn.comp_code comp_code,hn.ivc_code ivc_code,
                hn.ADDRESS_LINE1 ,ora_hash(hn.ADDRESS_LINE1, 4294967295, 0) ADDRESS_LINE1_hash,
                case when hn.country_code is null then 'GB' else hn.country_code end working_country_code
                from holder_addresses hn 
                where hn.ADDRESS_LINE1  is not null
              ) res
          )res1
        ) res2;

       execute immediate 'truncate table ADDRESS_LINE1_shuffle';
  
      insert into ADDRESS_LINE1_shuffle (HOLDER_ADDRESS_ID ,comp_code,ivc_code ,
      ADDRESS_LINE1 , ADDRESS_LINE1_hash,working_country_code,key_ns,ADDRESS_LINE1_min_key_ns ,ADDRESS_LINE1_max_key_ns,ADDRESS_LINE1_shuffle_key_ns,ADDRESS_LINE1_shuffle_hash,
      resolved ,ADDRESS_LINE1_shuffle)
    
      select res.HOLDER_ADDRESS_ID ,res.comp_code,res.ivc_code ,
      res.ADDRESS_LINE1 , res.ADDRESS_LINE1_hash,res.working_country_code,res.key_ns, 
      res.ADDRESS_LINE1_min_key_ns ,res.ADDRESS_LINE1_max_key_ns, res.ADDRESS_LINE1_shuffle_key_ns, ADDRESS_LINE1_shuffle_hash,
      case when res.ADDRESS_LINE1_hash=res.ADDRESS_LINE1_shuffle_hash then 'N' else 'Y' end   resolved,
      ADDRESS_LINE1_shuffle
      from
      (
        select  fs1.HOLDER_ADDRESS_ID ,fs1.comp_code,fs1.ivc_code ,
        fs1.ADDRESS_LINE1 , fs1.ADDRESS_LINE1_hash,fs1.working_country_code,fs1.key_ns, 
        fs1.ADDRESS_LINE1_min_key_ns ,fs1.ADDRESS_LINE1_max_key_ns, fs1.ADDRESS_LINE1_shuffle_key_ns,ora_hash(fs2.ADDRESS_LINE1, 4294967295, 0) ADDRESS_LINE1_shuffle_hash,
        fs2.ADDRESS_LINE1 ADDRESS_LINE1_shuffle
        from ADDRESS_line1_shuffle1 fs1 join ADDRESS_LINE1_shuffle1 fs2 on fs1.ADDRESS_LINE1_shuffle_key_ns = fs2.key_ns
      ) res;
      
      select max(key_ns) into nMaxKey from ADDRESS_LINE1_shuffle;
      
      nLoopCounter := 0;
      loop
      
        nloopcounter := nloopcounter + 1;
        dbms_output.put_line(to_char(nloopcounter));
        if nloopcounter > 9 then
          exit;
        end if;
      
        if nloopcounter < 5 then
          update ADDRESS_line1_shuffle set ADDRESS_LINE1_shuffle_key_ns = mod(abs(dbms_random.random),(ADDRESS_LINE1_max_key_ns - ADDRESS_LINE1_min_key_ns) + 1  )  + ADDRESS_LINE1_min_key_ns 
          where resolved = 'N';
        else
          update ADDRESS_line1_shuffle set ADDRESS_LINE1_shuffle_key_ns = mod(abs(dbms_random.random),(nMaxKey - 1) + 1  ) 
          where resolved = 'N';
        end if;
      
        if sql%rowcount = 0 then
          exit;
        end if;
  
        update ADDRESS_LINE1_shuffle fs1 set (fs1.ADDRESS_LINE1_shuffle,fs1.ADDRESS_LINE1_shuffle_hash )= (select fs2.ADDRESS_LINE1, ora_hash(fs2.ADDRESS_LINE1, 4294967295, 0) from ADDRESS_LINE1_shuffle fs2
                                                                                                      where fs1.ADDRESS_LINE1_shuffle_key_ns = fs2.key_ns)
        where resolved = 'N';
        
        update ADDRESS_LINE1_shuffle fs1 set fs1.resolved = 'Y'
        where fs1.resolved = 'N' and fs1.ADDRESS_LINE1_hash<>fs1.ADDRESS_LINE1_shuffle_hash;
        commit;
    end loop;

    commit;
    
    exception
       when others then
          g_code := SQLCODE;
          g_errm := SUBSTR(SQLERRM, 1 , 4000);     
          obfuscation_control.update_obfus_control(g_obfus_run_id, g_src_prefix, g_tgt_prefix, g_run_env, g_anon_version, p_obfus_status => 'FAILED');
          obfus_log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),g_code,g_errm,g_module); 
  end address_line1_shuffle;
  
  procedure address_line2_shuffle is 
    nMaxKey number;
    nLoopCounter number;
  begin
  
      g_module := 'address_line2_shuffle';
      
     -- ADRESS LINE 2
      
        execute immediate 'truncate table ADDRESS_LINE2_shuffle';
        execute immediate 'truncate table ADDRESS_LINE2_shuffle1';
            
      insert into ADDRESS_LINE2_shuffle1(HOLDER_ADDRESS_ID ,comp_code,ivc_code ,
      ADDRESS_LINE2 , ADDRESS_LINE2_hash,working_country_code,key_ns,ADDRESS_LINE2_min_key_ns ,ADDRESS_LINE2_max_key_ns,ADDRESS_LINE2_shuffle_key_ns)
        select  res2.HOLDER_ADDRESS_ID ,res2.comp_code,res2.ivc_code ,
        res2.ADDRESS_LINE2 , res2.ADDRESS_LINE2_hash,res2.working_country_code,res2.key_ns, 
        res2.ADDRESS_LINE2_min_key_ns ,res2.ADDRESS_LINE2_max_key_ns,
        mod(abs(dbms_random.random),(res2.ADDRESS_LINE2_max_key_ns - res2.ADDRESS_LINE2_min_key_ns) + 1  )   + res2.ADDRESS_LINE2_min_key_ns  ADDRESS_LINE2_shuffle_key_ns
        from
        (
          select   
          res1.HOLDER_ADDRESS_ID ,res1.comp_code,res1.ivc_code ,
          res1.ADDRESS_LINE2 , res1.ADDRESS_LINE2_hash,res1.working_country_code,res1.key_ns,   
          first_value(res1.key_ns) over (partition by res1.working_country_code order by res1.key_ns asc rows unbounded preceding ) ADDRESS_LINE2_min_key_ns ,
          last_value(res1.key_ns) over (partition by res1.working_country_code  order by res1.key_ns asc rows between  unbounded preceding and unbounded following ) ADDRESS_LINE2_max_key_ns
          from
          (
              select res.HOLDER_ADDRESS_ID ,res.comp_code,res.ivc_code,
              res.ADDRESS_LINE2 , res.ADDRESS_LINE2_hash,working_country_code,row_number() over (partition by 1 order by res.working_country_code) key_ns
              from 
              (
                select hn.HOLDER_ADDRESS_ID ,hn.comp_code comp_code,hn.ivc_code ivc_code,
                hn.ADDRESS_LINE2 ,ora_hash(hn.ADDRESS_LINE2, 4294967295, 0) ADDRESS_LINE2_hash,
                case when hn.country_code is null then 'GB' else hn.country_code end working_country_code
                from holder_addresses hn 
                where hn.ADDRESS_LINE2  is not null
              ) res
          )res1
        ) res2;
        
      execute immediate 'truncate table ADDRESS_LINE2_shuffle';
  
      insert into ADDRESS_LINE2_shuffle (HOLDER_ADDRESS_ID ,comp_code,ivc_code ,
      ADDRESS_LINE2 , ADDRESS_LINE2_hash,working_country_code,key_ns,ADDRESS_LINE2_min_key_ns ,ADDRESS_LINE2_max_key_ns,ADDRESS_LINE2_shuffle_key_ns,ADDRESS_LINE2_shuffle_hash,
      resolved ,ADDRESS_LINE2_shuffle)
    
      select res.HOLDER_ADDRESS_ID ,res.comp_code,res.ivc_code ,
      res.ADDRESS_LINE2 , res.ADDRESS_LINE2_hash,res.working_country_code,res.key_ns, 
      res.ADDRESS_LINE2_min_key_ns ,res.ADDRESS_LINE2_max_key_ns, res.ADDRESS_LINE2_shuffle_key_ns, ADDRESS_LINE2_shuffle_hash,
      case when res.ADDRESS_LINE2_hash=res.ADDRESS_LINE2_shuffle_hash then 'N' else 'Y' end   resolved,
      ADDRESS_LINE2_shuffle
      from
      (
        select  fs1.HOLDER_ADDRESS_ID ,fs1.comp_code,fs1.ivc_code ,
        fs1.ADDRESS_LINE2 , fs1.ADDRESS_LINE2_hash,fs1.working_country_code,fs1.key_ns, 
        fs1.ADDRESS_LINE2_min_key_ns ,fs1.ADDRESS_LINE2_max_key_ns, fs1.ADDRESS_LINE2_shuffle_key_ns,ora_hash(fs2.ADDRESS_LINE2, 4294967295, 0) ADDRESS_LINE2_shuffle_hash,
        fs2.ADDRESS_LINE2 ADDRESS_LINE2_shuffle
        from ADDRESS_LINE2_shuffle1 fs1 join ADDRESS_LINE2_shuffle1 fs2 on fs1.ADDRESS_LINE2_shuffle_key_ns = fs2.key_ns
      ) res;
      
      select max(key_ns) into nMaxKey from ADDRESS_LINE2_shuffle;
      nLoopCounter := 0;
      loop
      
        nloopcounter := nloopcounter + 1;
        dbms_output.put_line(to_char(nloopcounter));
        if nloopcounter > 9 then
          exit;
        end if;
      
        if nloopcounter < 5 then
          update ADDRESS_line2_shuffle set ADDRESS_LINE2_shuffle_key_ns = mod(abs(dbms_random.random),(ADDRESS_LINE2_max_key_ns - ADDRESS_LINE2_min_key_ns) + 1  )  + ADDRESS_LINE2_min_key_ns 
          where resolved = 'N';
        else
          update ADDRESS_line2_shuffle set ADDRESS_LINE2_shuffle_key_ns = mod(abs(dbms_random.random),(nMaxKey - 1) + 1  ) 
          where resolved = 'N';
        end if;

          
        if sql%rowcount = 0 then
          exit;
        end if;
        
        update ADDRESS_LINE2_shuffle fs1 set (fs1.ADDRESS_LINE2_shuffle,fs1.ADDRESS_LINE2_shuffle_hash )= (select fs2.ADDRESS_LINE2, ora_hash(fs2.ADDRESS_LINE2, 4294967295, 0) from ADDRESS_LINE2_shuffle fs2
                                                                     where fs1.ADDRESS_LINE2_shuffle_key_ns = fs2.key_ns)
        where resolved = 'N'  ;                                                         
        
        update ADDRESS_LINE2_shuffle fs1 set fs1.resolved = 'Y'
        where fs1.resolved = 'N' and fs1.ADDRESS_LINE2_hash<>fs1.ADDRESS_LINE2_shuffle_hash;
     end loop;

    commit;
    
    exception
       when others then
          g_code := SQLCODE;
          g_errm := SUBSTR(SQLERRM, 1 , 4000);     
          obfus_log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),g_code,g_errm,g_module); 
          RAISE; -- Error will propogate back to anonymise setting p_obfus_status => 'FAILED'
  end address_line2_shuffle;

  procedure address_line3_shuffle is
    nMaxKey number;
    nLoopCounter number;
  begin
  
   g_module := 'address_line3_shuffle';
-- ADRESS LINE 3
    
    execute immediate 'truncate table ADDRESS_LINE3_shuffle';
    execute immediate 'truncate table ADDRESS_LINE3_shuffle1';
            
      insert into ADDRESS_LINE3_shuffle1(HOLDER_ADDRESS_ID ,comp_code,ivc_code ,
      ADDRESS_LINE3 , ADDRESS_LINE3_hash,working_country_code,key_ns,ADDRESS_LINE3_min_key_ns ,ADDRESS_LINE3_max_key_ns,ADDRESS_LINE3_shuffle_key_ns)
        select  res2.HOLDER_ADDRESS_ID ,res2.comp_code,res2.ivc_code ,
        res2.ADDRESS_LINE3 , res2.ADDRESS_LINE3_hash,res2.working_country_code,res2.key_ns, 
        res2.ADDRESS_LINE3_min_key_ns ,res2.ADDRESS_LINE3_max_key_ns,
        mod(abs(dbms_random.random),(res2.ADDRESS_LINE3_max_key_ns - res2.ADDRESS_LINE3_min_key_ns) + 1  )  + ADDRESS_LINE3_min_key_ns  ADDRESS_LINE3_shuffle_key_ns
        from
        (
          select   
          res1.HOLDER_ADDRESS_ID ,res1.comp_code,res1.ivc_code ,
          res1.ADDRESS_LINE3 , res1.ADDRESS_LINE3_hash,res1.working_country_code,res1.key_ns,   
          first_value(res1.key_ns) over (partition by res1.working_country_code order by res1.key_ns asc rows unbounded preceding ) ADDRESS_LINE3_min_key_ns ,
          last_value(res1.key_ns) over (partition by res1.working_country_code  order by res1.key_ns asc rows between  unbounded preceding and unbounded following ) ADDRESS_LINE3_max_key_ns
          from
          (
              select res.HOLDER_ADDRESS_ID ,res.comp_code,res.ivc_code,
              res.ADDRESS_LINE3 , res.ADDRESS_LINE3_hash,working_country_code,row_number() over (partition by 1 order by res.working_country_code) key_ns
              from 
              (
                select hn.HOLDER_ADDRESS_ID ,hn.comp_code comp_code,hn.ivc_code ivc_code,
                hn.ADDRESS_LINE3 ,ora_hash(hn.ADDRESS_LINE3, 4294967295, 0) ADDRESS_LINE3_hash,
                case when hn.country_code is null then 'GB' else hn.country_code end working_country_code
                from holder_addresses hn 
                where hn.ADDRESS_LINE3  is not null
              ) res
          )res1
        ) res2;

       execute immediate 'truncate table ADDRESS_LINE3_shuffle';
  
      insert into ADDRESS_LINE3_shuffle (HOLDER_ADDRESS_ID ,comp_code,ivc_code ,
      ADDRESS_LINE3 , ADDRESS_LINE3_hash,working_country_code,key_ns,ADDRESS_LINE3_min_key_ns ,ADDRESS_LINE3_max_key_ns,ADDRESS_LINE3_shuffle_key_ns,ADDRESS_LINE3_shuffle_hash,
      resolved ,ADDRESS_LINE3_shuffle)
    
      select res.HOLDER_ADDRESS_ID ,res.comp_code,res.ivc_code ,
      res.ADDRESS_LINE3 , res.ADDRESS_LINE3_hash,res.working_country_code,res.key_ns, 
      res.ADDRESS_LINE3_min_key_ns ,res.ADDRESS_LINE3_max_key_ns, res.ADDRESS_LINE3_shuffle_key_ns, ADDRESS_LINE3_shuffle_hash,
      case when res.ADDRESS_LINE3_hash=res.ADDRESS_LINE3_shuffle_hash then 'N' else 'Y' end   resolved,
      ADDRESS_LINE3_shuffle
      from
      (
        select  fs1.HOLDER_ADDRESS_ID ,fs1.comp_code,fs1.ivc_code ,
        fs1.ADDRESS_LINE3 , fs1.ADDRESS_LINE3_hash,fs1.working_country_code,fs1.key_ns, 
        fs1.ADDRESS_LINE3_min_key_ns ,fs1.ADDRESS_LINE3_max_key_ns, fs1.ADDRESS_LINE3_shuffle_key_ns,ora_hash(fs2.ADDRESS_LINE3, 4294967295, 0) ADDRESS_LINE3_shuffle_hash,
        fs2.ADDRESS_LINE3 ADDRESS_LINE3_shuffle
        from ADDRESS_LINE3_shuffle1 fs1 join ADDRESS_LINE3_shuffle1 fs2 on fs1.ADDRESS_LINE3_shuffle_key_ns = fs2.key_ns
      ) res;
      
      select max(key_ns) into nMaxKey from ADDRESS_LINE3_shuffle;
      nLoopCounter := 0;
      
      loop
      
        nloopcounter := nloopcounter + 1;
        
        dbms_output.put_line(to_char(nloopcounter));
        
        if nloopcounter > 9 then
          exit;
        end if;
      
        if nloopcounter < 5 then
          update ADDRESS_line3_shuffle set ADDRESS_LINE3_shuffle_key_ns = mod(abs(dbms_random.random),(ADDRESS_LINE3_max_key_ns - ADDRESS_LINE3_min_key_ns) + 1  ) + ADDRESS_LINE3_min_key_ns 
          where resolved = 'N';
        else
          update ADDRESS_line3_shuffle set ADDRESS_LINE3_shuffle_key_ns = mod(abs(dbms_random.random),(nMaxKey + 1) - 1  ) 
          where resolved = 'N';
        end if;
          
        if sql%rowcount = 0 then
          exit;
        end if;
        
        update ADDRESS_LINE3_shuffle fs1 set (fs1.ADDRESS_LINE3_shuffle,fs1.ADDRESS_LINE3_shuffle_hash )= (select fs2.ADDRESS_LINE3, ora_hash(fs2.ADDRESS_LINE3, 4294967295, 0) from ADDRESS_LINE3_shuffle fs2
                                                                     where fs1.ADDRESS_LINE3_shuffle_key_ns = fs2.key_ns)
        where resolved = 'N'  ;                                                         
        
        update ADDRESS_LINE3_shuffle fs1 set fs1.resolved = 'Y'
        where fs1.resolved = 'N' and fs1.ADDRESS_LINE3_hash<>fs1.ADDRESS_LINE3_shuffle_hash;
     end loop;
     
    commit;
    
    exception
       when others then
          g_code := SQLCODE;
          g_errm := SUBSTR(SQLERRM, 1 , 4000);     
          obfus_log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),g_code,g_errm,g_module); 
          RAISE; -- Error will propogate back to anonymise setting p_obfus_status => 'FAILED'
  end address_line3_shuffle;  

  procedure address_line456_shuffle is
    nMaxKey number;
    nLoopCounter number;
  begin
  
    
     g_module := 'address_line456_shuffle';
  
      -- ADRESS LINE 456
      execute immediate 'truncate table ADDRESS_LINE456_shuffle';
      execute immediate 'truncate table ADDRESS_LINE456_shuffle1';
            
      insert into ADDRESS_LINE456_shuffle1(HOLDER_ADDRESS_ID ,comp_code,ivc_code ,
      ADDRESS_LINE4,ADDRESS_LINE5,ADDRESS_LINE6 , ADDRESS_LINE456_hash,working_country_code,key_ns,ADDRESS_LINE456_min_key_ns ,ADDRESS_LINE456_max_key_ns,ADDRESS_LINE456_shuffle_key_ns)
        select  res2.HOLDER_ADDRESS_ID ,res2.comp_code,res2.ivc_code ,
        res2.ADDRESS_LINE4 ,res2.ADDRESS_LINE5,res2.ADDRESS_LINE6, res2.ADDRESS_LINE456_hash,res2.working_country_code,res2.key_ns, 
        res2.ADDRESS_LINE456_min_key_ns ,res2.ADDRESS_LINE456_max_key_ns,
        mod(abs(dbms_random.random),(res2.ADDRESS_LINE456_max_key_ns - res2.ADDRESS_LINE456_min_key_ns) + 1  )  + ADDRESS_LINE456_min_key_ns  ADDRESS_LINE456_shuffle_key_ns
        from
        (
          select   
          res1.HOLDER_ADDRESS_ID ,res1.comp_code,res1.ivc_code ,
          res1.ADDRESS_LINE4,res1.ADDRESS_LINE5,res1.ADDRESS_LINE6 , res1.ADDRESS_LINE456_hash,res1.working_country_code,res1.key_ns,   
          first_value(res1.key_ns) over (partition by res1.working_country_code order by res1.key_ns asc rows unbounded preceding ) ADDRESS_LINE456_min_key_ns ,
          last_value(res1.key_ns) over (partition by res1.working_country_code  order by res1.key_ns asc rows between  unbounded preceding and unbounded following ) ADDRESS_LINE456_max_key_ns
          from
          (
              select res.HOLDER_ADDRESS_ID ,res.comp_code,res.ivc_code,
              res.ADDRESS_LINE4, res.ADDRESS_LINE5,res.ADDRESS_LINE6, res.ADDRESS_LINE456_hash,working_country_code,row_number() over (partition by 1 order by res.working_country_code) key_ns
              from 
              (
                select hn.HOLDER_ADDRESS_ID ,hn.comp_code comp_code,hn.ivc_code ivc_code,
                hn.ADDRESS_LINE4 ,hn.ADDRESS_LINE5,hn.ADDRESS_LINE6,ora_hash(hn.ADDRESS_LINE4||hn.ADDRESS_LINE5||hn.ADDRESS_LINE6, 4294967295, 0) ADDRESS_LINE456_hash,
                case when hn.country_code is null then 'GB' else hn.country_code end working_country_code
                from holder_addresses hn 
                where hn.ADDRESS_LINE4||hn.ADDRESS_LINE5||hn.ADDRESS_LINE6 is not null
              ) res
          )res1
        ) res2;

       execute immediate 'truncate table ADDRESS_LINE456_shuffle';
  
      insert into ADDRESS_LINE456_shuffle (HOLDER_ADDRESS_ID ,comp_code,ivc_code ,
      ADDRESS_LINE4 ,ADDRESS_LINE5 ,ADDRESS_LINE6 , ADDRESS_LINE456_hash,working_country_code,key_ns,ADDRESS_LINE456_min_key_ns ,ADDRESS_LINE456_max_key_ns,ADDRESS_LINE456_shuffle_key_ns,ADDRESS_LINE456_shuffle_hash,
      resolved ,ADDRESS_LINE4_shuffle,ADDRESS_LINE5_shuffle,ADDRESS_LINE6_shuffle)
    
      select res.HOLDER_ADDRESS_ID ,res.comp_code,res.ivc_code ,
      res.ADDRESS_LINE4 ,res.ADDRESS_LINE5,res.ADDRESS_LINE6, res.ADDRESS_LINE456_hash,res.working_country_code,res.key_ns, 
      res.ADDRESS_LINE456_min_key_ns ,res.ADDRESS_LINE456_max_key_ns, res.ADDRESS_LINE456_shuffle_key_ns, ADDRESS_LINE456_shuffle_hash,
      case when res.ADDRESS_LINE456_hash=res.ADDRESS_LINE456_shuffle_hash then 'N' else 'Y' end   resolved,
      ADDRESS_LINE4_shuffle,ADDRESS_LINE5_shuffle,ADDRESS_LINE6_shuffle
      from
      (
        select  fs1.HOLDER_ADDRESS_ID ,fs1.comp_code,fs1.ivc_code ,
        fs1.ADDRESS_LINE4 ,fs1.ADDRESS_LINE5 ,fs1.ADDRESS_LINE6 , fs1.ADDRESS_LINE456_hash,fs1.working_country_code,fs1.key_ns, 
        fs1.ADDRESS_LINE456_min_key_ns ,fs1.ADDRESS_LINE456_max_key_ns, fs1.ADDRESS_LINE456_shuffle_key_ns,
        ora_hash(fs2.ADDRESS_LINE4||fs2.ADDRESS_LINE5||fs2.ADDRESS_LINE6, 4294967295, 0) ADDRESS_LINE456_shuffle_hash,
        fs2.ADDRESS_LINE4 ADDRESS_LINE4_shuffle,
        fs2.ADDRESS_LINE5 ADDRESS_LINE5_shuffle,
        fs2.ADDRESS_LINE6 ADDRESS_LINE6_shuffle
        from ADDRESS_LINE456_shuffle1 fs1 join ADDRESS_LINE456_shuffle1 fs2 on fs1.ADDRESS_LINE456_shuffle_key_ns = fs2.key_ns
      ) res;
      
      commit;
      select max(key_ns) into nMaxKey from ADDRESS_LINE456_shuffle;
      nLoopCounter := 0;
      
      loop
      
        nloopcounter := nloopcounter + 1;
        
        dbms_output.put_line(to_char(nloopcounter));
        
        if nloopcounter > 9 then
          exit;
        end if;
      
        if nloopcounter < 5 then
          update ADDRESS_line456_shuffle set ADDRESS_LINE456_shuffle_key_ns = mod(abs(dbms_random.random),(ADDRESS_LINE456_max_key_ns - ADDRESS_LINE456_min_key_ns) +1   ) + ADDRESS_LINE456_min_key_ns 
          where resolved = 'N';
        else
          update ADDRESS_line456_shuffle set ADDRESS_LINE456_shuffle_key_ns = mod(abs(dbms_random.random),(nMaxKey - 1) - 1  ) 
          where resolved = 'N';
        end if;
          
        if sql%rowcount = 0 then
          exit;
        end if;
        
        update ADDRESS_LINE456_shuffle fs1 set (fs1.ADDRESS_LINE4_shuffle,fs1.ADDRESS_LINE5,fs1.ADDRESS_LINE6,fs1.ADDRESS_LINE456_shuffle_hash )= (select fs2.ADDRESS_LINE4,fs2.ADDRESS_LINE5,fs2.ADDRESS_LINE6 ,ora_hash(fs2.ADDRESS_LINE4||fs2.ADDRESS_LINE5||fs2.ADDRESS_LINE6, 4294967295, 0) from ADDRESS_LINE456_shuffle fs2
                                                                     where fs1.ADDRESS_LINE456_shuffle_key_ns = fs2.key_ns)
        where resolved = 'N';                                                         
        
        update ADDRESS_LINE456_shuffle fs1 set fs1.resolved = 'Y'
        where fs1.resolved = 'N' and fs1.ADDRESS_LINE456_hash<>fs1.ADDRESS_LINE456_shuffle_hash;
     end loop;

    commit;
  exception
     when others then
        g_code := SQLCODE;
        g_errm := SUBSTR(SQLERRM, 1 , 4000);     
        obfus_log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),g_code,g_errm,g_module); 
        RAISE; -- Error will propogate back to anonymise setting p_obfus_status => 'FAILED'
  end address_line456_shuffle; 
  
  procedure postcode_left_shuffle is
    nMaxKey number;
    nLoopCounter number;
  begin
  
      g_module := 'postcode_left_shuffle';
            -- POST CODE LEFT
    
       
      execute immediate 'truncate table POSTCODE_LEFT_shuffle';
      execute immediate 'truncate table POSTCODE_LEFT_shuffle1';
            
      insert into POSTCODE_LEFT_shuffle1(HOLDER_ADDRESS_ID ,comp_code,ivc_code ,
      POSTCODE_LEFT , POSTCODE_LEFT_hash,working_country_code,key_ns,POSTCODE_LEFT_min_key_ns ,POSTCODE_LEFT_max_key_ns,POSTCODE_LEFT_shuffle_key_ns)
        select  res2.HOLDER_ADDRESS_ID ,res2.comp_code,res2.ivc_code ,
        res2.POSTCODE_LEFT , res2.POSTCODE_LEFT_hash,res2.working_country_code,res2.key_ns, 
        res2.POSTCODE_LEFT_min_key_ns ,res2.POSTCODE_LEFT_max_key_ns,
        mod(abs(dbms_random.random),(res2.POSTCODE_LEFT_max_key_ns - res2.POSTCODE_LEFT_min_key_ns) + 1  )  + POSTCODE_LEFT_min_key_ns  POSTCODE_LEFT_shuffle_key_ns
        from
        (
          select   
          res1.HOLDER_ADDRESS_ID ,res1.comp_code,res1.ivc_code ,
          res1.POSTCODE_LEFT , res1.POSTCODE_LEFT_hash,res1.working_country_code,res1.key_ns,   
          first_value(res1.key_ns) over (partition by res1.working_country_code order by res1.key_ns asc rows unbounded preceding ) POSTCODE_LEFT_min_key_ns ,
          last_value(res1.key_ns) over (partition by res1.working_country_code  order by res1.key_ns asc rows between  unbounded preceding and unbounded following ) POSTCODE_LEFT_max_key_ns
          from
          (
              select res.HOLDER_ADDRESS_ID ,res.comp_code,res.ivc_code,
              res.POSTCODE_LEFT , res.POSTCODE_LEFT_hash,working_country_code,row_number() over (partition by 1 order by res.working_country_code) key_ns
              from 
              (
                select hn.HOLDER_ADDRESS_ID ,hn.comp_code comp_code,hn.ivc_code ivc_code,
                hn.POSTCODE_LEFT ,ora_hash(hn.POSTCODE_LEFT, 4294967295, 0) POSTCODE_LEFT_hash,
                case when hn.country_code is null then 'GB' else hn.country_code end working_country_code
                from holder_addresses hn 
                where hn.POSTCODE_LEFT  is not null
              ) res
          )res1
        ) res2;

       execute immediate 'truncate table POSTCODE_LEFT_shuffle';
  
      insert into POSTCODE_LEFT_shuffle (HOLDER_ADDRESS_ID ,comp_code,ivc_code ,
      POSTCODE_LEFT , POSTCODE_LEFT_hash,working_country_code,key_ns,POSTCODE_LEFT_min_key_ns ,POSTCODE_LEFT_max_key_ns,POSTCODE_LEFT_shuffle_key_ns,
      POSTCODE_LEFT_shuffle_hash,
      resolved ,POSTCODE_LEFT_shuffle)
    
      select res.HOLDER_ADDRESS_ID ,res.comp_code,res.ivc_code ,
      res.POSTCODE_LEFT , res.POSTCODE_LEFT_hash,res.working_country_code,res.key_ns, 
      res.POSTCODE_LEFT_min_key_ns ,res.POSTCODE_LEFT_max_key_ns, res.POSTCODE_LEFT_shuffle_key_ns, POSTCODE_LEFT_shuffle_hash,
      case when res.POSTCODE_LEFT_hash=res.POSTCODE_LEFT_shuffle_hash then 'N' else 'Y' end   resolved,
      POSTCODE_LEFT_shuffle
      from
      (
        select  fs1.HOLDER_ADDRESS_ID ,fs1.comp_code,fs1.ivc_code ,
        fs1.POSTCODE_LEFT , fs1.POSTCODE_LEFT_hash,fs1.working_country_code,fs1.key_ns, 
        fs1.POSTCODE_LEFT_min_key_ns ,fs1.POSTCODE_LEFT_max_key_ns, fs1.POSTCODE_LEFT_shuffle_key_ns,ora_hash(fs2.POSTCODE_LEFT, 4294967295, 0) POSTCODE_LEFT_shuffle_hash,
        fs2.POSTCODE_LEFT POSTCODE_LEFT_shuffle
        from POSTCODE_LEFT_shuffle1 fs1 join POSTCODE_LEFT_shuffle1 fs2 on fs1.POSTCODE_LEFT_shuffle_key_ns = fs2.key_ns
      ) res;
      
      select max(key_ns) into nMaxKey from postcode_left_shuffle;
      nLoopCounter := 0;
      
      loop
      
        nloopcounter := nloopcounter + 1;
        
        dbms_output.put_line(to_char(nloopcounter));
        
        if nloopcounter > 9 then
          exit;
        end if;
      
        if nloopcounter < 5 then
          update postcode_left_shuffle set postcode_left_shuffle_key_ns = mod(abs(dbms_random.random),(postcode_left_max_key_ns - postcode_left_min_key_ns) + 1  ) + POSTCODE_LEFT_min_key_ns
          where resolved = 'N';
        else
          update postcode_left_shuffle set postcode_left_shuffle_key_ns = mod(abs(dbms_random.random),(nMaxKey - 1) - 1  ) 
          where resolved = 'N';
        end if;
          
        if sql%rowcount = 0 then
          exit;
        end if;
        
        update POSTCODE_LEFT_shuffle fs1 set (fs1.POSTCODE_LEFT_shuffle,fs1.POSTCODE_LEFT_shuffle_hash )= (select fs2.POSTCODE_LEFT, ora_hash(fs2.POSTCODE_LEFT, 4294967295, 0) from POSTCODE_LEFT_shuffle fs2
                                                                     where fs1.POSTCODE_LEFT_shuffle_key_ns = fs2.key_ns)
        where resolved = 'N'  ;                                                         
        
        update POSTCODE_LEFT_shuffle fs1 set fs1.resolved = 'Y'
        where fs1.resolved = 'N' and fs1.POSTCODE_LEFT_hash<>fs1.POSTCODE_LEFT_shuffle_hash;
     end loop;
    
     commit;
     
    exception
       when others then
          g_code := SQLCODE;
          g_errm := SUBSTR(SQLERRM, 1 , 4000);     
          obfus_log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),g_code,g_errm,g_module); 
          RAISE; -- Error will propogate back to anonymise setting p_obfus_status => 'FAILED'        
  end postcode_left_shuffle; 
  
  procedure postcode_right_shuffle is
  
    nMaxKey number;
    nLoopCounter number;
  begin

      g_module := 'postcode_right_shuffle';
        -- POST CODE LEFT
    
       
      execute immediate 'truncate table POSTCODE_RIGHT_shuffle';
      execute immediate 'truncate table POSTCODE_RIGHT_shuffle1';
            
      insert into POSTCODE_RIGHT_shuffle1(HOLDER_ADDRESS_ID ,comp_code,ivc_code ,
      POSTCODE_RIGHT , POSTCODE_RIGHT_hash,working_country_code,key_ns,POSTCODE_RIGHT_min_key_ns ,POSTCODE_RIGHT_max_key_ns,POSTCODE_RIGHT_shuffle_key_ns)
        select  res2.HOLDER_ADDRESS_ID ,res2.comp_code,res2.ivc_code ,
        res2.POSTCODE_RIGHT , res2.POSTCODE_RIGHT_hash,res2.working_country_code,res2.key_ns, 
        res2.POSTCODE_RIGHT_min_key_ns ,res2.POSTCODE_RIGHT_max_key_ns,
        mod(abs(dbms_random.random),(res2.POSTCODE_RIGHT_max_key_ns - res2.POSTCODE_RIGHT_min_key_ns) + 1  )  + POSTCODE_RIGHT_min_key_ns POSTCODE_RIGHT_shuffle_key_ns
        from
        (
          select   
          res1.HOLDER_ADDRESS_ID ,res1.comp_code,res1.ivc_code ,
          res1.POSTCODE_RIGHT , res1.POSTCODE_RIGHT_hash,res1.working_country_code,res1.key_ns,   
          first_value(res1.key_ns) over (partition by res1.working_country_code order by res1.key_ns asc rows unbounded preceding ) POSTCODE_RIGHT_min_key_ns ,
          last_value(res1.key_ns) over (partition by res1.working_country_code  order by res1.key_ns asc rows between  unbounded preceding and unbounded following ) POSTCODE_RIGHT_max_key_ns
          from
          (
              select res.HOLDER_ADDRESS_ID ,res.comp_code,res.ivc_code,
              res.POSTCODE_RIGHT , res.POSTCODE_RIGHT_hash,working_country_code,row_number() over (partition by 1 order by res.working_country_code) key_ns
              from 
              (
                select hn.HOLDER_ADDRESS_ID ,hn.comp_code comp_code,hn.ivc_code ivc_code,
                hn.POSTCODE_RIGHT ,ora_hash(hn.POSTCODE_RIGHT, 4294967295, 0) POSTCODE_RIGHT_hash,
                case when hn.country_code is null then 'GB' else hn.country_code end working_country_code
                from holder_addresses hn 
                where hn.POSTCODE_RIGHT  is not null
              ) res
          )res1
        ) res2;

       execute immediate 'truncate table POSTCODE_RIGHT_shuffle';
  
      insert into POSTCODE_RIGHT_shuffle (HOLDER_ADDRESS_ID ,comp_code,ivc_code ,
      POSTCODE_RIGHT , POSTCODE_RIGHT_hash,working_country_code,key_ns,POSTCODE_RIGHT_min_key_ns ,POSTCODE_RIGHT_max_key_ns,POSTCODE_RIGHT_shuffle_key_ns,POSTCODE_RIGHT_shuffle_hash,
      resolved ,POSTCODE_RIGHT_shuffle)
    
      select res.HOLDER_ADDRESS_ID ,res.comp_code,res.ivc_code ,
      res.POSTCODE_RIGHT , res.POSTCODE_RIGHT_hash,res.working_country_code,res.key_ns, 
      res.POSTCODE_RIGHT_min_key_ns ,res.POSTCODE_RIGHT_max_key_ns, res.POSTCODE_RIGHT_shuffle_key_ns, POSTCODE_RIGHT_shuffle_hash,
      case when res.POSTCODE_RIGHT_hash=res.POSTCODE_RIGHT_shuffle_hash then 'N' else 'Y' end   resolved,
      POSTCODE_RIGHT_shuffle
      from
      (
        select  fs1.HOLDER_ADDRESS_ID ,fs1.comp_code,fs1.ivc_code ,
        fs1.POSTCODE_RIGHT , fs1.POSTCODE_RIGHT_hash,fs1.working_country_code,fs1.key_ns, 
        fs1.POSTCODE_RIGHT_min_key_ns ,fs1.POSTCODE_RIGHT_max_key_ns, fs1.POSTCODE_RIGHT_shuffle_key_ns,ora_hash(fs2.POSTCODE_RIGHT, 4294967295, 0) POSTCODE_RIGHT_shuffle_hash,
        fs2.POSTCODE_RIGHT POSTCODE_RIGHT_shuffle
        from POSTCODE_RIGHT_shuffle1 fs1 join POSTCODE_RIGHT_shuffle1 fs2 on fs1.POSTCODE_RIGHT_shuffle_key_ns = fs2.key_ns
      ) res;
      
      select max(key_ns) into nMaxKey from postcode_right_shuffle;
      nLoopCounter := 0;
      
      loop
      
        nloopcounter := nloopcounter + 1;
        
        dbms_output.put_line(to_char(nloopcounter));
        
        if nloopcounter > 9 then
          exit;
        end if;
      
        if nloopcounter < 5 then
          update postcode_right_shuffle set postcode_right_shuffle_key_ns = mod(abs(dbms_random.random),(postcode_right_max_key_ns - postcode_right_min_key_ns) + 1  )  + POSTCODE_RIGHT_min_key_ns
          where resolved = 'N';
        else
          update postcode_right_shuffle set postcode_right_shuffle_key_ns = mod(abs(dbms_random.random),(nMaxKey - 1) - 1  ) 
          where resolved = 'N';
        end if;
          
        if sql%rowcount = 0 then
          exit;
        end if;
        
        update POSTCODE_RIGHT_shuffle fs1 set (fs1.POSTCODE_RIGHT_shuffle,fs1.POSTCODE_RIGHT_shuffle_hash )= (select fs2.POSTCODE_RIGHT, ora_hash(fs2.POSTCODE_RIGHT, 4294967295, 0) from POSTCODE_RIGHT_shuffle fs2
                                                                     where fs1.POSTCODE_RIGHT_shuffle_key_ns = fs2.key_ns)
        where resolved = 'N'  ;                                                         
        
        update POSTCODE_RIGHT_shuffle fs1 set fs1.resolved = 'Y'
        where fs1.resolved = 'N' and fs1.POSTCODE_RIGHT_hash<>fs1.POSTCODE_RIGHT_shuffle_hash;
     end loop;

    commit; 

    exception
       when others then
          g_code := SQLCODE;
          g_errm := SUBSTR(SQLERRM, 1 , 4000);     
          obfus_log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),g_code,g_errm,g_module); 
          RAISE; -- Error will propogate back to anonymise setting p_obfus_status => 'FAILED'       
  end postcode_right_shuffle; 
  
  procedure irish_dist_code_shuffle is
    nMaxKey number;
    nLoopCounter number;
  begin

      g_module := 'irish_dist_code_shuffle';
      
        -- POST CODE LEFT
    
      execute immediate 'truncate table IRISH_DIST_CODE_shuffle';
      execute immediate 'truncate table IRISH_DIST_CODE_shuffle1';
            
      insert into IRISH_DIST_CODE_shuffle1(HOLDER_ADDRESS_ID ,comp_code,ivc_code ,
      IRISH_DIST_CODE , IRISH_DIST_CODE_hash,working_country_code,key_ns,IRISH_DIST_CODE_min_key_ns ,IRISH_DIST_CODE_max_key_ns,IRISH_DIST_CODE_shuffle_key_ns)
        select  res2.HOLDER_ADDRESS_ID ,res2.comp_code,res2.ivc_code ,
        res2.IRISH_DISTRIBUTION_CODE , res2.IRISH_DIST_CODE_hash,res2.working_country_code,res2.key_ns, 
        res2.IRISH_DIST_CODE_min_key_ns ,res2.IRISH_DIST_CODE_max_key_ns,
        mod(abs(dbms_random.random),(res2.IRISH_DIST_CODE_max_key_ns - res2.IRISH_DIST_CODE_min_key_ns) - 1  )  + IRISH_DIST_CODE_min_key_ns IRISH_DIST_CODE_shuffle_key_ns
        from
        (
          select   
          res1.HOLDER_ADDRESS_ID ,res1.comp_code,res1.ivc_code ,
          res1.IRISH_DISTRIBUTION_CODE , res1.IRISH_DIST_CODE_hash,res1.working_country_code,res1.key_ns,   
          first_value(res1.key_ns) over (partition by res1.working_country_code order by res1.key_ns asc rows unbounded preceding ) IRISH_DIST_CODE_min_key_ns ,
          last_value(res1.key_ns) over (partition by res1.working_country_code  order by res1.key_ns asc rows between  unbounded preceding and unbounded following ) IRISH_DIST_CODE_max_key_ns
          from
          (
              select res.HOLDER_ADDRESS_ID ,res.comp_code,res.ivc_code,
              res.IRISH_DISTRIBUTION_CODE , res.IRISH_DIST_CODE_hash,working_country_code,row_number() over (partition by 1 order by res.working_country_code) key_ns
              from 
              (
                select hn.HOLDER_ADDRESS_ID ,hn.comp_code comp_code,hn.ivc_code ivc_code,
                hn.IRISH_DISTRIBUTION_CODE ,ora_hash(hn.IRISH_DISTRIBUTION_CODE, 4294967295, 0) IRISH_DIST_CODE_hash,
                case when hn.country_code is null then 'RI' else hn.country_code end working_country_code
                from holder_addresses hn 
                where hn.IRISH_DISTRIBUTION_CODE  is not null
              ) res
          )res1
        ) res2;

       execute immediate 'truncate table IRISH_DIST_CODE_shuffle';
  
      insert into IRISH_DIST_CODE_shuffle (HOLDER_ADDRESS_ID ,comp_code,ivc_code ,
      IRISH_DIST_CODE , IRISH_DIST_CODE_hash,working_country_code,key_ns,IRISH_DIST_CODE_min_key_ns ,IRISH_DIST_CODE_max_key_ns,IRISH_DIST_CODE_shuffle_key_ns,IRISH_DIST_CODE_shuffle_hash,
      resolved ,IRISH_DIST_CODE_shuffle)
    
      select res.HOLDER_ADDRESS_ID ,res.comp_code,res.ivc_code ,
      res.IRISH_DIST_CODE , res.IRISH_DIST_CODE_hash,res.working_country_code,res.key_ns, 
      res.IRISH_DIST_CODE_min_key_ns ,res.IRISH_DIST_CODE_max_key_ns, res.IRISH_DIST_CODE_shuffle_key_ns, IRISH_DIST_CODE_shuffle_hash,
      case when res.IRISH_DIST_CODE_hash=res.IRISH_DIST_CODE_shuffle_hash then 'N' else 'Y' end   resolved,
      IRISH_DIST_CODE_shuffle
      from
      (
        select  fs1.HOLDER_ADDRESS_ID ,fs1.comp_code,fs1.ivc_code ,
        fs1.IRISH_DIST_CODE , fs1.IRISH_DIST_CODE_hash,fs1.working_country_code,fs1.key_ns, 
        fs1.IRISH_DIST_CODE_min_key_ns ,fs1.IRISH_DIST_CODE_max_key_ns, fs1.IRISH_DIST_CODE_shuffle_key_ns,ora_hash(fs2.IRISH_DIST_CODE, 4294967295, 0) IRISH_DIST_CODE_shuffle_hash,
        fs2.IRISH_DIST_CODE IRISH_DIST_CODE_shuffle
        from IRISH_DIST_CODE_shuffle1 fs1 join IRISH_DIST_CODE_shuffle1 fs2 on fs1.IRISH_DIST_CODE_shuffle_key_ns = fs2.key_ns
      ) res;
      
      select max(key_ns) into nMaxKey from irish_dist_code_shuffle;
      nLoopCounter := 0;
      
      loop
      
        nloopcounter := nloopcounter + 1;
        
        dbms_output.put_line(to_char(nloopcounter));
        
        if nloopcounter > 9 then
          exit;
        end if;
      
        if nloopcounter < 5 then
          update irish_dist_code_shuffle set irish_dist_code_shuffle_key_ns = mod(abs(dbms_random.random),(irish_dist_code_max_key_ns - irish_dist_code_min_key_ns) + 1  ) + IRISH_DIST_CODE_min_key_ns
          where resolved = 'N';
        else
          update irish_dist_code_shuffle set irish_dist_code_shuffle_key_ns = mod(abs(dbms_random.random),(nMaxKey - 1) - 1  ) 
          where resolved = 'N';
        end if;
          
        if sql%rowcount = 0 then
          exit;
        end if;
        
        update IRISH_DIST_CODE_shuffle fs1 set (fs1.IRISH_DIST_CODE_shuffle,fs1.IRISH_DIST_CODE_shuffle_hash )= (select fs2.IRISH_DIST_CODE, ora_hash(fs2.IRISH_DIST_CODE, 4294967295, 0) from IRISH_DIST_CODE_shuffle fs2
                                                                     where fs1.IRISH_DIST_CODE_shuffle_key_ns = fs2.key_ns)
        where resolved = 'N'  ;                                                         
        
        update IRISH_DIST_CODE_shuffle fs1 set fs1.resolved = 'Y'
        where fs1.resolved = 'N' and fs1.IRISH_DIST_CODE_hash<>fs1.IRISH_DIST_CODE_shuffle_hash;
     end loop;
     
    commit; 
    
  exception
     when others then
        g_code := SQLCODE;
        g_errm := SUBSTR(SQLERRM, 1 , 4000);     
        obfus_log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),g_code,g_errm,g_module); 
        RAISE; -- Error will propogate back to anonymise setting p_obfus_status => 'FAILED'        
  end irish_dist_code_shuffle; 
  

  procedure anon_holder_addresses
  is
     v_ncount number;
     nMaxKey number;
         
     nLoopCounter number;
   begin
    
      g_module := 'anon_holder_addresses';      
--      execute immediate 'create or replace synonym holder_addresses  for '|| g_src_prefix||'_PRISM_CORE.holder_addresses';  
      
      address_line1_shuffle;
      address_line2_shuffle;
      address_line3_shuffle;
      address_line456_shuffle;
      postcode_left_shuffle;
      postcode_right_shuffle;
      irish_dist_code_shuffle;
      
      begin
        execute immediate 'drop table holder_addresses_tmp1';
        exception when others then null;
      end;
        
      execute immediate  'create table holder_addresses_tmp1 as select * from holder_addresses where rownum <1';
          
      insert into holder_addresses_tmp1(
      holder_address_id,comp_code,ivc_code,address_type_id,correspondence_yn,address_line1,address_line2,
      address_line3,address_line4,address_line5,address_line6,postcode_left,postcode_right,country_code,
      irish_distribution_code,created_by,created_date,modified_by,mod_timestamp,posting_prism_task_id,
      posting_date,posting_batch_id,posting_transaction_id)
      
      select ha.holder_address_id,ha.comp_code,ha.ivc_code,ha.address_type_id,ha.correspondence_yn,
      als1.address_line1_shuffle,als2.address_line2_shuffle,als3.address_line3_shuffle,als4.address_line4_shuffle,
      als4.address_line5_shuffle,als4.address_line6_shuffle,
      pcl.postcode_left_shuffle,pcr.postcode_right_shuffle,ha.country_code,idc.irish_dist_code_shuffle,ha.created_by,
      ha.created_date,ha.modified_by,ha.mod_timestamp,ha.posting_prism_task_id,ha.posting_date,ha.posting_batch_id,
      ha.posting_transaction_id from holder_addresses ha 
      left join address_line1_shuffle als1 on als1.holder_address_id = ha.holder_address_id
      left join address_line2_shuffle als2 on  als2.holder_address_id = ha.holder_address_id
      left join address_line3_shuffle als3 on  als3.holder_address_id = ha.holder_address_id
      left join address_line456_shuffle als4 on  als4.holder_address_id = ha.holder_address_id
      left join postcode_left_shuffle pcl on  pcl.holder_address_id = ha.holder_address_id
      left join postcode_right_shuffle pcr on pcr.holder_address_id = ha.holder_address_id
      left join irish_dist_code_shuffle idc on  idc.holder_address_id = ha.holder_address_id;
--      execute immediate 'create or replace synonym holder_addresses  for '|| g_src_prefix||'_PRISM_CORE.holder_addresses';    
--      
--      execute immediate 'create or replace synonym holder_addresses  for holder_addresses_tmp1';     
            
--      insert into manual_transform (owner,table_name, actual_col,trans_function,technique)  
--  
--      select 'PRISM_CORE','HOLDER_ADDRESSES','ADDRESS_LINE1','shuffle(address_line1)', 'SHUFFLE_CNTRY' from dual union all
--      select 'PRISM_CORE','HOLDER_ADDRESSES','ADDRESS_LINE2','shuffle(address_line2)', 'SHUFFLE_CNTRY' from dual union all
--      select 'PRISM_CORE','HOLDER_ADDRESSES','ADDRESS_LINE3','shuffle(address_line3)', 'SHUFFLE_CNTRY' from dual union all
--      select 'PRISM_CORE','HOLDER_ADDRESSES','ADDRESS_LINE4','shuffle(address_line4)', 'SHUFFLE_CNTRY' from dual union all
--      select 'PRISM_CORE','HOLDER_ADDRESSES','ADDRESS_LINE5','shuffle(address_line5)', 'SHUFFLE_CNTRY' from dual union all
--      select 'PRISM_CORE','HOLDER_ADDRESSES','ADDRESS_LINE6','shuffle(address_line6)', 'SHUFFLE_CNTRY' from dual union all
--      select 'PRISM_CORE','HOLDER_ADDRESSES','POSTCODE_LEFT','shuffle(postcode_left)', 'SHUFFLE_CNTRY' from dual union all
--      select 'PRISM_CORE','HOLDER_ADDRESSES','POSTCODE_RIGHT','shuffle(postcode_right)', 'SHUFFLE_CNTRY' from dual union all
--      select 'PRISM_CORE','HOLDER_ADDRESSES','IRISH_DISTRIBUTION_CODE','shuffle(irish_distribution_code)', 'SHUFFLE_CNTRY' from dual ;
--         
      commit;
    
   exception
      when others then
          g_code := SQLCODE;
          g_errm := SUBSTR(SQLERRM, 1 , 4000);     
          obfus_log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),g_code,g_errm,g_module); 
          RAISE; -- Error will propogate back to anonymise setting p_obfus_status => 'FAILED'        
   end anon_holder_addresses;


procedure anon_bank_branches
   is
         v_ncount number;
         nMaxKey number;
         
        nLoopCounter number;
    begin
    
      g_module := 'anon_bank_branches';    
      
      execute immediate 'truncate  table bank_branches_addr_lu2';
      execute immediate 'truncate  table bank_branches_addr_lu1';
      execute immediate 'truncate  table bank_branches_addr_lu';

      insert into bank_branches_addr_lu2(num_addr_key,addr_key,address_line1,
              address_line2,address_line3,address_line4,address_line5,address_line6,postcode_left,postcode_right)
      select row_number() over (order by 1) num_addr_key,RPAD(ADDRESS_LINE1,35, '*') ||RPAD(ADDRESS_LINE2,35, '*')||RPAD(ADDRESS_LINE3,35, '*')||RPAD(ADDRESS_LINE4,35, '*')||RPAD(ADDRESS_LINE5,35, '*')||RPAD(ADDRESS_LINE6,35, '*')||RPAD(POSTCODE_LEFT,4, '*')||RPAD(POSTCODE_RIGHT,3, '*')  ,address_line1,
              address_line2,address_line3,address_line4,address_line5,address_line6,postcode_left,postcode_right
      from  bank_branches_tmp2
      group by addr_key,address_line1, address_line2,address_line3,address_line4,address_line5,address_line6,postcode_left,postcode_right;

      insert into   bank_branches_addr_lu1 (addr_key ,num_addr_key,min_addr_key_ns , max_addr_key_ns , 
                    address_line1,address_line2,address_line3,address_line4,address_line5,address_line6,
                    postcode_left,postcode_right,
                    address_line1_hash,address_line2_hash,address_line3_hash,address_line4_hash,address_line5_hash,address_line6_hash,
                    postcode_left_hash,postcode_right_hash,address_line1_shuffle_key,address_line2_shuffle_key, address_line3_shuffle_key,
                    address_line4_shuffle_key,address_line5_shuffle_key, address_line6_shuffle_key, postcode_left_shuffle_key, postcode_right_shuffle_key)
      select res.addr_key ,num_addr_key,min_addr_key_ns , max_addr_key_ns , 
             address_line1,address_line2,address_line3,address_line4,address_line5,address_line6,
             postcode_left,postcode_right,
             address_line1_hash,address_line2_hash,address_line3_hash,address_line4_hash,address_line5_hash,address_line6_hash,
             postcode_left_hash,postcode_right_hash,
             mod(abs(dbms_random.random), (max_addr_key_ns - min_addr_key_ns) +1) +1 address_line1_shuffle_key,
             mod(abs(dbms_random.random), (max_addr_key_ns - min_addr_key_ns) +1) +1 address_line2_shuffle_key,
             mod(abs(dbms_random.random), (max_addr_key_ns - min_addr_key_ns) +1) +1 address_line3_shuffle_key,
             mod(abs(dbms_random.random), (max_addr_key_ns - min_addr_key_ns) +1) +1 address_line4_shuffle_key,
             mod(abs(dbms_random.random), (max_addr_key_ns - min_addr_key_ns) +1) +1 address_line5_shuffle_key,
             mod(abs(dbms_random.random), (max_addr_key_ns - min_addr_key_ns) +1) +1 address_line6_shuffle_key,
             mod(abs(dbms_random.random), (max_addr_key_ns - min_addr_key_ns) +1) +1 postcode_left_shuffle_key,
             mod(abs(dbms_random.random), (max_addr_key_ns - min_addr_key_ns) +1) +1 postcode_right_shuffle_key
      from
      ( select bblu.addr_key ,num_addr_key,
        first_value(bblu.num_addr_key) over ( partition by 1 order by bblu.num_addr_key asc rows unbounded preceding ) min_addr_key_ns ,
        last_value(bblu.num_addr_key) over (partition by 1  order by bblu.num_addr_key asc rows between  unbounded preceding and unbounded following ) max_addr_key_ns,   
        bblu.address_line1 ,ora_hash(bblu.address_line1, 4294967295, 0) address_line1_hash,
        bblu.address_line2 ,ora_hash(bblu.address_line2, 4294967295, 0) address_line2_hash,
        bblu.address_line3 ,ora_hash(bblu.address_line3, 4294967295, 0) address_line3_hash,
        bblu.address_line4 ,ora_hash(bblu.address_line4, 4294967295, 0) address_line4_hash,
        bblu.address_line5 ,ora_hash(bblu.address_line5, 4294967295, 0) address_line5_hash,
        bblu.address_line6 ,ora_hash(bblu.address_line6, 4294967295, 0) address_line6_hash,
        bblu.postcode_left,ora_hash(bblu.postcode_left, 4294967295, 0) postcode_left_hash,
        bblu.postcode_right ,ora_hash(bblu.postcode_right, 4294967295, 0) postcode_right_hash
        from bank_branches_addr_lu2 bblu
      ) res;

      insert into  bank_branches_addr_lu(
      addr_key,num_addr_key,min_addr_key_ns,max_addr_key_ns,address_line1, 
      address_line2,address_line3, address_line4, address_line5,address_line6,postcode_left, postcode_right, 
      address_line1_hash,address_line2_hash,address_line3_hash,address_line4_hash,address_line5_hash,address_line6_hash,           
      postcode_left_hash,postcode_right_hash,           
      address_line1_shuffle_key,address_line2_shuffle_key,            
      address_line3_shuffle_key,address_line4_shuffle_key,address_line5_shuffle_key,address_line6_shuffle_key,           
      postcode_left_shuffle_key,postcode_right_shuffle_key,address_line1_shuffle,
      address_line2_shuffle,address_line3_shuffle,address_line4_shuffle,address_line5_shuffle,
      address_line6_shuffle,postcode_left_shuffle,postcode_right_shuffle,
      address_line1_shuffle_hash,address_line2_shuffle_hash,          
      address_line3_shuffle_hash, address_line4_shuffle_hash,address_line5_shuffle_hash,address_line6_shuffle_hash,
      postcode_left_shuffle_hash,postcode_right_shuffle_hash)

      select bbal1.addr_key ,  bbal1.num_addr_key  ,  bbal1.min_addr_key_ns , bbal1.max_addr_key_ns ,          
      bbal1.address_line1 , bbal1.address_line2,bbal1.address_line3, bbal1.address_line4, bbal1.address_line5, 
      bbal1.address_line6, bbal1.postcode_left ,
      bbal1.postcode_right, bbal1.address_line1_hash, bbal1.address_line2_hash, bbal1.address_line3_hash, bbal1.address_line4_hash, 
      bbal1.address_line5_hash,bbal1.address_line6_hash,bbal1.postcode_left_hash,bbal1.postcode_right_hash,          
      bbal1.address_line1_shuffle_key, bbal1.address_line2_shuffle_key,bbal1.address_line3_shuffle_key,
      bbal1.address_line4_shuffle_key,bbal1.address_line5_shuffle_key,bbal1.address_line6_shuffle_key,          
      bbal1.postcode_left_shuffle_key,           
      bbal1.postcode_right_shuffle_key , 
      bbal1_ad1.address_line1 address_line1_shuffle,
      bbal1_ad2.address_line2 address_line2_shuffle,
      bbal1_ad3.address_line3 address_line3_shuffle,
      bbal1_ad4.address_line4 address_line4_shuffle,
      bbal1_ad5.address_line5 address_line5_shuffle,
      bbal1_ad6.address_line6 address_line6_shuffle,
      bbal1_pcl.postcode_left postcode_left_shuffle,
      bbal1_pcr.postcode_right postcode_right_shuffle,
      ora_hash(bbal1_ad1.address_line1, 4294967295, 0) address_line1_shuffle_hash,
      ora_hash(bbal1_ad2.address_line2, 4294967295, 0) address_line2_shuffle_hash,
      ora_hash(bbal1_ad3.address_line3, 4294967295, 0) address_line3_shuffle_hash,
      ora_hash(bbal1_ad4.address_line4, 4294967295, 0) address_line4_shuffle_hash,
      ora_hash(bbal1_ad5.address_line5, 4294967295, 0) address_line5_shuffle_hash,
      ora_hash(bbal1_ad6.address_line6 , 4294967295, 0) address_line6_shuffle_hash,
      ora_hash(bbal1_pcl.postcode_left , 4294967295, 0) postcode_left_shuffle_hash,
      ora_hash(bbal1_pcr.postcode_right  , 4294967295, 0) postcode_right_shuffle_hash 
      from 
      bank_branches_addr_lu1 bbal1 
      join bank_branches_addr_lu1 bbal1_ad1  on bbal1.address_line1_shuffle_key =  bbal1_ad1.num_addr_key
      join bank_branches_addr_lu1 bbal1_ad2  on bbal1.address_line2_shuffle_key =  bbal1_ad2.num_addr_key
      join bank_branches_addr_lu1 bbal1_ad3  on bbal1.address_line3_shuffle_key =  bbal1_ad3.num_addr_key
      join bank_branches_addr_lu1 bbal1_ad4  on bbal1.address_line4_shuffle_key =  bbal1_ad4.num_addr_key
      join bank_branches_addr_lu1 bbal1_ad5  on bbal1.address_line5_shuffle_key =  bbal1_ad5.num_addr_key
      join bank_branches_addr_lu1 bbal1_ad6  on bbal1.address_line6_shuffle_key =  bbal1_ad6.num_addr_key
      join bank_branches_addr_lu1 bbal1_pcl   on bbal1.postcode_left_shuffle_key =  bbal1_pcl.num_addr_key
      join bank_branches_addr_lu1 bbal1_pcr   on bbal1.postcode_right_shuffle_key =  bbal1_pcr.num_addr_key;

      commit;

      dbms_stats.gather_table_stats(ownname=>user,tabname =>'bank_branches_addr_lu',estimate_percent=>100);
       
      execute immediate 'truncate table bank_branches_addr_lu_adjust';
      
      insert into bank_branches_addr_lu_adjust(addr_key,num_addr_key,min_addr_key_ns,
                                                  max_addr_key_ns,address_line1,address_line2,address_line3,address_line4,address_line5,
                                                  address_line6,postcode_left,postcode_right,address_line1_hash,address_line2_hash,address_line3_hash,
                                                  address_line4_hash,address_line5_hash,address_line6_hash,postcode_left_hash,postcode_right_hash,
                                                  address_line1_shuffle_key,address_line2_shuffle_key,address_line3_shuffle_key,address_line4_shuffle_key,
                                                  address_line5_shuffle_key,address_line6_shuffle_key,postcode_left_shuffle_key,postcode_right_shuffle_key,
                                                  address_line1_shuffle,address_line2_shuffle,address_line3_shuffle,address_line4_shuffle,address_line5_shuffle,
                                                  address_line6_shuffle,postcode_left_shuffle,postcode_right_shuffle,address_line1_shuffle_hash,address_line2_shuffle_hash,
                                                  address_line3_shuffle_hash,address_line4_shuffle_hash,address_line5_shuffle_hash,address_line6_shuffle_hash,
                                                  postcode_left_shuffle_hash,postcode_right_shuffle_hash,resolved)
      select addr_key,num_addr_key,min_addr_key_ns,
                                                  max_addr_key_ns,address_line1,address_line2,address_line3,address_line4,address_line5,
                                                  address_line6,postcode_left,postcode_right,address_line1_hash,address_line2_hash,address_line3_hash,
                                                  address_line4_hash,address_line5_hash,address_line6_hash,postcode_left_hash,postcode_right_hash,
                                
                                                  
                                                  mod(abs(dbms_random.random), (max_addr_key_ns - min_addr_key_ns) +1) +1 address_line1_shuffle_key,
                                                  mod(abs(dbms_random.random), (max_addr_key_ns - min_addr_key_ns) +1) +1 address_line2_shuffle_key,
                                                  mod(abs(dbms_random.random), (max_addr_key_ns - min_addr_key_ns) +1) +1 address_line3_shuffle_key,
                                                  mod(abs(dbms_random.random), (max_addr_key_ns - min_addr_key_ns) +1) +1 address_line4_shuffle_key,
                                                  mod(abs(dbms_random.random), (max_addr_key_ns - min_addr_key_ns) +1) +1 address_line5_shuffle_key,
                                                  mod(abs(dbms_random.random), (max_addr_key_ns - min_addr_key_ns) +1) +1 address_line6_shuffle_key,
                                                  mod(abs(dbms_random.random), (max_addr_key_ns - min_addr_key_ns) +1) +1 postcode_left_shuffle_key,
                                                  mod(abs(dbms_random.random), (max_addr_key_ns - min_addr_key_ns) +1) +1 postcode_right_shuffle_key   ,                
                                                  
                                                  address_line1_shuffle,address_line2_shuffle,address_line3_shuffle,address_line4_shuffle,address_line5_shuffle,
                                                  address_line6_shuffle,postcode_left_shuffle,postcode_right_shuffle,address_line1_shuffle_hash,address_line2_shuffle_hash,
                                                  address_line3_shuffle_hash,address_line4_shuffle_hash,address_line5_shuffle_hash,address_line6_shuffle_hash,
                                                  postcode_left_shuffle_hash,postcode_right_shuffle_hash,'N'
      from bank_branches_addr_lu base
          where (base.address_line1_shuffle_hash is not null and base.address_line1_hash = base.address_line1_shuffle_hash) or 
                (base.address_line2_shuffle_hash is not null and base.address_line2_hash = base.address_line2_shuffle_hash) or
                (base.address_line3_shuffle_hash is not null and base.address_line3_hash = base.address_line3_shuffle_hash) or
                (base.address_line4_shuffle_hash is not null and base.address_line4_hash = base.address_line4_shuffle_hash) or
                (base.address_line5_shuffle_hash is not null and base.address_line5_hash = base.address_line5_shuffle_hash) or
                (base.address_line6_shuffle_hash is not null and base.address_line6_hash = base.address_line6_shuffle_hash) or
                (base.postcode_left_shuffle_hash is not null and base.postcode_left_hash = base.postcode_left_shuffle_hash) or
                (base.postcode_right_shuffle_hash is not null and base.postcode_right_hash = base.postcode_right_shuffle_hash) ;


        commit;
        
        select max(num_addr_key) into nMaxKey from bank_branches_addr_lu_adjust ;
        
        dbms_stats.gather_table_stats(ownname=>user,tabname =>'BANK_BRANCHES_ADDR_LU',estimate_percent=>100); 
        
        nloopcounter := 0;
        loop          
          update bank_branches_addr_lu_adjust haa set 
              address_line1_shuffle = (select address_line1 from  bank_branches_addr_lu habf where haa.address_line1_shuffle_key = habf.num_addr_key),
              address_line2_shuffle = (select address_line2 from  bank_branches_addr_lu habf where haa.address_line2_shuffle_key = habf.num_addr_key),
              address_line3_shuffle = (select address_line3 from  bank_branches_addr_lu habf where haa.address_line3_shuffle_key = habf.num_addr_key),
              address_line4_shuffle = (select address_line4 from  bank_branches_addr_lu habf where haa.address_line4_shuffle_key = habf.num_addr_key),
              address_line5_shuffle = (select address_line5 from  bank_branches_addr_lu habf where haa.address_line5_shuffle_key = habf.num_addr_key),
              address_line6_shuffle = (select address_line6 from  bank_branches_addr_lu habf where haa.address_line6_shuffle_key = habf.num_addr_key),
              postcode_left_shuffle = (select postcode_left from  bank_branches_addr_lu habf where haa.postcode_left_shuffle_key = habf.num_addr_key),
              postcode_right_shuffle = (select postcode_right from  bank_branches_addr_lu habf where haa.postcode_right_shuffle_key = habf.num_addr_key)
              
                                           
              where resolved = 'N';
        
          commit;

          update bank_branches_addr_lu_adjust set  address_line1_shuffle_hash = ora_hash(address_line1_shuffle, 4294967295, 0) , 
                                              address_line2_shuffle_hash = ora_hash(address_line2_shuffle, 4294967295, 0) , 
                                              address_line3_shuffle_hash = ora_hash(address_line3_shuffle, 4294967295, 0) , 
                                              address_line4_shuffle_hash = ora_hash(address_line4_shuffle, 4294967295, 0) , 
                                              address_line5_shuffle_hash = ora_hash(address_line5_shuffle, 4294967295, 0) , 
                                              address_line6_shuffle_hash = ora_hash(address_line6_shuffle, 4294967295, 0) ,
                                              postcode_left_shuffle_hash = ora_hash(postcode_left_shuffle, 4294967295, 0) ,
                                              postcode_right_shuffle_hash = ora_hash(postcode_right_shuffle, 4294967295, 0) 
                                                              where resolved = 'N';
          
          commit;
  
          update bank_branches_addr_lu_adjust set resolved = 'Y'    where resolved = 'N' and nvl(address_line1_hash,-1)  <>  nvl(address_line1_shuffle_hash,-1) 
                                                            and nvl(address_line2_hash,-1)  <>  nvl(address_line2_shuffle_hash,-2)
                                                            and nvl(address_line3_hash,-1)  <>  nvl(address_line3_shuffle_hash,-2)
                                                            and nvl(address_line4_hash,-1)  <>  nvl(address_line4_shuffle_hash,-2)
                                                            and nvl(address_line5_hash,-1)  <>  nvl(address_line5_shuffle_hash,-2)
                                                            and nvl(address_line6_hash,-1)  <>  nvl(address_line6_shuffle_hash,-2)
                                                            and nvl(postcode_left_hash,-1)  <>  nvl(postcode_left_shuffle_hash,-2)
                                                            and nvl(postcode_right_hash,-1)  <>  nvl(postcode_right_shuffle_hash,-2);
  
          commit;
    
          nloopcounter := nloopcounter + 1;
          if v_ncount = 0  or nloopcounter > 10 then
            exit;
          end if;
          
         if nloopcounter > 5 then
         
            update  bank_branches_addr_lu_adjust  set 
                       
            address_line1_shuffle_key = mod(abs(dbms_random.random), (max_addr_key_ns - min_addr_key_ns) +1) +1 ,
            address_line2_shuffle_key = mod(abs(dbms_random.random), (max_addr_key_ns - min_addr_key_ns) +1) +1 ,
            address_line3_shuffle_key = mod(abs(dbms_random.random), (max_addr_key_ns - min_addr_key_ns) +1) +1 ,
            address_line4_shuffle_key = mod(abs(dbms_random.random), (max_addr_key_ns - min_addr_key_ns) +1) +1 ,
            address_line5_shuffle_key = mod(abs(dbms_random.random), (max_addr_key_ns - min_addr_key_ns) +1) +1 ,
            address_line6_shuffle_key = mod(abs(dbms_random.random), (max_addr_key_ns - min_addr_key_ns) +1) +1 ,
            postcode_left_shuffle_key  =mod(abs(dbms_random.random), (max_addr_key_ns - min_addr_key_ns) +1) +1 ,
            postcode_right_shuffle_key  = mod(abs(dbms_random.random), (max_addr_key_ns - min_addr_key_ns) +1) +1  
            where resolved = 'N';
         else
            update bank_branches_addr_lu_adjust haa set 
            address_line1_shuffle_key = mod(abs(dbms_random.random), (nmaxkey - 1)  +1) +1 ,
            address_line2_shuffle_key = mod(abs(dbms_random.random), (nmaxkey - 1)  +1) +1 ,
            address_line3_shuffle_key = mod(abs(dbms_random.random), (nmaxkey - 1)  +1) +1 ,
            address_line4_shuffle_key = mod(abs(dbms_random.random), (nmaxkey - 1)  +1) +1 ,
            address_line5_shuffle_key = mod(abs(dbms_random.random), (nmaxkey - 1)  +1) +1 ,
            address_line6_shuffle_key = mod(abs(dbms_random.random), (nmaxkey - 1)  +1) +1 ,
            postcode_left_shuffle_key  =mod(abs(dbms_random.random), (nmaxkey - 1)  +1) +1 ,
            postcode_right_shuffle_key  = mod(abs(dbms_random.random), (nmaxkey - 1)  +1) +1 
            where resolved = 'N';
        end if;
          
        commit;
      end loop; 
      
      begin
        execute immediate 'drop table bank_branches_tmp1';
        exception when others then null;
      end;
        
      execute immediate  'create table bank_branches_tmp1 as select * from bank_branches where rownum <1';
          
          
      insert into  bank_branches_tmp1(bank_sort_code ,finance_org_id,branch_name,closure_date,status ,                  
      address_line1,address_line2,address_line3,address_line4,address_line5,address_line6,postcode_left,                  
      postcode_right, country_code,created_by,created_date,modified_by,mod_timestamp)
      select  bb.bank_sort_code ,bb.finance_org_id,bb.branch_name,bb.closure_date,bb.status,
      CASE WHEN NVL(bbala.resolved,'X') = 'Y' THEN bbala.address_line1_shuffle else bbal.address_line1_shuffle end address_line1,
      CASE WHEN NVL(bbala.resolved,'X') = 'Y' THEN bbala.address_line2_shuffle else bbal.address_line2_shuffle end  address_line2 ,
      CASE WHEN NVL(bbala.resolved,'X') = 'Y' THEN bbala.address_line3_shuffle else bbal.address_line3_shuffle  end address_line3_suffle ,
      CASE WHEN NVL(bbala.resolved,'X') = 'Y' THEN bbala.address_line4_shuffle else bbal.address_line4_shuffle  end address_line4,
      CASE WHEN NVL(bbala.resolved,'X') = 'Y' THEN bbala.address_line5_shuffle else bbal.address_line5_shuffle end address_line5,
      CASE WHEN NVL(bbala.resolved,'X') = 'Y' THEN bbala.address_line6_shuffle else bbal.address_line6_shuffle end address_line6 ,
      CASE WHEN NVL(bbala.resolved,'X') = 'Y' THEN bbala.postcode_left_shuffle else  bbal.postcode_left_shuffle end postcode_left,
      CASE WHEN NVL(bbala.resolved,'X') = 'Y' THEN bbala.postcode_right_shuffle  else bbal.postcode_right_shuffle end postcode_right,
      bb.country_code,                  
      bb.created_by,bb.created_date,bb.modified_by,bb.mod_timestamp
      from bank_branches bb 
      left join bank_branches_addr_lu bbal
      on RPAD(bb.ADDRESS_LINE1,35, '*') ||RPAD(bb.ADDRESS_LINE2,35, '*')||RPAD(bb.ADDRESS_LINE3,35, '*')||RPAD(bb.ADDRESS_LINE4,35, '*')||RPAD(bb.ADDRESS_LINE5,35, '*')||RPAD(bb.ADDRESS_LINE6,35, '*')||RPAD(bb.POSTCODE_LEFT,4, '*')||RPAD(bb.POSTCODE_RIGHT,3, '*') = bbal.ADDR_KEY 
      left join bank_branches_addr_lu_adjust bbala
      on RPAD(bb.ADDRESS_LINE1,35, '*') ||RPAD(bb.ADDRESS_LINE2,35, '*')||RPAD(bb.ADDRESS_LINE3,35, '*')||RPAD(bb.ADDRESS_LINE4,35, '*')||RPAD(bb.ADDRESS_LINE5,35, '*')||RPAD(bb.ADDRESS_LINE6,35, '*')||RPAD(bb.POSTCODE_LEFT,4, '*')||RPAD(bb.POSTCODE_RIGHT,3, '*') = bbala.ADDR_KEY ;
--      execute immediate 'create or replace synonym bank_branches for bank_branches_tmp1';     
            
--      insert into manual_transform (owner,table_name, actual_col,trans_function,technique)  
--
--      select 'PRISM_CORE','BANK_BRANCHES','ADDRESS_LINE1','shuffle(address_line1)', 'SHUFFLE_BANK_ADDR' from dual union all
--      select 'PRISM_CORE','BANK_BRANCHES','ADDRESS_LINE2','shuffle(address_line2)', 'SHUFFLE_BANK_ADDR' from dual union all
--      select 'PRISM_CORE','BANK_BRANCHES','ADDRESS_LINE3','shuffle(address_line3)', 'SHUFFLE_BANK_ADDR' from dual union all
--      select 'PRISM_CORE','BANK_BRANCHES','ADDRESS_LINE4','shuffle(address_line4)', 'SHUFFLE_BANK_ADDR' from dual union all
--      select 'PRISM_CORE','BANK_BRANCHES','ADDRESS_LINE5','shuffle(address_line5)', 'SHUFFLE_BANK_ADDR' from dual union all
--      select 'PRISM_CORE','BANK_BRANCHES','ADDRESS_LINE6','shuffle(address_line6)', 'SHUFFLE_BANK_ADDR' from dual union all
--      select 'PRISM_CORE','BANK_BRANCHES','POSTCODE_LEFT','shuffle(postcode_left)', 'SHUFFLE_BANK_ADDR' from dual union all
--      select 'PRISM_CORE','BANK_BRANCHES','POSTCODE_RIGHT','shuffle(postcode_right)', 'SHUFFLE_BANK_ADDR'  from dual ;

    commit;

  exception
     when others then
        g_code := SQLCODE;
        g_errm := SUBSTR(SQLERRM, 1 , 4000);     
        obfus_log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),g_code,g_errm,g_module);         
        RAISE; -- Error will propogate back to anonymise setting p_obfus_status => 'FAILED'
  end anon_bank_branches;

  procedure anon_holder_mandates is
  begin

    g_module := 'anon_holder_mandates';     
    
    begin
      execute immediate 'drop table rnd_disp_mandate_adresss_lu';        
      exception when others then null;
    end;
     
    dbms_output.put_line('6'); 
    obfus_log('1 '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),g_code,g_errm,g_module);
    execute immediate 'create table  rnd_disp_mandate_adresss_lu (holder_mandate_id number,address_line1_key number,
    address_line2_key  number,address_line3_key  number,post_code_key number)';
        
            dbms_output.put_line('7');  


    insert into rnd_disp_mandate_adresss_lu (holder_mandate_id,address_line1_key ,address_line2_key,address_line3_key,post_code_key) 
    select holder_mandate_id,
           mod(abs(dbms_random.random), g_max_rnd_address_line_1_seq ) +1 address_line1_key ,
           mod(abs(dbms_random.random), g_max_rnd_address_line_2_seq ) +1 address_line2_key,
           mod(abs(dbms_random.random), g_max_rnd_address_line_3_seq ) +1 address_line3_key,   
           mod(abs(dbms_random.random), g_max_rnd_postcode_seq ) +1 post_code_key       
    from holder_mandates where holder_mandate_id in
    (
      select holder_mandate_id from holder_mandates hm
      minus
      select holder_mandate_id
      from holder_mandates hm2
      join holder_addresses_tmp1 hat on hat.comp_code = hm2.comp_code and hat.ivc_code = hm2.ivc_code and hat.address_type_id = 4
      join holder_names_tmp1 hnt on hnt.comp_code = hat.comp_code and hnt.ivc_code = hat.ivc_code and hnt.holder_seq = 1
      
      
    );
    obfus_log('2 '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),g_code,g_errm,g_module);    
                dbms_output.put_line('8');
    begin
      execute immediate 'drop index rnd_disp_pay_adrr_lu_idx1';        
      exception when others then null;
    end;
    
    execute immediate 'create index rnd_disp_man_adrr_lu_idx1 on rnd_disp_payment_adresss_lu(payment_id,address_line1_key,
                        address_line2_key,address_line3_key,post_code_key)'; 

    begin
      execute immediate 'drop table holder_mandates_tmp1';
      exception when others then null;
    end;
           
    execute immediate  'create table holder_mandates_tmp1 as select * from holder_mandates where rownum <1';

    obfus_log('3 '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),g_code,g_errm,g_module);

    insert into holder_mandates_tmp1(holder_mandate_id,comp_code,ivc_code,class_code,mandate_type_id,
                                    bank_sort_code,building_society_branch_id,currency_code,bank_account_number,
                                    society_acc_roll_number,account_reference,draft_mandate_ref,bic_code,payee_name,
                                    iban_number,bank_name,international_account_num,address_line1,address_line2,address_line3,
                                    address_line4,address_line5,address_line6,postcode_left,postcode_right,irish_distribution_code,
                                    country_code,created_by,created_date,modified_by,mod_timestamp,posting_prism_task_id,posting_date,
                                    posting_batch_id,posting_transaction_id)
    select  hm.holder_mandate_id,hm.comp_code,hm.ivc_code,hm.class_code,hm.mandate_type_id,
            hm.bank_sort_code,hm.building_society_branch_id,hm.currency_code,tdc(hm.bank_account_number),
            tdc(hm.society_acc_roll_number),hm.account_reference,hm.draft_mandate_ref,tdc(hm.bic_code),
            
            case when mandate_type_id not in (2,3) then REPLACE(DECODE (title_type_code,'OTHER', other_title,title_type_code)|| ' '|| forename|| ' '|| surname|| ' '|| 
            suffix|| ' '|| DECODE (NVL (trust_name, 1),'1', '',trust_name || '+' || trustee_name)|| ' '|| company_name,'  ',' ')  else null end payee_name,
            
            tdc(hm.iban_number),tdc(hm.bank_name),tdc(hm.international_account_num),
            
            case when mandate_type_id = 1 and hat.holder_address_id is not null then hat.address_line1 else al1r.address_line_1 end address_line1,
            case when mandate_type_id = 1 and hat.holder_address_id is not null then hat.address_line2 else al2r.address_line_2 end address_line2,
            case when mandate_type_id = 1 and hat.holder_address_id is not null then hat.address_line3 else al3r.address_line_3 end address_line3,
            case when mandate_type_id = 1 and hat.holder_address_id is not null then hat.address_line4 else null end address_line4,
            case when mandate_type_id = 1 and hat.holder_address_id is not null then hat.address_line5 else null end address_line5,
            case when mandate_type_id = 1 and hat.holder_address_id is not null then hat.address_line6 else null end address_line6,
            case when mandate_type_id = 1 and hat.holder_address_id is not null then hat.postcode_left  else pr.postcode_left  end postcode_left,
            case when mandate_type_id = 1 and hat.holder_address_id is not null then hat.postcode_right else pr.postcode_right end postcode_right,
            case when mandate_type_id = 1 and hat.holder_address_id is not null then hat.irish_distribution_code else null end irish_distribution_code,
            
            hm.country_code,hm.created_by,hm.created_date,hm.modified_by,hm.mod_timestamp,hm.posting_prism_task_id,hm.posting_date,
            hm.posting_batch_id,hm.posting_transaction_id 
            from holder_mandates hm 
            left join rnd_disp_mandate_adresss_lu rdmal on rdmal.holder_mandate_id = hm.holder_mandate_id
            left join holder_addresses_tmp1 hat on hat.comp_code = hm.comp_code and hat.ivc_code = hm.ivc_code and hat.address_type_id = 4
            left join holder_names_tmp1 hnt on hnt.comp_code = hm.comp_code and hnt.ivc_code = hm.ivc_code and hnt.holder_seq = 1
    
            left join address_line_1_rnd al1r on  al1r.key_ns = rdmal.address_line1_key
            left join address_line_2_rnd al2r on al2r.key_ns = rdmal.address_line2_key
            left join address_line_3_rnd al3r on al3r.key_ns = rdmal.address_line3_key
            left join postcode_rnd pr on pr.key_ns =  rdmal.post_code_key;
    
        obfus_log('4 '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),g_code,g_errm,g_module);
    
      -- clean up any anddresses that have the same value as the target (very few)
      commit;

    exception
       when others then
          g_code := SQLCODE;
          g_errm := SUBSTR(SQLERRM, 1 , 4000);     
          obfus_log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),g_code,g_errm,g_module); 
          RAISE; -- Error will propogate back to anonymise setting p_obfus_status => 'FAILED'           
end anon_holder_mandates;
--
  procedure anon_payments is
  begin
  
    g_module := 'anon_payments';  
    obfus_log('0 - truncate tables payments_base,payments_base_fin,cr_shift and recreate tables payments_tmp1, payments_tmp2, cheque_ranges_tmp1 as select',g_code,g_errm,g_module);
     
      execute immediate 'truncate table payments_base';
      execute immediate 'truncate table payments_base_fin';
      execute immediate 'truncate table cr_shift';
          
      begin
        execute immediate 'drop table payments_tmp1';
        exception when others then null;
      end;
      
      execute immediate  'create table payments_tmp1 as select * from payments  where rownum <1';
    
      begin
        execute immediate 'drop table payments_tmp2';
        exception when others then null;
      end;
      
       execute immediate  'create table payments_tmp2 as select * from payments  where rownum <1';
     
     begin
        execute immediate 'drop table cheque_ranges_tmp1';
     exception
        when x_table_not_exist then null;
        when others then raise;
      end;
       
    execute immediate  'create table cheque_ranges_tmp1 as select * from cheque_ranges  where rownum <1';
  
    obfus_log('1 - insert into cr_shift',g_code,g_errm,g_module);  
    
    insert into cr_shift select ba.bank_account_id ,mod(abs(dbms_random.random),100)+1 shift_no 
    from (select distinct bank_account_id from bank_accounts) ba;

    obfus_log('2 - truncate and insert into non_holder_ca',g_code,g_errm,g_module);

    execute immediate  'truncate table non_holder_ca';
    
    insert into non_holder_ca(cash_account_id ,comp_code,holder_name_id) 
    select cash_account_id ,comp_code,holder_name_id from 
    (
      select ca.cash_account_id ,ca.comp_code,hnt.holder_name_id,
      row_number() over (partition by ca.cash_account_id,ca.comp_code order by hnt.holder_name_id) aorder
      from cash_accounts ca
      left join holder_names  hnt on hnt.comp_code = ca.comp_code 
      where (nvl (ca.IVC_code, - 9999)) = -9999
    ) res
    where aorder = 1;

    begin
      execute immediate 'drop table rnd_dispatch_name_lookup';        
      exception when others then null;
    end;

    execute immediate 'create table  rnd_dispatch_name_lookup (payment_id number,surname_key number,forename_key number)';

    obfus_log('3 - insert into rnd_dispatch_name_lookup',g_code,g_errm,g_module);

    insert into rnd_dispatch_name_lookup (payment_id,surname_key ,forename_key ) 
    select payment_id, mod(abs(dbms_random.random), g_max_rnd_surname_seq ) +1 surname_key, mod(abs(dbms_random.random), g_max_rnd_forename_seq ) +1 forename_key
    from payments where payment_id in (
                                        select payment_id from payments py1
                                        minus
                                        select payment_id from payments py2
                                        join cash_transactions ct on py2.cash_transaction_id = ct.cash_transaction_id
                                        join cash_accounts ca on ct.cash_account_id = ca.cash_account_id 
                                        where py2.mandate_type_id = 4 or ca.ivc_code is null);    
    
      execute immediate 'create index rnd_dispatch_name_idx1 on rnd_dispatch_name_lookup(payment_id)';
--    Match on address_type 4 if no match get from seed_address					

    obfus_log('4 - drop and create table rnd_disp_payment_adresss_lu',g_code,g_errm,g_module);

    begin
      execute immediate 'drop table rnd_disp_payment_adresss_lu';        
      exception when others then null;
    end;
     
    execute immediate 'create table  rnd_disp_payment_adresss_lu (payment_id number,address_line1_key number,
    address_line2_key  number,address_line3_key  number,post_code_key number)';

    obfus_log('5 - insert into rnd_disp_payment_adresss_lu',g_code,g_errm,g_module);
    
    insert into rnd_disp_payment_adresss_lu (payment_id,address_line1_key ,address_line2_key,address_line3_key,post_code_key) 
    select payment_id,
           mod(abs(dbms_random.random), g_max_rnd_address_line_1_seq ) +1 address_line1_key ,
           mod(abs(dbms_random.random), g_max_rnd_address_line_2_seq ) +1 address_line2_key,
           mod(abs(dbms_random.random), g_max_rnd_address_line_3_seq ) +1 address_line3_key,   
           mod(abs(dbms_random.random), g_max_rnd_postcode_seq ) +1 post_code_key       
    from payments where payment_id in
    (
      select payment_id from payments py
      minus
      select payment_id
      from payments py2
      left outer join cash_transactions ct on py2.cash_transaction_id = ct.cash_transaction_id
      left outer join cash_accounts ca on ct.cash_account_id = ca.cash_account_id
      left outer join holder_addresses_tmp1 hat on ca.comp_code = hat.comp_code and ca.ivc_code = hat.ivc_code and hat.address_type_id = 4 
    );
    
/*      
    begin
      execute immediate 'drop index rnd_disp_pay_adrr_lu_idx1';        
      exception when others then null;
    end;
 */   
  
    obfus_log('6 - create index rnd_disp_pay_adrr_lu_idx1 on rnd_disp_payment_adresss_lu',g_code,g_errm,g_module);
    execute immediate 'create index rnd_disp_pay_adrr_lu_idx1 on rnd_disp_payment_adresss_lu(payment_id,address_line1_key,
                        address_line2_key,address_line3_key,post_code_key)';
 
    obfus_log('7 - insert into payments_base',g_code,g_errm,g_module);                   
    insert into payments_base (payment_id,cheque_no,new_cheque_no,key_ns,payer_account_number,payer_sortcode,payment_date,        
    bank_account_id,cheque_no_hash,cheque_g_min_key_ns,cheque_g_max_key_ns,randomise_order,
    payee_name1,payee_name2,payee_name3,payee_name4,payee_name5,
    address_line1,
    address_line2,address_line3,address_line4,address_line5,address_line6,postcode_left,postcode_right,irish_distribution_code,cheque_range_type_code,
    fx_comments_anon,fx_notes_anon,payment_reference_anon ,comment_text_anon,parent_cheque_number_anon,payer_alias_anon,
    sibling_account_number_anon,parent_account_number_anon,payer_bic_inherit,payer_iban_inherit,payer_sortcode_inherit, payee_building_soc_acc_no,payee_building_soc_roll_no,
    payee_internat_account_no,payee_internat_branch_ident,anon_dispatch_name1,anon_dispatch_name2 ) 
  
    select res2.payment_id,res2.cheque_no,res2.new_cheque_no,
    res2.key_ns,res2.payer_account_number,res2.payer_sortcode,res2.payment_date,res2.bank_account_id,res2.cheque_no_hash,
    res2.cheque_g_min_key_ns,res2.cheque_g_max_key_ns,abs(dbms_random.random) randomise_order, 
    res2.payee_name1,res2.payee_name2,res2.payee_name3,res2.payee_name4,res2.payee_name5,
    res2.address_line1,address_line2,res2.address_line3,res2.address_line4,res2.address_line5,res2.address_line6,res2.postcode_left,res2.postcode_right,res2.irish_distribution_code,cheque_range_type_code,
    res2.fx_comments_anon,res2.fx_notes_anon,res2.payment_reference_anon,
    res2.comment_text_anon,res2.parent_cheque_number_anon,res2.payer_alias_anon,res2.sibling_account_number_anon,
    res2.parent_account_number_anon,
    res2.payer_bic_inherit,res2.payer_iban_inherit,res2.payer_sortcode_inherit,
    res2.payee_building_soc_acc_no,res2.payee_building_soc_roll_no,res2.payee_internat_account_no,res2.payee_internat_branch_ident,
    res2.anon_dispatch_name1,res2.anon_dispatch_name2     
    from (           
      select res1.payment_id,res1.cheque_no,res1.new_cheque_no,
      res1.key_ns,res1.payer_account_number,res1.payer_sortcode,res1.payment_date,res1.bank_account_id,res1.cheque_no_hash,
--      first_value(res1.key_ns) over (partition by res1.payer_account_number,res1.payer_sortcode,res1.payment_date,res1.cheque_range_type_code   order by res1.key_ns asc rows unbounded preceding ) cheque_g_min_key_ns ,
--      last_value(res1.key_ns) over (partition by res1.payer_account_number,res1.payer_sortcode,res1.payment_date,res1.cheque_range_type_code   order by res1.key_ns asc rows between  unbounded preceding and unbounded following ) cheque_g_max_key_ns,
      
      first_value(res1.key_ns) over (partition by res1.payer_account_number,res1.payer_sortcode,res1.payment_date,res1.cheque_range_type_code order by res1.key_ns 

                                     ) cheque_g_min_key_ns ,
      last_value(res1.key_ns) over (partition by res1.payer_account_number,res1.payer_sortcode,res1.payment_date,res1.cheque_range_type_code  order by res1.key_ns rows between   unbounded preceding and unbounded following 

                                    ) cheque_g_max_key_ns,
      
      res1.payee_name1,res1.payee_name2,res1.payee_name3,res1.payee_name4,res1.payee_name5,
      res1.address_line1,res1.address_line2,res1.address_line3,res1.address_line4,res1.address_line5,res1.address_line6,res1.postcode_left,res1.postcode_right,res1.irish_distribution_code,cheque_range_type_code,
      res1.fx_comments_anon,res1.fx_notes_anon,res1.payment_reference_anon ,
      res1.comment_text_anon,res1.parent_cheque_number_anon,res1.payer_alias_anon,
      res1.sibling_account_number_anon,res1.parent_account_number_anon,res1.payer_bic_inherit,res1.payer_iban_inherit,res1.payer_sortcode_inherit,  
      res1.payee_building_soc_acc_no,res1.payee_building_soc_roll_no,res1.payee_internat_account_no,res1.payee_internat_branch_ident,
      res1.anon_dispatch_name1,res1.anon_dispatch_name2  
      from 
        (select py1.payment_id,payer_account_number,payer_sortcode,payment_date,py1.cheque_no,
        case when payment_method_code in ('ACHQ','MCHQ') then
          case when (py1.cheque_no + crs.shift_no  ) < 1000000 then py1.cheque_no + crs.shift_no else py1.cheque_no - crs.shift_no end 
        else null end new_cheque_no,
        ct.bank_account_id,
        row_number() over (partition by 1 order by payer_account_number,payer_sortcode,payment_date,crt.cheque_range_type_code) key_ns,
        ora_hash(cheque_no, 4294967295, 0) cheque_no_hash,
        substr(case when ca.cash_account_type_code in ( 'ADMIN','CHG','CHRTY','COMM','CORETN','CPS','MSD','LCIFEE','LCTFEE','PAYR','PTM','RESREPAY','TAXI','TAXREV') then cat.description 
               else
                case when py1.mandate_type_id = 1 then fl.name ||'  '||hn_surname.surname  else case when py1.mandate_type_id in  (2,3,4,5) then hl.label_line_1 else null end end
              end ,1,35) payee_name1,
        substr(case when ca.cash_account_type_code in ( 'ADMIN','CHG','CHRTY','COMM','CORETN','CPS','MSD','LCIFEE','LCTFEE','PAYR','PTM','RESREPAY','TAXI','TAXREV') then null 
               else case when py1.mandate_type_id = 1 then null else case when py1.mandate_type_id in  (2,3,4,5) then hl.label_line_2 else null end end 
               end ,1,35) payee_name2 ,
        substr(case when ca.cash_account_type_code in ( 'ADMIN','CHG','CHRTY','COMM','CORETN','CPS','MSD','LCIFEE','LCTFEE','PAYR','PTM','RESREPAY','TAXI','TAXREV') then null 
                else case when py1.mandate_type_id = 1 then null else case when py1.mandate_type_id in  (2,3,4,5) then hl.label_line_3 else null end end 
                end,1,35) payee_name3 ,
        substr(case when ca.cash_account_type_code in ( 'ADMIN','CHG','CHRTY','COMM','CORETN','CPS','MSD','LCIFEE','LCTFEE','PAYR','PTM','RESREPAY','TAXI','TAXREV') then null 
               else case when py1.mandate_type_id = 1 then null else case when py1.mandate_type_id in  (2,3,4,5) then hl.label_line_4 else null end end 
               end,1,35) payee_name4 ,
        null  payee_name5 ,
        case when hat.address_type_id = 4 then hat.address_line1 else al1r.address_line_1 end address_line1,
        case when hat.address_type_id = 4 then hat.address_line2 else al2r.address_line_2 end address_line2,
        case when hat.address_type_id = 4 then hat.address_line3 else  al3r.address_line_3 end address_line3,
        case when hat.address_type_id = 4 then hat.address_line4 else  null end address_line4,
        case when hat.address_type_id = 4 then hat.address_line5 else  null end address_line5,
        case when hat.address_type_id = 4 then hat.address_line6 else  null end address_line6,
        case when hat.address_type_id = 4 then hat.postcode_left else  pr.postcode_left end postcode_left,
        case when hat.address_type_id = 4 then hat.postcode_right else  pr.postcode_right end postcode_right,
        case when hat.address_type_id = 4 then hat.irish_distribution_code else  null end irish_distribution_code,      
        crt.cheque_range_type_code,
        py1.fx_comments fx_comments_anon,  -- rn(py1.fx_comments) fx_comments_anon,
        py1.fx_notes fx_notes_anon, --  rn(py1.fx_notes) fx_notes_anon,
        py1.payment_reference payment_reference_anon, --  tdc(py1.payment_reference) payment_reference_anon,
        py1.comment_text comment_text_anon, --  rn(py1.comment_text) comment_text_anon, 
        py1.parent_cheque_number parent_cheque_number_anon, --  tdc(py1.parent_cheque_number) parent_cheque_number_anon, 
        py1.payer_alias payer_alias_anon, --  tdc(py1.payer_alias) payer_alias_anon,
        sibling_ba.account_no_anon sibling_account_number_anon, --  tdc(py1.sibling_account_number) sibling_account_number_anon,
        parent_ba.account_no_anon parent_account_number_anon,
        payer_ba.bic payer_bic_inherit, payer_ba.iban payer_iban_inherit,payer_ba.sortcode payer_sortcode_inherit,
        tdc(py1.payee_building_soc_acc_no)  payee_building_soc_acc_no,tdc(py1.payee_building_soc_roll_no)  payee_building_soc_roll_no,
        tdc(py1.payee_internat_account_no) payee_internat_account_no,tdc(py1.payee_internat_branch_ident) payee_internat_branch_ident,
        substr(case when hl.label_line_1 is not null then hl.label_line_1 else fl.name||' '||hn_surname.surname  end ,1,35) anon_dispatch_name1,
        substr(case when hl.label_line_2 is not null then hl.label_line_2 else null end ,1,35) anon_dispatch_name2  

        from payments  py1                                  
        left outer join cash_transactions ct on py1.cash_transaction_id = ct.cash_transaction_id
        left outer join cash_accounts ca on ct.cash_account_id = ca.cash_account_id
        left join cash_account_types cat on cat.cash_account_type_code = ca.cash_account_type_code
        left outer join holder_addresses_tmp1 hat on ca.comp_code = hat.comp_code and ca.ivc_code = hat.ivc_code and hat.address_type_id = 4 
        left outer join cr_shift crs on crs.bank_account_id = ct.bank_account_id  
        left outer join cheque_ranges crt on ct.bank_account_id = crt.bank_account_id and py1.cheque_no >= crt.start_no and py1.cheque_no <= crt.end_no
        
        left outer join bank_accounts_base_fin payer_ba on payer_ba.account_no = py1.payer_account_number and payer_ba.sortcode = py1.payer_sortcode 
        left outer join bank_accounts_base_fin parent_ba on parent_ba.account_no = py1.parent_account_number and parent_ba.sortcode = py1.parent_sort_code
        left outer join bank_accounts_base_fin sibling_ba on sibling_ba.account_no = py1.sibling_account_number and sibling_ba.sortcode = py1.sibling_sort_code
        
        left outer join holder_labels_tmp1 hl on hl.comp_code =  ca.comp_code and hl.ivc_code = ca.ivc_code
        left outer join rnd_dispatch_name_lookup rdnl on py1.payment_id = rdnl.payment_id
        left outer join rnd_disp_payment_adresss_lu rdpal on py1.payment_id = rdpal.payment_id
        left outer join forename_list fl on fl.key_ns = rdnl.forename_key
        left outer join surname_list sl on sl.surname_seq = rdnl.surname_key
        left outer join holder_names_tmp1 hn_surname on hn_surname.holder_name_id = sl.holder_name_id
        left outer join address_line_1_rnd al1r on  al1r.key_ns = rdpal.address_line1_key
        left outer join address_line_2_rnd al2r on al2r.key_ns = rdpal.address_line2_key
        left outer join address_line_3_rnd al3r on al3r.key_ns = rdpal.address_line3_key
        left outer join postcode_rnd pr on pr.key_ns =  rdpal.post_code_key
                                
       ) res1 
   ) res2;
    
    obfus_log('8 - insert into cheque_ranges_tmp1',g_code,g_errm,g_module);

    commit; 
       
    insert into cheque_ranges_tmp1(cheque_range_id,bank_account_id,cheque_range_type_code,end_no,                                     
    exhaustive_action_cont_yn,last_cheque_no_used,start_no,warning_threshold,                               
    created_by,created_date,modified_by,mod_timestamp)
    select cr.cheque_range_id,cr.bank_account_id,cr.cheque_range_type_code,
    case when (cr.end_no+crs.shift_no) < 1000000 then cr.end_no+crs.shift_no else 999999 end ,                                     
    cr.exhaustive_action_cont_yn,
    case when cr.last_cheque_no_used+crs.shift_no < 1000000 then cr.last_cheque_no_used+crs.shift_no else 999999 end,
    case when cr.start_no+crs.shift_no < 1000000 then cr.start_no+crs.shift_no else 999999 end ,
    case when  cr.warning_threshold+crs.shift_no < 1000000 then cr.warning_threshold+crs.shift_no else 999999 end ,                               
    cr.created_by,cr.created_date,cr.modified_by,cr.mod_timestamp         
    from cheque_ranges cr left join cr_shift crs on crs.bank_account_id = cr.bank_account_id  ;

    commit; 

    obfus_log('9 - Refresh stats on table PAYMENTS_BASE',g_code,g_errm,g_module);  
    dbms_stats.gather_table_stats(ownname => 'ANONDEV_ANONYMISE', tabname => 'PAYMENTS_BASE');
    
    obfus_log('10 - insert into payments_base_fin',g_code,g_errm,g_module);    
    insert into  payments_base_fin ( payment_id ,cheque_no, key_ns, payer_account_number, payer_sortcode, payment_date,         
                                     bank_account_id, cheque_no_hash ,cheque_g_min_key_ns, cheque_g_max_key_ns, randomise_order,
                                     new_cheque_no, payee_name1, payee_name2, payee_name3, payee_name4, payee_name5,
                                     address_line1 ,address_line2,address_line3 ,address_line4 ,address_line5 ,address_line6 ,                   
                                     postcode_left ,postcode_right ,irish_distribution_code,shuffle_cheque_no ,cheque_range_type_code,fx_comments_anon,fx_notes_anon,
                                     payment_reference_anon,comment_text_anon,parent_cheque_number_anon,payer_alias_anon,
                                     sibling_account_number_anon,parent_account_number_anon,payer_bic_inherit,payer_iban_inherit,payer_sortcode_inherit,
                                     payee_bank_name,payee_branch_name,payee_building_soc_acc_no,payee_internat_account_no,payee_building_soc_roll_no,
                                     dispatch_name1,dispatch_name2) 
    with shuffle as
       (select key_ns,cheque_g_min_key_ns + row_number() over (partition by payer_account_number,payer_sortcode,cheque_range_type_code,payment_date
                                                               order by randomise_order) -1 as new_cheque_key
          from payments_base)
    select pb.payment_id,pb.cheque_no,pb.key_ns,pb.payer_account_number,pb.payer_sortcode,pb.payment_date , 
           pb.bank_account_id,pb.cheque_no_hash,pb.cheque_g_min_key_ns,pb.cheque_g_max_key_ns,pb.randomise_order,
           pb.new_cheque_no new_cheque_no,pb.payee_name1,pb.payee_name2,pb.payee_name3,pb.payee_name4,pb.payee_name5,
           pb.address_line1,pb.address_line2,pb.address_line3,pb.address_line4 ,pb.address_line5,pb.address_line6 ,
           pb.postcode_left,pb.postcode_right,pb.irish_distribution_code,pb1.cheque_no shuffle_cheque_no,pb1.cheque_range_type_code, pb.fx_comments_anon,pb.fx_notes_anon,
           pb.payment_reference_anon,pb.comment_text_anon,pb.parent_cheque_number_anon,pb.payer_alias_anon,
           pb.sibling_account_number_anon,pb.parent_account_number_anon,pb.payer_bic_inherit,pb.payer_iban_inherit,pb.payer_sortcode_inherit,
           pb.payee_bank_name,pb.payee_branch_name,pb.payee_building_soc_acc_no,pb.payee_internat_account_no,pb.payee_building_soc_roll_no,
           pb.anon_dispatch_name1,pb.anon_dispatch_name2
      from payments_base pb 
      join shuffle s on pb.key_ns = s.key_ns
      join payments_base pb1 on pb1.key_ns = s.new_cheque_key
      join payments p on p.payment_id = pb.payment_id;

    commit; 
    
    obfus_log('11 - insert into payments_tmp1',g_code,g_errm,g_module);

    insert into payments_tmp1 (payment_id,currency_code,cash_acc_grps_id,cash_transaction_id,country_code,mandate_type_id,
          ctype_code,reissue_for_payment_id,reissue_by_payment_id,cheque_no,comment_text,payment_reference,
          payment_status_ref,status_change_date ,
          payee_name1,payee_name2,payee_name3,payee_name4,payee_name5,
          dispatch_address_line1,dispatch_address_line2,dispatch_address_line3,
          dispatch_address_line4,dispatch_address_line5,dispatch_address_line6, postcode_left,postcode_right,
          irish_distribution_code,high_prioroty_yn,payment_amount,payment_date,post_date,extract_date,payer_alias,
          payer_account_number,payer_sortcode,cleared_date,payee_ref,payer_iban,payer_bic,dispatch_name1,dispatch_name2,
          conversion_date,conversion_rate,converted_amount,currency_code1,fx_reference,
          isdraft_yn,fx_comments,fx_notes,created_by,created_date,modified_by,mod_timestamp,payment_method_code,
          payment_status_type_code,payment_type_code,payment_file_name,payee_alias,payee_account_number,payee_bic,
          payee_iban,payee_sortcode,payee_branch_name,
          payee_chequeaddress_line1, payee_chequeaddress_line2,payee_chequeaddress_line3,
          payee_chequeaddress_line4,payee_chequeaddress_line5,payee_chequeaddress_line6, 
          payee_client_owned_bankacc,payee_building_soc_roll_no,
          payee_internat_branch_ident,payee_bank_name,payee_country,
          payee_building_soc_acc_no,payee_internat_account_no,swift_code,consolidation_ivc,laser_cheque_yn,global_crest_tran_id,
          payment_ptcp_id,print_group_code,print_location,parent_sort_code,parent_cheque_number,parent_account_number,
          parent_ivc_reference,sibling_sort_code,sibling_cheque_number,sibling_account_number,sibling_ivc_reference,
          payment_letter_type_code,fx_notes_type,fx_settlement_account_number,fx_settlement_sort_code,fx_client_ref,
          crest_message_type_code,society_acc_roll_number,reissue_date
          )
          
        select py.payment_id,py.currency_code,py.cash_acc_grps_id,py.cash_transaction_id,py.country_code,py.mandate_type_id,
        py.ctype_code,py.reissue_for_payment_id,py.reissue_by_payment_id,pbf.new_cheque_no,pbf.comment_text_anon,pbf.payment_reference_anon,
        py.payment_status_ref,py.status_change_date,
        pbf.payee_name1,pbf.payee_name2,pbf.payee_name3,pbf.payee_name4,pbf.payee_name5,
        pbf.address_line1,pbf.address_line2,
        pbf.address_line3,pbf.address_line4,pbf.address_line5,pbf.address_line6,
        pbf.postcode_left,pbf.postcode_right,pbf.irish_distribution_code,
        py.high_prioroty_yn,py.payment_amount,py.payment_date,py.post_date,py.extract_date,pbf.payer_alias_anon,py.payer_account_number,
        pbf.payer_sortcode_inherit,py.cleared_date,py.payee_ref,pbf.payer_iban_inherit,pbf.payer_bic_inherit,
        pbf.dispatch_name1,pbf.dispatch_name2,
        py.conversion_date,py.conversion_rate,py.converted_amount,py.currency_code1,py.fx_reference,py.isdraft_yn,
        pbf.fx_comments_anon,pbf.fx_notes_anon,py.created_by,py.created_date,py.modified_by,py.mod_timestamp,py.payment_method_code,py.
        payment_status_type_code,py.payment_type_code,py.payment_file_name,py.payee_alias,py.payee_account_number,py.payee_bic,py.
        payee_iban,py.payee_sortcode,pbf.payee_branch_name,pbf.address_line1,pbf.address_line2,pbf.address_line3,
        pbf.address_line4,pbf.address_line5,pbf.address_line6,py.payee_client_owned_bankacc,pbf.payee_building_soc_roll_no,py.
        payee_internat_branch_ident,pbf.payee_bank_name,py.payee_country,pbf.payee_building_soc_acc_no,pbf.payee_internat_account_no,
        py.swift_code,py.consolidation_ivc,py.laser_cheque_yn,py.global_crest_tran_id,py.payment_ptcp_id,py.print_group_code,
        py.print_location,py.parent_sort_code,pbf.parent_cheque_number_anon,pbf.parent_account_number_anon,py.parent_ivc_reference,
        py.sibling_sort_code,py.sibling_cheque_number,pbf.sibling_account_number_anon,py.sibling_ivc_reference,py.payment_letter_type_code,
        py.fx_notes_type,py.fx_settlement_account_number,py.fx_settlement_sort_code,py.fx_client_ref,py.crest_message_type_code,
        py.society_acc_roll_number,py.reissue_date
        from payments py join payments_base_fin pbf on pbf.payment_id = py.payment_id;
    

--    delete from  manual_transform  where owner = 'PRISM_CORE' and table_name ='PAYMENTS';

--    insert into manual_transform (owner,table_name, actual_col,trans_function,technique)          
            
--    select 'CASH_MANAGEMENT','PAYMENTS','DISPATCH_ADDRESS_LINE1','bepoke(address_line1)', 'INHERIT_HA' from dual union all
--    select 'CASH_MANAGEMENT','PAYMENTS','DISPATCH_ADDRESS_LINE2','bepoke(address_line2)', 'INHERIT_HA' from dual union all
--    select 'CASH_MANAGEMENT','PAYMENTS','DISPATCH_ADDRESS_LINE3','bepoke(address_line3)', 'INHERIT_HA' from dual union all
--    select 'CASH_MANAGEMENT','PAYMENTS','DISPATCH_ADDRESS_LINE4','bepoke(address_line4)', 'INHERIT_HA' from dual union all
--    select 'CASH_MANAGEMENT','PAYMENTS','DISPATCH_ADDRESS_LINE5','bepoke(address_line5)', 'INHERIT_HA' from dual union all
--    select 'CASH_MANAGEMENT','PAYMENTS','DISPATCH_ADDRESS_LINE6','bepoke(address_line6)', 'INHERIT_HA' from dual union all
--    select 'CASH_MANAGEMENT','PAYMENTS','POSTCODE_LEFT','bepoke(postcode_left)', 'INHERIT_HA' from dual union all
--    select 'CASH_MANAGEMENT','PAYMENTS','POSTCODE_RIGHT','bepoke(postcode_right)', 'INHERIT_HA' from dual union all
--    select 'CASH_MANAGEMENT','PAYMENTS','IRISH_DISTRIBUTION_CODE','inherited(irish_distribution_code)', 'INHERIT_HA' from dual union all
--    select 'CASH_MANAGEMENT','PAYMENTS','PAYEE_NAME1','bepoke(payee_name1)', 'INHERIT_HL' from dual union all
--    select 'CASH_MANAGEMENT','PAYMENTS','PAYEE_NAME2','bepoke(payee_name2)', 'INHERIT_HL' from dual union all
--    select 'CASH_MANAGEMENT','PAYMENTS','PAYEE_NAME3','bepoke(payee_name3)', 'INHERIT_HL' from dual union all
--    select 'CASH_MANAGEMENT','PAYMENTS','PAYEE_NAME4','bepoke(payee_name4)', 'INHERIT_HL' from dual union all
--    select 'CASH_MANAGEMENT','PAYMENTS','PAYEE_NAME5','bepoke(payee_name5)', 'INHERIT_HL' from dual union all
--
--    
--    
--    select 'CASH_MANAGEMENT','PAYMENTS','DISPATCH_NAME1','bepoke(dispatch_name1)', 'INHERIT_HL' from dual union all
--    select 'CASH_MANAGEMENT','PAYMENTS','DISPATCH_NAME2','bepoke(dispatch_name2)', 'INHERIT_HL' from dual union all
--    
--    select 'CASH_MANAGEMENT','PAYMENTS','PAYMENT_REFERENCE','tdc(paymet_reference)', 'SUBS_DIGITS_TRANSFORM_CAPS_CHAR' from dual union all
--    select 'CASH_MANAGEMENT','PAYMENTS','FX_COMMENTS','rn(fx_comments)', 'OBSFUS_TEXT' from dual union all
--    select 'CASH_MANAGEMENT','PAYMENTS','FX_NOTES','rn(fx_notes)', 'OBSFUS_TEXT' from dual union all
--    select 'CASH_MANAGEMENT','PAYMENTS','CHEQUE_NO','calculated(cheque_no)', 'TRANSFORM_CHEQUENO' from dual  union all
--    select 'CASH_MANAGEMENT','PAYMENTS','COMMENT_TEXT','rn(comment_text)', 'OBSFUS_TEXT' from dual union all
--    select 'CASH_MANAGEMENT','PAYMENTS','PARENT_CHEQUE_NUMBER','tdc(parent_cheque_number)', 'SUBS_DIGITS_TRANSFORM_CAPS_CHAR' from dual union all
--    select 'CASH_MANAGEMENT','PAYMENTS','PAYER_ALIAS','inherited(alias,bank_accounts)', 'INHERITED_BA' from dual union all
--    select 'CASH_MANAGEMENT','PAYMENTS','SIBLING_ACCOUNT_NUMBER','tdc(sibling_account_number)', 'SUBS_DIGITS_TRANSFORM_CAPS_CHAR' from dual union all
--
--
--    select 'CASH_MANAGEMENT','PAYMENTS','FX_CLIENT_REF','tdc(fx_client_ref)', 'SUBS_DIGITS_TRANSFORM_CAPS_CHAR' from dual union all
--    select 'CASH_MANAGEMENT','PAYMENTS','PAYMENT_PTCP_ID','tdc(payment_ptcp_id)', 'SUBS_DIGITS_TRANSFORM_CAPS_CHAR' from dual union all
--    select 'CASH_MANAGEMENT','PAYMENTS','FX_SETTLEMENT_ACCOUNT_NUMBER','tdc(fx_settlement_account_number)', 'SUBS_DIGITS_TRANSFORM_CAPS_CHAR' from dual union all
--    select 'CASH_MANAGEMENT','PAYMENTS','PAYEE_BANK_NAME','inherited(payee_bank_name,bank_accounts)', 'INHERIT_BA' from dual union all
--    select 'CASH_MANAGEMENT','PAYMENTS','PAYEE_BRANCH_NAME','inherited(payee_branch_name,bank_accounts)', 'INHERIT_BA' from dual union all
--    select 'CASH_MANAGEMENT','PAYMENTS','PAYEE_BUILDING_SOC_ACC_NO','inherited(payee_building_soc_acc_no,bank_accounts)', 'INHERIT_BA' from dual union all
--    select 'CASH_MANAGEMENT','PAYMENTS','PAYEE_BUILDING_SOC_ROLL_NO','inherited(payee_building_soc_roll_no,bank_accounts)', 'INHERIT_BA' from dual union all
--    select 'CASH_MANAGEMENT','PAYMENTS','PAYEE_INTERNAT_ACCOUNT_NO','inherited(payee_internat_account_no,bank_accounts)', 'INHERIT_BA' from dual union all
--    select 'CASH_MANAGEMENT','PAYMENTS','PAYEE_INTERNAT_BRANCH_IDENT','inherited(payee_internat_branch_ident,bank_accounts)', 'INHERIT_BA' from dual union all 
--    select 'CASH_MANAGEMENT','PAYMENTS','PAYER_SORTCODE','inherited(sort_code,bank_accounts)', 'INHERIT_BA' from dual;
    commit; 
    
    exception  
       when others then
          g_code := SQLCODE;
          g_errm := SUBSTR(SQLERRM, 1 , 4000);     
          obfus_log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),g_code,g_errm,g_module);        
          RAISE; -- Error will propogate back to anonymise setting p_obfus_status => 'FAILED'    
end anon_payments;
   
  procedure merge_patches
   is
   begin
   
         g_module := 'merge_patches';  
          
          execute immediate 'create index idx_rcrr1 on  tgt_rr556_cnb_reversal_report(rev_total_id,cash_transaction_id,ivc_code,registration_date,transaction_amount ,
                                            reversal_msg_type,error_description,account_forename,account_surname)';
   
          merge into tgt_rr556_cnb_reversal_report tgt
          using (select rev_total_id ,tdc(bank_sort_code) td_bank_sort_code, tdc(account_no) td_account_no, account_no ,bank_sort_code  ,           
                      cash_transaction_id, ivc_code,registration_date,                         
                      transaction_amount,reversal_msg_type,error_description,account_forename,account_surname     
                      from rr556_cnb_reversal_report) res
            on (nvl(tgt.rev_total_id,-1) =nvl(res.rev_total_id,-1) and
                nvl(tgt.cash_transaction_id,-1) = nvl(res.cash_transaction_id,-1) and
                nvl(tgt.ivc_code,'$') = nvl(res.ivc_code,'$') and
                nvl(tgt.registration_date,to_date('01-01-1900', 'MM-DD-YYYY')) = nvl(res.registration_date,to_date('01-01-1900', 'MM-DD-YYYY')) and
                nvl(tgt.transaction_amount,-1) = nvl(res.transaction_amount,-1) and                 
                nvl(tgt.reversal_msg_type,'$') = nvl(res.reversal_msg_type,'$') and
                nvl(tgt.error_description,'$') = nvl(res.error_description,'$') and                       
                nvl(tgt.account_forename,'$') = nvl(res.account_forename,'$') and
                nvl(tgt.account_surname,'$') =nvl(res.account_surname,'$')  )
          when matched then update set tgt.account_no = res.td_account_no , tgt.bank_sort_code = res.td_bank_sort_code ;
          
          
         execute immediate 'drop index idx_rcrr1';
          
          commit;


         merge into tgt_rr556_cnb_bank_reversal_to tgt
         
          using (select rev_total_id,bank_sort_code,tdc(account_no) account_no,currency_code,                 
                  reversed_transaction_count,transaction_suspense_count,already_reversed_count,                       
                  transaction_error_count,reversed_transaction_amount,transaction_suspense_amount,                     
                  already_reversed_amount,transaction_error_amount   from rr556_cnb_bank_reversal_totals) res
            on (nvl(tgt.rev_total_id,-1)  = nvl(res.rev_total_id,-1) and                      
                nvl(tgt.bank_sort_code,'$')   = nvl(res.bank_sort_code,'$') and             
                nvl(tgt.currency_code,'$')   = nvl(res.currency_code,'$') and  
                nvl(tgt.reversed_transaction_count ,-1)  = nvl(res.reversed_transaction_count,-1) and
                nvl(tgt.transaction_suspense_count,-1)   = nvl(res.transaction_suspense_count,-1) and  
                nvl(tgt.already_reversed_count,-1)   = nvl(res.already_reversed_count,-1) and  
                nvl(tgt.transaction_error_count,-1)   = nvl(res.transaction_error_count,-1) and  
                nvl(tgt.reversed_transaction_amount,-1)   = nvl(res.reversed_transaction_amount,-1) and  
                nvl(tgt.transaction_suspense_amount,-1)   = nvl(res.transaction_suspense_amount,-1) and  
                nvl(tgt.already_reversed_amount,-1)   = nvl(res.already_reversed_amount,-1) and  
                nvl(tgt.transaction_error_amount,-1)   = nvl(res.transaction_error_amount,-1)
               )
              when matched then update set tgt.account_no = res.account_no;
 
          commit;  
          
           merge into tgt_crest_participants tgt
          using (select crest_ptcp,
                    ofe(email_address) email_address,
                    tdc(investor_address_line_1) investor_address_line_1,
                    tdc(investor_address_line_2) investor_address_line_2,
                    tdc(investor_address_line_3) investor_address_line_3,
                    tdc(investor_address_line_4) investor_address_line_4,
                    tdc(investor_address_line_5) investor_address_line_5,
                    tdc(investor_post_code_left) investor_post_code_left,
                    tdc(investor_post_code_right) investor_post_code_right,
                    tdc(label_line_1_text) label_line_1_text,
                    tdc(label_line_2_text) label_line_2_text,
                    tdc(label_line_3_text) label_line_3_text,
                    tdc(label_line_4_text) label_line_4_text,
                    tdc(names_name_1) names_name_1,
                    tdc(participant_note) participant_note,
                    tdc(ptcp_address_line_1) ptcp_address_line_1,
                    tdc(ptcp_address_line_2) ptcp_address_line_2,
                    tdc(ptcp_address_line_3) ptcp_address_line_3,
                    tdc(ptcp_address_line_4) ptcp_address_line_4,
                    tdc(ptcp_address_line_5) ptcp_address_line_5,
                    tdc(ptcp_name) ptcp_name,
                    tdc(ptcp_postcode_left) ptcp_postcode_left,
                    tdc(ptcp_postcode_right) ptcp_postcode_right,
                    tdc(short_name) short_name,
                    tdc(sort_key) sort_key
                  from tgt_crest_participants cp) res
            on (tgt.crest_ptcp   =  res.crest_ptcp)
              when matched then update set 
                    tgt.email_address = res.email_address,
                    tgt.investor_address_line_1 = res.investor_address_line_1,
                    tgt.investor_address_line_2 = res.investor_address_line_2,
                    tgt.investor_address_line_3 = res.investor_address_line_3,
                    tgt.investor_address_line_4 = res.investor_address_line_4,
                    tgt.investor_address_line_5 = res.investor_address_line_5,
                    tgt.investor_post_code_left = res.investor_post_code_left,
                    tgt.investor_post_code_right = res.investor_post_code_right,
                    tgt.label_line_1_text = res.label_line_1_text,
                    tgt.label_line_2_text = res.label_line_2_text,
                    tgt.label_line_3_text = res.label_line_3_text,
                    tgt.label_line_4_text = res.label_line_4_text,
                    tgt.names_name_1 = res.names_name_1,
                    tgt.participant_note = res.participant_note,
                    tgt.ptcp_address_line_1 = res.ptcp_address_line_1,
                    tgt.ptcp_address_line_2 = res.ptcp_address_line_2,
                    tgt.ptcp_address_line_3 = res.ptcp_address_line_3,
                    tgt.ptcp_address_line_4 = res.ptcp_address_line_4,
                    tgt.ptcp_address_line_5 = res.ptcp_address_line_5,
                    tgt.ptcp_name = res.ptcp_name,
                    tgt.ptcp_postcode_left = res.ptcp_postcode_left,
                    tgt.ptcp_postcode_right = res.ptcp_postcode_right,
                    tgt.short_name = res.short_name,
                    tgt.sort_key = res.sort_key;
            commit;
          
         execute immediate 'create index idx_rcbrt on tgt_rr556_cnb_bank_reversal_to(
         rev_total_id,bank_sort_code,currency_code,reversed_transaction_count ,transaction_suspense_count,
         already_reversed_count,transaction_error_count,reversed_transaction_amount,transaction_suspense_amount,
         already_reversed_amount,transaction_error_amount)';        

          merge into tgt_fatca_clsf_extracts tfse
           using (select fatca_clsf_extract_id,
                          tdc(corres_address_line_1) corres_address_line_1,
                          tdc(corres_address_line_2) corres_address_line_2,
                          tdc(corres_address_line_3) corres_address_line_3,
                          tdc(corres_address_line_5) corres_address_line_5,
                          tdc(corres_address_line_6) corres_address_line_6,
                          tdc(corres_post_code_left) corres_post_code_left,
                          tdc(corres_post_code_right) corres_post_code_right,
                          tdc(date_of_birth) date_of_birth,
                          tdc(forenames) forenames,
                          tdc(mandate_1_address_line_1) mandate_1_address_line_1,
                          tdc(mandate_1_address_line_2) mandate_1_address_line_2,
                          tdc(mandate_1_address_line_3) mandate_1_address_line_3,
                          tdc(mandate_1_address_line_4) mandate_1_address_line_4,
                          tdc(mandate_1_address_line_5) mandate_1_address_line_5,
                          tdc(mandate_1_address_line_6) mandate_1_address_line_6,
                          tdc(mandate_1_int_bank_acc_noiban) mandate_1_int_bank_acc_noiban,
                          tdc(mandate_1_post_code_left) mandate_1_post_code_left,
                          tdc(mandate_1_post_code_right) mandate_1_post_code_right,
                          tdc(name) name,
                          tdc(reged_address_line_1) reged_address_line_1,
                          tdc(reged_address_line_3) reged_address_line_3,
                          tdc(reged_address_line_4) reged_address_line_4,
                          tdc(reged_address_line_5) reged_address_line_5,
                          tdc(reged_address_line_6) reged_address_line_6,
                          tdc(reged_postcodeleft) reged_postcodeleft,
                          tdc(reged_postcoderight) reged_postcoderight,
                          tdc(surname) surname,
                          tdc(tax_ref_no_1) tax_ref_no_1,
                          tdc(tax_ref_no_3) tax_ref_no_3,
                          tdc(tax_ref_no_4) tax_ref_no_4
                  from fatca_clsf_extracts ) fse
              on (fse.fatca_clsf_extract_id = tfse.fatca_clsf_extract_id)
              when matched then
              update set 
              tfse.corres_address_line_1= fse.corres_address_line_1,
              tfse.corres_address_line_2= fse.corres_address_line_2,
              tfse.corres_address_line_3= fse.corres_address_line_3,
              tfse.corres_address_line_5= fse.corres_address_line_5,
              tfse.corres_address_line_6= fse.corres_address_line_6,
              tfse.corres_post_code_left= fse.corres_post_code_left,
              tfse.corres_post_code_right= fse.corres_post_code_right,
              tfse.date_of_birth= fse.date_of_birth,
              tfse.forenames= fse.forenames,
              tfse.mandate_1_address_line_1= fse.mandate_1_address_line_1,
              tfse.mandate_1_address_line_2= fse.mandate_1_address_line_2,
              tfse.mandate_1_address_line_3= fse.mandate_1_address_line_3,
              tfse.mandate_1_address_line_4= fse.mandate_1_address_line_4,
              tfse.mandate_1_address_line_5= fse.mandate_1_address_line_5,
              tfse.mandate_1_address_line_6= fse.mandate_1_address_line_6,
              tfse.mandate_1_int_bank_acc_noiban= fse.mandate_1_int_bank_acc_noiban,
              tfse.mandate_1_post_code_left= fse.mandate_1_post_code_left,
              tfse.mandate_1_post_code_right= fse.mandate_1_post_code_right,
              tfse.name= fse.name,
              tfse.reged_address_line_1= fse.reged_address_line_1,
              tfse.reged_address_line_3= fse.reged_address_line_3,
              tfse.reged_address_line_4= fse.reged_address_line_4,
              tfse.reged_address_line_5= fse.reged_address_line_5,
              tfse.reged_address_line_6= fse.reged_address_line_6,
              tfse.reged_postcodeleft= fse.reged_postcodeleft,
              tfse.reged_postcoderight= fse.reged_postcoderight,
              tfse.surname= fse.surname,
              tfse.tax_ref_no_1= fse.tax_ref_no_1,
              tfse.tax_ref_no_3= fse.tax_ref_no_3,
              tfse.tax_ref_no_4= fse.tax_ref_no_4;
              commit;
       
         execute immediate 'drop index idx_rcbrt';
         
         obfus_log('gather_table_stats for RANDOMISED_NOTES '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,g_module);       
                      
         begin 
            dbms_stats.gather_table_stats(ownname=>g_run_env, tabname=>'RANDOMISED_NOTES', cascade=>true);
         end;
    
         
         obfus_log('updating tgt_cash_transactions.comment_text '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,g_module); 
         
         update /*+ PARALLEL(4) */ tgt_cash_transactions CA
            set ca.comment_text = ( select SUBSTR(RN.RND_WORDS,LENGTH(CA.COMMENT_TEXT))
                                      from RANDOMISED_NOTES RN
                                     where MOD(CA.CASH_TRANSACTION_ID,g_max_rnd_note) + 1 = RN.KEY_NS )
          where exists ( select 1
                           from RANDOMISED_NOTES RN1
                          where MOD(CA.CASH_TRANSACTION_ID,g_max_rnd_note) + 1 = RN1.KEY_NS ); 
 
         obfus_log('updated ' || SQL%ROWCOUNT || ' tgt_cash_transactions records '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,g_module);
         
         commit;           
          
          merge into TGT_IDEAL_TRANS tit
          using (select tit1.IDEAL_TRAN_id,tit1.DATE_OF_BIRTH from TGT_IDEAL_TRANS tit1
          join IDEAL_TRANS ti on ti.IDEAL_TRAN_id = tit1.IDEAL_TRAN_id and ti.DATE_OF_BIRTH = tit1.DATE_OF_BIRTH)res
          on ( tit.IDEAL_TRAN_id = res.IDEAL_TRAN_id)
          when matched
          then
            update set tit.DATE_OF_BIRTH  =  rd30(res.DATE_OF_BIRTH);


        merge into TGT_MONEY_LAUNDER_CTLS tmlc
        using (select tmlc1.MONEY_LAUNDER_CTL_ID,tmlc1.DATE_OF_BIRTH from TGT_MONEY_LAUNDER_CTLS tmlc1
        join MONEY_LAUNDER_CTLS mlc on mlc.MONEY_LAUNDER_CTL_ID = tmlc1.MONEY_LAUNDER_CTL_ID and mlc.DATE_OF_BIRTH = tmlc1.DATE_OF_BIRTH)res
        on ( tmlc.MONEY_LAUNDER_CTL_ID= res.MONEY_LAUNDER_CTL_ID)
        when matched
        then
           update set tmlc.DATE_OF_BIRTH  =  rd30(res.DATE_OF_BIRTH);

--        delete from manual_transform where owner = 'PRISM_CORE' and table_name IN ('FATCA_CLSF_EXTRACTS','CASH_TRANSACTIONS') ;
--        
--        insert into manual_transform (owner,table_name, actual_col,trans_function,technique)          
--        
--        select 'CASH_MANAGEMENT','CASH_TRANSACTIONS','COMMENT_TEXT','tdc(comment_text)', 'SUBS_DIGITS_TRANSFORM_CAPS_CHAR' from dual union all  
--        select 'INTEGRATION','FATCA_CLSF_EXTRACTS','CORRES_ADDRESS_LINE_1','tdc(corres_address_line_1)', 'SUBS_DIGITS_TRANSFORM_CAPS_CHAR' from dual union all 
--        select 'INTEGRATION','FATCA_CLSF_EXTRACTS','CORRES_ADDRESS_LINE_2','tdc(corres_address_line_2)', 'SUBS_DIGITS_TRANSFORM_CAPS_CHAR' from dual union all 
--        select 'INTEGRATION','FATCA_CLSF_EXTRACTS','CORRES_ADDRESS_LINE_3','tdc(corres_address_line_3)', 'SUBS_DIGITS_TRANSFORM_CAPS_CHAR' from dual union all 
--        select 'INTEGRATION','FATCA_CLSF_EXTRACTS','CORRES_ADDRESS_LINE_5','tdc(corres_address_line_5)', 'SUBS_DIGITS_TRANSFORM_CAPS_CHAR' from dual union all 
--        select 'INTEGRATION','FATCA_CLSF_EXTRACTS','CORRES_ADDRESS_LINE_6','tdc(corres_address_line_6)', 'SUBS_DIGITS_TRANSFORM_CAPS_CHAR' from dual union all 
--        select 'INTEGRATION','FATCA_CLSF_EXTRACTS','CORRES_POST_CODE_LEFT','tdc(corres_post_code_left)', 'SUBS_DIGITS_TRANSFORM_CAPS_CHAR' from dual union all 
--        select 'INTEGRATION','FATCA_CLSF_EXTRACTS','CORRES_POST_CODE_RIGHT','tdc(corres_post_code_right)', 'SUBS_DIGITS_TRANSFORM_CAPS_CHAR' from dual union all 
--        select 'INTEGRATION','FATCA_CLSF_EXTRACTS','DATE_OF_BIRTH','tdc(date_of_birth)', 'SUBS_DIGITS_TRANSFORM_CAPS_CHAR' from dual union all 
--        select 'INTEGRATION','FATCA_CLSF_EXTRACTS','FORENAMES','tdc(forenames)', 'SUBS_DIGITS_TRANSFORM_CAPS_CHAR' from dual union all 
--        select 'INTEGRATION','FATCA_CLSF_EXTRACTS','MANDATE_1_ADDRESS_LINE_1','tdc(mandate_1_address_line_1)', 'SUBS_DIGITS_TRANSFORM_CAPS_CHAR' from dual union all 
--        select 'INTEGRATION','FATCA_CLSF_EXTRACTS','MANDATE_1_ADDRESS_LINE_2','tdc(mandate_1_address_line_2)', 'SUBS_DIGITS_TRANSFORM_CAPS_CHAR' from dual union all 
--        select 'INTEGRATION','FATCA_CLSF_EXTRACTS','MANDATE_1_ADDRESS_LINE_3','tdc(mandate_1_address_line_3)', 'SUBS_DIGITS_TRANSFORM_CAPS_CHAR' from dual union all 
--        select 'INTEGRATION','FATCA_CLSF_EXTRACTS','MANDATE_1_ADDRESS_LINE_4','tdc(mandate_1_address_line_4)', 'SUBS_DIGITS_TRANSFORM_CAPS_CHAR' from dual union all 
--        select 'INTEGRATION','FATCA_CLSF_EXTRACTS','MANDATE_1_ADDRESS_LINE_5','tdc(mandate_1_address_line_5)', 'SUBS_DIGITS_TRANSFORM_CAPS_CHAR' from dual union all 
--        select 'INTEGRATION','FATCA_CLSF_EXTRACTS','MANDATE_1_ADDRESS_LINE_6','tdc(mandate_1_address_line_6)', 'SUBS_DIGITS_TRANSFORM_CAPS_CHAR' from dual union all 
--        select 'INTEGRATION','FATCA_CLSF_EXTRACTS','MANDATE_1_INT_BANK_ACC_NOIBAN','tdc(mandate_1_int_bank_acc_noiban)', 'SUBS_DIGITS_TRANSFORM_CAPS_CHAR' from dual union all 
--        select 'INTEGRATION','FATCA_CLSF_EXTRACTS','MANDATE_1_POST_CODE_LEFT','tdc(mandate_1_post_code_left)', 'SUBS_DIGITS_TRANSFORM_CAPS_CHAR' from dual union all 
--        select 'INTEGRATION','FATCA_CLSF_EXTRACTS','MANDATE_1_POST_CODE_RIGHT','tdc(mandate_1_post_code_right)', 'SUBS_DIGITS_TRANSFORM_CAPS_CHAR' from dual union all 
--        select 'INTEGRATION','FATCA_CLSF_EXTRACTS','NAME','tdc(name)', 'SUBS_DIGITS_TRANSFORM_CAPS_CHAR' from dual union all 
--        select 'INTEGRATION','FATCA_CLSF_EXTRACTS','REGED_ADDRESS_LINE_1','tdc(reged_address_line_1)', 'SUBS_DIGITS_TRANSFORM_CAPS_CHAR' from dual union all 
--        select 'INTEGRATION','FATCA_CLSF_EXTRACTS','REGED_ADDRESS_LINE_3','tdc(reged_address_line_3)', 'SUBS_DIGITS_TRANSFORM_CAPS_CHAR' from dual union all 
--        select 'INTEGRATION','FATCA_CLSF_EXTRACTS','REGED_ADDRESS_LINE_4','tdc(reged_address_line_4)', 'SUBS_DIGITS_TRANSFORM_CAPS_CHAR' from dual union all 
--        select 'INTEGRATION','FATCA_CLSF_EXTRACTS','REGED_ADDRESS_LINE_5','tdc(reged_address_line_5)', 'SUBS_DIGITS_TRANSFORM_CAPS_CHAR' from dual union all 
--        select 'INTEGRATION','FATCA_CLSF_EXTRACTS','REGED_ADDRESS_LINE_6','tdc(reged_address_line_6)', 'SUBS_DIGITS_TRANSFORM_CAPS_CHAR' from dual union all 
--        select 'INTEGRATION','FATCA_CLSF_EXTRACTS','REGED_POSTCODELEFT','tdc(reged_postcodeleft)', 'SUBS_DIGITS_TRANSFORM_CAPS_CHAR' from dual union all 
--        select 'INTEGRATION','FATCA_CLSF_EXTRACTS','REGED_POSTCODERIGHT','tdc(reged_postcoderight)', 'SUBS_DIGITS_TRANSFORM_CAPS_CHAR' from dual union all 
--        select 'INTEGRATION','FATCA_CLSF_EXTRACTS','SURNAME','tdc(surname)', 'SUBS_DIGITS_TRANSFORM_CAPS_CHAR' from dual union all 
--        select 'INTEGRATION','FATCA_CLSF_EXTRACTS','TAX_REF_NO_1','tdc(tax_ref_no_1)', 'SUBS_DIGITS_TRANSFORM_CAPS_CHAR' from dual union all 
--        select 'INTEGRATION','FATCA_CLSF_EXTRACTS','TAX_REF_NO_3','tdc(tax_ref_no_3)', 'SUBS_DIGITS_TRANSFORM_CAPS_CHAR' from dual union all 
--        select 'INTEGRATION','FATCA_CLSF_EXTRACTS','TAX_REF_NO_4','tdc(tax_ref_no_4)', 'SUBS_DIGITS_TRANSFORM_CAPS_CHAR' from dual ;
        --      

      commit;
      exception when others then 
        g_code := SQLCODE;
        g_errm := SUBSTR(SQLERRM, 1 , 4000);
        obfus_log(g_module||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),g_code,g_errm,g_module);
   end merge_patches;
  
  procedure merge_holders
   is
   begin
   
      g_module := 'merge_holders'; 
      --
      merge into tgt_holders th
           using (select comp_code,ivc_code,
                         share_master_holder_code,
                         reference_ivc_code,
                         designation_name,
                         date_of_death,
                         sort_key,
                         previous_sortkey
                  from holders_tmp1) h
              on (h.comp_code= th.comp_code and h.ivc_code= th.ivc_code )
              
      when matched
      then
         update set th.share_master_holder_code = h.share_master_holder_code,
                    th.reference_ivc_code = h.reference_ivc_code,
                    th.designation_name = h.designation_name,
                    th.date_of_death = h.date_of_death,
                    th.sort_key = h.sort_key,
                    th.previous_sortkey = h.previous_sortkey;
         
      commit;
      
      exception when others then 
        g_code := SQLCODE;
        g_errm := SUBSTR(SQLERRM, 1 , 4000);
        obfus_log(g_module||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),g_code,g_errm,g_module);

   end merge_holders;
   
  
procedure merge_holder_names
   is
   begin
      g_module := 'merge_holder_names'; 
      
      merge into tgt_holder_names hn
           using (select holder_name_id,
                         title_type_code,
                         surname,
                         forename,
                         suffix,
                         salutation,
                         other_title,
                         preferred_name,
                         trust_name,
                         trustee_name,
                         company_name
                    from holder_names_tmp1) ahn
              on (ahn.holder_name_id = hn.holder_name_id)
      when matched
      then
         update set hn.title_type_code = ahn.title_type_code,
                    hn.surname = ahn.surname,
                    hn.forename = ahn.forename,
                    hn.suffix = ahn.suffix,
                    hn.salutation = ahn.salutation,
                    hn.other_title = ahn.other_title,
                    hn.preferred_name = ahn.preferred_name,
                    hn.trust_name = ahn.trust_name,
                    hn.trustee_name = ahn.trustee_name,
                    hn.company_name = ahn.company_name;
                    

      --commit is necessary for the get_sort_key_function to work
      commit;

      exception when others then 
        g_code := SQLCODE;
        g_errm := SUBSTR(SQLERRM, 1 , 4000);
        obfus_log(g_module||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),g_code,g_errm,g_module);
   end merge_holder_names;
   
  procedure merge_bank_branches
   is
   begin
   
      g_module := 'merge_bank_branches'; 
      
      merge into tgt_bank_branches bb
             using (select  bank_sort_code,finance_org_id,address_line1,
                    address_line2,address_line3,address_line4,address_line5,
                    address_line6,postcode_left,postcode_right from bank_branches_tmp1) abb
              on (bb.bank_sort_code = abb.bank_sort_code and bb.finance_org_id = abb.finance_org_id)
      when matched
      then
         update set
         bb.address_line1 = abb.address_line1,
         bb.address_line2 = abb.address_line2,
         bb.address_line3 = abb.address_line3,
         bb.address_line4 = abb.address_line4,
         bb.address_line5 = abb.address_line5,
         bb.address_line6 = abb.address_line6,
         bb.postcode_left = abb.postcode_left,
         bb.postcode_right = abb.postcode_right;

      commit;
      
      exception when others then 
        g_code := SQLCODE;
        g_errm := SUBSTR(SQLERRM, 1 , 4000);
        obfus_log(g_module||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),g_code,g_errm,g_module);
   end merge_bank_branches;

   
   procedure merge_bank_accounts
   is
   begin
   
      g_module := 'merge_bank_accounts'; 
      
      merge into tgt_bank_accounts ba
             using (select bank_account_id,
                           account_no,
                           capita_reference,
--                           bacs_code,
--                           iban,
--                           alias_name  ,
                           account_name  , 
--                           branch_name    ,
                           chequeaddress_line1_text,                
                           chequeaddress_line2_text  ,                
                           chequeaddress_line3_text ,                 
                           chequeaddress_line4_text,                 
                           chequeaddress_line5_text ,                
                           chequeaddress_line6_text ,   
                           international_branch_id ,                  
                           international_account_no  
                           --,                                
--                           fax_number ,                 
--                           swift_code 
                    from bank_accounts_tmp1) aba
              on (ba.bank_account_id = aba.bank_account_id)
      when matched
      then
         update set ba.account_no = aba.account_no,
                    ba.capita_reference = aba.capita_reference,
--                    ba.bacs_code = aba.bacs_code,
--                    ba.iban = aba.iban,
--                    ba.alias_name  =aba.alias_name,
                    ba.account_name  =aba.account_name, 
--                    ba.branch_name  = aba.branch_name ,
                    ba.chequeaddress_line1_text =aba.chequeaddress_line1_text,                
                    ba.chequeaddress_line2_text = aba.chequeaddress_line2_text,                
                    ba.chequeaddress_line3_text =aba.chequeaddress_line3_text,                 
                    ba.chequeaddress_line4_text =aba.chequeaddress_line4_text,                 
                    ba.chequeaddress_line5_text =aba.chequeaddress_line5_text,                
                    ba.chequeaddress_line6_text =aba.chequeaddress_line6_text
                    --,   
--                    ba.international_branch_id =aba.international_branch_id,                  
--                    ba.international_account_no  =aba.international_account_no,                                
--                    ba.fax_number =aba.fax_number,                 
--                    ba.swift_code =aba.swift_code
;

      exception when others then 
        g_code := SQLCODE;
        g_errm := SUBSTR(SQLERRM, 1 , 4000);
        obfus_log(g_module||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),g_code,g_errm,g_module);
   end merge_bank_accounts;
   

   procedure merge_holder_address
   is
   begin
   
      g_module := 'merge_holder_address'; 
      
      merge into tgt_holder_addresses ha
           using (select holder_address_id,
                         address_line1,
                         address_line2,
                         address_line3,
                         address_line4,
                         address_line5,
                         address_line6,
                         postcode_left,
                         postcode_right,
                         country_code,
                         irish_distribution_code
                    from holder_addresses_tmp1) aha
              on (aha.holder_address_id = ha.holder_address_id)
      when matched
      then
         update set
            ha.address_line1 = aha.address_line1,
            ha.address_line2 = aha.address_line2,
            ha.address_line3 = aha.address_line3,
            ha.address_line4 = aha.address_line4,
            ha.address_line5 = aha.address_line5,
            ha.address_line6 = aha.address_line6,
            ha.postcode_left = aha.postcode_left,
            ha.postcode_right = aha.postcode_right,
            ha.country_code = aha.country_code,
            ha.irish_distribution_code = aha.irish_distribution_code;
            
      exception when others then 
        g_code := SQLCODE;
        g_errm := SUBSTR(SQLERRM, 1 , 4000);
        obfus_log(g_module||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),g_code,g_errm,g_module);

   end merge_holder_address;

   procedure merge_holder_employee_details
   is
   begin
      
      g_module := 'merge_holder_employee_details'; 
      
      merge into tgt_holder_employee_details hed
           using (select comp_code,
                         ivc_code,
                         national_insurance_no,
                         date_of_birth,
                         personnel_number,
                         payroll_number
                    from holder_employee_details_tmp1) ah
              on (hed.comp_code = ah.comp_code and hed.ivc_code = ah.ivc_code)
      when matched
      then
         update set hed.national_insurance_no = ah.national_insurance_no,
                    hed.date_of_birth = ah.date_of_birth,
                    hed.personnel_number = ah.personnel_number,
                    hed.payroll_number = ah.payroll_number;

      exception when others then 
        g_code := SQLCODE;
        g_errm := SUBSTR(SQLERRM, 1 , 4000);
        obfus_log(g_module||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),g_code,g_errm,g_module);
   end merge_holder_employee_details;

   procedure merge_holder_labels
   is
   begin

      g_module := 'merge_holder_labels'; 
      
      merge into tgt_holder_labels hl
           using (select holder_label_id,label_line_1, label_line_2, label_line_3,label_line_4
                    from  holder_labels_tmp1 ) ahn 
              on (hl.holder_label_id = ahn.holder_label_id)
      when matched
      then
         update set hl.line1_text =  substr(ahn.label_line_1,1,35),
                    hl.line2_text  = substr(ahn.label_line_2,1,35),
                    hl.line3_text =  substr(ahn.label_line_3,1,35),
                    hl.line4_text =  substr(ahn.label_line_4,1,35);

      commit;
      
      exception when others then 
        g_code := SQLCODE;
        g_errm := SUBSTR(SQLERRM, 1 , 4000);
        obfus_log(g_module||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),g_code,g_errm,g_module);
   end merge_holder_labels;

   procedure merge_holder_mandate_details
   is
   begin
   
      g_module := 'merge_holder_mandate_details'; 
      
      merge into tgt_holder_mandates hm
             using (select hmt.holder_mandate_id,
                           hmt.address_line1,
                           hmt.address_line2,
                           hmt.address_line3,
                           hmt.address_line4,
                           hmt.address_line5,
                           hmt.address_line6,
                           hmt.postcode_left,
                           hmt.postcode_right,
                           hmt.irish_distribution_code,
                           hmt.payee_name ,
                           hmt.bank_account_number,
                           hmt.bank_name,
                           hmt.bic_code,
                           hmt.iban_number,
                           hmt.international_account_num,
                           hmt.society_acc_roll_number
                    from  holder_mandates_tmp1 hmt) res
              on (hm.holder_mandate_id = res.holder_mandate_id )
      when matched
      then
         update set 
            hm.address_line1 = res.address_line1,
            hm.address_line2 = res.address_line2,
            hm.address_line3 = res.address_line3,
            hm.address_line4 = res.address_line4,
            hm.address_line5 = res.address_line5,
            hm.address_line6 = res.address_line6,
            hm.postcode_left = res.postcode_left,
            hm.postcode_right = res.postcode_right,
            hm.irish_distribution_code = res.irish_distribution_code,
            hm.payee_name = res.payee_name,
            hm.bank_account_number =res.bank_account_number ,
            hm.bank_name = res.bank_name,
            hm.bic_code= res.bic_code,
            hm.iban_number=res.iban_number,
            hm.international_account_num=res.international_account_num,
            hm.society_acc_roll_number=res.society_acc_roll_number;
   
      exception when others then 
        g_code := SQLCODE;
        g_errm := SUBSTR(SQLERRM, 1 , 4000);
        obfus_log(g_module||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),g_code,g_errm,g_module);
   
   end merge_holder_mandate_details;

  procedure merge_disc_exer_req_mandates
  is
   begin
   
      g_module := 'merge_disc_exer_req_mandates';
      
      merge into tgt_disc_exer_req_mandates tgt
      using (select disc_exer_req_mandate_id ,bank_acc_no,society_acc_roll_no,                    
                    bank_id_code_bic,int_bank_acc_no_iban,int_acc_no,address_line_1,                    
                    address_line_2,address_line_3,address_line_6,country_code                   
             from  disc_exer_req_mandates desd ) res 
      on (tgt.disc_exer_req_mandate_id = res.disc_exer_req_mandate_id) 
      when matched then update
          set tgt.bank_acc_no = res.bank_acc_no,
                tgt.society_acc_roll_no = res.society_acc_roll_no,
                tgt.bank_id_code_bic = res.bank_id_code_bic,
                tgt.int_bank_acc_no_iban = res.int_bank_acc_no_iban,
                tgt.int_acc_no = res.int_acc_no,
                tgt.address_line_1 = res.address_line_1,
                tgt.address_line_2 = res.address_line_2, 
                tgt.address_line_3 = res.address_line_3,   
                tgt.address_line_6 = res.address_line_6,   
                tgt.country_code = res.country_code;                
                
    exception when others then 
      g_code := SQLCODE;
      g_errm := SUBSTR(SQLERRM, 1 , 4000);
      obfus_log(g_module||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),g_code,g_errm,g_module);
  end merge_disc_exer_req_mandates;
  

  procedure merge_disc_exer_spouse_dtls
   is
   begin
      g_module := 'merge_disc_exer_spouse_dtls';
      
      merge into tgt_disc_exer_spouse_dtls tgt
      using (select discret_exercise_req_id,surname,forenames,title,                            
             address_line_1,address_line_2,address_line_3,address_line_4,                   
             address_line_5,address_line_6,post_code_left,post_code_right,country_code                    
             from  disc_exer_spouse_dtls desd ) res 
      on (tgt.discret_exercise_req_id = res.discret_exercise_req_id) 
      when matched then update
          set tgt.surname = res.surname,
                tgt.forenames = res.forenames,
                tgt.title = res.title,
                tgt.address_line_1 = res.address_line_1,
                tgt.address_line_2 = res.address_line_2,
                tgt.address_line_3 = res.address_line_3,
                tgt.address_line_4 = res.address_line_4,
                tgt.address_line_5 = res.address_line_5,
                tgt.address_line_6 = res.address_line_6, 
                tgt.post_code_left = res.post_code_left,   
                tgt.post_code_right = res.post_code_right,   
                tgt.country_code = res.country_code;                
                
    exception when others then 
      g_code := SQLCODE;
      g_errm := SUBSTR(SQLERRM, 1 , 4000);
      obfus_log(g_module||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),g_code,g_errm,g_module);
  end merge_disc_exer_spouse_dtls;
  
  procedure merge_mifid_entities
   is
   begin
   
      g_module := 'merge_mifid_entities';
      
      merge into tgt_mifid_entities tgt
      using (select mifid_entity_id,surname,forenames,register_surname,                            
             register_forenames                    
             from  mifid_entities me ) res 
      on (tgt.mifid_entity_id = res.mifid_entity_id) 
      when matched then update
          set   tgt.surname = res.surname,
                tgt.forenames = res.forenames,
                tgt.register_surname = res.register_surname,
                tgt.register_forenames = res.register_forenames;
        
    exception when others then 
      g_code := SQLCODE;
      g_errm := SUBSTR(SQLERRM, 1 , 4000);
      obfus_log(g_module||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),g_code,g_errm,g_module);
  end merge_mifid_entities;

  procedure merge_cash_ivc_class_copies
   is
   begin
   
      g_module := 'merge_cash_ivc_class_copies';
         
      merge into tgt_cash_ivc_class_copies tgt
      using (select cash_ivc_class_copy_id,bank_name,sort_code,country,account_no,building_society_acc_no,
                    bic,international_acc_no,branch_name,international_branch_id,iban,society_acc_roll_no,
                    address_line_1,address_line_2,address_line_3,address_line_4,address_line_5,address_line_6,
                    post_code_left,post_code_right,irish_distribution_code               
             from  cash_ivc_class_copies_tmp1 cicc ) res 
      on (tgt.cash_ivc_class_copy_id = res.cash_ivc_class_copy_id) 
      when matched then update
          set tgt.bank_name = res.bank_name,
          tgt.sort_code = res.sort_code,
          tgt.country  = res.country,
          tgt.account_no  = res.account_no,
          tgt.building_society_acc_no  = res.building_society_acc_no,
          tgt.bic  =  res.bic,
          tgt.international_acc_no =  res.international_acc_no,
          tgt.branch_name =  res.branch_name,
          tgt.international_branch_id =  res.international_branch_id,
          tgt.iban = res.iban,
          tgt.society_acc_roll_no = society_acc_roll_no,
          tgt.address_line_1  = res.address_line_1,
          tgt.address_line_2  = res.address_line_2,
          tgt.address_line_3  = res.address_line_3,
          tgt.address_line_4  = res.address_line_4,
          tgt.address_line_5  = res.address_line_5,
          tgt.address_line_6  = res.address_line_6,
          tgt.post_code_left  = res.post_code_left,
          tgt.post_code_right  = res.post_code_right,
          tgt.irish_distribution_code  = res.irish_distribution_code  ;
                              
    exception when others then 
      g_code := SQLCODE;
      g_errm := SUBSTR(SQLERRM, 1 , 4000);
      obfus_log(g_module||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),g_code,g_errm,g_module);
  end merge_cash_ivc_class_copies;
  
    procedure merge_comp_payee_mandates
   is
   begin
   
      g_module := 'merge_comp_payee_mandates';
         
      merge into tgt_comp_payee_mandates tgt
      using (select comp_payee_id,mandate_type_id,country_code,
                    currency_code,bank_sort_code,building_society_branch_id,
                    bank_account_number,international_account_num,account_reference,
                    society_acc_roll_number,bic_code,iban_number,bank_name,
                    address_line1,address_line2,address_line3,address_line4,
                    address_line5,address_line6,post_code_left,post_code_right,irish_distribution_code,
                    created_by,created_date,modified_by,mod_timestamp
             from  comp_payee_mandates_tmp1 cpm ) res 
      on (tgt.comp_payee_id = res.comp_payee_id) 
      when matched then update
          set tgt.mandate_type_id = tgt.mandate_type_id,
              tgt.country_code  = tgt.country_code,
              tgt.currency_code  =  tgt.currency_code,
              tgt.bank_sort_code  = tgt.bank_sort_code,
              tgt.building_society_branch_id  = tgt.building_society_branch_id,
              tgt.bank_account_number   =  tgt.bank_account_number,
              tgt.international_account_num  = tgt.international_account_num,
              tgt.account_reference  = tgt.account_reference,
              tgt.society_acc_roll_number  = tgt.society_acc_roll_number,
              tgt.bic_code  = tgt.bic_code,
              tgt.iban_number  = tgt.iban_number,
              tgt.bank_name  = tgt.bank_name,
              tgt.address_line1  = tgt.address_line1,
              tgt.address_line2  = tgt.address_line2,
              tgt.address_line3  = tgt.address_line3,
              tgt.address_line4  = tgt.address_line4,
              tgt.address_line5  = tgt.address_line5,
              tgt.address_line6  = tgt.address_line6,
              tgt.post_code_left  = tgt.post_code_left,
              tgt.post_code_right  = tgt.post_code_right,
              tgt.irish_distribution_code  = tgt.irish_distribution_code,
              tgt.created_by  = tgt.created_by,
              tgt.created_date  = tgt.created_date,
              tgt.modified_by  = tgt.modified_by,
              tgt.mod_timestamp  = tgt.mod_timestamp;
                              
    exception when others then 
      g_code := SQLCODE;
      g_errm := SUBSTR(SQLERRM, 1 , 4000);
      obfus_log(g_module||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),g_code,g_errm,g_module);
  end merge_comp_payee_mandates;

  
  procedure merge_mifid_integration
   is
   begin
   
      g_module := 'merge_mifid_integration';
      
      merge into tgt_mifid_transaction_details tgt
      using (select transaction_detail_id,holder_surname                   
             from  mifid_transaction_details mtd ) res 
      on (tgt.transaction_detail_id = res.transaction_detail_id) 
      when matched then update
          set tgt.holder_surname = res.holder_surname;
              
      merge into tgt_mifid_bulk_trades tgt
      using (select bulk_trade_id,holder_surname                   
             from   mifid_bulk_trades mbt ) res 
      on (tgt.bulk_trade_id = res.bulk_trade_id) 
      when matched then update
          set tgt.holder_surname = res.holder_surname;  
                
    exception when others then 
      g_code := SQLCODE;
      g_errm := SUBSTR(SQLERRM, 1 , 4000);
      obfus_log(g_module||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),g_code,g_errm,g_module);
  end merge_mifid_integration;

   procedure merge_cheque_ranges
   is
   begin
   
      g_module := 'merge_cheque_ranges'; 
      
      merge into tgt_cheque_ranges tgt
      using (select crt.cheque_range_id,crt.end_no,crt.last_cheque_no_used,crt.start_no,crt.warning_threshold 
            from  cheque_ranges_tmp1 crt ) res 
      on (tgt.cheque_range_id = res.cheque_range_id ) 
      when matched then update
          set tgt.end_no = res.end_no,
                tgt.last_cheque_no_used = res.last_cheque_no_used,
                tgt.start_no = res.start_no,
                tgt.warning_threshold = res.warning_threshold;                
                
      exception when others then 
        g_code := SQLCODE;
        g_errm := SUBSTR(SQLERRM, 1 , 4000);
        obfus_log(g_module||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),g_code,g_errm,g_module);
  end merge_cheque_ranges;
  
   procedure merge_payments
   is
   begin
   
      g_module := 'merge_payments'; 
   
       merge into tgt_payments tp
           using (select py.payment_id,
                         py.dispatch_address_line1,
                         py.dispatch_address_line2,
                         py.dispatch_address_line3,
                         py.dispatch_address_line4,
                         py.dispatch_address_line5,
                         py.dispatch_address_line6,
                         py.postcode_left,
                         py.postcode_right,
                         py.irish_distribution_code, 
                         py.payee_chequeaddress_line1,
                         py.payee_chequeaddress_line2,
                         py.payee_chequeaddress_line3,
                         py.payee_chequeaddress_line4,
                         py.payee_chequeaddress_line5,
                         py.payee_chequeaddress_line6,
                         py.cheque_no,
                         py.payee_name1,
                         py.payee_name2,
                         py.payee_name3,
                         py.payee_name4,
                         py.payee_name5,
                         py.dispatch_name1,
                         py.dispatch_name2,
                         py.payee_building_soc_acc_no,
                         py.payee_building_soc_roll_no,
                         py.payee_internat_account_no,
                         tdc(py.payment_reference) payment_reference,
                         substr(rn(py.fx_notes),1,255) fx_notes,
                         substr(rn(py.fx_comments),1,255) fx_comments,
                         substr(rn(py.comment_text),1,255) comment_text,
                         tdc(py.parent_cheque_number) parent_cheque_number,
                         tdc(py.payer_alias) payer_alias,
                         py.sibling_account_number,
                         py.parent_account_number,
                         py.payer_account_number,
                         py.payer_bic,
                         py.payer_iban,
                         py.payer_sortcode,
                         tdc(py.fx_client_ref)fx_client_ref,                    
                         tdc(py.fx_settlement_account_number) fx_settlement_account_number, 
                         tdc(py.payment_ptcp_id)	 payment_ptcp_id, 
                         tdc(py.sibling_cheque_number)	 sibling_cheque_number, 
                         tdc(py.swift_code)	swift_code
                    from payments_tmp1 py) res
              on (res.payment_id = tp.payment_id)
      when matched
      then
         update set
            dispatch_address_line1 =  res.dispatch_address_line1,
            dispatch_address_line2 =  res.dispatch_address_line2,
            dispatch_address_line3 =  res.dispatch_address_line3,
            dispatch_address_line4 =  res.dispatch_address_line4,
            dispatch_address_line5 =  res.dispatch_address_line5,
            dispatch_address_line6 =  res.dispatch_address_line6,
            dispatch_name1 =  res.dispatch_name1,
            dispatch_name2 =  res.dispatch_name2,
            payment_reference = res.payment_reference,
            fx_notes = res.fx_notes,
            fx_comments  = res.fx_comments,
            postcode_left = res.postcode_left,
            postcode_right = res.postcode_right,
            irish_distribution_code = res.irish_distribution_code,
            payee_chequeaddress_line1 = res.payee_chequeaddress_line1,
            payee_chequeaddress_line2  = res.payee_chequeaddress_line2,
            payee_chequeaddress_line3 = res.payee_chequeaddress_line3,
            payee_chequeaddress_line4 = res.payee_chequeaddress_line4,
            payee_chequeaddress_line5 = res.payee_chequeaddress_line5,
            payee_chequeaddress_line6 = res.payee_chequeaddress_line6,
            cheque_no =res.cheque_no,
            payee_name1=res.payee_name1,
            payee_name2=res.payee_name2,
            payee_name3=res.payee_name3,
            payee_name4=res.payee_name4,
            payee_name5=res.payee_name5,
            payee_building_soc_acc_no=res.payee_building_soc_acc_no,
            payee_building_soc_roll_no=res.payee_building_soc_roll_no,
            payee_internat_account_no=res.payee_internat_account_no,
            comment_text = res.comment_text,
            parent_cheque_number = res.parent_cheque_number,
            payer_alias = res.payer_alias,
            sibling_account_number = res.sibling_account_number,
            parent_account_number = res.parent_account_number,
            payer_account_number = res.payer_account_number,
            payer_bic = res.payer_bic,
            payer_iban = res.payer_iban,
            payer_sortcode = res.payer_sortcode,
            fx_client_ref = res.fx_client_ref, 
            fx_settlement_account_number = res.fx_settlement_account_number,
            payment_ptcp_id = res.payment_ptcp_id,
            sibling_cheque_number = res.sibling_cheque_number,
            swift_code  = res.swift_code;
            
      commit;
      
  exception
     when others then
        g_code := SQLCODE;
        g_errm := SUBSTR(SQLERRM, 1 , 4000);     
        obfus_log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),g_code,g_errm,g_module);   
  end merge_payments;
    
  procedure reset_synonyms is
  begin
  
      g_module := 'reset_synonyms'; 
      
--      execute immediate 'create or replace synonym holders for '||g_src_prefix||'_prism_core.holders';
--      execute immediate 'create or replace synonym holder_employee_details for '||g_src_prefix||'_prism_core.holder_employee_details';
--      execute immediate 'create or replace synonym bank_accounts for '|| g_src_prefix||'_PRISM_CORE.bank_accounts';  
--      execute immediate 'create or replace synonym holder_names for '|| g_src_prefix||'_PRISM_CORE.holder_names';  
--      execute immediate 'create or replace synonym holder_labels  for '|| g_src_prefix||'_PRISM_CORE.holder_labels';  
--      execute immediate 'create or replace synonym holder_addresses  for '|| g_src_prefix||'_PRISM_CORE.holder_addresses';   
--      execute immediate 'create or replace synonym bank_branches  for '|| g_src_prefix||'_PRISM_CORE.bank_branches';   
--      execute immediate 'create or replace synonym holder_mandates  for '|| g_src_prefix||'_PRISM_CORE.holder_mandates';  
--      execute immediate 'create or replace synonym payments  for '||g_src_prefix||'_cash_management.payments';   
--      execute immediate 'create or replace synonym cheque_ranges  for '||g_src_prefix||'_cash_management.cheque_ranges';   
  end reset_synonyms;

  procedure apply_temp_patches is
     v_sql varchar2(2000);
  begin
     g_module := 'apply_temp_patches';   
     
     begin
        execute immediate 'CREATE SEQUENCE Prevent_UK_Exceptions_SEQ MINVALUE 1 MAXVALUE 999999 INCREMENT BY 1 START WITH 1 CACHE 999 CYCLE';
     exception
        when others then
           null; 
     end;

     begin
        v_sql := 'merge into '||g_tgt_prefix||'_PRISM_CORE.EMPLOYING_COMP_TAX_DETAILS tgt
                  using (SELECT * FROM '||g_run_env||'.EMPLOYING_COMP_TAX_DETAILS_TMP
                         where DISPLAY_EMPLOYER_NAME IN ( select DISPLAY_EMPLOYER_NAME
                                                            from (
                                                                   select DISPLAY_EMPLOYER_NAME,count(*) 
                                                                     from EMPLOYING_COMP_TAX_DETAILS_tmp 
                                                                   group by DISPLAY_EMPLOYER_NAME having count(*) > 1))) tmp
                    on (tgt.tax_detail_id = tmp.tax_detail_id)       
                  when matched then update set tgt.DISPLAY_EMPLOYER_NAME = SUBSTR(tmp.DISPLAY_EMPLOYER_NAME || Prevent_UK_Exceptions_SEQ.NEXTVAL,1,240)';     

        execute immediate v_sql;          

        anonymisation_process.obfus_log(SQL%ROWCOUNT || ' rows merged into '||g_tgt_prefix||'_PRISM_CORE.EMPLOYING_COMP_TAX_DETAILS: ' || v_sql,null,null,g_module);

     exception
        when others then
           g_code := SQLCODE;
           g_errm := SUBSTR(SQLERRM, 1 , 4000);
           obfus_log('Error merging into '||g_tgt_prefix||'_PRISM_CORE.EMPLOYING_COMP_TAX_DETAILS: ' || v_sql,g_code,g_errm,g_module);        
     end;
  end apply_temp_patches;

end anonymisation_process;