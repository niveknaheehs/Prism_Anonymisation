create or replace PACKAGE BODY anonymisation_process is

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

  procedure  set_globals ( p_obfus_run_id in number,
                           p_src_prefix   in varchar2,
                           p_tgt_prefix   in varchar2,
                           p_run_env      in varchar2,
                           p_anon_version in varchar2 )
  as
  begin
      g_obfus_run_id  := p_obfus_run_id;
      g_src_prefix    := p_src_prefix;
      g_tgt_prefix    := p_tgt_prefix;
      g_run_env       := p_run_env;
      g_anon_version  := p_anon_version;
      --select count(rnd_words) into g_max_rnd_note  from randomised_notes;
      select max(key_ns) into anonymisation_process.g_max_rnd_address_line_1_seq from  address_line_1_rnd;
      select max(key_ns) into anonymisation_process.g_max_rnd_address_line_2_seq from  address_line_2_rnd;
      select max(key_ns) into anonymisation_process.g_max_rnd_address_line_3_seq from  address_line_3_rnd;
      select max(key_ns) into anonymisation_process.g_max_rnd_postcode_seq from  postcode_rnd;
      select max(surname_seq) into anonymisation_process.g_max_rnd_surname_seq from  surname_list;
      select max(key_ns) into anonymisation_process.g_max_rnd_forename_seq from  forename_list;
  end set_globals;

  procedure reset_sequence (p_seq_name varchar2, p_reset_to number default 1)
  as
    v_nextval  number;
    v_sql_stmt varchar2(4000);
    v_seq_name varchar2(30);
  begin
     v_seq_name := p_seq_name;
     begin
        g_run_date := sysdate;
        g_module := 'reset_sequence';

        v_sql_stmt := 'select ' || v_seq_name || '.NEXTVAL from dual';
        execute immediate v_sql_stmt into v_nextval;

        if v_nextval > 1 then
          v_sql_stmt := 'ALTER SEQUENCE ' || v_seq_name || ' INCREMENT BY ' || -(v_nextval-1);
          execute immediate v_sql_stmt;
  
          v_sql_stmt := 'select ' || v_seq_name || '.NEXTVAL from dual';
          execute immediate v_sql_stmt into v_nextval;
        end if;

        v_sql_stmt := 'ALTER SEQUENCE ' || v_seq_name || ' INCREMENT BY 1';
        execute immediate v_sql_stmt;

     exception when others then
        g_code := SQLCODE;
        g_errm := SUBSTR(SQLERRM, 1 , 4000);
        obfus_log('reset_sequence'||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),g_code,g_errm,g_module);
        raise;
     end;
  end reset_sequence;

  procedure anon_manual (p_stage IN NUMBER)
  is
  begin
     begin
        g_module := 'anon_manual';
     end;
  end anon_manual;

procedure anon_disc_exer_req_mandates is
begin
  begin

    g_module := 'anon_disc_exer_req_mandates';

    obfus_log('1: truncate table rnd_derm_adresss_lu',null,null,g_module);

    execute immediate 'truncate table rnd_derm_adresss_lu';

    obfus_log('2: insert into rnd_derm_adresss_lu',null,null,g_module);

    execute immediate 'insert into rnd_derm_adresss_lu(disc_exer_req_mandate_id,address_line1_key,address_line2_key,address_line3_key)
    select disc_exer_req_mandate_id,
           mod(abs(dbms_random.random),' || anonymisation_process.g_max_rnd_address_line_1_seq || ') +1 address_line1_key ,
           mod(abs(dbms_random.random),' || anonymisation_process.g_max_rnd_address_line_2_seq || ') +1 address_line2_key,
           mod(abs(dbms_random.random),' || anonymisation_process.g_max_rnd_address_line_3_seq || ') +1 address_line3_key
    from disc_exer_req_mandates' ;

    begin
      execute immediate 'drop index rnd_derm_adresss_lu_idx1';
      exception when others then null;
    end;

    obfus_log('3: create index rnd_derm_adresss_lu_idx1',null,null,g_module);

    execute immediate 'create index rnd_derm_adresss_lu_idx1 on rnd_derm_adresss_lu(disc_exer_req_mandate_id)';

    obfus_log('4: reset_sequences ns1 and ns2',null,null,g_module);

    anonymisation_process.reset_sequence('ns1');
    anonymisation_process.reset_sequence('ns2');

    obfus_log('5: truncate table disc_exer_req_mandates_tmp1',null,null,g_module);

    execute immediate 'truncate table disc_exer_req_mandates_tmp1';

    obfus_log('6: insert into disc_exer_req_mandates_tmp1',null,null,g_module);

    execute immediate ' insert into disc_exer_req_mandates_tmp1 ( disc_exer_req_mandate_id,discret_exercise_req_id,mandate_type_id,
                bank_acc_no,society_acc_roll_no ,
                bank_sort_code,bank_name ,bank_id_code_bic,int_bank_acc_no_iban,int_acc_no ,
                address_line_1, address_line_2, address_line_3, address_line_6, country_code)
    select  derm.disc_exer_req_mandate_id,derm.discret_exercise_req_id,derm.mandate_type_id,
            case when bank_acc_no is not null then lpad(ns1.nextval, 10, 0) else null end bank_acc_no,
            ut.tdc(derm.society_acc_roll_no),derm.bank_sort_code,bank_name,ut.tdc(derm.bank_id_code_bic),ut.tdc(derm.int_bank_acc_no_iban),
            case when derm.int_acc_no is not null then lpad(ns2.nextval, 10, 0) else null end int_acc_no,
            SUBSTR(addr1.address_line_1,1,35),SUBSTR(addr2.address_line_2,1,35),SUBSTR(addr3.address_line_3,1,35) ,null address_line_6, ''GB'' country_code
    from disc_exer_req_mandates derm
    left join rnd_derm_adresss_lu rn_addr on rn_addr.disc_exer_req_mandate_id = derm.DISCRET_EXERCISE_REQ_ID
    left join address_line_1_rnd addr1 on addr1.key_ns  = rn_addr.address_line1_key
    left join address_line_2_rnd addr2 on addr2.key_ns  = rn_addr.address_line2_key
    left join address_line_3_rnd addr3 on addr3.key_ns  = rn_addr.address_line3_key';

    commit;

    exception
       when others then
          g_code := SQLCODE;
          g_errm := SUBSTR(SQLERRM, 1 , 4000);
          obfus_log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),g_code,g_errm,g_module);
          RAISE;
  end;
end anon_disc_exer_req_mandates;


procedure anon_disc_exer_spouse_dtls is
  v_sql varchar2(4000);
begin

  begin

    g_module := 'anon_disc_exer_spouse_dtls';

    obfus_log('1: truncate table  rnd_adesd_name_lookup',null,null,g_module);
    execute immediate 'truncate table  rnd_adesd_name_lookup';

    obfus_log('2: insert into rnd_adesd_name_lookup',null,null,g_module);
    
    execute immediate 'insert into rnd_adesd_name_lookup (discret_exercise_req_id,surname_key ,forename_key )
    select discret_exercise_req_id, mod(abs(dbms_random.random),'|| anonymisation_process.g_max_rnd_surname_seq ||') +1 surname_key,
           mod(abs(dbms_random.random),'|| anonymisation_process.g_max_rnd_forename_seq ||') +1 forename_key
    from disc_exer_spouse_dtls';

    begin
      execute immediate 'drop index rnd_adesd_name_idx1';
      exception when others then null;
    end;

    obfus_log('3: create index rnd_adesd_name_idx1',null,null,g_module);
    execute immediate 'create index rnd_adesd_name_idx1 on rnd_adesd_name_lookup(discret_exercise_req_id)';
--    Match on address_type 4 if no match get from seed_address

    obfus_log('4: truncate table rnd_adesd_adresss_lu',null,null,g_module);
    execute immediate 'truncate table rnd_adesd_adresss_lu';

    obfus_log('5: insert into rnd_adesd_adresss_lu',null,null,g_module);
    
    execute immediate 'insert into rnd_adesd_adresss_lu(discret_exercise_req_id,address_line1_key,address_line2_key,address_line3_key,post_code_key)
    select discret_exercise_req_id,
           mod(abs(dbms_random.random),'|| anonymisation_process.g_max_rnd_address_line_1_seq ||')+1 address_line1_key ,
           mod(abs(dbms_random.random),'|| anonymisation_process.g_max_rnd_address_line_2_seq ||')+1 address_line2_key,
           mod(abs(dbms_random.random),'|| anonymisation_process.g_max_rnd_address_line_3_seq ||')+1 address_line3_key,
           mod(abs(dbms_random.random),'|| anonymisation_process.g_max_rnd_postcode_seq ||')      +1 post_code_key
    from disc_exer_spouse_dtls ';

    begin
      execute immediate 'drop index rnd_adesd_adresss_lu_idx1';
      exception when others then null;
    end;

    obfus_log('6: create index rnd_adesd_adresss_lu_idx1',null,null,g_module);
    execute immediate 'create index rnd_adesd_adresss_lu_idx1 on rnd_adesd_adresss_lu(discret_exercise_req_id)';

    obfus_log('7: truncate table disc_exer_spouse_dtls_tmp1',null,null,g_module);
    execute immediate 'truncate table disc_exer_spouse_dtls_tmp1';

    obfus_log('8: insert into disc_exer_spouse_dtls_tmp1',null,null,g_module);

    v_sql := 'insert into disc_exer_spouse_dtls_tmp1 ( discret_exercise_req_id, surname, forenames, title,
        address_line_1, address_line_2, address_line_3, address_line_4, address_line_5, address_line_6,
        post_code_left, post_code_right, country_code)
    select desd.discret_exercise_req_id,nvl(hn_surname.surname,''Smith''),
        case when fl_forename.name is null
          then case when fl_forename.gender = ''M'' then ''John'' else ''Jane'' end
        else fl_forename.name end forenames,
        case when fl_forename.gender = ''M'' then ''MR'' else ''MRS'' end title,
        addr1.address_line_1,substr(addr2.address_line_2,1,35),addr3.address_line_3,null address_line_4,null address_line_5, null address_line_6,
        postcode.postcode_left post_code_left, postcode.postcode_right post_code_right, ''GB'' country_code
    from disc_exer_spouse_dtls desd
    left join rnd_adesd_adresss_lu rn_addr on rn_addr.discret_exercise_req_id = desd.discret_exercise_req_id
    left join address_line_1_rnd addr1 on addr1.key_ns  = rn_addr.address_line1_key
    left join address_line_2_rnd addr2 on addr2.key_ns  = rn_addr.address_line2_key
    left join address_line_3_rnd addr3 on addr3.key_ns  = rn_addr.address_line3_key
    left join postcode_rnd postcode on postcode.key_ns  = rn_addr.post_code_key
    left join  rnd_adesd_name_lookup rn_name on rn_name.discret_exercise_req_id = desd.discret_exercise_req_id
    left join  surname_list fl_surname on fl_surname.surname_seq = rn_name.surname_key
    left join holder_names_tmp1 hn_surname on hn_surname.holder_name_id = fl_surname.holder_name_id and hn_surname.holder_seq = 1
    left join  forename_list fl_forename on fl_forename.key_ns = rn_name.forename_key';
    
    obfus_log('Executing: ' || v_sql,null,null,g_module);
    execute immediate v_sql;

    commit;

    exception
       when others then
          g_code := SQLCODE;
          g_errm := SUBSTR(SQLERRM, 1 , 4000);
          obfus_log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),g_code,g_errm,g_module);
          RAISE;
  end;

end anon_disc_exer_spouse_dtls;


procedure anon_cash_ivc_class_copies  is
begin

    g_module := 'anon_cash_ivc_class_copies';

    obfus_log('1: truncate table  cash_ivc_class_copies_tmp1',null,null,g_module);
    execute immediate 'truncate table cash_ivc_class_copies_tmp1';

    obfus_log('1: insert into cash_ivc_class_copies_tmp1',null,null,g_module);

    execute immediate ' insert into cash_ivc_class_copies_tmp1 (cash_ivc_class_copy_id,payee_name,
    address_line_1,address_line_2,address_line_3,address_line_4,address_line_5,address_line_6,
    post_code_left,post_code_right,irish_distribution_code)

    select cicc.cash_ivc_class_copy_id,hm.payee_name,
    hm.address_line1,hm.address_line2,hm.address_line3,hm.address_line4,hm.address_line5,hm.address_line6,
    hm.postcode_left,hm.postcode_right,hm.irish_distribution_code
    from cash_ivc_class_copies cicc
    left outer join cash_accounts ca on ca.CASH_ACCOUNT_ID  = cicc.CASH_ACC_ID
    left outer join holder_mandates_tmp1 hm on ca.comp_code = hm.comp_code and hm.ivc_code = ca.ivc_code  and nvl(hm.class_code,-1) = nvl(cicc.class_code,-1)  and hm.mandate_type_id = cicc.mandate_type_id
    left outer join building_society_branches bsb on bsb.building_society_branch_id = hm.building_society_branch_id';

exception
   when others then
      g_code := SQLCODE;
      g_errm := SUBSTR(SQLERRM, 1 , 4000);
      obfus_log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),g_code,g_errm,g_module);
      RAISE;
end anon_cash_ivc_class_copies;


procedure anon_comp_payee_mandates  is
begin
  begin

    g_module := 'anon_comp_payee_mandates';

    obfus_log('1: truncate table rnd_cpm_adresss_lu',null,null,g_module);

    execute immediate 'truncate table rnd_cpm_adresss_lu';

    obfus_log('2: insert into rnd_cpm_adresss_lu',null,null,g_module);

    execute immediate 'insert into rnd_cpm_adresss_lu(comp_payee_id,address_line1_key,address_line2_key,address_line3_key,post_code_key)
    select comp_payee_id,
           mod(abs(dbms_random.random),'|| anonymisation_process.g_max_rnd_address_line_1_seq || ') +1 address_line1_key ,
           mod(abs(dbms_random.random),'|| anonymisation_process.g_max_rnd_address_line_2_seq || ') +1 address_line2_key,
           mod(abs(dbms_random.random),'|| anonymisation_process.g_max_rnd_address_line_3_seq || ') +1 address_line3_key ,
           mod(abs(dbms_random.random),'|| anonymisation_process.g_max_rnd_postcode_seq || ') +1 post_code_key
    from comp_payee_mandates';

    obfus_log('3: drop index rnd_cpm_adresss_lu_idx1',null,null,g_module);
    begin
      execute immediate 'drop index rnd_cpm_adresss_lu_idx1';
      exception when others then null;
    end;

    obfus_log('4: create index rnd_cpm_adresss_lu_idx1',null,null,g_module);
    execute immediate 'create index rnd_cpm_adresss_lu_idx1 on rnd_cpm_adresss_lu(comp_payee_id)';

    obfus_log('5: reset sequences ns1 and ns2',null,null,g_module);
    anonymisation_process.reset_sequence('ns1');
    anonymisation_process.reset_sequence('ns2');

    obfus_log('6: truncate table comp_payee_mandates_tmp1',null,null,g_module);
    execute immediate 'truncate table comp_payee_mandates_tmp1';

    obfus_log('7: insert into comp_payee_mandates_tmp1',null,null,g_module);

    execute immediate 'insert into comp_payee_mandates_tmp1 (comp_payee_id,mandate_type_id,address_line1,address_line2,address_line3,
                                          address_line4,address_line5,address_line6,post_code_left,post_code_right,
                                          irish_distribution_code)
    select  cpm.comp_payee_id,cpm.mandate_type_id,
            addr1.address_line_1,addr2.address_line_2,addr3.address_line_3, null address_line4,null address_line5,null address_line6,
            postcode.postcode_left,postcode.postcode_right, null irish_distribution_code
    from comp_payee_mandates cpm
    left join rnd_cpm_adresss_lu rn_addr on rn_addr.comp_payee_id = cpm.comp_payee_id
    left join address_line_1_rnd addr1 on addr1.key_ns  = rn_addr.address_line1_key
    left join address_line_2_rnd addr2 on addr2.key_ns  = rn_addr.address_line2_key
    left join address_line_3_rnd addr3 on addr3.key_ns  = rn_addr.address_line3_key
    left join postcode_rnd postcode on postcode.key_ns  = rn_addr.post_code_key';

    commit;

  exception
    when others then
       g_code := SQLCODE;
       g_errm := SUBSTR(SQLERRM, 1 , 4000);
       obfus_log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),g_code,g_errm,g_module);
       RAISE;
  end;
end anon_comp_payee_mandates;

procedure anon_mifid_entities is
begin

  begin

    g_module := 'anon_mifid_entities';

    obfus_log('1: truncate table rnd_me_name_lookup',null,null,g_module);

    execute immediate 'truncate table rnd_me_name_lookup';

    obfus_log('2: insert into rnd_me_name_lookup',null,null,g_module);

    execute immediate 'insert into rnd_me_name_lookup (mifid_entity_id,surname_key ,forename_key )
    select discret_exercise_req_id, mod(abs(dbms_random.random),'|| anonymisation_process.g_max_rnd_surname_seq ||') +1 surname_key, 
           mod(abs(dbms_random.random),'|| anonymisation_process.g_max_rnd_forename_seq ||') +1 forename_key
    from disc_exer_spouse_dtls';

    obfus_log('3: drop index rnd_me_name_idx1',null,null,g_module);

    begin
      execute immediate 'drop index rnd_me_name_idx1';
      exception when others then null;
    end;

    obfus_log('4: create index rnd_me_name_idx1 on rnd_me_name_lookup(mifid_entity_id)',null,null,g_module);

    execute immediate 'create index rnd_me_name_idx1 on rnd_me_name_lookup(mifid_entity_id)';
--    Match on address_type 4 if no match get from seed_address

    obfus_log('5: truncate table mifid_entities_tmp1',null,null,g_module);

    execute immediate 'truncate table mifid_entities_tmp1';

    obfus_log('6: insert into mifid_entities_tmp1',null,null,g_module);

    execute immediate 'insert into mifid_entities_tmp1 ( mifid_entity_id	,forenames,surname,register_forenames,register_surname)
    select me.mifid_entity_id,fl_forename.name,hn_surname.surname,fl_forename.name register_forenames,hn_surname.surname
      from mifid_entities me
    left join rnd_me_name_lookup rn_name on rn_name.mifid_entity_id = me.mifid_entity_id
    left join surname_list fl_surname on fl_surname.surname_seq = rn_name.surname_key
    left join holder_names_tmp1 hn_surname on hn_surname.holder_name_id = fl_surname.holder_name_id and hn_surname.holder_seq = 1
    left join forename_list fl_forename on fl_forename.key_ns = rn_name.forename_key';

    commit;

    exception
       when others then
          g_code := SQLCODE;
          g_errm := SUBSTR(SQLERRM, 1 , 4000);
          obfus_log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),g_code,g_errm,g_module);
          RAISE;
  end;

end anon_mifid_entities;


procedure anon_mifid_trans_details is
begin

  begin

    g_module := 'anon_mifid_trans_details';

    obfus_log('1: truncate table rnd_mi_name_lookup',null,null,g_module);

    begin
      execute immediate 'truncate table rnd_mi_name_lookup';
      exception when others then null;
    end;

    obfus_log('2: insert into rnd_mi_name_lookup ',null,null,g_module);

    execute immediate 'insert into rnd_mi_name_lookup (transaction_detail_id,bulk_trade_id,surname_key)
    select transaction_detail_id,bulk_trade_id,mod(abs(dbms_random.random),'|| anonymisation_process.g_max_rnd_surname_seq ||') +1 surname_key
    from mifid_transaction_details mbt
    left join holder_names hn on hn.comp_code =  mbt.company_code and hn.ivc_code =  substr(''0000000''||mbt.ivc_code,1,11)
    where hn.ivc_code is null';

    obfus_log('3: drop index rnd_mi_name_idx1',null,null,g_module);

    begin
      execute immediate 'drop index rnd_mi_name_idx1';
    exception when others then null;
    end;

    obfus_log('4: create index rnd_mi_name_idx1 on rnd_mi_name_lookup(transaction_detail_id)',null,null,g_module);

    begin
       execute immediate 'create index rnd_mi_name_idx1 on rnd_mi_name_lookup(transaction_detail_id)';
    exception when others then null;
    end;

    obfus_log('5: drop index rnd_mi_name_idx2',null,null,g_module);

    begin
      execute immediate 'drop index rnd_mi_name_idx2';
      exception when others then null;
    end;

    obfus_log('6: create index rnd_mi_name_idx2 on rnd_mi_name_lookup(bulk_trade_id)',null,null,g_module);

    execute immediate 'create index rnd_mi_name_idx2 on rnd_mi_name_lookup(bulk_trade_id)';

    obfus_log('7: truncate table mifid_transaction_details_tmp1',null,null,g_module);

    execute immediate 'truncate table mifid_transaction_details_tmp1';

    obfus_log('8: insert into mifid_transaction_details_tmp1',null,null,g_module);

    execute immediate 'insert into mifid_transaction_details_tmp1 (transaction_detail_id,ivc_code,holder_surname)
      select mtd.transaction_detail_id,mtd.ivc_code,
        case when hn.surname is null then hn_surname.surname else hn.surname end holder_surname
      from mifid_transaction_details mtd
      left join holder_names hn on hn.comp_code =  mtd.company_code and hn.ivc_code =  substr(''0000000''||mtd.ivc_code,1,11) and hn.holder_seq = 1
      left join rnd_mi_name_lookup rn_name on rn_name.transaction_detail_id = mtd.transaction_detail_id
      left join surname_list fl_surname on fl_surname.surname_seq = rn_name.surname_key
      left join holder_names_tmp1 hn_surname on hn_surname.holder_name_id = fl_surname.holder_name_id';

    commit;

    exception
       when others then
          g_code := SQLCODE;
          g_errm := SUBSTR(SQLERRM, 1 , 4000);
          obfus_log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),g_code,g_errm,g_module);
          RAISE;
  end;

end anon_mifid_trans_details;

procedure anon_mifid_bulk_trades is
begin

  begin

    g_module := 'anon_mifid_bulk_trades';

    obfus_log('1: truncate table mifid_bulk_trades_tmp1',null,null,g_module);

    execute immediate 'truncate table mifid_bulk_trades_tmp1';

    obfus_log('2: insert into mifid_bulk_trades_tmp1(bulk_trade_id,ivc_code,holder_surname)',null,null,g_module);

    execute immediate 'insert into mifid_bulk_trades_tmp1(bulk_trade_id,ivc_code,holder_surname)
    select mbt.bulk_trade_id,ivc_code,case when trans_surname.holder_surname is null then ''N/A'' else trans_surname.holder_surname end holder_surname
    from mifid_bulk_trades mbt
    left join (select bulk_trade_id,no_in_bulk,holder_surname from
                (
                    select bulk_trade_id, count(*) over (partition by bulk_trade_id) no_in_bulk ,
                    first_value(holder_surname) over (partition by bulk_trade_id order by 1) holder_surname
                    from mifid_transaction_details mtd
                ) where no_in_bulk = 1
              ) trans_surname on  mbt.bulk_trade_id = trans_surname.bulk_trade_id';

    commit;

    exception
       when others then
          g_code := SQLCODE;
          g_errm := SUBSTR(SQLERRM, 1 , 4000);
          obfus_log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),g_code,g_errm,g_module);
          RAISE;
  end;

end anon_mifid_bulk_trades;

procedure gen_rnd_addresses is
   l_counter number;
begin

    g_module := 'gen_rnd_addresses';

    begin execute immediate 'drop index idx_address_line_1_rnd1'; exception when others then null; end;
    begin execute immediate 'drop index idx_address_line_2_rnd1'; exception when others then null; end;
    begin execute immediate 'drop index idx_address_line_3_rnd1'; exception when others then null; end;
    begin execute immediate 'drop index idx_postcode_rnd';        exception when others then null; end;

    obfus_log('1: reset sequence ns',null,null,g_module);
    anonymisation_process.reset_sequence('ns');

    obfus_log('2: truncate table address_line_1_rnd',null,null,g_module);
    execute immediate 'truncate table address_line_1_rnd';

    obfus_log('3: insert into address_line_1_rnd',null,null,g_module);
    insert into  address_line_1_rnd( key_ns,address_line_1)
    select  ns.nextval as key_ns ,name1  as address_line_1 from places
    where local_type in ('Section Of Named Road','Named Road');

    obfus_log('4: reset sequence ns',null,null,g_module);
    anonymisation_process.reset_sequence('ns');

    obfus_log('5: truncate table address_line_2_rnd',null,null,g_module);
    execute immediate 'truncate table address_line_2_rnd';

    obfus_log('6: insert into  address_line_2_rnd',null,null,g_module);
    insert into  address_line_2_rnd( key_ns,address_line_2)
    select  ns.nextval as key_ns,name1 as address_line_2 from places
    where local_type in ('Other Settlement','Suburban Area','Hamlet');

    obfus_log('7: reset sequence ns',null,null,g_module);
    anonymisation_process.reset_sequence('ns');

    obfus_log('8: truncate table address_line_3_rnd',null,null,g_module);
    execute immediate 'truncate table address_line_3_rnd';

    obfus_log('9: insert into address_line_3_rnd',null,null,g_module);
    insert into  address_line_3_rnd( key_ns,address_line_3)
    select  ns.nextval as key_ns,name1 as address_line_3 from places
    where local_type in ('Town','City');

    obfus_log('10: reset sequence ns',null,null,g_module);
    anonymisation_process.reset_sequence('ns');

    obfus_log('11: truncate table postcode_rnd',null,null,g_module);
    execute immediate 'truncate table postcode_rnd';

    obfus_log('12: insert into  postcode_rnd',null,null,g_module);
    insert into  postcode_rnd( key_ns,postcode_left,postcode_right)
    select ns.nextval as key_ns,substr (name1,1,instr(name1,' ')-1) postcode_left,substr (name1,instr(name1,' ')+1) as postcode_right from places
    where local_type in ('Postcode');

    obfus_log('13: create indexes',null,null,g_module);
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
end gen_rnd_addresses;

procedure gen_rnd_names is
   l_counter number;
begin

   g_module := 'gen_rnd_names';

   obfus_log('reset sequence surname_seq',null,null,g_module);
   anonymisation_process.reset_sequence('surname_seq');

   obfus_log('reset sequence forename_seq',null,null,g_module);
   anonymisation_process.reset_sequence('forename_seq');

   begin execute immediate 'drop index sl_idx1'; exception when others then null; end;
   begin execute immediate 'drop index fl_idx1'; exception when others then null; end;

   execute immediate 'truncate table surname_list';

   execute immediate 'truncate table forename_list';

   execute immediate 'insert into  surname_list(surname_seq,holder_name_id) select surname_seq.nextval surname_seq,holder_name_id from holder_names
   where surname is not null and holder_type_code = ''I''';

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
      RAISE; -- Er  ror will propogate back to anonymise setting p_obfus_status => 'FAILED'
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

   obfus_log('reset sequence forename_seq',null,null,g_module);
   anonymisation_process.reset_sequence('notes_seq');

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

  procedure merge_to_target_manual (p_stage NUMBER)
  as
  begin
     begin
        g_module := 'merge_to_target_manual';
--        DBMS_SESSION.set_identifier ('adcfs\ksheehan1' || ':' || '1');
--        begin
--          insert into tgt_audit_events select * from audit_events where event_id =1;
--          exception when others then null;
--        end;
--
--        obfus_log('1: to be implemented',null,null,g_module);
     exception
        when others then
          g_code := SQLCODE;
          g_errm := SUBSTR(SQLERRM, 1 , 4000);
          obfus_log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),g_code,g_errm,'merge_to_target_manual');
     end;
  end merge_to_target_manual;


   PROCEDURE run_purge_data(p_prefix varchar2)
   IS

      cursor purge_tables(p_prefix varchar2,p_escape varchar2)
      is
         select owner, table_name, single_column, num_rows
           from peripheral_tables;

        ncount number;

   begin
      begin
         g_module := 'run_purge_data';
         execute immediate 'truncate table purge_transform';

         for purge_tables_rec in purge_tables(p_prefix ,'\' )  loop
            begin

               execute immediate 'select count(*)   from '||purge_tables_rec.owner||'.'||purge_tables_rec.table_name||' where rownum = 1' into ncount;

               if ncount=1 then
                  ut.truncate_table_new (purge_tables_rec.owner,purge_tables_rec.table_name);
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


procedure anon_holder as
begin
   begin

      g_module := 'anon_holder';

      obfus_log('1: truncate table holder_base',null,null,g_module);
      execute immediate 'truncate table holder_base';

      obfus_log('2: insert into holder_base',null,null,g_module);
      
      
      execute immediate 'insert into holder_base(key_ns, ni_key, comp_code, ivc_code,	sort_key,
                              share_master_holder_code, reference_ivc_code, designation_name, country_code, date_of_death,
                              gender,date_of_birth,national_insurance_no,personnel_number,payroll_number,
                              sort_key_anon,share_master_holder_code_anon,reference_ivc_code_anon,
                              designation_name_anon,country_code_anon,date_of_death_anon,gender_anon,date_of_birth_anon,
                              national_insurance_no_anon,personnel_number_anon,payroll_number_anon)

      select res.key_ns key_ns, res.ni_key ni_key ,res.comp_code comp_code,res.ivc_code ivc_code,res.sort_key sort_key,
                   res.share_master_holder_code,res.reference_ivc_code,res.designation_name,res.country_code,res.date_of_death,
                   res.gender,res.date_of_birth,res.national_insurance_no,res.personnel_number,res.payroll_number,
                   res.sort_key as sort_key_anon,res.share_master_holder_code as share_master_holder_code_anon,res.reference_ivc_code as reference_ivc_code_anon,
                    CASE WHEN substr(res.ivc_code,1,1) <> ''7'' then ut.tdc(res.designation_name) else  res.designation_name end as designation_name_anon,res.country_code  as country_code_anon,
                    ut.randomise_date_30(res.date_of_death) as date_of_death_anon, gender as gender_anon,ut.randomise_date_30 (res.date_of_birth) as date_of_birth_anon,
                    CASE WHEN res.national_insurance_no is not null THEN
                          CHR(mod(ni_key,25+1)+ 65)||CHR((mod(trunc(ni_key/(25+1)),26)+ 65))||lpad(ni_key,6,''0'')||CHR((mod(trunc(ni_key/(675+1)),26)+ 65)) ELSE NULL END as a_national_insurance_no_anon,
                    decode (res.personnel_number,  null, null, ''PE'' || res.ivc_code) as a_personnel_number_anon,
                    decode (res.payroll_number,  null, null, ''PR'' || res.ivc_code) as a_payroll_number_anon
        from
           (select row_number() over (order by hld.comp_code,hld.ivc_code) key_ns,
                   ora_hash(hld.comp_code||hld.ivc_code, 4294967295, 0) ni_key,
                   hld.comp_code,hld.ivc_code,hld.sort_key,
                   hld.share_master_holder_code,hld.reference_ivc_code,hld.designation_name,
                   hld.country_code,hld.date_of_death,hed.gender,hed.date_of_birth,
                   hed.national_insurance_no,hed.personnel_number,hed.payroll_number
              from holders hld
              left outer join holder_employee_details hed on hld.comp_code = hed.comp_code and hld.ivc_code = hed.ivc_code) res';

      obfus_log('3: truncate table holders_tmp1',null,null,g_module);
      begin
         execute immediate 'truncate table holders_tmp1';
      exception when others then null;
      end;

      obfus_log('4: insert into holders_tmp1',null,null,g_module);
      
      execute immediate 'insert into holders_tmp1(
               comp_code,ivc_code,sort_key,share_master_holder_code,reference_ivc_code,
               designation_name,previous_sortkey,date_of_death)
      select h.comp_code,h.ivc_code,hb.sort_key_anon,hb.share_master_holder_code_anon,hb.reference_ivc_code_anon,
             hb.designation_name_anon,h.previous_sortkey,hb.date_of_death_anon
      from holders h
      join holder_base hb
        on h.comp_code = hb.comp_code and h.ivc_code = hb.ivc_code';

      obfus_log('5: Generating Sort_Key',null,null,g_module);

      obfus_log('6: truncate table sk_temp',null,null,g_module);
      begin
        execute immediate 'truncate table sk_temp';
        exception when others then null;
      end;

      obfus_log('7: insert into sk_temp',null,null,g_module);
      insert into sk_temp(comp_code,ivc_code,holder_seq,sk_gen)

      select comp_code,ivc_code,holder_seq,ut.get_formatted_sk(res1.sort_key_names,res1.holder_type_code,res1.designation_name) sk_gen
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

      obfus_log('8: drop index sk_temp_12345678',null,null,g_module);

      begin
        execute immediate 'drop index sk_temp_12345678';
        exception when others then null;
      end;

      obfus_log('9: create index sk_temp_12345678 on sk_temp(comp_code,ivc_code)',null,null,g_module);

      begin
        execute immediate 'create index sk_temp_12345678 on sk_temp(comp_code,ivc_code)';
        exception when others then null;
      end;


      obfus_log('10: merge into holders_tmp1',null,null,g_module);

      merge into holders_tmp1 h
      using (select comp_code,ivc_code,sk_gen sort_key ,sk_gen previous_sortkey from sk_temp
             where holder_seq = 1) ahn on (ahn.comp_code = h.comp_code and ahn.ivc_code = h.ivc_code  )
          when matched
          then
                   update set h.sort_key = ahn.sort_key,
                              h.previous_sortkey = ahn.previous_sortkey;

      commit;

    exception
       when others then
          g_code := SQLCODE;
          g_errm := SUBSTR(SQLERRM, 1 , 4000);
          obfus_log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),g_code,g_errm,g_module);
          RAISE;
    end;
  end anon_holder;

  procedure anon_holder_employee_details as
  begin
    begin

      g_module := 'anon_holder_employee_details';

      obfus_log('1: truncate table holder_employee_details_tmp1',null,null,g_module);

      begin
         execute immediate 'truncate table holder_employee_details_tmp1';
      exception when others then null;
      end;

      obfus_log('2: insert into holder_employee_details_tmp1',null,null,g_module);

      execute immediate 'insert into holder_employee_details_tmp1
             (holder_employee_detail_id, comp_code, ivc_code, gender, date_of_birth, national_insurance_no, personnel_number, payroll_number)
       select hed.holder_employee_detail_id, hed.comp_code, hed.ivc_code, hb.gender_anon, hb.date_of_birth_anon, hb.national_insurance_no_anon, hb.personnel_number_anon,
              hb.payroll_number_anon
        from holder_employee_details  hed
        join holder_base hb on hed.comp_code = hb.comp_code and hed.ivc_code = hb.ivc_code';

      commit;

    exception
       when others then
          g_code := SQLCODE;
          g_errm := SUBSTR(SQLERRM, 1 , 4000);
          obfus_log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),g_code,g_errm,g_module);
          RAISE;
    end;
  end anon_holder_employee_details;
										   

   procedure anon_holder_names
   is
      v_ncount number;
      nMaxKey number;
   begin

      g_module := 'anon_holder_names';

      obfus_log('1: truncate table forename_shuffle1',null,null,g_module);
      execute immediate 'truncate table forename_shuffle1';

      obfus_log('2: insert into forename_shuffle1',null,null,g_module);
      
      execute immediate 'insert into forename_shuffle1(holder_name_id, comp_code, ivc_code, holder_seq, forename, forename_hash,
                                    working_gender, key_ns,forename_min_key_ns ,forename_max_key_ns,forename_shuffle_key_ns)
       select res2.HOLDER_NAME_ID, res2.comp_code, res2.ivc_code, res2.holder_seq, res2.forename, res2.forename_hash,
              res2.working_gender, res2.key_ns, res2.forename_min_key_ns, res2.forename_max_key_ns,
              mod(abs(dbms_random.random),(res2.forename_max_key_ns - res2.forename_min_key_ns) - 1  ) forename_shuffle_key_ns
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
                  nvl(tt.gender,case when mod(abs(dbms_random.random),2) = 1 then ''F'' else ''M'' end) working_gender
                from holder_names hn
                left outer join title_types tt on hn.title_type_code = tt.title_type_code
                where hn.forename  is not null and holder_type_code = ''I''
              ) res
          )res1
        ) res2';

      obfus_log('3: truncate table forename_shuffle',null,null,g_module);
      execute immediate 'truncate table forename_shuffle';

      obfus_log('4: insert into forename_shuffle',null,null,g_module);
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

        obfus_log('5: IN LOOP update forename_shuffle',null,null,g_module);
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

     obfus_log('6: truncate table surname_shuffle1',null,null,g_module);
     execute immediate 'truncate table surname_shuffle1';

     obfus_log('7: insert into surname_shuffle1',null,null,g_module);
     
     execute immediate 'insert into surname_shuffle1(HOLDER_NAME_ID,comp_code,ivc_code ,holder_seq,
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
                  nvl(tt.gender,case when mod(abs(dbms_random.random),2) = 1 then ''F'' else ''M'' end) working_gender
                from holder_names hn
                left outer join title_types tt on hn.title_type_code = tt.title_type_code
                where hn.surname  is not null and holder_type_code = ''I''
              ) res
          )res1
        ) res2';

      obfus_log('8: truncate table surname_shuffle',null,null,g_module);
      execute immediate 'truncate table surname_shuffle';

      obfus_log('9: insert into surname_shuffle',null,null,g_module);
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

        obfus_log('10: IN LOOP update surname_shuffle',null,null,g_module);
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

    obfus_log('11: truncate table holder_names_tmp1',null,null,g_module);
    begin
      execute immediate 'truncate table holder_names_tmp1';
      exception when others then null;
    end;

    obfus_log('12: insert into holder_names_tmp1',null,null,g_module);
    
    execute immediate 'insert into holder_names_tmp1(holder_name_id,comp_code,ivc_code,holder_seq,holder_type_code,title_type_code,
       surname,forename,suffix,salutation,other_title,preferred_name,trust_name,trustee_name,company_name)

    select hn.holder_name_id,hn.comp_code,hn.ivc_code,hn.holder_seq,hn.holder_type_code,hn.title_type_code,
       ss.surname_shuffle,fs.forename_shuffle,ut.tdc(SUFFIX) SUFFIX_ANON,ut.tdc(SALUTATION) SALUTATION_ANON,ut.tdc(other_title) other_title,
       SUBSTR (NVL (SUBSTR (fs.forename_shuffle, 1, INSTR (fs.forename_shuffle, '' '')),fs.forename_shuffle),1,25) preferred_name,
       ut.tdc(trust_name) trust_name, ut.tdc(trustee_name) trustee_name, ut.tdc(company_name) company_name
    from holder_names hn
    left join surname_shuffle ss on ss.holder_name_id = hn.holder_name_id
    left join forename_shuffle fs on fs.holder_name_id = hn.holder_name_id';

    commit;

    exception
       when others then
          g_code := SQLCODE;
          g_errm := SUBSTR(SQLERRM, 1 , 4000);
          obfus_log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),g_code,g_errm,g_module);
          RAISE;
  end anon_holder_names;

  procedure anon_holder_labels
  is
    v_sql varchar2(32000);
  begin
      g_module := 'anon_holder_labels';

      obfus_log('1: truncate holder_labels_tmp tables',null,null,g_module);

      execute immediate 'truncate table holder_labels_tmp3';
      execute immediate 'truncate table holder_labels_tmp2';
      execute immediate 'truncate table holder_labels_tmp1';

      obfus_log('2: insert into  holder_labels_tmp3',null,null,g_module);
      
      v_sql := 'insert into holder_labels_tmp3 (holder_type_code,other_title,holder_label_id,holder_label_type_code,comp_code,ivc_code,forename,surname,title,initials,
      first_forename,tail_inits,suffix,company_name,trustee_name,trust_name,trust_name_initials,trust_name_first_name,trust_name_tail_inits,jh_surnames,jh_company_names,
      jh_trustee_names,jh_forenames_surnames,jh_init_surnames,jh_first_forename_tail_init,designation_name)' ||
     
     'select hn.holder_type_code,hn.other_title,hl.holder_label_id,hl.holder_label_type_code,hl.comp_code,hl.ivc_code,hn.forename,hn.surname,
      case when htt.description = ''OTHER'' then null else htt.description  end title,
      regexp_replace(initcap(regexp_replace(hn.forename,''([[:punct:]])'')),''([[:lower:]])'') initials,
      case when instr(hn.forename,'' '') > 0 then substr(hn.forename,1,instr(hn.forename,'' '')-1) else hn.forename end first_forename,
      case when instr(hn.forename,'' '') > 0 then regexp_replace(initcap(regexp_replace(substr(hn.forename,instr(hn.forename,'' '')+1) ,''([[:punct:]])'')),''([[:lower:]])'') else null end tail_inits,
      suffix,hn.company_name,hn.trustee_name,hn.trust_name,regexp_replace(initcap(regexp_replace(hn.trust_name,''([[:punct:]])'')),''([[:lower:]])'') trust_name_initials,
      case when instr(hn.trust_name,'' '') > 0 then substr(hn.trust_name,1,instr(hn.trust_name,'' '')-1) else hn.trust_name end trust_name_first_name,
      case when instr(hn.trust_name,'' '') > 0 then regexp_replace(initcap(regexp_replace(substr(hn.trust_name,instr(hn.trust_name,'' '')+1) ,''([[:punct:]])'')),''([[:lower:]])'') else null end  trust_name_tail_inits,
      case when jhn.surnames is not null then ''+ ''||jhn.surnames else null end jh_surnames,
      case when jhn.company_names is not null then ''+ ''||jhn.company_names else null end jh_company_names,
      case when jhn.trustee_names is not null then ''+ ''||jhn.trustee_names else null end jh_trustee_names,
      case when jhn.forenames_surnames is not null then ''+ ''||jhn.forenames_surnames else null end jh_forenames_surnames,
      case when jhn.init_surnames is not null then ''+ ''||jhn.init_surnames  else null end jh_init_surnames,
      case when jhn.first_forename_tail_init is not null then ''+ ''||jhn.first_forename_tail_init else null end jh_first_forename_tail_init,h.designation_name
      from holder_labels hl
      left join holder_names_tmp1 hn on  hl.comp_code = hn.comp_code and hl.ivc_code = hn.ivc_code
      left join holders_tmp1 h on  hl.comp_code = h.comp_code and hl.ivc_code = h.ivc_code
      left join ' ||
                '(select distinct comp_code,ivc_code,
                 listagg(regexp_replace(initcap(regexp_replace(forename,''([[:punct:]])'')),''([[:lower:]])'')||'' ''||surname , '' + '') 
                    within group (order by holder_seq) over (partition by comp_code,ivc_code) init_surnames,
                 listagg(forename||'' ''||surname , '' + '') within group (order by holder_seq) over (partition by comp_code,ivc_code) forenames_surnames,
                 listagg(surname , '' + '') within group (order by holder_seq) over (partition by comp_code,ivc_code)  surnames,
                 listagg(substr(forename,1,instr(forename,'' '')-1)||'' ''||regexp_replace(initcap(regexp_replace(substr(forename,instr(forename,'' '')+1),''([[:punct:]])'')),''([[:lower:]])''), '' + '')  
                    within group (order by holder_seq) over (partition by comp_code,ivc_code) first_forename_tail_init,
                 listagg(company_name, '' + '') within group (order by holder_seq) over (partition by comp_code,ivc_code) company_names,
                 listagg(trustee_name, '' + '') within group (order by holder_seq) over (partition by comp_code,ivc_code) trustee_names
                 from holder_names_tmp1
                 where holder_seq <> 1 and (forename is not null or surname is not null or company_name is not null or trustee_name is not null ) ) jhn 
               on  hl.comp_code = jhn.comp_code and hl.ivc_code = jhn.ivc_code
      left join title_types htt on htt.title_type_code = hn.title_type_code
      where hn.holder_seq = 1
        and hn.holder_type_code IN (''I'',''C'',''T'') and hl.holder_label_type_code in (''1'',''2'',''3'',''4'',''5'',''6'',''7'',''8'',''9'')' ;

      --obfus_log(substr('Executing: ' || v_sql),1,4000),null,null,g_module);
      execute immediate v_sql;
      
      obfus_log('2: insert into  holder_labels_tmp2',null,null,g_module);
      v_sql := 'insert into holder_labels_tmp2 (holder_label_id  ,
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
            REPLACE(RTRIM(LTRIM(case when to_char(holder_label_type_code) = ''1'' and holder_type_code = ''I'' then other_title||'' ''||title||'' ''|| forename ||'' ''||surname ||'' ''||suffix ||
                 case when designation_name is not null then '' ''||designation_name ||'' ACCT'' else null  end
            when to_char(holder_label_type_code) = ''2''  and holder_type_code = ''I'' then other_title||'' ''||title||'' ''|| forename||'' ''||surname  ||'' ''||suffix ||
                 case when designation_name is not null then '' ''||designation_name ||'' ACCT'' else null   end
            when to_char(holder_label_type_code) = ''3''  and holder_type_code = ''I'' then other_title||'' ''||title||'' ''|| first_forename ||'' ''||tail_inits||'' ''||surname ||'' ''||suffix ||
                 case when designation_name is not null then '' ''||designation_name ||'' ACCT'' else null end
            when to_char(holder_label_type_code) = ''4''  and holder_type_code = ''I'' then other_title||'' ''||title||'' ''|| first_forename||'' ''||tail_inits||'' ''||surname ||'' ''||suffix ||
                 case when designation_name is not null then '' ''||designation_name ||'' ACCT'' else null   end
            when to_char(holder_label_type_code) = ''5''  and holder_type_code = ''I'' then other_title||'' ''||title||'' ''|| first_forename ||'' ''||tail_inits||'' ''||surname ||'' ''||suffix ||
                 case when designation_name is not null then '' ''||designation_name ||'' ACCT'' else null  end
            when to_char(holder_label_type_code) = ''6''  and holder_type_code = ''I'' then other_title||'' ''||title||'' ''|| forename||'' ''||surname  ||'' ''||suffix ||
                 case when designation_name is not null then '' ''||designation_name ||'' ACCT'' else null   end
            when to_char(holder_label_type_code) = ''7''  and holder_type_code = ''I'' then other_title||'' ''||title||'' ''|| forename ||'' ''||surname ||'' ''||suffix ||
                 case when designation_name is not null then '' ''||designation_name ||'' ACCT'' else null   end
            when to_char(holder_label_type_code) = ''8''  and holder_type_code = ''I'' then other_title||'' ''||title||'' ''|| forename||'' ''||surname ||'' ''||suffix ||
                 case when designation_name is not null then '' ''||designation_name ||'' ACCT'' else null   end
          when to_char(holder_label_type_code) = ''9''  and holder_type_code = ''I'' then other_title||'' ''||title||'' ''|| forename ||'' ''||surname ||'' ''||suffix ||
                 case when designation_name is not null then '' ''||designation_name ||'' ACCT'' else null  end
            end)),''  '','' '') main_holder,
            RTRIM(LTRIM(case when to_char(holder_label_type_code) = ''1''  and holder_type_code = ''I''  then
                 case when jh_surnames is not null then jh_surnames else null end
            when to_char(holder_label_type_code) = ''2''  and holder_type_code = ''I'' then
                 case when jh_init_surnames is not null then jh_init_surnames else null end
            when to_char(holder_label_type_code) = ''3''  and holder_type_code = ''I'' then
                 case when jh_surnames is not null then jh_surnames else null end
            when to_char(holder_label_type_code) = ''4''  and holder_type_code = ''I'' then
                 case when jh_init_surnames is not null then jh_init_surnames else null end
            when to_char(holder_label_type_code) = ''5''  and holder_type_code = ''I'' then
                 case when jh_first_forename_tail_init is not null then jh_first_forename_tail_init else null end
            when to_char(holder_label_type_code) = ''6''  and holder_type_code = ''I'' then
                 case when jh_surnames is not null then jh_surnames else null end
            when to_char(holder_label_type_code) = ''7''  and holder_type_code = ''I''  then
                 case when jh_init_surnames is not null then jh_init_surnames else null end
            when to_char(holder_label_type_code) = ''8''  and holder_type_code = ''I'' then
                 case when jh_first_forename_tail_init is not null then jh_first_forename_tail_init else null end
          when to_char(holder_label_type_code) = ''9''  and holder_type_code = ''I'' then
                 case when jh_init_surnames is not null then jh_init_surnames else null end
          end)) joint_holders  ,
          case when holder_type_code = ''C'' then  company_name ||
            case when designation_name is not null then '' ''||designation_name ||'' ACCT'' else null end
          end companies,
          case when holder_type_code = ''C''    then
            case when jh_company_names is not null then jh_company_names else null end end joint_companies,
          case when to_char(holder_label_type_code) IN (''1'',''2'') and holder_type_code = ''T'' then  trust_name_initials||'' ''||trustee_name ||
            case when designation_name is not null then '' ''||designation_name ||'' ACCT'' else null end
          when to_char(holder_label_type_code) IN (''3'',''4'',''5'') and holder_type_code = ''T'' then  trust_name_first_name||'' ''||trust_name_tail_inits||'' ''|| trustee_name ||
            case when designation_name is not null then '' ''||designation_name ||'' ACCT'' else null end
          when to_char(holder_label_type_code) IN (''6'',''7'',''8'',''9'') and holder_type_code = ''T'' then  trustee_name ||
            case when designation_name is not null then '' ''||designation_name ||'' ACCT'' else null end
          end trustees,
         case when holder_type_code = ''T''    then
            case when jh_trustee_names is not null then jh_trustee_names else null end end joint_trustees
          from holder_labels_tmp3';

    --obfus_log(substr('Executing: ' || v_sql,1,4000),null,null,g_module);
    execute immediate v_sql;

    obfus_log('3: insert into  holder_labels_tmp1',null,null,g_module);
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

    obfus_log('4: merge into holder_labels_tmp1',null,null,g_module);
    
    execute immediate 'merge into holder_labels_tmp1 hl
           using (select hlt.holder_label_id, ut.tdc(hl.line1_text)  label_line_1
                    from  holder_labels_tmp1 hlt
                    join holder_labels hl on hl.holder_label_id = hlt.holder_label_id
                    where hlt.label_line_1 is null) ahn
              on (hl.holder_label_id = ahn.holder_label_id)
      when matched
      then
         update set hl.label_line_1  =  ahn.label_line_1';

      commit;

  exception
     when others then
        g_code := SQLCODE;
        g_errm := SUBSTR(SQLERRM, 1 , 4000);
        obfus_log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),g_code,g_errm,g_module);
        RAISE;
  end anon_holder_labels;

  procedure address_line1_shuffle is
         nMaxKey number;
         nLoopCounter number;
  begin

      g_module := 'address_line1_shuffle';

      execute immediate 'truncate table ADDRESS_LINE1_shuffle';
      execute immediate 'truncate table ADDRESS_LINE1_shuffle1';

      execute immediate 'insert into ADDRESS_LINE1_shuffle1(HOLDER_ADDRESS_ID ,comp_code,ivc_code ,
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
                case when hn.country_code is null then ''GB'' else hn.country_code end working_country_code
                from holder_addresses hn
                where hn.ADDRESS_LINE1  is not null
              ) res
          )res1
        ) res2';

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

      execute immediate 'insert into ADDRESS_LINE2_shuffle1(HOLDER_ADDRESS_ID ,comp_code,ivc_code ,
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
                case when hn.country_code is null then ''GB'' else hn.country_code end working_country_code
                from holder_addresses hn
                where hn.ADDRESS_LINE2  is not null
              ) res
          )res1
        ) res2';

      execute immediate 'truncate table ADDRESS_LINE2_shuffle';

      execute immediate 'insert into ADDRESS_LINE2_shuffle (HOLDER_ADDRESS_ID ,comp_code,ivc_code ,
      ADDRESS_LINE2 , ADDRESS_LINE2_hash,working_country_code,key_ns,ADDRESS_LINE2_min_key_ns ,ADDRESS_LINE2_max_key_ns,ADDRESS_LINE2_shuffle_key_ns,ADDRESS_LINE2_shuffle_hash,
      resolved ,ADDRESS_LINE2_shuffle)

      select res.HOLDER_ADDRESS_ID ,res.comp_code,res.ivc_code ,
      res.ADDRESS_LINE2 , res.ADDRESS_LINE2_hash,res.working_country_code,res.key_ns,
      res.ADDRESS_LINE2_min_key_ns ,res.ADDRESS_LINE2_max_key_ns, res.ADDRESS_LINE2_shuffle_key_ns, ADDRESS_LINE2_shuffle_hash,
      case when res.ADDRESS_LINE2_hash=res.ADDRESS_LINE2_shuffle_hash then ''N'' else ''Y'' end   resolved,
      ADDRESS_LINE2_shuffle
      from
      (
        select  fs1.HOLDER_ADDRESS_ID ,fs1.comp_code,fs1.ivc_code ,
        fs1.ADDRESS_LINE2 , fs1.ADDRESS_LINE2_hash,fs1.working_country_code,fs1.key_ns,
        fs1.ADDRESS_LINE2_min_key_ns ,fs1.ADDRESS_LINE2_max_key_ns, fs1.ADDRESS_LINE2_shuffle_key_ns,ora_hash(fs2.ADDRESS_LINE2, 4294967295, 0) ADDRESS_LINE2_shuffle_hash,
        fs2.ADDRESS_LINE2 ADDRESS_LINE2_shuffle
        from ADDRESS_LINE2_shuffle1 fs1 join ADDRESS_LINE2_shuffle1 fs2 on fs1.ADDRESS_LINE2_shuffle_key_ns = fs2.key_ns
      ) res';

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
  end address_line2_shuffle;

  procedure address_line3_shuffle is
    nMaxKey number;
    nLoopCounter number;
  begin

   g_module := 'address_line3_shuffle';
-- ADRESS LINE 3

    execute immediate 'truncate table ADDRESS_LINE3_shuffle';
    execute immediate 'truncate table ADDRESS_LINE3_shuffle1';

      execute immediate 'insert into ADDRESS_LINE3_shuffle1(HOLDER_ADDRESS_ID ,comp_code,ivc_code ,
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
                case when hn.country_code is null then ''GB'' else hn.country_code end working_country_code
                from holder_addresses hn
                where hn.ADDRESS_LINE3  is not null
              ) res
          )res1
        ) res2';

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
  end address_line3_shuffle;

  procedure address_line456_shuffle is
    nMaxKey number;
    nLoopCounter number;
  begin


     g_module := 'address_line456_shuffle';

      -- ADRESS LINE 456
      execute immediate 'truncate table ADDRESS_LINE456_shuffle';
      execute immediate 'truncate table ADDRESS_LINE456_shuffle1';

      execute immediate 'insert into ADDRESS_LINE456_shuffle1(HOLDER_ADDRESS_ID ,comp_code,ivc_code ,
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
                case when hn.country_code is null then ''GB'' else hn.country_code end working_country_code
                from holder_addresses hn
                where hn.ADDRESS_LINE4||hn.ADDRESS_LINE5||hn.ADDRESS_LINE6 is not null
              ) res
          )res1
        ) res2';

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
  end address_line456_shuffle;

  procedure postcode_left_shuffle is
    nMaxKey number;
    nLoopCounter number;
  begin

      g_module := 'postcode_left_shuffle';
            -- POST CODE LEFT


      execute immediate 'truncate table POSTCODE_LEFT_shuffle';
      execute immediate 'truncate table POSTCODE_LEFT_shuffle1';

      execute immediate 'insert into POSTCODE_LEFT_shuffle1(HOLDER_ADDRESS_ID ,comp_code,ivc_code ,
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
                case when hn.country_code is null then ''GB'' else hn.country_code end working_country_code
                from holder_addresses hn
                where hn.POSTCODE_LEFT  is not null
              ) res
          )res1
        ) res2';

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
  end postcode_left_shuffle;

  procedure postcode_right_shuffle is

    nMaxKey number;
    nLoopCounter number;
  begin

      g_module := 'postcode_right_shuffle';
        -- POST CODE LEFT


      execute immediate 'truncate table POSTCODE_RIGHT_shuffle';
      execute immediate 'truncate table POSTCODE_RIGHT_shuffle1';

      execute immediate 'insert into POSTCODE_RIGHT_shuffle1(HOLDER_ADDRESS_ID ,comp_code,ivc_code ,
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
                case when hn.country_code is null then ''GB'' else hn.country_code end working_country_code
                from holder_addresses hn
                where hn.POSTCODE_RIGHT  is not null
              ) res
          )res1
        ) res2';

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
  end postcode_right_shuffle;

  procedure irish_dist_code_shuffle is
    nMaxKey number;
    nLoopCounter number;
  begin

      g_module := 'irish_dist_code_shuffle';

        -- POST CODE LEFT

      execute immediate 'truncate table IRISH_DIST_CODE_shuffle';
      execute immediate 'truncate table IRISH_DIST_CODE_shuffle1';

      execute immediate 'insert into IRISH_DIST_CODE_shuffle1(HOLDER_ADDRESS_ID ,comp_code,ivc_code ,
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
                case when hn.country_code is null then ''RI'' else hn.country_code end working_country_code
                from holder_addresses hn
                where hn.IRISH_DISTRIBUTION_CODE  is not null
              ) res
          )res1
        ) res2';

       execute immediate 'truncate table IRISH_DIST_CODE_shuffle';

      execute immediate 'insert into IRISH_DIST_CODE_shuffle (HOLDER_ADDRESS_ID ,comp_code,ivc_code ,
      IRISH_DIST_CODE , IRISH_DIST_CODE_hash,working_country_code,key_ns,IRISH_DIST_CODE_min_key_ns ,IRISH_DIST_CODE_max_key_ns,IRISH_DIST_CODE_shuffle_key_ns,IRISH_DIST_CODE_shuffle_hash,
      resolved ,IRISH_DIST_CODE_shuffle)

      select res.HOLDER_ADDRESS_ID ,res.comp_code,res.ivc_code ,
      res.IRISH_DIST_CODE , res.IRISH_DIST_CODE_hash,res.working_country_code,res.key_ns,
      res.IRISH_DIST_CODE_min_key_ns ,res.IRISH_DIST_CODE_max_key_ns, res.IRISH_DIST_CODE_shuffle_key_ns, IRISH_DIST_CODE_shuffle_hash,
      case when res.IRISH_DIST_CODE_hash=res.IRISH_DIST_CODE_shuffle_hash then ''N'' else ''Y'' end   resolved,
      IRISH_DIST_CODE_shuffle
      from
      (
        select  fs1.HOLDER_ADDRESS_ID ,fs1.comp_code,fs1.ivc_code ,
        fs1.IRISH_DIST_CODE , fs1.IRISH_DIST_CODE_hash,fs1.working_country_code,fs1.key_ns,
        fs1.IRISH_DIST_CODE_min_key_ns ,fs1.IRISH_DIST_CODE_max_key_ns, fs1.IRISH_DIST_CODE_shuffle_key_ns,ora_hash(fs2.IRISH_DIST_CODE, 4294967295, 0) IRISH_DIST_CODE_shuffle_hash,
        fs2.IRISH_DIST_CODE IRISH_DIST_CODE_shuffle
        from IRISH_DIST_CODE_shuffle1 fs1 join IRISH_DIST_CODE_shuffle1 fs2 on fs1.IRISH_DIST_CODE_shuffle_key_ns = fs2.key_ns
      ) res';

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
  end irish_dist_code_shuffle;


  procedure anon_holder_addresses
  is
     v_ncount number;
     nMaxKey number;

     nLoopCounter number;
   begin

      g_module := 'anon_holder_addresses';

      address_line1_shuffle;
      address_line2_shuffle;
      address_line3_shuffle;
      address_line456_shuffle;
      postcode_left_shuffle;
      postcode_right_shuffle;
      irish_dist_code_shuffle;

      begin
        execute immediate 'truncate table holder_addresses_tmp1';
        exception when others then null;
      end;

      execute immediate 'insert into holder_addresses_tmp1(
        holder_address_id,comp_code,ivc_code,address_type_id,address_line1,address_line2,
        address_line3,address_line4,address_line5,address_line6,
        postcode_left,postcode_right,country_code,irish_distribution_code)

      select ha.holder_address_id,ha.comp_code,ha.ivc_code,ha.address_type_id,als1.address_line1_shuffle,als2.address_line2_shuffle,
        als3.address_line3_shuffle,als4.address_line4_shuffle,als4.address_line5_shuffle,als4.address_line6_shuffle,
        pcl.postcode_left_shuffle,pcr.postcode_right_shuffle,ha.country_code,idc.irish_dist_code_shuffle
      from holder_addresses ha
      left join address_line1_shuffle als1 on als1.holder_address_id = ha.holder_address_id
      left join address_line2_shuffle als2 on  als2.holder_address_id = ha.holder_address_id
      left join address_line3_shuffle als3 on  als3.holder_address_id = ha.holder_address_id
      left join address_line456_shuffle als4 on  als4.holder_address_id = ha.holder_address_id
      left join postcode_left_shuffle pcl on  pcl.holder_address_id = ha.holder_address_id
      left join postcode_right_shuffle pcr on pcr.holder_address_id = ha.holder_address_id
      left join irish_dist_code_shuffle idc on  idc.holder_address_id = ha.holder_address_id';

      commit;

   exception
      when others then
          g_code := SQLCODE;
          g_errm := SUBSTR(SQLERRM, 1 , 4000);
          obfus_log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),g_code,g_errm,g_module);
          RAISE;
   end anon_holder_addresses;


procedure anon_bank_branches
   is
         v_ncount number;
         nMaxKey number;

        nLoopCounter number;
    begin

      g_module := 'anon_bank_branches';

      obfus_log('1: truncating tables bank_branches_addr_lu2, bank_branches_addr_lu1 ,bank_branches_addr_lu',null,null,g_module);

      execute immediate 'truncate  table bank_branches_addr_lu2';
      execute immediate 'truncate  table bank_branches_addr_lu1';
      execute immediate 'truncate  table bank_branches_addr_lu';

      obfus_log('2: insert into bank_branches_addr_lu2',null,null,g_module);

      insert into bank_branches_addr_lu2(num_addr_key,addr_key,address_line1,
              address_line2,address_line3,address_line4,address_line5,address_line6,postcode_left,postcode_right)
      select row_number() over (order by 1) num_addr_key,RPAD(ADDRESS_LINE1,35, '*') ||RPAD(ADDRESS_LINE2,35, '*')||RPAD(ADDRESS_LINE3,35, '*')||RPAD(ADDRESS_LINE4,35, '*')||RPAD(ADDRESS_LINE5,35, '*')||RPAD(ADDRESS_LINE6,35, '*')||RPAD(POSTCODE_LEFT,4, '*')||RPAD(POSTCODE_RIGHT,3, '*')  ,address_line1,
              address_line2,address_line3,address_line4,address_line5,address_line6,postcode_left,postcode_right
      from  bank_branches_tmp2
      group by addr_key,address_line1, address_line2,address_line3,address_line4,address_line5,address_line6,postcode_left,postcode_right;

      obfus_log('3: insert into bank_branches_addr_lu1',null,null,g_module);

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

      obfus_log('4: insert into bank_branches_addr_lu',null,null,g_module);

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

      obfus_log('5: gather_table_stats bank_branches_addr_lu',null,null,g_module);

      dbms_stats.gather_table_stats(ownname=>g_run_env,tabname =>'bank_branches_addr_lu',estimate_percent=>100);
      execute immediate 'truncate table bank_branches_addr_lu_adjust';

      obfus_log('6: insert into bank_branches_addr_lu_adjust',null,null,g_module);

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

        obfus_log('7: gather_table_stats BANK_BRANCHES_ADDR_LU',null,null,g_module);
        dbms_stats.gather_table_stats(ownname=>g_run_env,tabname =>'BANK_BRANCHES_ADDR_LU',estimate_percent=>100);

        nloopcounter := 0;
        loop
          obfus_log('8: IN LOOP update bank_branches_addr_lu_adjust where resolved = N',null,null,g_module);

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

          obfus_log('9: gather_table_stats BANK_BRANCHES_ADDR_LU',null,null,g_module);

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

          obfus_log('10: IN LOOP: update bank_branches_addr_lu_adjust set resolved = Y',null,null,g_module);

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

         obfus_log('11: IN LOOP: update bank_branches_addr_lu_adjust',null,null,g_module);

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

      obfus_log('12: truncate table bank_branches_tmp1',null,null,g_module);
      begin
        execute immediate 'truncate table bank_branches_tmp1';
        exception when others then null;
      end;

      obfus_log('13: insert into bank_branches_tmp1',null,null,g_module);

      execute immediate 'insert into  bank_branches_tmp1(bank_sort_code ,finance_org_id,branch_name,closure_date,status ,
      address_line1,address_line2,address_line3,address_line4,address_line5,address_line6,postcode_left,
      postcode_right, country_code)
      select  bb.bank_sort_code ,bb.finance_org_id,bb.branch_name,bb.closure_date,bb.status,
      CASE WHEN NVL(bbala.resolved,''X'') = ''Y'' THEN bbala.address_line1_shuffle else bbal.address_line1_shuffle end address_line1,
      CASE WHEN NVL(bbala.resolved,''X'') = ''Y'' THEN bbala.address_line2_shuffle else bbal.address_line2_shuffle end  address_line2 ,
      CASE WHEN NVL(bbala.resolved,''X'') = ''Y'' THEN bbala.address_line3_shuffle else bbal.address_line3_shuffle  end address_line3_suffle ,
      CASE WHEN NVL(bbala.resolved,''X'') = ''Y'' THEN bbala.address_line4_shuffle else bbal.address_line4_shuffle  end address_line4,
      CASE WHEN NVL(bbala.resolved,''X'') = ''Y'' THEN bbala.address_line5_shuffle else bbal.address_line5_shuffle end address_line5,
      CASE WHEN NVL(bbala.resolved,''X'') = ''Y'' THEN bbala.address_line6_shuffle else bbal.address_line6_shuffle end address_line6 ,
      CASE WHEN NVL(bbala.resolved,''X'') = ''Y'' THEN bbala.postcode_left_shuffle else  bbal.postcode_left_shuffle end postcode_left,
      CASE WHEN NVL(bbala.resolved,''X'') = ''Y'' THEN bbala.postcode_right_shuffle  else bbal.postcode_right_shuffle end postcode_right,
      bb.country_code
      from bank_branches bb
      left join bank_branches_addr_lu bbal
      on RPAD(bb.ADDRESS_LINE1,35, ''*'') ||RPAD(bb.ADDRESS_LINE2,35, ''*'')||RPAD(bb.ADDRESS_LINE3,35, ''*'')||RPAD(bb.ADDRESS_LINE4,35, ''*'')||RPAD(bb.ADDRESS_LINE5,35, ''*'')||RPAD(bb.ADDRESS_LINE6,35, ''*'')||RPAD(bb.POSTCODE_LEFT,4, ''*'')||RPAD(bb.POSTCODE_RIGHT,3, ''*'') = bbal.ADDR_KEY
      left join bank_branches_addr_lu_adjust bbala
      on RPAD(bb.ADDRESS_LINE1,35, ''*'') ||RPAD(bb.ADDRESS_LINE2,35, ''*'')||RPAD(bb.ADDRESS_LINE3,35, ''*'')||RPAD(bb.ADDRESS_LINE4,35, ''*'')||RPAD(bb.ADDRESS_LINE5,35, ''*'')||RPAD(bb.ADDRESS_LINE6,35, ''*'')||RPAD(bb.POSTCODE_LEFT,4, ''*'')||RPAD(bb.POSTCODE_RIGHT,3, ''*'') = bbala.ADDR_KEY' ;

    commit;

  exception
     when others then
        g_code := SQLCODE;
        g_errm := SUBSTR(SQLERRM, 1 , 4000);
        obfus_log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),g_code,g_errm,g_module);
        RAISE;
  end anon_bank_branches;

  procedure anon_holder_mandates is
    v_sql varchar2(4000);
  begin

    g_module := 'anon_holder_mandates';

    obfus_log('1: truncate table rnd_disp_mandate_adresss_lu',null,null,g_module);

    execute immediate 'truncate table rnd_disp_mandate_adresss_lu';

    obfus_log('2: insert into rnd_disp_mandate_adresss_lu',null,null,g_module);

    v_sql := 'insert into rnd_disp_mandate_adresss_lu (holder_mandate_id,address_line1_key ,address_line2_key,address_line3_key,post_code_key)
    select holder_mandate_id,
           mod(abs(dbms_random.random), ' || anonymisation_process.g_max_rnd_address_line_1_seq || ') +1 address_line1_key ,
           mod(abs(dbms_random.random), ' || anonymisation_process.g_max_rnd_address_line_2_seq || ') +1 address_line2_key,
           mod(abs(dbms_random.random), ' || anonymisation_process.g_max_rnd_address_line_3_seq || ') +1 address_line3_key,
           mod(abs(dbms_random.random), ' || anonymisation_process.g_max_rnd_postcode_seq || ') +1 post_code_key
    from holder_mandates where holder_mandate_id in
    (
      select holder_mandate_id from holder_mandates hm
      minus
      select holder_mandate_id
      from holder_mandates hm2
      join holder_addresses_tmp1 hat on hat.comp_code = hm2.comp_code and hat.ivc_code = hm2.ivc_code and hat.address_type_id = 4
      join holder_names_tmp1 hnt on hnt.comp_code = hat.comp_code and hnt.ivc_code = hat.ivc_code and hnt.holder_seq = 1
    )';
 
    obfus_log('Executing: ' || v_sql,null,null,g_module);
    execute immediate v_sql;

    obfus_log('3: truncate table holder_mandates_tmp1',null,null,g_module);
    execute immediate 'truncate table holder_mandates_tmp1';

    obfus_log('4: insert into holder_mandates_tmp1',g_code,g_errm,g_module);
    
    v_sql := 'insert into holder_mandates_tmp1(holder_mandate_id,comp_code,ivc_code,class_code,mandate_type_id,BUILDING_SOCIETY_BRANCH_ID,
                                     address_line1,address_line2,address_line3,address_line4,address_line5,address_line6,
                                     postcode_left,postcode_right,irish_distribution_code,payee_name)
                                     --comp_code ivc_code address_type_id = 4, hnt.holder_seq = 1
    select  hm.holder_mandate_id,hm.comp_code,hm.ivc_code,hm.class_code,hm.mandate_type_id,hm.BUILDING_SOCIETY_BRANCH_ID,
            case when mandate_type_id = 1 and hat.holder_address_id is not null then hat.address_line1 else al1r.address_line_1 end address_line1,
            case when mandate_type_id = 1 and hat.holder_address_id is not null then hat.address_line2 else al2r.address_line_2 end address_line2,
            case when mandate_type_id = 1 and hat.holder_address_id is not null then hat.address_line3 else al3r.address_line_3 end address_line3,
            case when mandate_type_id = 1 and hat.holder_address_id is not null then hat.address_line4 else null end address_line4,
            case when mandate_type_id = 1 and hat.holder_address_id is not null then hat.address_line5 else null end address_line5,
            case when mandate_type_id = 1 and hat.holder_address_id is not null then hat.address_line6 else null end address_line6,
            case when mandate_type_id = 1 and hat.holder_address_id is not null then hat.postcode_left  else pr.postcode_left  end postcode_left,
            case when mandate_type_id = 1 and hat.holder_address_id is not null then hat.postcode_right else pr.postcode_right end postcode_right,
            case when mandate_type_id = 1 and hat.holder_address_id is not null then hat.irish_distribution_code else null end irish_distribution_code,
            case when mandate_type_id not in (2,3) then REPLACE(DECODE (title_type_code,''OTHER'', other_title,title_type_code)|| '' ''|| forename|| '' ''|| surname|| '' ''||
            suffix|| '' ''|| DECODE (NVL (trust_name, 1),''1'', '''',trust_name || ''+'' || trustee_name)|| '' ''|| company_name,''  '','' '')  else null end payee_name
            from holder_mandates hm
            left join rnd_disp_mandate_adresss_lu rdmal on rdmal.holder_mandate_id = hm.holder_mandate_id
            left join holder_addresses_tmp1 hat on hat.comp_code = hm.comp_code and hat.ivc_code = hm.ivc_code and hat.address_type_id = 4
            left join holder_names_tmp1 hnt on hnt.comp_code = hm.comp_code and hnt.ivc_code = hm.ivc_code and hnt.holder_seq = 1

            left join address_line_1_rnd al1r on  al1r.key_ns = rdmal.address_line1_key
            left join address_line_2_rnd al2r on al2r.key_ns = rdmal.address_line2_key
            left join address_line_3_rnd al3r on al3r.key_ns = rdmal.address_line3_key
            left join postcode_rnd pr on pr.key_ns =  rdmal.post_code_key';

     obfus_log('Executing: ' || v_sql,null,null,g_module);
     execute immediate v_sql;

     commit;

  exception
     when others then
        g_code := SQLCODE;
        g_errm := SUBSTR(SQLERRM, 1 , 4000);
        obfus_log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),g_code,g_errm,g_module);
        RAISE;
  end anon_holder_mandates;
--

 procedure anon_payments is
  begin

     g_module := 'anon_payments';
     obfus_log('0 - truncate tables payments_base,payments_base_fin,rnd_dispatch_name_lookup,rnd_disp_payment_adresss_lu,rnd_disp_payment_adresss_lu,payments_tmp1',g_code,g_errm,g_module);

     begin
        execute immediate 'truncate table payments_base';
     exception
        when others then
          g_code := SQLCODE;
          g_errm := SUBSTR(SQLERRM, 1 , 4000);
          obfus_log('ERROR: truncate table payments_base.',g_code,g_errm,g_module);
     end;
     
     begin
        execute immediate 'truncate table rnd_dispatch_name_lookup';
     exception
        when others then
          g_code := SQLCODE;
          g_errm := SUBSTR(SQLERRM, 1 , 4000);
          obfus_log('ERROR: truncate table rnd_dispatch_name_lookup.',g_code,g_errm,g_module);
     end;

     begin
        execute immediate 'truncate table rnd_disp_payment_adresss_lu';
     exception
        when others then
          g_code := SQLCODE;
          g_errm := SUBSTR(SQLERRM, 1 , 4000);
          obfus_log('ERROR: truncate table rnd_disp_payment_adresss_lu.',g_code,g_errm,g_module);
     end;

     begin
        execute immediate 'truncate table payments_tmp1';
     exception
        when others then
          g_code := SQLCODE;
          g_errm := SUBSTR(SQLERRM, 1 , 4000);
          obfus_log('ERROR: truncate table payments_tmp1.',g_code,g_errm,g_module);
     end;

     obfus_log('1 - insert into rnd_dispatch_name_lookup',g_code,g_errm,g_module);

     execute immediate 'insert into rnd_dispatch_name_lookup (payment_id,surname_key ,forename_key )
     select payment_id, mod(abs(dbms_random.random),'|| anonymisation_process.g_max_rnd_surname_seq ||') +1 surname_key, 
            mod(abs(dbms_random.random),'|| anonymisation_process.g_max_rnd_forename_seq ||') +1 forename_key
       from payments where payment_id in (
                                         select payment_id from payments py1
                                         minus
                                         select payment_id from payments py2
                                           join cash_transactions ct on py2.cash_transaction_id = ct.cash_transaction_id
                                           join cash_accounts ca on ct.cash_account_id = ca.cash_account_id
                                          where py2.mandate_type_id = 4 or ca.ivc_code is null)';

      obfus_log('2 - drop and create index rnd_dispatch_name_idx1',g_code,g_errm,g_module);
    begin
      execute immediate 'drop index rnd_dispatch_name_idx1';
      exception when others then null;
    end;

      execute immediate 'create index rnd_dispatch_name_idx1 on rnd_dispatch_name_lookup(payment_id)';
--    Match on address_type 4 if no match get from seed_address

     obfus_log('3 - insert into rnd_disp_payment_adresss_lu',g_code,g_errm,g_module);

     execute immediate 'insert into rnd_disp_payment_adresss_lu (payment_id,address_line1_key ,address_line2_key,address_line3_key,post_code_key)
     select payment_id,
            mod(abs(dbms_random.random),'|| anonymisation_process.g_max_rnd_address_line_1_seq ||')+1 address_line1_key ,
            mod(abs(dbms_random.random),'|| anonymisation_process.g_max_rnd_address_line_2_seq ||')+1 address_line2_key,
            mod(abs(dbms_random.random),'|| anonymisation_process.g_max_rnd_address_line_3_seq ||')+1 address_line3_key,
            mod(abs(dbms_random.random),'|| anonymisation_process.g_max_rnd_postcode_seq ||')+1 post_code_key
     from payments where payment_id in
     (
        select payment_id from payments py
        minus
        select payment_id
        from payments py2
        left outer join cash_transactions ct on py2.cash_transaction_id = ct.cash_transaction_id
        left outer join cash_accounts ca on ct.cash_account_id = ca.cash_account_id
        left outer join holder_addresses_tmp1 hat on ca.comp_code = hat.comp_code and ca.ivc_code = hat.ivc_code and hat.address_type_id = 4
      )';

    obfus_log('4 - drop and create index rnd_disp_pay_adrr_lu_idx1 on rnd_disp_payment_adresss_lu',g_code,g_errm,g_module);

    begin
      execute immediate 'drop index rnd_disp_pay_adrr_lu_idx1';
      exception when others then null;
    end;

    execute immediate 'create index rnd_disp_pay_adrr_lu_idx1 on rnd_disp_payment_adresss_lu(payment_id,address_line1_key,
                        address_line2_key,address_line3_key,post_code_key)';

    obfus_log('5 - deleting duplicates from holder_labels_tmp1',g_code,g_errm,g_module);

    delete from holder_labels_tmp1 a
    where rowid > ( select min(rowid) from holder_labels_tmp1 b
                     where a.comp_code = b.comp_code 
                       and a.ivc_code  = b.ivc_code
                       and ( b.comp_code, b.ivc_code ) in ( select comp_code, ivc_code
                                                              from (
                                                                      select comp_code, ivc_code, count(*)
                                                                        from holder_labels_tmp1
                                                                       group by comp_code, ivc_code
                                                                       having count(*) > 1 ))); 
   
    obfus_log('6 - deleted ' || sql%rowcount || ' duplicates from holder_labels_tmp1',g_code,g_errm,g_module);

    obfus_log('7 - insert into payments_base',g_code,g_errm,g_module);
    
    execute immediate 'insert into payments_base (payment_id,payer_account_number,payer_sortcode,cheque_no,sibling_sortcode,sibling_cheque_number,
                               sibling_account_number, parent_account_number,parent_cheque_number,parent_sort_code, bank_account_id,payee_name1,
                               payee_name2,payee_name3, payee_name4,payee_name5,address_line1,address_line2,			  
                               address_line3, address_line4,address_line5,address_line6, postcode_left,postcode_right,
                               irish_distribution_code,fx_comments,fx_notes,comment_text,																																										 
                               payee_building_soc_acc_no,payee_building_soc_roll_no,payee_internat_account_no,payee_internat_branch_ident,
                               dispatch_name1,dispatch_name2 )

															 
        select  py1.payment_id,py1.payer_account_number,py1.payer_sortcode,py1.cheque_no,py1.sibling_sort_code,py1.sibling_cheque_number,
        py1.sibling_account_number,py1.parent_account_number,py1.parent_cheque_number,py1.parent_sort_code,ct.bank_account_id,								  
        substr(case when ca.cash_account_type_code in ( ''ADMIN'',''CHG'',''CHRTY'',''COMM'',''CORETN'',''CPS'',''MSD'',''LCIFEE'',''LCTFEE'',''PAYR'',''PTM'',''RESREPAY'',''TAXI'',''TAXREV'') then cat.description
               else
                case when py1.mandate_type_id = 1 then fl.name ||''  ''||hn_surname.surname  else case when py1.mandate_type_id in  (2,3,4,5) then null else nvl2(py1.payee_name1,hl.label_line_1,null) end end
              end ,1,35) payee_name1,
        substr(case when ca.cash_account_type_code in ( ''ADMIN'',''CHG'',''CHRTY'',''COMM'',''CORETN'',''CPS'',''MSD'',''LCIFEE'',''LCTFEE'',''PAYR'',''PTM'',''RESREPAY'',''TAXI'',''TAXREV'') then null
               else case when py1.mandate_type_id = 1 then null else case when py1.mandate_type_id in  (2,3,4,5) then null else nvl2(py1.payee_name2,hl.label_line_2,null) end end
               end ,1,35) payee_name2 ,
        substr(case when ca.cash_account_type_code in ( ''ADMIN'',''CHG'',''CHRTY'',''COMM'',''CORETN'',''CPS'',''MSD'',''LCIFEE'',''LCTFEE'',''PAYR'',''PTM'',''RESREPAY'',''TAXI'',''TAXREV'') then null
                else case when py1.mandate_type_id = 1 then null else case when py1.mandate_type_id in  (2,3,4,5) then  null else nvl2(py1.payee_name3,hl.label_line_3,null)  end end
                end,1,35) payee_name3 ,
        substr(case when ca.cash_account_type_code in ( ''ADMIN'',''CHG'',''CHRTY'',''COMM'',''CORETN'',''CPS'',''MSD'',''LCIFEE'',''LCTFEE'',''PAYR'',''PTM'',''RESREPAY'',''TAXI'',''TAXREV'') then null
               else case when py1.mandate_type_id = 1 then null else case when py1.mandate_type_id in  (2,3,4,5) then null else nvl2(py1.payee_name4,hl.label_line_4,null)  end end
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
        substr(ut.rn(py1.fx_comments),1,255) fx_comments, 
        substr(ut.rn(py1.fx_notes),1,255) fx_notes, 																				 
        substr(ut.rn(py1.comment_text),1,255) comment_text, 																				  
        ut.tdc(py1.payee_building_soc_acc_no)  payee_building_soc_acc_no,
        ut.tdc(py1.payee_building_soc_roll_no)  payee_building_soc_roll_no,
        ut.tdc(py1.payee_internat_account_no) payee_internat_account_no,
        ut.tdc(py1.payee_internat_branch_ident) payee_internat_branch_ident,        
        substr(case when hl.label_line_1 is not null then hl.label_line_1 else fl.name||'' ''||hn_surname.surname  end ,1,35) dispatch_name1,
        substr(case when hl.label_line_2 is not null then hl.label_line_2 else null end ,1,35) dispatch_name2
        from payments  py1
        left outer join cash_transactions ct on py1.cash_transaction_id = ct.cash_transaction_id
        left outer join cash_accounts ca on ct.cash_account_id = ca.cash_account_id
        left join cash_account_types cat on cat.cash_account_type_code = ca.cash_account_type_code
        left outer join holder_addresses_tmp1 hat on ca.comp_code = hat.comp_code and ca.ivc_code = hat.ivc_code and hat.address_type_id = 4
        left outer join holder_labels_tmp1 hl on hl.comp_code =  ca.comp_code and hl.ivc_code = ca.ivc_code
        left outer join rnd_dispatch_name_lookup rdnl on py1.payment_id = rdnl.payment_id
        left outer join rnd_disp_payment_adresss_lu rdpal on py1.payment_id = rdpal.payment_id
        left outer join forename_list fl on fl.key_ns = rdnl.forename_key
        left outer join surname_list sl on sl.surname_seq = rdnl.surname_key
        left outer join holder_names_tmp1 hn_surname on hn_surname.holder_name_id = sl.holder_name_id
        left outer join address_line_1_rnd al1r on  al1r.key_ns = rdpal.address_line1_key
        left outer join address_line_2_rnd al2r on al2r.key_ns = rdpal.address_line2_key
        left outer join address_line_3_rnd al3r on al3r.key_ns = rdpal.address_line3_key
        left outer join postcode_rnd pr on pr.key_ns =  rdpal.post_code_key';

    commit;

    obfus_log('8 - Refresh stats on table PAYMENTS_BASE',g_code,g_errm,g_module);
    dbms_stats.gather_table_stats(ownname => g_run_env, tabname => 'PAYMENTS_BASE');

    commit;

    obfus_log('10 - insert into payments_tmp1',g_code,g_errm,g_module);

    execute immediate 'insert into payments_tmp1 (payment_id,cheque_no,comment_text,payment_reference,
          payee_name1,payee_name2,payee_name3,payee_name4,payee_name5,
          dispatch_address_line1,dispatch_address_line2,dispatch_address_line3,dispatch_address_line4,dispatch_address_line5,dispatch_address_line6,
          postcode_left,postcode_right,irish_distribution_code,payer_alias,payer_account_number,
          payer_sortcode,payer_iban,payer_bic,dispatch_name1,dispatch_name2,
          fx_comments, fx_notes,
          payee_chequeaddress_line1, payee_chequeaddress_line2,payee_chequeaddress_line3,payee_chequeaddress_line4,payee_chequeaddress_line5,payee_chequeaddress_line6,
          payee_building_soc_roll_no,payee_building_soc_acc_no,payee_internat_account_no,payee_internat_branch_ident,swift_code,payment_ptcp_id,
          parent_cheque_number,parent_account_number,
          sibling_cheque_number,sibling_account_number,
          fx_settlement_account_number,fx_client_ref
        )
        select py.payment_id,py.cheque_no,pb.comment_text,py.payment_reference,
               pb.payee_name1,pb.payee_name2,pb.payee_name3,pb.payee_name4,pb.payee_name5,
               pb.address_line1,pb.address_line2,pb.address_line3,pb.address_line4,pb.address_line5,pb.address_line6,
               pb.postcode_left,pb.postcode_right,pb.irish_distribution_code,py.payer_alias,py.payer_account_number,
               py.payer_sortcode,py.payer_iban,py.payer_bic,pb.dispatch_name1,pb.dispatch_name2,
               pb.fx_comments,py.fx_notes,
               pb.address_line1,pb.address_line2,pb.address_line3,pb.address_line4,pb.address_line5,pb.address_line6,
               pb.payee_building_soc_roll_no,pb.payee_building_soc_acc_no,pb.payee_internat_account_no,pb.payee_internat_branch_ident,py.swift_code,py.payment_ptcp_id,
               py.parent_cheque_number,py.parent_account_number,
               py.sibling_cheque_number,py.sibling_account_number,
               py.fx_settlement_account_number,py.fx_client_ref
          from payments py
          join payments_base pb on pb.payment_id = py.payment_id';

       commit;

    exception
       when others then
          g_code := SQLCODE;
          g_errm := SUBSTR(SQLERRM, 1 , 4000);
          obfus_log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),g_code,g_errm,g_module);
          RAISE;
  end anon_payments;

   procedure anon_rr556_cnb_reversal_rpt
   is
   begin

       g_module := 'anon_rr556_cnb_reversal_rpt';

      commit;
   exception when others then
        g_code := SQLCODE;
        g_errm := SUBSTR(SQLERRM, 1 , 4000);
        obfus_log(g_module||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),g_code,g_errm,g_module);
   end anon_rr556_cnb_reversal_rpt;

    procedure merge_rr556_cnb_reversal_rpt
   is
   begin

      g_module := 'merge_rr556_cnb_reversal_rpt';

      obfus_log('1: create index idx_rcrr1 on tgt_rr556_cnb_reversal_report',null,null,g_module);

      execute immediate 'create index idx_rcrr1 on  tgt_rr556_cnb_reversal_report(rev_total_id,cash_transaction_id,ivc_code,registration_date,transaction_amount ,
                                            reversal_msg_type,error_description,account_forename,account_surname)';

      obfus_log('2: merge into tgt_rr556_cnb_reversal_report',null,null,g_module);

      execute immediate 'merge into tgt_rr556_cnb_reversal_report tgt
      using (select rev_total_id ,ut.tdc(bank_sort_code) td_bank_sort_code, ut.tdc(account_no) td_account_no, account_no ,bank_sort_code,
                        cash_transaction_id, ivc_code,registration_date,
                        transaction_amount,reversal_msg_type,error_description,account_forename,account_surname
                   from rr556_cnb_reversal_report) res
            on (nvl(tgt.rev_total_id,-1) =nvl(res.rev_total_id,-1) and
                nvl(tgt.cash_transaction_id,-1) = nvl(res.cash_transaction_id,-1) and
                nvl(tgt.ivc_code,''$'') = nvl(res.ivc_code,''$'') and
                nvl(tgt.registration_date,to_date(''01-01-1900'', ''MM-DD-YYYY'')) = nvl(res.registration_date,to_date(''01-01-1900'', ''MM-DD-YYYY'')) and
                nvl(tgt.transaction_amount,-1) = nvl(res.transaction_amount,-1) and
                nvl(tgt.reversal_msg_type,''$'') = nvl(res.reversal_msg_type,''$'') and
                nvl(tgt.error_description,''$'') = nvl(res.error_description,''$'') and
                nvl(tgt.account_forename,''$'') = nvl(res.account_forename,''$'') and
                nvl(tgt.account_surname,''$'') =nvl(res.account_surname,''$'')  )
          when matched then update set tgt.account_no = res.td_account_no , tgt.bank_sort_code = res.td_bank_sort_code ';

      begin
         execute immediate 'drop index idx_rcrr1';
      exception when others then null;
      end;

      commit;
   exception when others then
      g_code := SQLCODE;
      g_errm := SUBSTR(SQLERRM, 1 , 4000);
      obfus_log(g_module||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),g_code,g_errm,g_module);
      RAISE;
   end merge_rr556_cnb_reversal_rpt;
/*
   procedure anon_rr556_cnb_bank_reversal
   is
   begin

       g_module := 'anon_rr556_cnb_reversal_rpt';

      commit;
   exception when others then
        g_code := SQLCODE;
        g_errm := SUBSTR(SQLERRM, 1 , 4000);
        obfus_log(g_module||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),g_code,g_errm,g_module);
        RAISE;
   end anon_rr556_cnb_bank_reversal;


  procedure merge_rr556_cnb_bank_reversal
   is
   begin

         g_module := 'merge_rr556_cnb_bank_reversal';

         obfus_log('1: merge into tgt_rr556_cnb_bank_reversal_to tgt',g_code,g_errm,g_module);

         merge into tgt_rr556_cnb_bank_reversal_to tgt
          using (select rev_total_id,bank_sort_code,ut.tdc(account_no) account_no,currency_code,
                    reversed_transaction_count,transaction_suspense_count,already_reversed_count,
                    transaction_error_count,reversed_transaction_amount,transaction_suspense_amount,
                    already_reversed_amount,transaction_error_amount
                  from rr556_cnb_bank_reversal_totals) res
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

--         obfus_log('2: create index idx_rcbrt',g_code,g_errm,g_module);
--         execute immediate 'create index idx_rcbrt on tgt_rr556_cnb_bank_reversal_to(
--         rev_total_id,bank_sort_code,currency_code,reversed_transaction_count ,transaction_suspense_count,
--         already_reversed_count,transaction_error_count,reversed_transaction_amount,transaction_suspense_amount,
--         already_reversed_amount,transaction_error_amount)';
--
--         begin
--             execute immediate 'drop index idx_rcbrt';
--         exception when others then null;
--         end;

          commit;

      exception when others then
        g_code := SQLCODE;
        g_errm := SUBSTR(SQLERRM, 1 , 4000);
        obfus_log(g_module||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),g_code,g_errm,g_module);
   end merge_rr556_cnb_bank_reversal;
*/

   procedure anon_crest_participants
   is
   begin

       g_module := 'anon_crest_participants';

      commit;
   exception when others then
        g_code := SQLCODE;
        g_errm := SUBSTR(SQLERRM, 1 , 4000);
        obfus_log(g_module||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),g_code,g_errm,g_module);
        RAISE;
   end anon_crest_participants;


  procedure merge_crest_participants
   is
   begin

         g_module := 'merge_crest_participants';

         obfus_log('1: merge into tgt_crest_participants',g_code,g_errm,g_module);

          execute immediate 'merge into tgt_crest_participants tgt
          using (select crest_ptcp,
                    ut.ofe(email_address) email_address,
                    ut.tdc(investor_address_line_1) investor_address_line_1,
                    ut.tdc(investor_address_line_2) investor_address_line_2,
                    ut.tdc(investor_address_line_3) investor_address_line_3,
                    ut.tdc(investor_address_line_4) investor_address_line_4,
                    ut.tdc(investor_address_line_5) investor_address_line_5,
                    ut.tdc(investor_post_code_left) investor_post_code_left,
                    ut.tdc(investor_post_code_right) investor_post_code_right,
                    ut.tdc(label_line_1_text) label_line_1_text,
                    ut.tdc(label_line_2_text) label_line_2_text,
                    ut.tdc(label_line_3_text) label_line_3_text,
                    ut.tdc(label_line_4_text) label_line_4_text,
                    ut.tdc(names_name_1) names_name_1,
                    ut.tdc(participant_note) participant_note,
                    ut.tdc(ptcp_address_line_1) ptcp_address_line_1,
                    ut.tdc(ptcp_address_line_2) ptcp_address_line_2,
                    ut.tdc(ptcp_address_line_3) ptcp_address_line_3,
                    ut.tdc(ptcp_address_line_4) ptcp_address_line_4,
                    ut.tdc(ptcp_address_line_5) ptcp_address_line_5,
                    ut.tdc(ptcp_name) ptcp_name,
                    ut.tdc(ptcp_postcode_left) ptcp_postcode_left,
                    ut.tdc(ptcp_postcode_right) ptcp_postcode_right,
                    ut.tdc(short_name) short_name,
                    ut.tdc(sort_key) sort_key
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
                    tgt.sort_key = res.sort_key';
            commit;

       exception when others then
        g_code := SQLCODE;
        g_errm := SUBSTR(SQLERRM, 1 , 4000);
        obfus_log(g_module||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),g_code,g_errm,g_module);
        RAISE;
   end merge_crest_participants;

   procedure anon_fatca_clsf_extracts
   is
   begin

       g_module := 'anon_fatca_clsf_extracts';

      commit;
   exception when others then
        g_code := SQLCODE;
        g_errm := SUBSTR(SQLERRM, 1 , 4000);
        obfus_log(g_module||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),g_code,g_errm,g_module);
        RAISE;
   end anon_fatca_clsf_extracts;


  procedure merge_fatca_clsf_extracts
   is
   begin

         g_module := 'merge_fatca_clsf_extracts';


         obfus_log('1: merge into tgt_fatca_clsf_extracts',g_code,g_errm,g_module);
          
          execute immediate 'merge into tgt_fatca_clsf_extracts tfse
           using (select fatca_clsf_extract_id,
                          ut.tdc(corres_address_line_1) corres_address_line_1,
                          ut.tdc(corres_address_line_2) corres_address_line_2,
                          ut.tdc(corres_address_line_3) corres_address_line_3,
                          ut.tdc(corres_address_line_5) corres_address_line_5,
                          ut.tdc(corres_address_line_6) corres_address_line_6,
                          ut.tdc(corres_post_code_left) corres_post_code_left,
                          ut.tdc(corres_post_code_right) corres_post_code_right,
                          ut.tdc(date_of_birth) date_of_birth,
                          ut.tdc(forenames) forenames,
                          ut.tdc(mandate_1_address_line_1) mandate_1_address_line_1,
                          ut.tdc(mandate_1_address_line_2) mandate_1_address_line_2,
                          ut.tdc(mandate_1_address_line_3) mandate_1_address_line_3,
                          ut.tdc(mandate_1_address_line_4) mandate_1_address_line_4,
                          ut.tdc(mandate_1_address_line_5) mandate_1_address_line_5,
                          ut.tdc(mandate_1_address_line_6) mandate_1_address_line_6,
                          ut.tdc(mandate_1_int_bank_acc_noiban) mandate_1_int_bank_acc_noiban,
                          ut.tdc(mandate_1_post_code_left) mandate_1_post_code_left,
                          ut.tdc(mandate_1_post_code_right) mandate_1_post_code_right,
                          ut.tdc(name) name,
                          ut.tdc(reged_address_line_1) reged_address_line_1,
                          ut.tdc(reged_address_line_3) reged_address_line_3,
                          ut.tdc(reged_address_line_4) reged_address_line_4,
                          ut.tdc(reged_address_line_5) reged_address_line_5,
                          ut.tdc(reged_address_line_6) reged_address_line_6,
                          ut.tdc(reged_postcodeleft) reged_postcodeleft,
                          ut.tdc(reged_postcoderight) reged_postcoderight,
                          ut.tdc(surname) surname,
                          ut.tdc(tax_ref_no_1) tax_ref_no_1,
                          ut.tdc(tax_ref_no_3) tax_ref_no_3,
                          ut.tdc(tax_ref_no_4) tax_ref_no_4
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
              tfse.tax_ref_no_4= fse.tax_ref_no_4';
              commit;

      exception when others then
        g_code := SQLCODE;
        g_errm := SUBSTR(SQLERRM, 1 , 4000);
        obfus_log(g_module||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),g_code,g_errm,g_module);
        RAISE;
   end merge_fatca_clsf_extracts;


   procedure anon_cash_transactions
   is
   begin

       g_module := 'anon_cash_transactions';

      commit;
   exception when others then
        g_code := SQLCODE;
        g_errm := SUBSTR(SQLERRM, 1 , 4000);
        obfus_log(g_module||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),g_code,g_errm,g_module);
        RAISE;
   end anon_cash_transactions;

  procedure update_cash_transactions (p_partition_name in varchar2, p_part_update_seq in number)
  is
    v_sql                 varchar2(4000);
    v_update_count        number := 0;
    v_total_update_count  number := 0;
  begin
     begin
        g_module := 'update_cash_transactions';   
        
        v_sql := 'update tgt_cash_transactions partition(' || p_partition_name || ')' || 
                 '   set comment_text = ( select SUBSTR(RN.RND_WORDS,LENGTH(COMMENT_TEXT)) ' ||
                 '                          from RANDOMISED_NOTES RN ' ||
                 '                         where MOD(CASH_TRANSACTION_ID,' || const.k_max_rnd_note || ') + 1 = RN.KEY_NS ) ' ||
                 '   where exists ( select 1
                             from RANDOMISED_NOTES RN1
                            where MOD(CASH_TRANSACTION_ID,' || const.k_max_rnd_note || ') + 1 = RN1.KEY_NS )';

        obfus_log('Partition ' || p_partition_name || ': Executing tgt_CASH_TRANSACTION update: ' || v_sql,null,null,g_module);                                                        
        execute immediate v_sql;
  
        v_update_count := SQL%ROWCOUNT;

        obfus_log(to_char(v_update_count) || ' tgt_CASH_TRANSACTION records updated in partition ' || p_partition_name,null,null,g_module); 
        
        insert into partition_update_counts (table_name, partition_name, update_count, updated_date, partition_update_id)  
        values ('CASH_TRANSACTIONS',p_partition_name,v_update_count,sysdate,p_part_update_seq);

        commit;
        
     exception when others then
        g_code := SQLCODE;
        g_errm := SUBSTR(SQLERRM, 1 , 4000);
        obfus_log(v_sql,g_code,g_errm,g_module);        
     end;                     
  end update_cash_transactions;

  procedure sleep ( p_sleep_seconds number )
  is 
    v_sql varchar2(4000);
  begin
    dbms_lock.sleep(p_sleep_seconds);
  exception
    when others then
      g_code := SQLCODE;
      g_errm := SUBSTR(SQLERRM, 1 , 4000);
      obfus_log('sleep error:'||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),g_code,g_errm,g_module);
   end sleep; 
   
   
  procedure parallel_partition_update (p_table_owner in varchar2, p_table_name in varchar2, p_part_update_seq in number)
  is

      cursor c_partitions ( cp_tgt_prefix in varchar2, cp_table_owner in varchar2, cp_table_name in varchar2)
      is
         select tp.partition_name 
           from all_tab_partitions tp
           join all_tab_statistics ts on  tp.table_owner = ts.owner 
                                      and tp.table_name = ts.table_name
                                      and tp.partition_name = ts.partition_name 
          left outer join partition_update_counts puc on tp.table_name = puc.table_name 
                                                    and tp.partition_name = puc.partition_name                                         
          where tp.table_owner = cp_tgt_prefix || '_' || cp_table_owner
            and tp.table_name = cp_table_name
            and puc.partition_name IS NULL            
            and ts.num_rows > 0;       

      cursor c_running_jobs ( cp_job_category in varchar2, cp_start_date in date )
      is            
        select count(*) 
          from all_scheduler_jobs
         where state = 'RUNNING'
           and job_name like cp_job_category ||'%'
           and start_date > cp_start_date;  
      
      cursor c_job_completion ( cp_job_category in varchar2, cp_start_date in date )
      is                       
         select count(*) 
           from all_scheduler_job_run_details 
          where job_name like cp_job_category ||'%'
            and status = 'SUCCEEDED'
            and actual_start_date > cp_start_date;
            
      cursor c_job_failure ( cp_job_category in varchar2, cp_start_date in date )
      is                       
         select job_name, run_duration, errors 
           from all_scheduler_job_run_details 
          where job_name like cp_job_category ||'%'
            and status = 'FAILED'
            and actual_start_date > cp_start_date;    
            
      cursor c_job_not_successful ( cp_job_category in varchar2, cp_start_date in date )
      is                       
         select job_name, status, additional_info 
           from all_scheduler_job_run_details 
          where job_name like cp_job_category ||'%'
            and status <> 'SUCCEEDED'
            and actual_start_date > cp_start_date;              
      
      cursor c_overrun_jobs (cp_run_env in varchar2, cp_job_name in varchar2) -- PREVENT_OVERRUN_OF_MERGE_PAYMENTS / PREVENT_OVERRUN_OF_UPDATE_CASH_TRANSACTIONS
      is
         select * 
           from all_scheduler_jobs 
          where owner = cp_run_env
            and job_name like cp_job_name ||'%';                   
     
      v_job_action                 varchar2(4000);
      v_event_condition            varchar2(4000); 
      v_queue_spec                 varchar2(1000);
      v_event_queue_subscriber     varchar2(100);
      v_job_name                   varchar2(50);
      v_job_category               varchar2(50);
      v_comments                   varchar2(1000);
      v_start_date                 date := null;
      v_running_job_count          number := 0;      
      v_job_count                  number := 0;
      v_job_success_count          number := 0; 
      v_counter                    number := 0; 
      v_total_update_count         number := 0; 
      v_part_update_count          number := 0;
      v_prev_part_update_count     number := 0;
      v_job_failure_msg            varchar2(4000);   
      v_job_not_successful_msg     varchar2(4000);
      v_overrun_job_prefix         varchar(100);
            
   begin
      begin
         g_module := 'anonymisation_process.parallel_partition_update';

         v_start_date := sysdate;

       -- partition_update_counts truncated by obfuscation_control.truncate_report_tables on creation of new obfus_run_id
                
         for r in c_partitions(g_tgt_prefix, p_table_owner, p_table_name)
         loop -- JOB CREATION LOOP
           
           loop
             if v_counter > const.k_max_parallel_jobs
             then
               sleep(const.k_sleep_seconds);              
               obfus_log('3: opening c_running_jobs with job_category: '||v_job_category||' and start_date '|| to_char(v_start_date,'dd-mon-yyyy hh24:mi:ss'),null,null,g_module);
               open c_running_jobs(v_job_category,v_start_date);
               fetch c_running_jobs into v_running_job_count;
               close c_running_jobs;
               obfus_log('v_running_job_count = '||to_char(v_running_job_count),null,null,g_module);               
             end if;  
             
             if v_running_job_count > const.k_max_parallel_jobs 
             then
               obfus_log('4: Running job count '||to_char(v_running_job_count)||' exceeds const.k_max_parallel_jobs, so continuing to next iteration after sleep of ' || to_char(const.k_sleep_seconds),null,null,g_module);
               sleep(const.k_sleep_seconds);
               continue;
             else   
             
               if p_table_name = 'CASH_TRANSACTIONS' then
                 obfus_log('5: Creating DBMS_SCHEDULER job update_tgt_CASH_TRANSACTIONS_'|| r.partition_name,null,null,g_module);                                                        
                 v_job_action := 'BEGIN anonymisation_process.update_cash_transactions('||chr(39)||r.partition_name||chr(39)||','||chr(39)||p_part_update_seq||chr(39)||'); END;';
                 v_job_category := 'UPDATE_CASH_TRANSACTIONS_';
                 v_job_name   := v_job_category|| r.partition_name;
                 v_comments   := 'update tgt_CASH_TRANSACTIONS by partition.';
               elsif p_table_name = 'PAYMENTS' then
                 obfus_log('5: Creating DBMS_SCHEDULER job MERGE_PAYMENTS_'|| r.partition_name,null,null,g_module);                                                        
                 v_job_action := 'BEGIN anonymisation_process.merge_payments('||chr(39)||r.partition_name||chr(39)||','||chr(39)||p_part_update_seq||chr(39)||'); END;';
                 v_job_category := 'MERGE_PAYMENTS_';                 
                 v_job_name   := v_job_category|| r.partition_name;    
                 v_comments   := 'merge tgt_PAYMENTS by partition.';
               end if;
               
               obfus_log('v_job_action: '|| v_job_action,null,null,g_module);
               --dbms_output.put_line(v_job_action);
               obfus_log('v_job_name: '|| v_job_name,null,null,g_module);           
               --dbms_output.put_line(v_job_name); 

               begin
                 DBMS_SCHEDULER.DROP_JOB(job_name => v_job_name); 
               exception
                 when others then 
                   null;
               end;
               
               sleep(const.k_sleep_seconds); -- prevent contention / dbms_scheduler lock up
               
               DBMS_SCHEDULER.CREATE_JOB (
                  job_name           =>  v_job_name,
                  job_type           =>  'PLSQL_BLOCK',
                  job_action         =>  v_job_action,
                  start_date         =>  SYSDATE,
                  enabled            =>  FALSE,
                  comments           =>  v_comments
               );      
                
               -- set max run time 
               dbms_scheduler.set_attribute (name => v_job_name, attribute => 'max_run_duration', value => numtodsinterval(const.k_max_run_duration_mins, 'MINUTE'));
               -- set all events to be raised (for debugging)
               dbms_scheduler.set_attribute(name => v_job_name, attribute => 'raise_events', value => DBMS_SCHEDULER.job_all_events);
               -- start the job
               dbms_scheduler.enable(name => v_job_name);                  
                              
               v_job_count := v_job_count + 1;               
               
               v_job_action := 'BEGIN DBMS_SCHEDULER.STOP_JOB(job_name => '||chr(39)||v_job_name||chr(39)||'); END;';
               --dbms_output.put_line('v_job_action: ' || v_job_action);
               v_event_condition := 'tab.user_data.object_name = '||chr(39)||v_job_name||chr(39)||' and tab.user_data.event_type = ' ||chr(39)||'JOB_OVER_MAX_DUR'||chr(39);
               --dbms_output.put_line('v_event_condition: ' || v_event_condition);
               v_event_queue_subscriber := g_run_env || '_obfus_jobs_agent';
               --dbms_output.put_line('v_event_queue_subscriber: ' || v_event_queue_subscriber);
               v_queue_spec := 'sys.scheduler$_event_queue,' || v_event_queue_subscriber;
               --dbms_output.put_line('v_queue_spec: ' || v_queue_spec);
               v_job_name := const.k_overrun_job_prefix || v_job_name;
                
               begin
                 DBMS_SCHEDULER.DROP_JOB(job_name => v_job_name); 
               exception
                 when others then 
                   null;
               end;               
               
               sleep(const.k_sleep_seconds);  -- prevent contention / dbms_scheduler lock up
               
               dbms_scheduler.create_job( job_name        =>  v_job_name,
                                          job_type        => 'PLSQL_BLOCK',               
                                          job_action      =>  v_job_action,
                                          event_condition =>  v_event_condition,
                                          queue_spec      =>  v_queue_spec,
                                          enabled         =>  true);             
              
             end if;  
             exit;
           end loop;
           
           v_counter := v_counter+1;
           obfus_log('v_counter: '|| to_char(v_counter),null,null,g_module);            
                                  
         end loop; -- END JOB CREATION LOOP
         
         while v_job_count > v_part_update_count -- JOB MONITORING LOOP
         loop
               
            for r in c_job_failure(v_job_category,v_start_date)
            loop
               v_job_failure_msg := r.job_name || ' failed with errors: ' || r.errors;
               RAISE excep.x_job_failure;
            end loop;
            
            --obfus_log( 'Opening c_job_not_successful for v_job_category '|| v_job_category || ' and start_date ' || to_char(v_start_date,'dd-mon-yyyy hh24:mi:ss'),null,null,g_module);    
            for r in c_job_not_successful(v_job_category,v_start_date)
            loop
               v_job_not_successful_msg := r.job_name || ' status: ' || r.status || ' additional_info: ' || r.additional_info;
               obfus_log( 'RAISE x_job_not_successful for '|| v_job_not_successful_msg,null,null,g_module);   
               RAISE excep.x_job_not_successful;
            end loop;            
                             
            --obfus_log( 'Successfully completed '|| to_char(v_job_success_count) ||' of '||  to_char(v_job_count) ||' jobs.',null,null,g_module);   
            v_prev_part_update_count := v_part_update_count;   
               
            begin
              select count(*)
                into v_part_update_count
                from partition_update_counts
               where table_name = p_table_name
                 and partition_update_id = p_part_update_seq;              
            exception
              when others then
                g_code := SQLCODE;
                g_errm := SUBSTR(SQLERRM, 1 , 4000);
                obfus_log('Error counting ' || p_table_name || ' partitions updated',g_code,g_errm,g_module);         
            end;           
   
            if v_prev_part_update_count < v_part_update_count
            then
              obfus_log( 'Successfully updated '|| to_char(v_part_update_count) ||' of '||  to_char(v_job_count) ||' partitions.',null,null,g_module);    
            end if;
            
            sleep(const.k_sleep_seconds);             
            
            obfus_log( 'v_job_count = '|| to_char(v_job_count) ||' and v_part_update_count = '|| to_char(v_part_update_count),null,null,g_module);
         end loop;  -- END JOB MONITORING LOOP       
         
         begin
           select sum(update_count)
             into v_total_update_count
             from partition_update_counts
            where table_name = p_table_name;
         exception
           when others then
             g_code := SQLCODE;
             g_errm := SUBSTR(SQLERRM, 1 , 4000);
             obfus_log('Error summing partition_update_counts',g_code,g_errm,g_module);         
         end;
         
         obfus_log('Successfully completed ' || to_char(v_part_update_count) || ' jobs to update ' || to_char(v_total_update_count) ||' '|| p_table_name || ' records in parallel by partition.',null,null,g_module);   

         obfus_log('Dropping overrun jobs which are no longer required due to successful completion.',null,null,g_module);   

         if p_table_name = 'CASH_TRANSACTIONS' then
            v_overrun_job_prefix := const.k_overrun_job_prefix || 'UPDATE_CASH_TRANSACTIONS';
         elsif p_table_name = 'PAYMENTS' then
            v_overrun_job_prefix := const.k_overrun_job_prefix || 'MERGE_PAYMENTS';
         end if;
         
         for r in c_overrun_jobs(g_run_env, v_overrun_job_prefix)
         loop
            DBMS_SCHEDULER.DROP_JOB (job_name => r.job_name);
         end loop;   
                  
         commit;
        
      exception 
         when excep.x_job_failure then 
           obfus_log(v_job_failure_msg,g_code,g_errm,g_module);
           RAISE;         
         when excep.x_job_not_successful then 
           obfus_log(v_job_not_successful_msg,g_code,g_errm,g_module);
           RAISE;            
         when others then
           g_code := SQLCODE;
           g_errm := SUBSTR(SQLERRM, 1 , 4000);
           obfus_log(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),g_code,g_errm,g_module);
           RAISE;
      end;
   end parallel_partition_update;   
  
  procedure merge_cash_transactions
  is
    v_part_update_seq  number(38,1);         
   begin
      begin
         g_module := 'merge_cash_transactions';

         obfus_log('1: fetching partition_update_seq.nextval '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,g_module);

         select partition_update_seq.nextval
          into v_part_update_seq
          from dual;

         obfus_log('2: gather_table_stats for RANDOMISED_NOTES '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,g_module);

         begin
            dbms_stats.gather_table_stats(ownname=>g_run_env, tabname=>'RANDOMISED_NOTES', cascade=>true);
         end;

         obfus_log('3: updating tgt_cash_transactions.comment_text '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,g_module);

         anonymisation_process.parallel_partition_update('CASH_MANAGEMENT', 'CASH_TRANSACTIONS', v_part_update_seq);

         commit;
        
      exception            
         when others then
           g_code := SQLCODE;
           g_errm := SUBSTR(SQLERRM, 1 , 4000);
           obfus_log(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),g_code,g_errm,g_module);
           RAISE;
      end;
   end merge_cash_transactions;


   procedure anon_ideal_trans
   is
   begin

       g_module := 'anon_ideal_trans';

      commit;
   exception when others then
        g_code := SQLCODE;
        g_errm := SUBSTR(SQLERRM, 1 , 4000);
        obfus_log(g_module||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),g_code,g_errm,g_module);
        RAISE;
   end anon_ideal_trans;


  procedure merge_ideal_trans
   is
   begin

         g_module := 'merge_ideal_trans';

         obfus_log('1: merge into TGT_IDEAL_TRANS',g_code,g_errm,g_module);

          execute immediate 'merge into TGT_IDEAL_TRANS tit
          using (select tit1.IDEAL_TRAN_id,tit1.DATE_OF_BIRTH
          from TGT_IDEAL_TRANS tit1
          join IDEAL_TRANS ti on ti.IDEAL_TRAN_id = tit1.IDEAL_TRAN_id and ti.DATE_OF_BIRTH = tit1.DATE_OF_BIRTH)res
          on ( tit.IDEAL_TRAN_id = res.IDEAL_TRAN_id)
          when matched
          then
            update set tit.DATE_OF_BIRTH  = ut.RD30(res.DATE_OF_BIRTH)';

      commit;
      exception when others then
        g_code := SQLCODE;
        g_errm := SUBSTR(SQLERRM, 1 , 4000);
        obfus_log(g_module||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),g_code,g_errm,g_module);
        RAISE;
   end merge_ideal_trans;

   procedure anon_money_launder_ctls
   is
   begin

       g_module := 'anon_money_launder_ctls';

      commit;
   exception when others then
        g_code := SQLCODE;
        g_errm := SUBSTR(SQLERRM, 1 , 4000);
        obfus_log(g_module||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),g_code,g_errm,g_module);
        RAISE;
   end anon_money_launder_ctls;

  procedure merge_money_launder_ctls
   is
   begin

         g_module := 'merge_money_launder_ctls';


        obfus_log('11: merge into TGT_MONEY_LAUNDER_CTLS',g_code,g_errm,g_module);
        execute immediate 'merge into TGT_MONEY_LAUNDER_CTLS tmlc
        using (select tmlc1.MONEY_LAUNDER_CTL_ID,tmlc1.DATE_OF_BIRTH
        from TGT_MONEY_LAUNDER_CTLS tmlc1
        join MONEY_LAUNDER_CTLS mlc on mlc.MONEY_LAUNDER_CTL_ID = tmlc1.MONEY_LAUNDER_CTL_ID and mlc.DATE_OF_BIRTH = tmlc1.DATE_OF_BIRTH)res
        on ( tmlc.MONEY_LAUNDER_CTL_ID= res.MONEY_LAUNDER_CTL_ID)
        when matched
        then
           update set tmlc.DATE_OF_BIRTH = ut.RD30(res.DATE_OF_BIRTH)';

      commit;
      exception when others then
        g_code := SQLCODE;
        g_errm := SUBSTR(SQLERRM, 1 , 4000);
        obfus_log(g_module||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),g_code,g_errm,g_module);
        RAISE;
   end merge_money_launder_ctls;


  procedure merge_holders
   is
   begin

      g_module := 'merge_holders';

      obfus_log('1: disable HOLDERS_BRIUD trigger',null,null,g_module);
      execute immediate 'ALTER TRIGGER '||g_tgt_prefix||'_PRISM_CORE.HOLDERS_BRIUD disable';
      --
      obfus_log('2: merge into tgt_holders',null,null,g_module);

      execute immediate 'merge into tgt_holders th
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
                    th.previous_sortkey = h.previous_sortkey';

      commit;

      obfus_log('3: enable HOLDERS_BRIUD trigger',null,null,g_module);
      execute immediate 'ALTER TRIGGER '||g_tgt_prefix||'_PRISM_CORE.HOLDERS_BRIUD enable';

   exception when others then
      g_code := SQLCODE;
      g_errm := SUBSTR(SQLERRM, 1 , 4000);
      obfus_log(g_module||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),g_code,g_errm,g_module);
      RAISE;
   end merge_holders;


procedure merge_holder_names
   is
   begin
      g_module := 'merge_holder_names';

      obfus_log('merge into tgt_holder_names',null,null,g_module);

      execute immediate 'merge into tgt_holder_names hn
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
                    hn.company_name = ahn.company_name';

      --commit is necessary for the get_sort_key_function to work
      commit;

      exception when others then
        g_code := SQLCODE;
        g_errm := SUBSTR(SQLERRM, 1 , 4000);
        obfus_log(g_module||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),g_code,g_errm,g_module);
        RAISE;
   end merge_holder_names;

  procedure merge_bank_branches
   is
   begin

      g_module := 'merge_bank_branches';

      obfus_log('merge into tgt_bank_branches',null,null,g_module);

      execute immediate 'merge into tgt_bank_branches bb
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
         bb.postcode_right = abb.postcode_right';

      commit;

      exception when others then
        g_code := SQLCODE;
        g_errm := SUBSTR(SQLERRM, 1 , 4000);
        obfus_log(g_module||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),g_code,g_errm,g_module);
        RAISE;
   end merge_bank_branches;


   procedure merge_holder_address
   is
   begin

      g_module := 'merge_holder_address';

      obfus_log('merge into tgt_holder_addresses',null,null,g_module);

      execute immediate 'merge into tgt_holder_addresses ha
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
            ha.irish_distribution_code = aha.irish_distribution_code';

   exception when others then
      g_code := SQLCODE;
      g_errm := SUBSTR(SQLERRM, 1 , 4000);
      obfus_log(g_module||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),g_code,g_errm,g_module);
      RAISE;
   end merge_holder_address;

   procedure merge_holder_employee_details
   is
   begin

      g_module := 'merge_holder_employee_details';

      obfus_log('merge into tgt_holder_employee_details',null,null,g_module);

      execute immediate 'merge into tgt_holder_employee_details hed
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
                    hed.payroll_number = ah.payroll_number';

      exception when others then
        g_code := SQLCODE;
        g_errm := SUBSTR(SQLERRM, 1 , 4000);
        obfus_log(g_module||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),g_code,g_errm,g_module);
        RAISE;
   end merge_holder_employee_details;

   procedure merge_holder_labels
   is
   begin

      g_module := 'merge_holder_labels';

      obfus_log('merge into tgt_holder_labels',null,null,g_module);

      execute immediate 'merge into tgt_holder_labels hl
           using (select holder_label_id,label_line_1, label_line_2, label_line_3,label_line_4
                    from  holder_labels_tmp1 ) ahn
              on (hl.holder_label_id = ahn.holder_label_id)
      when matched
      then
         update set hl.line1_text =  substr(ahn.label_line_1,1,35),
                    hl.line2_text  = substr(ahn.label_line_2,1,35),
                    hl.line3_text =  substr(ahn.label_line_3,1,35),
                    hl.line4_text =  substr(ahn.label_line_4,1,35)';

      commit;

      exception when others then
        g_code := SQLCODE;
        g_errm := SUBSTR(SQLERRM, 1 , 4000);
        obfus_log(g_module||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),g_code,g_errm,g_module);
        RAISE;
   end merge_holder_labels;

   procedure merge_holder_mandate_details
   is
   begin

      g_module := 'merge_holder_mandate_details';

      obfus_log('merge into tgt_holder_mandates',null,null,g_module);

      execute immediate 'merge into tgt_holder_mandates hm
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
                           hmt.payee_name
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
            hm.payee_name = res.payee_name';

      exception when others then
        g_code := SQLCODE;
        g_errm := SUBSTR(SQLERRM, 1 , 4000);
        obfus_log(g_module||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),g_code,g_errm,g_module);
        RAISE;
   end merge_holder_mandate_details;


   procedure merge_disc_exer_req_mandates
   is
   begin

      g_module := 'merge_disc_exer_req_mandates';

      obfus_log('merge into tgt_disc_exer_req_mandates',null,null,g_module);

      execute immediate 'merge into tgt_disc_exer_req_mandates tgt
      using (select disc_exer_req_mandate_id ,bank_acc_no,society_acc_roll_no,
                    bank_id_code_bic,int_bank_acc_no_iban,int_acc_no,address_line_1,
                    address_line_2,address_line_3,address_line_6,country_code
             from  disc_exer_req_mandates_tmp1 desd ) res
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
                tgt.country_code = res.country_code';

    exception when others then
      g_code := SQLCODE;
      g_errm := SUBSTR(SQLERRM, 1 , 4000);
      obfus_log(g_module||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),g_code,g_errm,g_module);
      RAISE;
  end merge_disc_exer_req_mandates;


  procedure merge_disc_exer_spouse_dtls
   is
   begin
      g_module := 'merge_disc_exer_spouse_dtls';

      obfus_log('merge into tgt_disc_exer_spouse_dtls',null,null,g_module);

      execute immediate 'merge into tgt_disc_exer_spouse_dtls tgt
      using (select discret_exercise_req_id,surname,forenames,title,
             address_line_1,address_line_2,address_line_3,address_line_4,
             address_line_5,address_line_6,post_code_left,post_code_right,country_code
             from  disc_exer_spouse_dtls_tmp1 desd ) res
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
                tgt.country_code = res.country_code';

    exception when others then
      g_code := SQLCODE;
      g_errm := SUBSTR(SQLERRM, 1 , 4000);
      obfus_log(g_module||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),g_code,g_errm,g_module);
      RAISE;
  end merge_disc_exer_spouse_dtls;

  procedure merge_mifid_entities
   is
   begin

      g_module := 'merge_mifid_entities';

      obfus_log('merge into tgt_mifid_entities',null,null,g_module);

      execute immediate 'merge into tgt_mifid_entities tgt
      using (select mifid_entity_id,surname,forenames,register_surname,
             register_forenames
             from  mifid_entities_tmp1 me ) res
      on (tgt.mifid_entity_id = res.mifid_entity_id)
      when matched then update
          set   tgt.surname = res.surname,
                tgt.forenames = res.forenames,
                tgt.register_surname = res.register_surname,
                tgt.register_forenames = res.register_forenames';

    exception when others then
      g_code := SQLCODE;
      g_errm := SUBSTR(SQLERRM, 1 , 4000);
      obfus_log(g_module||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),g_code,g_errm,g_module);
      RAISE;
  end merge_mifid_entities;

  procedure merge_cash_ivc_class_copies
   is
   begin

      g_module := 'merge_cash_ivc_class_copies';

      obfus_log('merge into tgt_cash_ivc_class_copies',null,null,g_module);

      execute immediate 'merge into tgt_cash_ivc_class_copies tgt
      using (select cash_ivc_class_copy_id,payee_name,
                    address_line_1,address_line_2,address_line_3,address_line_4,address_line_5,address_line_6,
                    post_code_left,post_code_right,irish_distribution_code
             from  cash_ivc_class_copies_tmp1 cicc ) res
      on (tgt.cash_ivc_class_copy_id = res.cash_ivc_class_copy_id)
      when matched then update
          set tgt.payee_name      = res.payee_name,
              tgt.address_line_1  = res.address_line_1,
              tgt.address_line_2  = res.address_line_2,
              tgt.address_line_3  = res.address_line_3,
              tgt.address_line_4  = res.address_line_4,
              tgt.address_line_5  = res.address_line_5,
              tgt.address_line_6  = res.address_line_6,
              tgt.post_code_left  = res.post_code_left,
              tgt.post_code_right  = res.post_code_right,
              tgt.irish_distribution_code  = res.irish_distribution_code';

    exception when others then
      g_code := SQLCODE;
      g_errm := SUBSTR(SQLERRM, 1 , 4000);
      obfus_log(g_module||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),g_code,g_errm,g_module);
      RAISE;
  end merge_cash_ivc_class_copies;

  procedure merge_comp_payee_mandates
  is
  begin

     g_module := 'merge_comp_payee_mandates';

     obfus_log('merge into tgt_comp_payee_mandates',null,null,g_module);

     execute immediate 'merge into tgt_comp_payee_mandates tgt
     using (select comp_payee_id,mandate_type_id,
                    address_line1,address_line2,address_line3,address_line4,
                    address_line5,address_line6,post_code_left,post_code_right,irish_distribution_code
             from  comp_payee_mandates_tmp1 cpm ) res
        on (tgt.comp_payee_id = res.comp_payee_id)
      when matched then update
          set tgt.mandate_type_id = tgt.mandate_type_id,
              tgt.address_line1  = tgt.address_line1,
              tgt.address_line2  = tgt.address_line2,
              tgt.address_line3  = tgt.address_line3,
              tgt.address_line4  = tgt.address_line4,
              tgt.address_line5  = tgt.address_line5,
              tgt.address_line6  = tgt.address_line6,
              tgt.post_code_left  = tgt.post_code_left,
              tgt.post_code_right  = tgt.post_code_right,
              tgt.irish_distribution_code  = tgt.irish_distribution_code';

  exception when others then
     g_code := SQLCODE;
     g_errm := SUBSTR(SQLERRM, 1 , 4000);
     obfus_log(g_module||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),g_code,g_errm,g_module);
     RAISE;
  end merge_comp_payee_mandates;

   procedure merge_mifid_trans_details
   is
   begin

      g_module := 'merge_mifid_trans_details';

      obfus_log('merge into tgt_mifid_transaction_details',null,null,g_module);

      execute immediate 'merge into tgt_mifid_transaction_details tgt
      using (select transaction_detail_id,holder_surname
             from  mifid_transaction_details_tmp1 mtd ) res
      on (tgt.transaction_detail_id = res.transaction_detail_id)
      when matched then update
          set tgt.holder_surname = res.holder_surname';

    exception when others then
      g_code := SQLCODE;
      g_errm := SUBSTR(SQLERRM, 1 , 4000);
      obfus_log(g_module||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),g_code,g_errm,g_module);
      RAISE;
  end merge_mifid_trans_details;

  procedure merge_mifid_bulk_trades
  is
  begin

      g_module := 'merge_mifid_bulk_trades';

      obfus_log('merge into tgt_mifid_bulk_trades',null,null,g_module);

      execute immediate 'merge into tgt_mifid_bulk_trades tgt
      using (select bulk_trade_id,holder_surname
             from   mifid_bulk_trades_tmp1 mbt ) res
      on (tgt.bulk_trade_id = res.bulk_trade_id)
      when matched then update
          set tgt.holder_surname = res.holder_surname';

  exception when others then
      g_code := SQLCODE;
      g_errm := SUBSTR(SQLERRM, 1 , 4000);
      obfus_log(g_module||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),g_code,g_errm,g_module);
      RAISE;
  end merge_mifid_bulk_trades;

   procedure merge_cheque_ranges
   is
   begin

      g_module := 'merge_cheque_ranges';

      DBMS_SESSION.set_identifier ('adcfs\ksheehan1' || ':' || '1');
      begin
        execute immediate 'insert into tgt_audit_events select * from audit_events where event_id =1';
        exception when others then null;
      end;

      obfus_log('merge into tgt_cheque_ranges',null,null,g_module);

      execute immediate 'merge into tgt_cheque_ranges tgt
      using (select crt.cheque_range_id,crt.end_no,crt.last_cheque_no_used,crt.start_no,crt.warning_threshold
            from  cheque_ranges_tmp1 crt ) res
      on (tgt.cheque_range_id = res.cheque_range_id )
      when matched then update
          set tgt.end_no = res.end_no,
                tgt.last_cheque_no_used = res.last_cheque_no_used,
                tgt.start_no = res.start_no,
                tgt.warning_threshold = res.warning_threshold';

      exception when others then
        g_code := SQLCODE;
        g_errm := SUBSTR(SQLERRM, 1 , 4000);
        obfus_log(g_module||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),g_code,g_errm,g_module);
        RAISE;
  end merge_cheque_ranges;

  procedure merge_payments (p_partition_name in varchar2, p_part_update_seq in number)
  is
     v_sql           varchar2(4000); 
     v_update_count  number;
  begin

     g_module := 'merge_payments';

     obfus_log('merge into tgt_payments('||p_partition_name||')',null,null,g_module);
  
     v_sql := 'merge into tgt_payments partition('||p_partition_name||') tp ' ||
              ' using (select py.payment_id,
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
                         py.payee_internat_branch_ident,
                         py.payment_reference,
                         py.fx_notes,
                         py.fx_comments,
                         py.fx_client_ref,
                         py.fx_settlement_account_number,
                         py.comment_text  comment_text  
                    from payments_tmp1 py
                    join payments partition('||p_partition_name||') p on py.payment_id = p.payment_id) res
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
            postcode_left = res.postcode_left,
            postcode_right = res.postcode_right,
            irish_distribution_code = res.irish_distribution_code,
            payee_chequeaddress_line1 = res.payee_chequeaddress_line1,
            payee_chequeaddress_line2  = res.payee_chequeaddress_line2,
            payee_chequeaddress_line3 = res.payee_chequeaddress_line3,
            payee_chequeaddress_line4 = res.payee_chequeaddress_line4,
            payee_chequeaddress_line5 = res.payee_chequeaddress_line5,
            payee_chequeaddress_line6 = res.payee_chequeaddress_line6,
            payee_name1=res.payee_name1,
            payee_name2=res.payee_name2,
            payee_name3=res.payee_name3,
            payee_name4=res.payee_name4,
            payee_name5=res.payee_name5,
            dispatch_name1 =  res.dispatch_name1,
            dispatch_name2 =  res.dispatch_name2,
            payment_reference = res.payment_reference,
            payee_building_soc_acc_no=res.payee_building_soc_acc_no,
            payee_building_soc_roll_no=res.payee_building_soc_roll_no,
            payee_internat_account_no=res.payee_internat_account_no,
            payee_internat_branch_ident = res.payee_internat_branch_ident,
            comment_text = res.comment_text,
            fx_notes = res.fx_notes,
            fx_comments = res.fx_comments,
            fx_client_ref = res.fx_client_ref,
            fx_settlement_account_number = res.fx_settlement_account_number';

      obfus_log('Executing tgt_payments merge for partition: '||p_partition_name||' sql: '||v_sql,null,null,g_module);                                                        
      execute immediate v_sql;
  
      v_update_count := SQL%ROWCOUNT;

      obfus_log('Updated ' || to_char(v_update_count) || ' payments records in partition ' || p_partition_name,null,null,g_module); 
        
      insert into partition_update_counts (table_name, partition_name, update_count, updated_date, partition_update_id) 
      values ('PAYMENTS',p_partition_name,v_update_count, sysdate, p_part_update_seq);
        
      commit;               

  exception
     when others then
        g_code := SQLCODE;
        g_errm := SUBSTR(SQLERRM, 1 , 4000);
        obfus_log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),g_code,g_errm,g_module);
        RAISE;
  end merge_payments;  

  procedure merge_payments
  is
    v_part_update_seq  number(38,1); 
  begin

    g_module := 'merge_payments';

    obfus_log('1: fetching partition_update_seq.nextval '||to_char(sysdate,'dd-mm-yyyy hh24.mi.ss'),null,null,g_module);
  
    select partition_update_seq.nextval
      into v_part_update_seq
      from dual;
      
    anonymisation_process.parallel_partition_update('CASH_MANAGEMENT', 'PAYMENTS', v_part_update_seq);

    commit;

  exception
     when others then
        g_code := SQLCODE;
        g_errm := SUBSTR(SQLERRM, 1 , 4000);
        obfus_log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),g_code,g_errm,g_module);
        RAISE;
  end merge_payments;


  procedure apply_temp_patches is
     v_sql varchar2(2000);
     v_sParam varchar2(60);
  begin
     g_module := 'apply_temp_patches';
     
     -- SIP_CASH_RECON_TEMP
     
--     begin
       
--      begin
--        execute immediate 'DROP TABLE '||g_run_env||'.S_STG_SIP_CASH_RECON_TEMP';
--        exception when others then null;
--       end;
--       
--      v_sql := 'create table '||g_run_env||'.S_STG_SIP_CASH_RECON_TEMP (COMP_CODE VARCHAR2(4 CHAR), TEXTLINE  VARCHAR2(4000 CHAR),CREATED_DATE TIMESTAMP(6))';
--      anonymisation_process.obfus_log('Executing: ' || v_sql,null,null,g_module);
--      execute immediate v_sql;
--      
--      v_sql := 'INSERT  INTO '||g_run_env||'.S_STG_SIP_CASH_RECON_TEMP(COMP_CODE,TEXTLINE,CREATED_DATE) 	
--                   SELECT COMP_CODE	,ut.fn_char_mask(TEXTLINE)	,CREATED_DATE FROM '||g_tgt_prefix||'_PRISM_CORE.SIP_CASH_RECON_TEMP NOOGGING';	
--
--      anonymisation_process.obfus_log('Executing: ' || v_sql,null,null,g_module);
--      execute immediate v_sql;
--      v_sParam := g_tgt_prefix||'_PRISM_CORE';
--  
--      ut.truncate_table_new(v_sParam,'SIP_CASH_RECON_TEMP');
--
--      ut.rebuild_indexes(v_sParam,'SIP_CASH_RECON_TEMP');
--       
--      v_sql := 'INSERT  INTO '||g_tgt_prefix||'_PRISM_CORE.SIP_CASH_RECON_TEMP(COMP_CODE	,TEXTLINE	,CREATED_DATE) 	
--      SELECT COMP_CODE	,ut.fn_char_mask(TEXTLINE)	,CREATED_DATE FROM '||g_run_env||'.S_STG_SIP_CASH_RECON_TEMP NOOGGING' ;
--      
--      dbms_output.put_line(v_sql);
--      anonymisation_process.obfus_log('Executing: ' || v_sql,null,null,g_module);
--      execute immediate v_sql;
--
--      anonymisation_process.obfus_log(SQL%ROWCOUNT || ' rows inserted into '||g_tgt_prefix||'_PRISM_CORE.SIP_CASH_RECON_TEMP: ' || v_sql,null,null,g_module);
--       
--       
--      exception when others then
--          g_code := SQLCODE;
--          g_errm := SUBSTR(SQLERRM, 1 , 4000);
--          obfus_log('Error inserting into '||g_tgt_prefix||'_PRISM_CORE.SIP_CASH_RECON_TEMP: ' || v_sql,g_code,g_errm,g_module);
--          raise;
--    end;
    
    begin  
      v_sql := 'merge into tgt_EMPLOYING_COMP_TAX_DETAILS tgt
                using ( select UT.TDC(SUBSTR(TGT1.DISPLAY_EMPLOYER_NAME
                            || ROUND(dbms_random.value(1,999999)),1,60)) DISPLAY_EMPLOYER_NAME,
                               TGT1.TAX_DETAIL_ID 
                 from TGT_EMPLOYING_COMP_TAX_DETAILS TGT1 ,S_STG_EMPLOYING_COMP_TAX_DETAILS SRC  
         where  SRC.DISPLAY_EMPLOYER_NAME=TGT1.DISPLAY_EMPLOYER_NAME 
           and (SRC.TAX_DETAIL_ID=TGT1.TAX_DETAIL_ID)) src 
            on (tgt.TAX_DETAIL_ID=src.TAX_DETAIL_ID) 
       when matched then update set tgt.DISPLAY_EMPLOYER_NAME= SRC.DISPLAY_EMPLOYER_NAME';
 
      anonymisation_process.obfus_log('Executing: ' || v_sql,null,null,g_module);         
      execute immediate v_sql;
     
    exception when others then
       g_code := SQLCODE;
       g_errm := SUBSTR(SQLERRM, 1 , 4000);
       obfus_log('Error merging into tgt_EMPLOYING_COMP_TAX_DETAILS',g_code,g_errm,g_module);
       raise;
    end;     
     
  end apply_temp_patches;

 
  procedure generate_table_qa_reports(p_owner varchar2,p_table_name varchar2, p_src_prefix varchar2,p_run_date date,p_anon_version varchar2,p_src_rep_syn_prefix varchar2,p_tgt_rep_syn_prefix varchar2)  as

     cursor c_get_stmt(p_owner varchar2,p_table_name varchar2,p_src_prefix varchar2, p_anon_version varchar2) is 
          
       select distinct pt.owner,pt.table_name,pt.column_name, 'Eq_unique' stat_type,pt.technique,pt.stereo_type,
        ' select listagg('||'X'||','','') within group (ORDER BY 1 ) 
            from
            ( select ''<'' ||substr( x.'||pt.column_name||',1,(4000/25) -2) || ''>'' X ' ||
             '  from ' ||p_src_rep_syn_prefix||'_'||pt.table_name||  ' x , '||p_tgt_rep_syn_prefix||'_'||pt.table_name||' y ' ||
             ' where ' ||pk.pkjoin ||' and '|| 'x.' ||pt.column_name || ' = y.' ||pt.column_name ||
             '   and rownum < 25 group by '||'x.' ||pt.column_name || ',y.' ||pt.column_name ||')'
           AS stmt
        from ( select owner,table_name,column_name,technique,stereo_type  
                 from stats_results_2 
                where anon_version = p_anon_version 
                  and equals > 0 and trans_function <> 'ut.EXCLUDE') pt 
        join tmp_tab_pk pk on pk.table_name = pt.table_name
                          and pk.owner = p_src_prefix||'_'||pt.owner 
      where pk.table_name = p_table_name;
              
              
      cursor c_get_stmt2(p_owner varchar2,p_table_name varchar2,p_src_prefix varchar2, p_anon_version varchar2) is 
      
       select   owner,table_name,column_name ,'PC_Scope_Missing' stat_type,stereo_type
        from
        (
          select res.owner,res.table_name,res.column_name,
                 listagg(regexp_substr (pc.property_appliedstereotype, 'PII+[^\|]+', 1) , ',') within group (order by 1) over (partition by res.owner,res.table_name ,res.column_name) as stereo_type
            from
              ( select distinct owner,table_name,column_name from privacy_catalog pc
                 where pc.table_name = p_table_name and pc.owner = p_owner
                minus
                select distinct owner,table_name,column_name from stats_results_2
                 where anon_version = p_anon_version and table_name = p_table_name and owner = p_owner ) res
            join privacy_catalog pc on res.owner = pc.owner and res.table_name = pc.table_name and res.column_name = pc.column_name
          group by res.owner,res.table_name,res.column_name,regexp_substr (pc.property_appliedstereotype, 'PII+[^\|]+', 1)
        );
        
      cursor c_get_stmt3(p_owner varchar2,p_table_name varchar2,p_src_prefix varchar2, p_anon_version varchar2) is 
      
       select   owner,table_name,column_name ,'Missing_From_PC' stat_type,stereo_type
        from
        (
          select res.owner,res.table_name,res.column_name,sr2.stereo_type
            from
              ( select distinct owner,table_name,column_name from stats_results_2
                 where anon_version = p_anon_version and table_name = p_table_name and owner = p_owner
                minus
                select distinct owner,table_name,column_name from privacy_catalog pc
                 where pc.table_name = p_table_name and pc.owner = p_owner) res
            join stats_results_2 sr2 on res.owner = sr2.owner and res.table_name = sr2.table_name and res.column_name = sr2.column_name
         );        

      cursor c_get_stmt4(p_owner varchar2,p_table_name varchar2,p_src_prefix varchar2, p_anon_version varchar2) is 
    
            select owner,table_name,column_name,'Stereo_Type_Mismatch' stat_type,stereo_type,pc_stereo_type
            from (                  
                select distinct sr2.owner,sr2.table_name,sr2.column_name,sr2.stereo_type, 
                listagg(regexp_substr (pc.property_appliedstereotype, 'PII+[^\|]+', 1) , ',') within group (order by 1) 
                                                           over (partition by sr2.owner,sr2.table_name,sr2.column_name) as pc_stereo_type
                  from stats_results_2 sr2 join privacy_catalog pc 
                                             on sr2.owner = pc.owner and sr2.table_name = pc.table_name and sr2.column_name = pc.column_name
                 where sr2.anon_version = p_anon_version and sr2.table_name = p_table_name and sr2.owner = p_owner
                   and sr2.stereo_type <> regexp_substr (pc.property_appliedstereotype, 'PII+[^\|]+', 1)
               )
             where stereo_type <> pc_stereo_type 
             order by owner,table_name,column_name;
       
--
     l_val VARCHAR2(4000);
     l_table_name varchar2(30);
     l_type varchar2(20);    

  begin

     g_module := 'generate_table_qa_reports'; 

      anonymisation_process.obfus_log('Re-populating qa_results_tmp for owner ' || p_owner ||' and table_name = ' || p_table_name,null,null,g_module);
      delete from  qa_results_tmp where owner = p_owner and table_name = p_table_name;
          
      anonymisation_process.obfus_log('Processing cursor c_get_stmt',null,null,g_module);           
      for get_stmt_rec in c_get_stmt(p_owner,p_table_name,p_src_prefix,p_anon_version) loop

        begin
          --dbms_output.put_line(get_stmt_rec.stmt);
        --  anonymisation_process.obfus_log('Executing stmt ' || get_stmt_rec.stmt,null,null,g_module);
          execute immediate get_stmt_rec.stmt INTO l_val;
          --commit;
        exception when others then
            l_val := 'N/A';
        end;
        
       -- anonymisation_process.obfus_log('insert into qa_results_tmp for owner ' || p_owner ||' and table_name = ' || p_table_name,null,null,g_module);        
        insert into qa_results_tmp(owner,table_name,column_name,stat_type,val,technique,stereo_type)
        values(get_stmt_rec.owner,get_stmt_rec.table_name,get_stmt_rec.column_name,get_stmt_rec.stat_type, l_val,get_stmt_rec.technique,get_stmt_rec.stereo_type);

      end loop;
     
      anonymisation_process.obfus_log('Processing cursor c_get_stmt2 for PC_Scope_Missing',null,null,g_module);       
      for get_stmt_rec2 in c_get_stmt2(p_owner,p_table_name,p_src_prefix,p_anon_version) loop
        insert into qa_results_tmp(owner,table_name,column_name,stat_type,stereo_type)
        values(get_stmt_rec2.owner,get_stmt_rec2.table_name,get_stmt_rec2.column_name,get_stmt_rec2.stat_type,get_stmt_rec2.stereo_type);
      end loop;
 
      anonymisation_process.obfus_log('Processing cursor c_get_stmt3 for Missing_From_PC',null,null,g_module); 
      for get_stmt_rec3 in c_get_stmt3(p_owner,p_table_name,p_src_prefix,p_anon_version) loop
        insert into qa_results_tmp(owner,table_name,column_name,stat_type,stereo_type)
        values(get_stmt_rec3.owner,get_stmt_rec3.table_name,get_stmt_rec3.column_name,get_stmt_rec3.stat_type,get_stmt_rec3.stereo_type);
      end loop;      
 
      anonymisation_process.obfus_log('Processing cursor c_get_stmt4 for Stereo_Type_Mismatch',null,null,g_module); 
      for get_stmt_rec4 in c_get_stmt4(p_owner,p_table_name,p_src_prefix,p_anon_version) loop
        insert into qa_results_tmp(owner,table_name,column_name,stat_type,stereo_type,pc_stereo_type)
        values(get_stmt_rec4.owner,get_stmt_rec4.table_name,get_stmt_rec4.column_name,get_stmt_rec4.stat_type,get_stmt_rec4.stereo_type,get_stmt_rec4.pc_stereo_type);
      end loop; 
      
      anonymisation_process.obfus_log('Re-populating qa_results_pivot for owner ' || p_owner ||' and table_name = ' || p_table_name,null,null,g_module);        
      delete  qa_results_pivot where owner =  p_owner and  table_name = p_table_name;

      insert into  qa_results_pivot(table_name,column_name,technique,owner,stereo_type,pc_stereo_type,Eq_unique,PC_Scope_Missing,Missing_From_PC,Stereo_Type_Mismatch )
      select * from qa_results_tmp
          pivot
          (
            max(val)
            for stat_type in ('Eq_unique','PC_Scope_Missing','Missing_From_PC','Stereo_Type_Mismatch')
          )
          where owner =  p_owner and  table_name = p_table_name;

      anonymisation_process.obfus_log('Updating qa_results_pivot for owner ' || p_owner ||' and table_name = ' || p_table_name,null,null,g_module);      
      update qa_results_pivot set anon_version = p_anon_version, run_dttm = p_run_date 
      where owner =  p_owner and  table_name = p_table_name;

      update qa_results_pivot set Eq_unique = 'N/A' where  technique IN ('PURGE_COLUMN','PURGE_INTEGRATION','PURGE_AUDIT')
      and owner =  p_owner and  table_name = p_table_name;

      commit;

    exception
       when others then
          g_code := SQLCODE;
          g_errm := SUBSTR(SQLERRM, 1 , 4000);
          obfus_log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),g_code,g_errm,g_module);
          RAISE;      

  end generate_table_qa_reports;


  procedure generate_table_stats(p_owner varchar2,p_table_name varchar2, p_src_prefix varchar2,p_run_date date,p_anon_version varchar2,p_prepost_anomolies number)  as

    cursor c_get_stmt is 
       select owner,table_name,column_name,stmt,technique,trans_function,stereo_type
         from  stats_stmts
        where owner = p_owner and table_name  = p_table_name
      order by table_name,column_name;

    stat_type_rec  TMP_STAT_TYPE%ROWTYPE;

  begin
     g_module := 'generate_table_stats';
     begin
    
        If p_prepost_anomolies = 1 then    
           anonymisation_process.obfus_log('Deleting stats_results_1 for owner ' || p_owner ||' and table_name = ' || p_table_name,null,null,g_module);
           delete stats_results_1 where owner = p_owner and table_name = p_table_name;  
        elsif  p_prepost_anomolies = 2 then   
           anonymisation_process.obfus_log('Deleting stats_results_2 for owner ' || p_owner ||' and table_name = ' || p_table_name,null,null,g_module);
           delete stats_results_2 where owner = p_owner and table_name = p_table_name;        
        end if;
             
        for r in c_get_stmt 
        loop
           anonymisation_process.obfus_log('Executing: '||r.stmt,null,null,g_module);      
           execute immediate r.stmt INTO stat_type_rec;
           
           If p_prepost_anomolies = 1 then
           
             anonymisation_process.obfus_log('Inserting stats_results_1',null,null,g_module);
             insert into stats_results_1(owner,table_name,column_name,technique,trans_function,stereo_type,
                                         not_equals,equals,value_to_null,value_from_null,
                                         avg_sim_dist,total_recs_src,total_recs_tgt,
                                         total_nulls_src,total_nulls_tgt,
                                         anon_version,run_dttm)
         
             values(r.owner,r.table_name,r.column_name,r.technique,r.trans_function,r.stereo_type,
                    nvl(stat_type_rec.not_equals,0), nvl(stat_type_rec.equals,0), nvl(stat_type_rec.value_to_null,0), nvl(stat_type_rec.value_from_null,0),
                    nvl(stat_type_rec.avg_sim_dist,0),nvl(stat_type_rec.total_recs_src,0),nvl(stat_type_rec.total_recs_tgt,0),
                    nvl(stat_type_rec.total_nulls_src,0),nvl(stat_type_rec.total_nulls_tgt,0),
                    p_anon_version,p_run_date);
            
             update stats_results_1 
                set equals = -1, not_equals = -1, avg_sim_dist = -1 
              where technique IN ('PURGE_COLUMN','PURGE_INTEGRATION','PURGE_AUDIT') 
                and owner = p_owner and table_name = p_table_name;   
           
           elsif p_prepost_anomolies = 2 then
   
             anonymisation_process.obfus_log('Inserting stats_results_2',null,null,g_module);
             insert into stats_results_2(owner,table_name,column_name,technique,trans_function,stereo_type,
                                         not_equals,equals,value_to_null,value_from_null,
                                         avg_sim_dist,total_recs_src,total_recs_tgt,
                                         total_nulls_src,total_nulls_tgt,
                                         anon_version,run_dttm)
         
             values(r.owner,r.table_name,r.column_name,r.technique,r.trans_function,r.stereo_type,
                    nvl(stat_type_rec.not_equals,0), nvl(stat_type_rec.equals,0), nvl(stat_type_rec.value_to_null,0), nvl(stat_type_rec.value_from_null,0),
                    nvl(stat_type_rec.avg_sim_dist,0),nvl(stat_type_rec.total_recs_src,0),nvl(stat_type_rec.total_recs_tgt,0),
                    nvl(stat_type_rec.total_nulls_src,0),nvl(stat_type_rec.total_nulls_tgt,0),
                    p_anon_version,p_run_date); 
                    
             update stats_results_2 
                set equals = -1, not_equals = -1, avg_sim_dist = -1 
              where technique IN ('PURGE_COLUMN','PURGE_INTEGRATION','PURGE_AUDIT') 
                and owner = p_owner and table_name = p_table_name; 
                
           end if;
           
        end loop; 
           
        commit;
      
     exception
        when others then
          g_code := SQLCODE;
          g_errm := SUBSTR(SQLERRM, 1 , 4000);
          obfus_log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),g_code,g_errm,g_module);
          RAISE;
     end;

  end generate_table_stats;

  
  procedure table_merge_fix_anomalies(p_owner varchar2,p_table_name varchar2, p_src_prefix varchar2,p_run_date date,p_anon_version varchar2)
  is
       cursor c_get_stmt is

      with merge_parts as
      (select --*
      p_src_prefix||'_'||owner owner,table_name table_name, 'merge into tgt_'|| table_name  ||' tgt using ' as merge_txt ,
          fix_func||'(TGT1.'||column_name||') '||column_name  select_cols,
           'TGT_'|| table_name ||' TGT1 ,'||'S_STG_'||table_name||' SRC ' as select_table,
          ' when matched then update set ' ||'tgt.'||column_name ||'= SRC.'||column_name set_txt,
          ' where  ' ||'SRC.'||column_name ||'=' ||'TGT1.'||column_name where_txt1
      from (
              select srt.owner,srt.table_name,srt.column_name,case when atc.data_type = 'DATE' then 'ut.RD30' else 'ut.TDC' end fix_func
              from stats_results_1 srt
              join all_tab_columns atc on atc.owner =  p_src_prefix||'_'||srt.owner  and atc.table_name =  srt.table_name and atc.column_name = srt.column_name
              where equals > 0 and trans_function  not in ('ut.EXCLUDE' ) 
            
            
            ) ) ,
      pk as (select
          ac.owner,ac.table_name,' on ('||listagg('tgt.'||acc.column_name ||'=' ||'src.'||acc.column_name, ' and ') within group (ORDER BY column_name)||')'  as on_txt,
          listagg('TGT1.'||acc.column_name,',') within group (ORDER BY column_name) as pk_cols,
          ' and ('||listagg('SRC.'||acc.column_name ||'=' ||'TGT1.'||acc.column_name, ' and ') within group (ORDER BY column_name)||')'  as  where_txt2
          from all_constraints ac  join all_cons_columns  acc on ac.constraint_name = acc.constraint_name and ac.owner = acc.owner and ac.table_name = acc.table_name
          where ac.constraint_type = 'P' and  ac.owner =  p_src_prefix||'_'||p_owner and ac.table_name = p_table_name
          group by ac.owner,ac.table_name)

        select merge_parts.merge_txt||'( select '||merge_parts.select_cols||','||pk.pk_cols||' from '||merge_parts.select_table||merge_parts.where_txt1||
        pk.where_txt2 ||') src'||pk.on_txt||merge_parts.set_txt as stmt from merge_parts,pk
        where merge_parts.owner = pk.owner and merge_parts.table_name = pk.table_name;

    v_nCounter number;
    begin
      begin

        g_module := 'table_merge_fix_anomalies';
        v_nCounter := 1;

        loop
            for get_stmt_rec in c_get_stmt loop

              begin
                  obfus_log(substr('Pass : '||to_char(v_nCounter)|| 'Executing : '|| get_stmt_rec.stmt,1,4000),null,null,g_module);
                  execute immediate get_stmt_rec.stmt;
                  obfus_log('Pass : '||to_char(v_nCounter)||' Complete : ' ||to_char(sql%rowcount) ||' merged.',null,null,g_module);
                  
                  --dbms_output.put_line( get_stmt_rec.stmt);
                  commit;
                exception when others then
                  obfus_log(substr(get_stmt_rec.stmt,1,4000),sqlcode,substr(sqlerrm,1,4000),g_module);
                  RAISE;
              end;

            end loop;

            v_nCounter := v_nCounter + 1;
            if v_nCounter > 3 then
              exit;
            end if;
        end loop;
        commit;

      exception
        when others then
          g_code := SQLCODE;
          g_errm := SUBSTR(SQLERRM, 1 , 4000);
          obfus_log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),g_code,g_errm,g_module);
          RAISE;
      end;

  end table_merge_fix_anomalies;  
  
  procedure per_col_masking_exceptions (p_owner VARCHAR2, p_table_name VARCHAR2,p_rep_tgt_syn_prefix VARCHAR2) AS
     pragma autonomous_transaction;
     v_sql                   varchar2(4000);
     v_mask_check_sql        varchar2(4000);
     v_count                 number := 0;

     cursor c_masked_cols (c_in_owner VARCHAR2, c_in_table_name VARCHAR2) is
       with masked_cols as (
              select ptc.obfus_run_id, ptc.owner, ptc.table_name, ptc.column_name, ptc.trans_func, ptc.transform, atc.column_id, rownum key_ns
                from per_trans_cols ptc
                join all_tab_columns atc on g_tgt_prefix||'_'||ptc.owner = atc.owner and ptc.table_name = atc.table_name and ptc.column_name = atc.column_name          
               where (ptc.column_name <> ptc.trans_func OR upper(ptc.transform) NOT IN ('NONE','N'))
                 and ptc.owner = c_in_owner
                 and ptc.table_name = c_in_table_name )
          select obfus_run_id, owner, table_name, column_name, trans_func, transform, column_id, key_ns
            from masked_cols
           where key_ns in ( select ROUND(dbms_random.value(1,x.max_key_ns)) num 
                               from (select max(key_ns) max_key_ns from masked_cols) x
                             connect by level <= const.k_mask_chk_col_sample_size );      

  begin
    g_module :=  'anonymisation_process.per_col_masking_exceptions';

    for r in c_masked_cols(p_owner,p_table_name)
    loop
      case
      when r.transform = const.k_char_mask
      then
        v_mask_check_sql := ' and length(' || r.column_name || ') <> regexp_count(' || r.column_name || ',' ||chr(39)||'\'||r.transform||chr(39)||','|| 1 || ',' || '''i''' || ')';
      when r.transform = const.k_number_mask
      then
        v_mask_check_sql := ' and length(' || r.column_name || ') <> regexp_count(' || r.column_name || ',' ||chr(39)||r.transform||chr(39)|| ',' || 1 || ',' || '''i''' || ')';
      when r.transform = const.k_date_mask
      then
        v_mask_check_sql := ' and to_char(' || r.column_name||','||chr(39)||const.k_date_mask_format||chr(39)||') <> to_char(to_date('||chr(39)||r.transform||chr(39)||','||chr(39)||const.k_date_mask_format||chr(39)||'),'||chr(39)||const.k_date_mask_format||chr(39)||')';
      else
        RAISE_APPLICATION_ERROR(-20004,const.k_x_unknown_mask_errm || ': ' || r.transform);
      end case;

      v_sql := 'select count(*) 
                  from ( select * from '||p_rep_tgt_syn_prefix||'_'|| r.table_name ||
                        ' where rownum < ' || const.k_mask_chk_row_sample_size ||')'|| 
              ' where ' || r.column_name || ' is not null ' || v_mask_check_sql;

     -- dbms_output.put_line(v_sql);
      obfus_log(substr('Executing : '||v_sql,1,4000),null,null,g_module);
      execute immediate v_sql into v_count;

      if v_count > 0 then
        insert into qa_results_tmp (table_name, stat_type, column_name, technique, owner, val)
        values (r.table_name,'Maskfail ' || to_char(const.k_mask_chk_row_sample_size-1) || ' sample',
                r.column_name,r.trans_func,r.owner,to_char(v_count));
      end if;
      
    end loop;
    commit;
  exception
    when others then
       g_code := SQLCODE;
       g_errm := SUBSTR(SQLERRM, 1 , 4000);
       obfus_log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),g_code,g_errm,g_module);
       RAISE;
  end per_col_masking_exceptions;

procedure apply_mask(p_owner VARCHAR2, p_table_name VARCHAR2) AS

begin
  null;
end apply_mask;


procedure apply_fast_mask(p_owner VARCHAR2, p_table_name VARCHAR2) AS

  cursor cCreMaskCols (p_owner VARCHAR2, p_table_name VARCHAR2) is
                     select 'alter table '||g_tgt_prefix||'_'||owner||'.'||table_name||' add MASK_'||column_name||' '|| data_type ||
                     case when    data_type in('VARCHAR2','CHAR') then '(' ||char_length||' CHAR)' else null end ||
                     case when    data_type in('NUMBER') then '(' ||nvl(to_char(data_precision),'*')||','||data_scale||')' else null end ||
                     ' DEFAULT ' || CASE WHEN data_type in('VARCHAR2','CHAR') then ''''||substr(RPAD('*',30,'*'),1,char_length)||'''' else null end ||
                     case when data_type in('NUMBER') then substr(RPAD('9',10,'9'),1,nvl(data_precision,10))||'' else null end ||
                     case when data_type in('DATE') then 'TO_DATE('||chr(39)||const.k_date_mask||chr(39)||','||chr(39)||const.k_date_mask_format||chr(39)||')' else null end stmt
                     from per_trans_cols
                     where owner = p_owner and table_name = p_table_name
                     and transform <> 'NONE'; 
 
   cursor cRenameUnused (p_owner VARCHAR2, p_table_name VARCHAR2) is 
                        select 'alter table '||g_tgt_prefix||'_'||owner||'.'||table_name||'  rename column ' ||column_name ||' to ' || 'UNUSED_' || column_name stmt
                        from per_trans_cols
                        where owner = p_owner and table_name = p_table_name
                        and transform <> 'NONE';
 
   cursor cSetUnused (p_owner VARCHAR2, p_table_name VARCHAR2) is 
                        select 'alter table '||g_tgt_prefix||'_'||owner||'.'||table_name||' set unused column '|| 'UNUSED_' ||column_name stmt
                        from per_trans_cols
                        where owner = p_owner and table_name = p_table_name
                        and transform <> 'NONE';

 
  cursor cRename (p_owner VARCHAR2, p_table_name VARCHAR2) is 
                        select 'alter table '||g_tgt_prefix||'_'||owner||'.'||table_name||'  rename column '||'MASK_'||column_name ||' to ' || column_name stmt
                        from per_trans_cols
                        where owner = p_owner and table_name = p_table_name
                        and transform <> 'NONE';
                        
  cursor cDropDefault (p_owner VARCHAR2, p_table_name VARCHAR2) is 
                        select 'alter table '||g_tgt_prefix||'_'||owner||'.'||table_name||'  MODIFY ( '||column_name ||' DEFAULT NULL )' stmt
                        from per_trans_cols
                        where owner = p_owner and table_name = p_table_name
                        and transform <> 'NONE';                     

 
  cursor c_build_contraints(p_owner VARCHAR2, p_table_name VARCHAR2)
  is
     select * from Per_trans_col_con
     where owner = p_owner and table_name = p_table_name;
                        
  cursor c_build_indexes(p_owner VARCHAR2, p_table_name VARCHAR2)
  is
     select * from Per_trans_col_ind
     where owner = p_owner and table_name = p_table_name;
       
  v_sStmt varchar2(4000);
 
begin
  
  g_module :=  'anonymisation_process.apply_fast_mask';

  for cCreMaskColsRec in cCreMaskCols(p_owner,p_table_name) loop
     begin
        obfus_log(substr('Executing : '||cCreMaskColsRec.stmt,1,4000),null,null,g_module);
        execute immediate cCreMaskColsRec.stmt;
     exception 
        when others then 
            g_code := SQLCODE;
            g_errm := SUBSTR(SQLERRM, 1 ,4000);   
            obfus_log('Error  cCreMaskColsRec.stmt: '|| cCreMaskColsRec.stmt,g_code,g_errm,g_module);      
     end;
  end loop;

  for cRenameUnusedRec in cRenameUnused(p_owner,p_table_name) loop
     begin
        obfus_log(substr('Executing : '||cRenameUnusedRec.stmt,1,4000),null,null,g_module);
        execute immediate cRenameUnusedRec.stmt;
     exception 
        when others then 
            g_code := SQLCODE;
            g_errm := SUBSTR(SQLERRM, 1 ,4000);   
            obfus_log('Error  cRenameUnusedRec.stmt: '|| cRenameUnusedRec.stmt,g_code,g_errm,g_module);         
     end;
  end loop;
  
  for cSetUnusedRec in cSetUnused(p_owner,p_table_name) loop
     begin
        obfus_log(substr('Executing : '||cSetUnusedRec.stmt,1,4000),null,null,g_module);
        execute immediate cSetUnusedRec.stmt;
     exception 
        when others then 
            g_code := SQLCODE;
            g_errm := SUBSTR(SQLERRM, 1 ,4000);   
            obfus_log('Error  cSetUnusedRec.stmt: '|| cSetUnusedRec.stmt,g_code,g_errm,g_module);         
     end;        
  end loop;
  
--  v_sStmt := 'alter table '||g_tgt_prefix||'_'||p_owner||'.'||p_table_name|| ' drop unused columns';
--  obfus_log(substr('Executing : '||v_sStmt,1,4000),null,null,g_module);
--  execute immediate v_sStmt;
  
  for cRenameRec in cRename(p_owner,p_table_name) loop
     begin  
        obfus_log(substr('Executing : '||cRenameRec.stmt,1,4000),null,null,g_module);
        execute immediate cRenameRec.stmt;
     exception 
        when others then 
            g_code := SQLCODE;
            g_errm := SUBSTR(SQLERRM, 1 ,4000);   
            obfus_log('Error  cRenameRec.stmt: '|| cRenameRec.stmt,g_code,g_errm,g_module);         
     end;       
  end loop;
  
  for cDropDefaultRec in cDropDefault(p_owner,p_table_name) loop
     begin   
        obfus_log(substr('Executing : '||cDropDefaultRec.stmt,1,4000),null,null,g_module);
        execute immediate cDropDefaultRec.stmt;
     exception 
        when others then 
            g_code := SQLCODE;
            g_errm := SUBSTR(SQLERRM, 1 ,4000);   
            obfus_log('Error  cDropDefaultRec.stmt: '|| cDropDefaultRec.stmt,g_code,g_errm,g_module);         
     end;       
  end loop;
    
  
  begin
    for r in c_build_contraints(p_owner,p_table_name)
    loop
      begin
        obfus_log(substr('Executing : '||r.constraint_ddl,1,4000),null,null,g_module);
        execute immediate r.constraint_ddl;
      exception
        when others then
          g_code := SQLCODE;
          g_errm := SUBSTR(SQLERRM, 1 , 4000);        
          obfus_log('Error creating constraint for ' || p_owner||'.'|| p_table_name,g_code,g_errm,g_module);
      end;
    end loop;
  end;
  
  begin
    for r in c_build_indexes(p_owner,p_table_name)
    loop
      begin
        obfus_log(substr('Executing : '||r.index_ddl,1,4000),null,null,g_module);
        execute immediate r.index_ddl;
      exception
        when excep.x_idx_name_already_used then
          begin
              obfus_log(substr('Executing : '||v_sStmt,1,4000),null,null,g_module);
              v_sStmt := 'ALTER TABLE ' || g_tgt_prefix||'_'||r.index_owner ||'.'||r.table_name ||' REBUILD INDEX ' || r.index_name;
              execute immediate v_sStmt ;
          exception
            when others then
              g_code := SQLCODE;
              g_errm := SUBSTR(SQLERRM, 1 ,4000);   
              obfus_log('Error rebuilding index : '||r.index_name,g_code,g_errm,g_module);
          end;
        when others then
          --e.g. ORA-01452: cannot CREATE UNIQUE INDEX; duplicate keys found
          g_code := SQLCODE;
          g_errm := SUBSTR(SQLERRM, 1 , 4000);        
          obfus_log('Error building index',g_code,g_errm,g_module);
      end;
    end loop;
  end;
  
exception
    when others then
      g_code := SQLCODE;
      g_errm := SUBSTR(SQLERRM, 1 , 4000);
      obfus_log(substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),1,4000),g_code,g_errm,g_module);
      RAISE;
  

end apply_fast_mask;

end anonymisation_process;
/