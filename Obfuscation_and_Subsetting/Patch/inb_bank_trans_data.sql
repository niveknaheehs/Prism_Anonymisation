declare
  cursor cGetParts is select partition_name from dba_tab_partitions
  where table_owner like 'PSM\_%' escape '\' and table_name = 'INBOUND_BANK_TRANSACTIONS';
begin

  For cGetPartsRec in cGetParts loop

    execute immediate(
    'merge into PSM_PRISM_CORE.INBOUND_BANK_TRANSACTIONS tgt using 
      ( select INBOUND_BANK_TRANSACTION_ID,ut.TDC(CAPITA_REFERENCE) CAPITA_REFERENCE	,ut.TDC(CHEQUE_NUMBER) CHEQUE_NUMBER 
        from PSM_PRISM_CORE.INBOUND_BANK_TRANSACTIONS partition ('||cGetPartsRec.partition_name||') ) src	 
        on (tgt.INBOUND_BANK_TRANSACTION_ID=src.INBOUND_BANK_TRANSACTION_ID )  
        when matched then update set TGT.CAPITA_REFERENCE=SRC.CAPITA_REFERENCE,
                                     TGT.CHEQUE_NUMBER=SRC.CHEQUE_NUMBER');
    
      commit; 

    end loop;
    commit;
end;
/