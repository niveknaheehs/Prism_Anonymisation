begin

      DBMS_SESSION.set_identifier ('adcfs\ksheehan1' || ':' || '1');

end;
merge into tgt_HOLDERS tgt using 
(select td(DESIGNATION_NAME) DESIGNATION_NAME,COMP_CODE,IVC_CODE 
from HOLDERS_tmp) src on (tgt.COMP_CODE=src.COMP_CODE and tgt.IVC_CODE=src.IVC_CODE) 
when matched then update set TGT.DESIGNATION_NAME=SRC.DESIGNATION_NAME;

commit;

merge into tgt_COMP_PAYEE tgt using 
(select td(ADDRESS_LINE1) ADDRESS_LINE1 , td(ADDRESS_LINE2) ADDRESS_LINE2, td(ADDRESS_LINE3) ADDRESS_LINE3, td(ADDRESS_LINE5) ADDRESS_LINE5, td(POST_CODE_LEFT) POST_CODE_LEFT, td(POST_CODE_RIGHT) POST_CODE_RIGHT,COMP_PAYEE_ID
from COMP_PAYEE_tmp) src on (tgt.COMP_PAYEE_ID=src.COMP_PAYEE_ID) 
when matched then update set TGT.ADDRESS_LINE1=SRC.ADDRESS_LINE1, TGT.ADDRESS_LINE2=SRC.ADDRESS_LINE2, TGT.ADDRESS_LINE3=SRC.ADDRESS_LINE3, TGT.ADDRESS_LINE5=SRC.ADDRESS_LINE5, TGT.POST_CODE_LEFT=SRC.POST_CODE_LEFT, TGT.POST_CODE_RIGHT=SRC.POST_CODE_RIGHT;

commit;

merge into tgt_BANK_BRANCHES tgt using 
(select td(ADDRESS_LINE1) ADDRESS_LINE1, td(ADDRESS_LINE2) ADDRESS_LINE2, td(ADDRESS_LINE3) ADDRESS_LINE3, td(ADDRESS_LINE4) ADDRESS_LINE4, td(ADDRESS_LINE5) ADDRESS_LINE5, td(POSTCODE_LEFT) POSTCODE_LEFT, td(POSTCODE_RIGHT) POSTCODE_RIGHT,BANK_SORT_CODE 
from BANK_BRANCHES) src on (tgt.BANK_SORT_CODE=src.BANK_SORT_CODE) 
when matched then update set TGT.ADDRESS_LINE1=SRC.ADDRESS_LINE1, TGT.ADDRESS_LINE2=SRC.ADDRESS_LINE2, TGT.ADDRESS_LINE3=SRC.ADDRESS_LINE3, TGT.ADDRESS_LINE4=SRC.ADDRESS_LINE4, TGT.ADDRESS_LINE5=SRC.ADDRESS_LINE5, TGT.POSTCODE_LEFT=SRC.POSTCODE_LEFT, TGT.POSTCODE_RIGHT=SRC.POSTCODE_RIGHT;

commit;

merge into tgt_BROKER_CONTACTS tgt using 
(select td(ADDRESS_LINE1) ADDRESS_LINE1, td(ADDRESS_LINE2) ADDRESS_LINE2, td(ADDRESS_LINE3) ADDRESS_LINE3, td(ADDRESS_LINE4) ADDRESS_LINE4, td(POSTCODE_LEFT) POSTCODE_LEFT, td(POSTCODE_RIGHT) POSTCODE_RIGHT,BROKER_CONTACT_ID 
from BROKER_CONTACTS_tmp) src on (tgt.BROKER_CONTACT_ID=src.BROKER_CONTACT_ID) 
when matched then update set TGT.ADDRESS_LINE1=SRC.ADDRESS_LINE1, TGT.ADDRESS_LINE2=SRC.ADDRESS_LINE2, TGT.ADDRESS_LINE3=SRC.ADDRESS_LINE3, TGT.ADDRESS_LINE4=SRC.ADDRESS_LINE4, TGT.POSTCODE_LEFT=SRC.POSTCODE_LEFT, TGT.POSTCODE_RIGHT=SRC.POSTCODE_RIGHT;

commit;

merge into tgt_DISP_REQ_INT_MANDATES tgt using 
(select td(ADDRESS_LINE_1) ADDRESS_LINE_1, td(ADDRESS_LINE_2) ADDRESS_LINE_2, td(ADDRESS_LINE_3) ADDRESS_LINE_3, td(ADDRESS_LINE_4) ADDRESS_LINE_4, td(INT_BANK_ACC_NO_IBAN) INT_BANK_ACC_NO_IBAN,SIP_DISP_REQUEST_MANDATE_ID 
from DISP_REQ_INT_MANDATES_tmp) src on (tgt.SIP_DISP_REQUEST_MANDATE_ID=src.SIP_DISP_REQUEST_MANDATE_ID) 
when matched then update set TGT.ADDRESS_LINE_1=SRC.ADDRESS_LINE_1, TGT.ADDRESS_LINE_2=SRC.ADDRESS_LINE_2, TGT.ADDRESS_LINE_3=SRC.ADDRESS_LINE_3, TGT.ADDRESS_LINE_4=SRC.ADDRESS_LINE_4, TGT.INT_BANK_ACC_NO_IBAN=SRC.INT_BANK_ACC_NO_IBAN;

commit;
--merge into tgt_HOLDER_EMPLOYEE_DETAILS tgt using 
--( select td(GENDER),HOLDER_EMPLOYEE_DETAIL_ID 
--from HOLDER_EMPLOYEE_DETAILS_tmp) src on (tgt.HOLDER_EMPLOYEE_DETAIL_ID=src.HOLDER_EMPLOYEE_DETAIL_ID) 
--when matched then update set TGT.GENDER=SRC.GENDER;

merge into tgt_BUILDING_SOCIETY_BRANCHES tgt using 
(select td(ADDRESS_LINE1) ADDRESS_LINE1, td(ADDRESS_LINE2) ADDRESS_LINE2, td(ADDRESS_LINE3) ADDRESS_LINE3, td(ADDRESS_LINE4) ADDRESS_LINE4, td(ADDRESS_LINE5) ADDRESS_LINE5, td(POSTCODE_LEFT) POSTCODE_LEFT, td(POSTCODE_RIGHT) POSTCODE_RIGHT,BUILDING_SOCIETY_BRANCH_ID 
from BUILDING_SOCIETY_BRANCHES_tmp) src on (tgt.BUILDING_SOCIETY_BRANCH_ID=src.BUILDING_SOCIETY_BRANCH_ID) 
when matched then update set TGT.ADDRESS_LINE1=SRC.ADDRESS_LINE1, TGT.ADDRESS_LINE2=SRC.ADDRESS_LINE2, TGT.ADDRESS_LINE3=SRC.ADDRESS_LINE3, TGT.ADDRESS_LINE4=SRC.ADDRESS_LINE4, TGT.ADDRESS_LINE5=SRC.ADDRESS_LINE5, TGT.POSTCODE_LEFT=SRC.POSTCODE_LEFT, TGT.POSTCODE_RIGHT=SRC.POSTCODE_RIGHT;

commit;

merge into tgt_SHARE_PLAN_CNOTE_RECIPIENT tgt using 
( select td(ADDRESS_LINE1) ADDRESS_LINE1, td(ADDRESS_LINE2) ADDRESS_LINE2, td(ADDRESS_LINE3) ADDRESS_LINE3, td(ADDRESS_LINE4) ADDRESS_LINE4, td(POSTCODE_LEFT) POSTCODE_LEFT, td(POSTCODE_RIGHT) POSTCODE_RIGHT ,PLAN_ID 
from SHARE_PLAN_CNOTE_RECIPIENT_tmp) src on (tgt.PLAN_ID=src.PLAN_ID) 
when matched then update set TGT.ADDRESS_LINE1=SRC.ADDRESS_LINE1, TGT.ADDRESS_LINE2=SRC.ADDRESS_LINE2, TGT.ADDRESS_LINE3=SRC.ADDRESS_LINE3, TGT.ADDRESS_LINE4=SRC.ADDRESS_LINE4, TGT.POSTCODE_LEFT=SRC.POSTCODE_LEFT, TGT.POSTCODE_RIGHT=SRC.POSTCODE_RIGHT;

commit;

merge into tgt_PAYMENT_METHOD_COLLECTIONS tgt using ( select td(PAYEE_ACCOUNT_NUMBER) PAYEE_ACCOUNT_NUMBER,PAY_METHOD_COLLECTION_ID 
from PAYMENT_METHOD_COLLECTIONS_tmp) src on (tgt.PAY_METHOD_COLLECTION_ID=src.PAY_METHOD_COLLECTION_ID) 
when matched then update set TGT.PAYEE_ACCOUNT_NUMBER=SRC.PAYEE_ACCOUNT_NUMBER;

commit;
 
         merge into TGT_RR556_CNB_REVERSAL_REPORT tgt
          using (select REV_TOTAL_ID ,td(BANK_SORT_CODE) TD_BANK_SORT_CODE, td(ACCOUNT_NO) TD_ACCOUNT_NO, ACCOUNT_NO ,BANK_SORT_CODE  ,           
                      CASH_TRANSACTION_ID, IVC_CODE,REGISTRATION_DATE,                         
                      TRANSACTION_AMOUNT,REVERSAL_MSG_TYPE,ERROR_DESCRIPTION,ACCOUNT_FORENAME,ACCOUNT_SURNAME     
                      from RR556_CNB_REVERSAL_REPORT) res
            on (NVL(tgt.REV_TOTAL_ID,-1) =NVL(res.REV_TOTAL_ID,-1) and
                NVL(tgt.CASH_TRANSACTION_ID,-1) = NVL(res.CASH_TRANSACTION_ID,-1) and
                NVL(tgt.IVC_CODE,'$') = NVL(res.IVC_CODE,'$') and
                NVL(tgt.REGISTRATION_DATE,TO_DATE('01-01-1900', 'MM-DD-YYYY')) = NVL(res.REGISTRATION_DATE,TO_DATE('01-01-1900', 'MM-DD-YYYY')) and
                NVL(tgt.TRANSACTION_AMOUNT,-1) = NVL(res.TRANSACTION_AMOUNT,-1) and                 
                NVL(tgt.REVERSAL_MSG_TYPE,'$') = NVL(res.REVERSAL_MSG_TYPE,'$') and
                NVL(tgt.ERROR_DESCRIPTION,'$') = NVL(res.ERROR_DESCRIPTION,'$') and                       
                NVL(tgt.ACCOUNT_FORENAME,'$') = NVL(res.ACCOUNT_FORENAME,'$') and
                NVL(tgt.ACCOUNT_SURNAME,'$') =NVL(res.ACCOUNT_SURNAME,'$')  )
          when matched then update set tgt.ACCOUNT_NO = res.TD_ACCOUNT_NO , tgt.BANK_SORT_CODE = res.TD_BANK_SORT_CODE ;
commit

         merge into tgt_RR556_CNB_BANK_REVERSAL_TO tgt
          using (select REV_TOTAL_ID,BANK_SORT_CODE,td(ACCOUNT_NO) ACCOUNT_NO,CURRENCY_CODE,                 
                  REVERSED_TRANSACTION_COUNT,TRANSACTION_SUSPENSE_COUNT,ALREADY_REVERSED_COUNT,                       
                  TRANSACTION_ERROR_COUNT,REVERSED_TRANSACTION_AMOUNT,TRANSACTION_SUSPENSE_AMOUNT,                     
                  ALREADY_REVERSED_AMOUNT,TRANSACTION_ERROR_AMOUNT   from RR556_CNB_BANK_REVERSAL_TOTALS) res
            on (NVL(tgt.REV_TOTAL_ID,-1)  = NVL(res.REV_TOTAL_ID,-1) and                      
                NVL(tgt.BANK_SORT_CODE,'$')   = NVL(res.BANK_SORT_CODE,'$') and             
                NVL(tgt.CURRENCY_CODE,'$')   = NVL(res.CURRENCY_CODE,'$') and  
                NVL(tgt.REVERSED_TRANSACTION_COUNT ,-1)  = NVL(res.REVERSED_TRANSACTION_COUNT,-1) and
                NVL(tgt.TRANSACTION_SUSPENSE_COUNT,-1)   = NVL(res.TRANSACTION_SUSPENSE_COUNT,-1) and  
                NVL(tgt.ALREADY_REVERSED_COUNT,-1)   = NVL(res.ALREADY_REVERSED_COUNT,-1) and  
                NVL(tgt.TRANSACTION_ERROR_COUNT,-1)   = NVL(res.TRANSACTION_ERROR_COUNT,-1) and  
                NVL(tgt.REVERSED_TRANSACTION_AMOUNT,-1)   = NVL(res.REVERSED_TRANSACTION_AMOUNT,-1) and  
                NVL(tgt.TRANSACTION_SUSPENSE_AMOUNT,-1)   = NVL(res.TRANSACTION_SUSPENSE_AMOUNT,-1) and  
                NVL(tgt.ALREADY_REVERSED_AMOUNT,-1)   = NVL(res.ALREADY_REVERSED_AMOUNT,-1) and  
                NVL(tgt.TRANSACTION_ERROR_AMOUNT,-1)   = NVL(res.TRANSACTION_ERROR_AMOUNT,-1)  
               )
          when matched then update set tgt.ACCOUNT_NO = res.ACCOUNT_NO;
     commit;