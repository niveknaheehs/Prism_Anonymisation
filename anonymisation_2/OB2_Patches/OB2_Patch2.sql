merge into tgt_PAYMENTS tgt using 
(
    select p.payment_id,res.account_no,td(p.comment_text) comment_text, td(p.parent_cheque_number) parent_cheque_number, 
    td(p.payer_alias) payer_alias,res.BIC PAYER_BIC, res.iban PAYER_IBAN, res.sortcode PAYER_SORTCODE, td(p.sibling_account_number) sibling_account_number
    --select count(*)
    from payments p  left join (select src_py.payment_id,src_ba.account_no,src_ba.sortcode,src_ba.BIC,src_ba.iban
                                from payments src_py  
                                left outer join bank_accounts src_ba on src_ba.account_no = src_py.payer_account_number and src_ba.sortcode = src_py.payer_sortcode
                                where src_py.payer_account_number is not null ) res on res.payment_id = p.payment_id
) src on  (src.payment_id = tgt.payment_id)
 when matched then update set tgt.comment_text=src.comment_text, tgt.parent_cheque_number=src.parent_cheque_number, 
  tgt.payer_alias=src.payer_alias, tgt.payer_bic=src.payer_bic, tgt.payer_iban=src.payer_iban, tgt.payer_sortcode=src.payer_sortcode, 
  tgt.sibling_account_number=src.sibling_account_number,tgt.payer_account_number=src.account_no


commit;