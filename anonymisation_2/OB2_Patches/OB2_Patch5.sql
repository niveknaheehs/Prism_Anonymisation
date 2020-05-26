merge into tgt_cheque_ranges tgt
using (select crt.cheque_range_id,crt.end_no,crt.last_cheque_no_used,crt.start_no,crt.warning_threshold 
      from  cheque_ranges_tmp crt ) res 
on (tgt.cheque_range_id = res.cheque_range_id ) 
when matched then update
	  set tgt.end_no = res.end_no,
          tgt.last_cheque_no_used = res.last_cheque_no_used,
          tgt.start_no = res.start_no,
          tgt.warning_threshold = res.warning_threshold;