SELECT partner_name FROM partners
WHERE date_timestamp = {{ ds }}
AND ds_prev = {{ prev_ds }}
AND ds_next = {{ next_ds }}