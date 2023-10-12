select
	ke.eic,
	date(from_unixtime(ldr.date_record)) as 'datetime',
	(Max(CASE WHEN month(from_unixtime(ldr.date_record)) = '1' then ldr.Plyn_VB  END) - Min(CASE WHEN month(from_unixtime(ldr.date_record)) = '1' then ldr.Plyn_VB  END)) AS "01_2022"
	(Max(CASE WHEN month(from_unixtime(ldr.date_record)) = '2' then ldr.Plyn_VB  END) - Min(CASE WHEN month(from_unixtime(ldr.date_record)) = '2' then ldr.Plyn_VB  END)) AS "02_2022"
	(Max(CASE WHEN month(from_unixtime(ldr.date_record)) = '3' then ldr.Plyn_VB  END) - Min(CASE WHEN month(from_unixtime(ldr.date_record)) = '3' then ldr.Plyn_VB  END)) AS "03_2022"
	(Max(CASE WHEN month(from_unixtime(ldr.date_record)) = '4' then ldr.Plyn_VB  END) - Min(CASE WHEN month(from_unixtime(ldr.date_record)) = '4' then ldr.Plyn_VB  END)) AS "04_2022"
	(Max(CASE WHEN month(from_unixtime(ldr.date_record)) = '5' then ldr.Plyn_VB  END) - Min(CASE WHEN month(from_unixtime(ldr.date_record)) = '5' then ldr.Plyn_VB  END)) AS "05_2022"
	(Max(CASE WHEN month(from_unixtime(ldr.date_record)) = '6' then ldr.Plyn_VB  END) - Min(CASE WHEN month(from_unixtime(ldr.date_record)) = '6' then ldr.Plyn_VB  END)) AS "06_2022"
	(Max(CASE WHEN month(from_unixtime(ldr.date_record)) = '7' then ldr.Plyn_VB  END) - Min(CASE WHEN month(from_unixtime(ldr.date_record)) = '7' then ldr.Plyn_VB  END)) AS "07_2022"
	(Max(CASE WHEN month(from_unixtime(ldr.date_record)) = '8' then ldr.Plyn_VB  END) - Min(CASE WHEN month(from_unixtime(ldr.date_record)) = '8' then ldr.Plyn_VB  END)) AS "08_2022"
	(Max(CASE WHEN month(from_unixtime(ldr.date_record)) = '9' then ldr.Plyn_VB  END) - Min(CASE WHEN month(from_unixtime(ldr.date_record)) = '9' then ldr.Plyn_VB  END)) AS "09_2022"
	(Max(CASE WHEN month(from_unixtime(ldr.date_record)) = '10' then ldr.Plyn_VB  END) - Min(CASE WHEN month(from_unixtime(ldr.date_record)) = '10' then ldr.Plyn_VB  END)) AS "10_2022"
	(Max(CASE WHEN month(from_unixtime(ldr.date_record)) = '11' then ldr.Plyn_VB  END) - Min(CASE WHEN month(from_unixtime(ldr.date_record)) = '11' then ldr.Plyn_VB  END)) AS "11_21022"
	(Max(CASE WHEN month(from_unixtime(ldr.date_record)) = '12' then ldr.Plyn_VB  END) - Min(CASE WHEN month(from_unixtime(ldr.date_record)) = '12' then ldr.Plyn_VB  END)) AS "12_2022"
from
	ld_d_records ldr
left join kgj_eic ke on
	ke.device_id = ldr.id_kgj
where
	year(from_unixtime(ldr.date_record)) = '2023' and (ldr.id_kgj = 157)
group by ldr.id_kgj