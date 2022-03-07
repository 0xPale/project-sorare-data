select * FROM (
	select * from player
	where 
	player."transferType" IS NOT NULL
	AND
	player."transferType" <> 'referral'
	AND
	player."transferType" <> 'direct_offer'
	order by 1 ASC
	) p