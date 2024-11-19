
    SET NOCOUNT ON

	BEGIN
		truncate table cns_cpo_mid_capacity.oil_gas_inventory_temp_maxInventory_CTE 
		insert into cns_cpo_mid_capacity.oil_gas_inventory_temp_maxInventory_CTE
		select ogi.data_provider_key, ogi.capacity_utilization_pct, ogi.commerciality_type_key, ogi.first_report_dt, ogi.geopolitical_entity_key, ogi.inventory_dt, ogi.material_type_key, ogi.measurement_dt,
		ogi.period_type_key, ogi.provider_location_nm, ogi.provider_material_type_nm, ogi.provider_storage_location_cd, ogi.report_confidence_type_key,
		ogi.report_dt, ogi.storage_location_class_key, ogi.storage_location_key, ogi.storage_tank_key, ogi.universal_capacity_qty, 
		ogi.universal_inventory_qty, ogi.universal_uom_key, ogi.version_active_ind, ogi.version_effective_dt, ogi.version_termination_dt, ogi.version_type_key,
		DATEPART(wk, ogi.inventory_dt) as inventory_week, ogiwdp.week_start_dt, week_num, ogiwdp.calendar_week_key,
		ogi.original_inventory_qty, ogi.original_capacity_qty
		from cns_cpo_mid_capacity.OIL_GAS_INVENTORY ogi
		join (select MAX(inventory_dt) as weeklyinventorydt, cw.week_start_dt, DATEPART(wk, cw.week_end_dt) as week_num, data_provider_key, storage_Tank_key,   
		storage_location_key, cw.calendar_week_key
		from cns_cpo_mid_capacity.OIL_GAS_INVENTORY ogi
		join cns_glb_reference.calendar_week cw on cw.week_start_dt <= ogi.inventory_dt and cw.week_end_dt >= ogi.inventory_dt and cw.delete_ind = 'N'
		join cns_glb_reference.week_start_type wst on wst.week_start_type_key = cw.week_start_type_key and wst.week_start_type_cd = 'SUN' and wst.delete_ind = 'N'
		where version_active_ind = 'Y' and ogi.delete_ind = 'N' and ogi.inventory_dt >= '2009-11-01'
		group by data_provider_key, storage_Tank_key, DATEPART(wk, cw.week_end_dt), cw.week_start_dt, storage_location_key, cw.calendar_week_key) ogiwdp
		on ogi.data_provider_key = ogiwdp.data_provider_key and ogi.storage_location_key = ogiwdp.storage_location_key and ogi.storage_tank_key = ogiwdp.storage_Tank_key
		and ogi.inventory_dt = ogiwdp.weeklyinventorydt
		where ogi.delete_ind = 'N' and ogi.version_active_ind = 'Y'  and ogi.inventory_dt >= '2009-11-01'  and ogi.version_type_key = 1  
	END

	BEGIN
		truncate table cns_cpo_mid_capacity.oil_gas_inventory_temp_dateSeq_CTE
		insert into cns_cpo_mid_capacity.oil_gas_inventory_temp_dateSeq_CTE
		select date_d.*
		from(
		select distinct cw.week_start_dt, cw.week_end_dt, cw.calendar_week_key, ogi.storage_tank_key, ogi.data_provider_key, ogi.storage_location_key
		from (
			select cw.*
			from cns_glb_reference.calendar_week cw
			     join cns_glb_reference.week_start_type wst 
					on wst.week_start_type_key = cw.week_start_type_key and wst.week_start_type_cd = 'SUN' and wst.delete_ind = 'N'
		) cw,
		(select distinct storage_tank_key, data_provider_key, storage_location_key from cns_cpo_mid_capacity.OIL_GAS_INVENTORY where version_active_ind = 'Y' and delete_ind = 'N'  and inventory_dt >= '2009-11-01' ) ogi 
		where cw.delete_ind = 'N' and cw.week_start_dt >= (select min(inventory_dt) from cns_cpo_mid_capacity.OIL_GAS_INVENTORY where version_active_ind = 'Y' and delete_ind = 'N') 
		and cw.week_start_dt <= getdate()) date_d
		join (select distinct storage_tank_key, data_provider_key, storage_location_key, min(inventory_dt) as inventory_dt 
		from cns_cpo_mid_capacity.OIL_GAS_INVENTORY where version_active_ind = 'Y' and delete_ind = 'N'  and version_type_key = 1 group by storage_tank_key, data_provider_key, storage_location_key)min_da
		on min_da.data_provider_key = date_d.data_provider_key and min_da.storage_tank_key = date_d.storage_tank_key and min_da.storage_location_key = date_d.storage_location_key 
		and DATEADD(wk, DATEDIFF(week,  -1, min_da.inventory_dt),-1)<= date_d.week_start_dt
	END

	BEGIN
		truncate table cns_cpo_mid_capacity.oil_gas_inventory_temp_missing_CTE
		insert into cns_cpo_mid_capacity.oil_gas_inventory_temp_missing_CTE
		select c.*, m.week_num, m.inventory_week, m.capacity_utilization_pct, m.commerciality_type_key,m.first_report_dt, m.geopolitical_entity_key, m.inventory_dt, 
		m.material_type_key, m.measurement_dt,
		m.period_type_key, m.provider_location_nm, m.provider_material_type_nm, m.provider_storage_location_cd, m.report_confidence_type_key, m.report_dt,m.storage_location_class_key, m.universal_capacity_qty,
		m.universal_inventory_qty, m.universal_uom_key, m.version_active_ind, m.version_effective_dt, m.version_termination_dt,m.version_type_key,
		m.original_inventory_qty, m.original_capacity_qty
		from cns_cpo_mid_capacity.oil_gas_inventory_temp_dateSeq_CTE c
		left outer join cns_cpo_mid_capacity.oil_gas_inventory_temp_maxInventory_CTE m on m.calendar_week_key = c.calendar_week_key and m.storage_tank_key = c.storage_tank_key and m.storage_location_key = c.storage_location_key and m.data_provider_key = c.data_provider_key
	END

	--Added on 23/Feb/2023 to get preferred data provider using baseline_data_provider table
	BEGIN
		truncate table cns_cpo_mid_capacity.oil_gas_inventory_temp_preferred_dataprovider_CTE
		insert into cns_cpo_mid_capacity.oil_gas_inventory_temp_preferred_dataprovider_CTE
		SELECT storage_tank_key, data_provider_key
		FROM (
			SELECT ogi.storage_tank_key, ogi.data_provider_key,
					row_number() OVER(PARTITION BY ogi.storage_tank_key ORDER BY bdp.baselining_priority_num ASC) AS rnk
			FROM (
					SELECT distinct storage_tank_key, data_provider_key
					FROM cns_cpo_mid_capacity.oil_gas_inventory
					WHERE delete_ind = 'N' and version_active_ind = 'Y' and inventory_dt >= '2009-11-01'
				) ogi
				JOIN cns_cpo_mid_reference.baseline_data_provider bdp
				ON ogi.data_provider_key = bdp.data_provider_key
				JOIN cns_cpo_mid_reference.baseline_type bt
				ON bdp.baseline_type_key = bt.baseline_type_key
			WHERE bdp.delete_ind = 'N'
				  AND bt.delete_ind = 'N' and bt.baseline_type_cd = 'INV'
		) a
		WHERE rnk = 1
	END

	IF OBJECT_ID('cns_cpo_mid_capacity.oil_gas_inventory_weekly_pbi_st_stage', 'U') IS NOT NULL 
		BEGIN
			TRUNCATE TABLE cns_cpo_mid_capacity.oil_gas_inventory_weekly_pbi_st_stage

			insert into cns_cpo_mid_capacity.oil_gas_inventory_weekly_pbi_st_stage
				select * from cns_cpo_mid_capacity.oil_gas_inventory_weekly_pbi_vw_tmp
		END
	ELSE
		BEGIN
			SELECT * INTO cns_cpo_mid_capacity.oil_gas_inventory_weekly_pbi_st_stage
				FROM cns_cpo_mid_capacity.oil_gas_inventory_weekly_pbi_vw_tmp
		END


	--Insert EIA weekly data at the end of the standing table
	INSERT INTO [cns_cpo_mid_capacity].[oil_gas_inventory_weekly_pbi_st_stage] (
		week_start_dt, 
		week_end_dt, 
		calendar_week_key, 
		storage_location_key, 
		data_provider_key, 
		storage_tank_key, 
		week_num, 
		inventory_dt, 
		inventory_dt_orig, 
		inventory_dt_carry, 
		inventory_week, 
		universal_inventory_qty, 
		universal_inventory_qty_orig, 
		universal_capacity_qty, 
		universal_capacity_qty_orig, 
--------------------------------------------
		original_inventory_qty,
		original_inventory_qty_orig,
--------------------------------------------
		provider_location_nm, 
		geopolitical_entity_key, 
		material_type_key, 
		provider_material_type_nm, 
		provider_storage_location_cd, 
		version_type_key, 
		data_provider_abbr, 
		version_type_cd, 
		country, 
		country_key, 
		[state], 
		city, 
		region_name, 
		region_team_name, 
		subregion_name, 
		oecd, 
		opec, 
		opecplus, 
		bric, 
		family_class_nm, 
		group_class_nm, 
		provider_storage_tank_id, 
		storage_location_nm, 
		universal_historic_maximum_qty, 
		universal_total_capacity_qty, 
		tank_operational_status_type_desc, 
		tank_commerciality_type_desc, 
		location_geopolitical_entity_key
	)
	SELECT
		cw.week_start_dt,
		CASE WHEN cw.week_end_dt > max_pbi_wk.max_week_end_dt
			THEN max_pbi_wk.max_week_end_dt
			ELSE cw.week_end_dt
		END AS week_end_dt, 
		cw.calendar_week_key,
		slk.storage_location_key,
		ogi.data_provider_key,
		stk.storage_tank_key,
		datepart(wk, cw.week_end_dt) AS week_num,
		CASE WHEN cw.week_end_dt > max_pbi_wk.max_week_end_dt
			THEN max_pbi_wk.max_week_end_dt
			ELSE cw.week_end_dt
		END AS inventory_dt,
		CASE WHEN cw.week_end_dt > max_pbi_wk.max_week_end_dt
			THEN max_pbi_wk.max_week_end_dt
			ELSE cw.week_end_dt
		END AS inventory_dt_orig,
		CASE WHEN cw.week_end_dt > max_pbi_wk.max_week_end_dt
			THEN max_pbi_wk.max_week_end_dt
			ELSE cw.week_end_dt
		END AS inventory_dt_carry,
		datepart(wk, cw.week_end_dt) AS inventory_week,
		ogi.universal_inventory_qty,
		ogi.universal_inventory_qty AS universal_inventory_qty_orig,
		max_qty_dt.universal_historic_maximum_qty as universal_capacity_qty,
		max_qty_dt.universal_historic_maximum_qty AS universal_capacity_qty_orig,
--------------------------------------------
		ogi.original_inventory_qty,
		ogi.original_inventory_qty AS original_inventory_qty_orig,
--------------------------------------------
		'PADD3_SPR' AS provider_location_nm,
		ge.geopolitical_entity_key,
		ogi.material_type_key,
		mvw.material_type_desc AS provider_material_type_nm,
		'PADD3_SPR' AS provider_storage_location_cd,
		vt.version_type_key,
		'SHELL-BASELINE' AS data_provider_abbr,
		'B' AS version_type_cd,
		gev.country_nm AS country,
		gev.country_key,
		NULL AS [state],
		NULL AS city,
		'United States' AS region_name,
		NULL AS region_team_name,
		NULL AS subregion_name,
		'Y' AS oecd,
		'N' AS opec,
		'N' AS opecplus,
		'N' AS bric,
		mvw.family_class_nm,
		mvw.group_class_nm,
		'PADD3-SPR' AS provider_storage_tank_id,
		'PADD3-SPR' AS storage_location_nm,
		max_qty_dt.universal_historic_maximum_qty,
		max_qty_dt.universal_historic_maximum_qty AS universal_total_capacity_qty,
		tost.tank_operational_status_type_desc,
		ct.tank_commerciality_type_desc,
		ge.geopolitical_entity_key as location_geopolitical_entity_key
	FROM cns_cpo_mid_capacity.oil_gas_inventory ogi
		CROSS JOIN (
			SELECT max(universal_inventory_qty) AS universal_historic_maximum_qty, max(inventory_dt) AS max_inventory_dt
			FROM cns_cpo_mid_capacity.oil_gas_inventory ogi JOIN cns_glb_reference.dataset ds ON ogi.dataset_key = ds.dataset_key
			WHERE ogi.delete_ind = 'N' AND ogi.version_active_ind = 'Y' AND ds.delete_ind = 'N' AND ds.dataset_nm = 'EIA INVENTORY WEEK API'
		) max_qty_dt
		JOIN cns_glb_reference.dataset ds ON ogi.dataset_key = ds.dataset_key
		JOIN cns_glb_reference.calendar_week cw
		ON ogi.inventory_dt = cw.week_end_dt
			OR (
				cw.week_start_dt <= cast(getdate() AS DATE)
				and cw.week_end_dt > max_qty_dt.max_inventory_dt
				and ogi.inventory_dt = max_qty_dt.max_inventory_dt
			)
		JOIN cns_glb_reference.week_start_type wst ON wst.week_start_type_key = cw.week_start_type_key
		JOIN cns_glb_reference.material_type_vw mvw ON ogi.material_type_key = mvw.material_type_key
		JOIN cns_glb_reference.geopolitical_entity ge ON ogi.geopolitical_entity_key = ge.geopolitical_entity_key
		JOIN cns_glb_reference.geopolitical_entity_type gt ON ge.geopolitical_entity_type_key = gt.geopolitical_entity_type_key
		JOIN cns_glb_reference.geopolitical_entity_vw gev ON ge.geopolitical_entity_key = gev.geopolitical_entity_key
		JOIN cns_cpo_mid_reference.tank_commerciality_type ct on ogi.commerciality_type_key = ct.tank_commerciality_type_key
		CROSS JOIN cns_cpo_mid_reference.version_type vt
		CROSS JOIN cns_cpo_mid_reference.tank_operational_status_type tost
		CROSS JOIN (SELECT max(storage_location_key) + 1 AS storage_location_key FROM cns_cpo_mid_capacity.storage_location WHERE delete_ind = 'N') slk
		CROSS JOIN (SELECT max(storage_tank_key) + 1 AS storage_tank_key FROM cns_cpo_mid_capacity.storage_tank WHERE delete_ind = 'N') stk
		CROSS JOIN (SELECT max(week_end_dt) as max_week_end_dt from cns_cpo_mid_capacity.oil_gas_inventory_weekly_pbi_st_stage) max_pbi_wk
	WHERE ogi.delete_ind = 'N'
		AND ogi.version_active_ind = 'Y'
		AND ogi.inventory_dt >= '2009-11-01'
		AND ds.delete_ind = 'N'
		AND ds.dataset_nm = 'EIA INVENTORY WEEK API'
		AND ge.delete_ind = 'N'
		AND ge.geopolitical_entity_cd = 'PADD 3'
		AND gt.delete_ind = 'N'
		AND gt.geopolitical_entity_type_cd = 'P'
		AND vt.delete_ind = 'N'
		AND vt.version_type_cd = 'O'
		AND tost.delete_ind = 'N'
		AND tost.tank_operational_status_type_cd = 'OP'
		AND ct.delete_ind = 'N'
		AND ct.tank_commerciality_type_cd = 'S'
		-- AND mvw.group_class_nm = 'CRUDE' -- Removing crude filter
		AND cw.delete_ind = 'N' 
		AND wst.delete_ind = 'N'
		AND wst.week_start_type_cd = 'SUN'


    IF OBJECT_ID('cns_cpo_mid_capacity.oil_gas_inventory_weekly_pbi_st', 'U') IS NOT NULL 
		BEGIN
			TRUNCATE TABLE cns_cpo_mid_capacity.oil_gas_inventory_weekly_pbi_st

			insert into cns_cpo_mid_capacity.oil_gas_inventory_weekly_pbi_st
				select * from cns_cpo_mid_capacity.oil_gas_inventory_weekly_pbi_st_stage
		END
	ELSE
		BEGIN
			SELECT * INTO cns_cpo_mid_capacity.oil_gas_inventory_weekly_pbi_st
				FROM cns_cpo_mid_capacity.oil_gas_inventory_weekly_pbi_st_stage
		END

    DROP TABLE cns_cpo_mid_capacity.oil_gas_inventory_weekly_pbi_st_stage;
 
