/* ВЫГРУЗКА СВОБОДНЫХ ЯЧЕЕК ПО ВРЕМЕНИ ИЗ ЕМИАС 

resource_id - ID ресурса
begin_time - время начала приема
resource_type - тип ресурса (врач, кабинет, оборудование и т.д.)
doctor_full_name - ФИО врача, если ресурс = врач
equipment_name - название оборудования, если ресурс = оборудование
equipment_type - тип оборудования
cell_type - тип ячейки
access_code - код прав доступа к ячейке
is_used - занята ли ячейка, 1 = да, 0 = нет
is_out_schedule - ячейка вне расписания, 1 = да, 0 = нет
specialty_name - специальность врача
department_name - отделение
subdivision_name - подразделение
 + разложение кода прав доступа к ячейке (1 = да, 0 = нет)

*/

SELECT res_spec.[DocPRVDID] AS [resource_id],
	dtt.[Begin_Time] AS [begin_time],
	dtt.[End_Time] AS [end_time],
	res_type.[Name] AS [resource_type],
	UPPER(doc.[FAM_V] + ' ' + doc.[IM_V] + ' ' + doc.[OT_V]) AS [doctor_full_name],
	equip.[Name] AS [equipment_name],
	equip_type.[Name] AS [equipment_type],
	cabinets.[Num] AS [cabinet_number],
	dbt.[Name] AS [cell_type],
	dtt.[FlagAccess] AS [access_code],
	dtt.[UsedUE] AS [is_used],
	dtt.[IsOutSchedule] AS [is_out_schedule],
	spec.[PRVDID] AS [specialty_id],
	spec.[NAME] AS [specialty_name],
	dep.[DepartmentNAME] AS [department_name],
	subdivision.[M_NAMES] AS [subdivision_name],
	 -- разложение кода прав доступа к ячейке (access_code):
	CASE WHEN dtt.[FlagAccess] & 1 > 0 THEN 1 ELSE 0 END AS [ac_registrar], -- регистратура
	CASE WHEN dtt.[FlagAccess] & 2 > 0 THEN 1 ELSE 0 END AS [ac_doctor], -- врач
	CASE WHEN dtt.[FlagAccess] & 4 > 0 THEN 1 ELSE 0 END AS [ac_internet], -- интернет
	CASE WHEN dtt.[FlagAccess] & 8 > 0 THEN 1 ELSE 0 END AS [ac_other_mo], -- другая МО
	CASE WHEN dtt.[FlagAccess] & 32 > 0 THEN 1 ELSE 0 END AS [ac_infomat], -- инфомат
	CASE WHEN dtt.[FlagAccess] & 64 > 0 THEN 1 ELSE 0 END AS [ac_call_center] -- колл-центр
FROM [hlt_Pod_OKB_363001].[dbo].[hlt_DoctorTimeTable] AS dtt -- расписание ресурсов медицинской организации
LEFT JOIN [hlt_Pod_OKB_363001].[dbo].[hlt_DocBusyType] AS dbt -- справочник типа занятости ресурса
	ON [rf_DocBusyType] = [DocBusyTypeID]
LEFT JOIN [hlt_Pod_OKB_363001].[dbo].[hlt_LPUDoctor] doc -- данные врача-сотрудника 
	ON dtt.[rf_LPUDoctorID] = doc.[LPUDoctorID]
LEFT JOIN [hlt_Pod_OKB_363001].[dbo].[hlt_DocPRVD] AS res_spec -- ресурс медицинской организации
	ON dtt.[rf_DocPRVDID] = res_spec.[DocPRVDID]
LEFT JOIN [hlt_Pod_OKB_363001].[dbo].[hlt_ResourceType] AS res_type -- справочник типов ресурсов
	ON res_spec.[rf_ResourceTypeID] = res_type.[ResourceTypeID]
LEFT JOIN [hlt_Pod_OKB_363001].[dbo].[hlt_Equipment] AS equip -- данные оборудования
	ON res_spec.[rf_EquipmentID] = equip.[EquipmentID]
LEFT JOIN [hlt_Pod_OKB_363001].[dbo].[hlt_HealingRoom] AS cabinets -- данные кабинетов
	ON res_spec.[rf_HealingRoomID] = cabinets.[HealingRoomID]
LEFT JOIN [hlt_Pod_OKB_363001].[dbo].[hlt_EquipmentType] AS equip_type -- справочник типа оборудования
	ON equip.[rf_EquipmentTypeID] = equip_type.[EquipmentTypeID]
LEFT JOIN [hlt_Pod_OKB_363001].[dbo].[oms_PRVD] AS spec -- специальность ресурса из справочника
	ON res_spec.[rf_PRVDID]  = spec.[PRVDID]
LEFT JOIN [hlt_Pod_OKB_363001].[dbo].[oms_Department] AS dep -- отделение
	ON res_spec.[rf_DepartmentID] = dep.[DepartmentID]
LEFT JOIN [hlt_Pod_OKB_363001].[dbo].[oms_LPU] AS subdivision -- подразделение
	ON dep.[rf_LPUID] = subdivision.[LPUID]
WHERE CAST(dtt.[Date] AS DATE) >= CAST(GETDATE() AS DATE) -- все дни после сегодняшнего числа
	-- AND dtt.[UsedUE] = 1 -- только свободные ячейки
	-- AND res_type.[Name] = 'Врач' -- только врач
	AND res_spec.[InTime] = 1 -- Доступен в расписании
	-- AND dtt.[IsOutSchedule] = 0 -- убираем Вне расписания
	-- AND doc.[FAM_V] + ' ' + doc.[IM_V] + ' ' + doc.[OT_V] = 'БАГДАДЯН АРУСЯК ВАРДГЕСОВНА'
ORDER BY [Begin_Time] ASC; -- сортируем по возрастанию времени начала приема