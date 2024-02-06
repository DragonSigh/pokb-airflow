-- Выгрузка всех ТАП по диспансеризации с начала года
SELECT
	tap.[Number] AS [tap_number],
	CAST([DateTAP] AS Date) AS [tap_date],
	MKAB.[NUM] AS [mkab_number],
	UPPER(MKAB.[Family]) AS [last_name],
	LEFT(MKAB.[Name], 1) AS [first_name],
	LEFT(MKAB.[OT], 1) AS [middle_name],
	LEFT(CAST(MKAB.[DATE_BD] AS Date), 4) AS [birth_year],
	dep.[DepartmentNAME] AS [department_name],
	subdivision.[M_NAMES] AS [subdivision_name]
  FROM [hlt_Pod_OKB_363001].[dbo].[hlt_TAP] AS tap -- ТАПы
  LEFT JOIN [hlt_Pod_OKB_363001].[dbo].[hlt_MKAB] AS MKAB -- МКАБы
  ON tap.[rf_MKABID] = MKAB.[MKABID]
  LEFT JOIN [hlt_Pod_OKB_363001].[dbo].[hlt_DocPRVD] AS doc_spec -- врачи
  ON tap.[rf_DocPRVDID] = doc_spec.[DocPRVDID]
  LEFT JOIN [hlt_Pod_OKB_363001].[dbo].[oms_PRVD] AS spec -- ресурсы
  ON doc_spec.[rf_PRVDID]  = spec.[PRVDID] 
  LEFT JOIN [hlt_Pod_OKB_363001].[dbo].[oms_Department] AS dep -- отделения
  ON doc_spec.[rf_DepartmentID] = dep.[DepartmentID]
  LEFT JOIN [hlt_Pod_OKB_363001].[dbo].[oms_LPU] AS subdivision -- подразделения
  ON dep.[rf_LPUID] = subdivision.[LPUID]
  WHERE [DateTAP] >= '2024-01-01'
  AND tap.[isClosed] = 1 -- ТАП закрыт
  AND tap.[rf_kl_ReasonTypeID] = 11 -- 2.2 Диспансеризация
ORDER BY [DateTAP] DESC