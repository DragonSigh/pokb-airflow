-- Выгрузка карт диспансеризации и ПМО
SELECT
	disp_card.[DateOpen] AS date_open,
	disp_card.[DateClose] AS date_close,
	disp_card.[Number] AS card_number,
	lpu.[M_NAMES] AS subdivision_name,
	UPPER(doc.[FAM_V] + ' ' + doc.[IM_V] + ' ' + doc.[OT_V]) AS [doctor_full_name]
  FROM [hlt_Pod_OKB_363001].[dbo].[hlt_disp_Card] AS disp_card -- карты диспансеризации
  LEFT JOIN [hlt_Pod_OKB_363001].[dbo].[oms_LPU] AS lpu -- подразделения
  ON disp_card.[rf_LpuGuid] = lpu.[GUIDLPU]
  LEFT JOIN [hlt_Pod_OKB_363001].[dbo].[hlt_DocPRVD] AS doc_spec -- врачи
  ON disp_card.[rf_DocPRVDID] = doc_spec.[DocPRVDID]
  LEFT JOIN [hlt_Pod_OKB_363001].[dbo].[hlt_LPUDoctor] doc -- данные врача-сотрудника 
  ON doc_spec.[rf_LPUDoctorID] = doc.[LPUDoctorID]
  WHERE [DateClose] >= '2024-01-01' AND [DateClose] <= '2025-01-01'
  AND [IsClosed] = 1 -- Закрыта
  AND [rf_disp_ReasonCloseGuid] = '51D750AC-74C8-493E-B643-A486425449A1' -- Обследование пройдено
  AND ([rf_DispTypeGuid] = '1DEC26C7-13A2-4DAF-8C2C-38E33619C82E' -- 404н Диспансеризация
  OR [rf_DispTypeGuid] = '0DA8DFF7-0EF0-4AD8-B933-B4E2F80B7988') -- 404н Профилактические медицинские осмотры
  ORDER BY disp_card.[DateClose] DESC