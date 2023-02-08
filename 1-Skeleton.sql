/*
  Building the base structure for AOR model
  - Skeleton includes CROSS JOIN of article x date x PPL (x AOR)
*/


SET start_date = '20201214'; -- 01-01-2021 je wilt lagged data dus vanaf week 51 2020 - week 53 2022
SET end_date = '20230101';
SET category = 'Kaas'--'Drinken';
SET start_date2 = '2021-01-01'; -- gaat om assortiment vanaf dit moment
SET end_date2 = '2023-01-01' -- assortiment




CREATE OR REPLACE TABLE temp.fpiet_skeleton AS (
                                               WITH
                                                 select_articles AS (
                                                 SELECT DISTINCT
                                                   key_article
                                                 FROM dm_article AS dma
                                                 WHERE dma.art_in_store_date_last_added::DATE <= $start_date2 AND (
                                                       art_in_store_date_last_removed::DATE > $end_date2 OR
                                                       art_in_store_date_last_removed IS NULL)
                                                   AND art_p_cat_lev_1 = $category
                                                                    ),
                                                 dates AS (
                                                 SELECT
                                                 key_date, key_week, DATE
                                                 FROM dim.dm_date
                                                 WHERE key_date BETWEEN $start_date AND $end_date
                                                 )
                                                 , ppls AS (
                                                 SELECT DISTINCT
                                                 price_line_type AS ppl
                                                 FROM ft_price
                                                 )

                                               SELECT *
                                               FROM select_articles
                                                 CROSS JOIN dates
                                                 CROSS JOIN ppls
                                               ORDER BY key_article, key_date, ppl
                                               )
;

SELECT *
FROM temp.fpiet_skeleton
ORDER BY key_article, key_date, ppl

/* deprecated :*/

-- --articles based on weekly orders>0:
-- WITH
--                                                  articles AS (
--                                                  SELECT
--                                                    dma.key_article,
--                                                    dmd.key_week,
--                                                    IFF(COUNT(DISTINCT ftol.key_order) > 0,
--                                                        COUNT(DISTINCT ftol.key_order), 0) AS orders_per_week
--                                                  FROM dim.dm_article AS dma
--                                                    CROSS JOIN dim.dm_date AS dmd
--                                                    LEFT OUTER JOIN dim.ft_orderline AS ftol
--                                                                    ON ftol.key_article = dma.key_article
--                                                                      AND ftol.key_order_date = dmd.key_date
--                                                  WHERE dmd.key_date >= $start_date
--                                                    AND dmd.key_date <= $end_date
--                                                    AND dma.art_p_cat_lev_1 = $category -- and dma.ART_HAS_REGIONAL_PRICING = 'yes' / 'Drinken'
--                                                    AND dma.art_assortment_status = 'In'
--                                                  GROUP BY dma.key_article, dmd.key_week
--                                                  ORDER BY dma.key_article, dmd.key_week
--                                                              )
--                                                  ,
--                                                  select_articles AS (
--                                                  SELECT DISTINCT
--                                                    key_article
--                                                  FROM articles
--
--                                                  GROUP BY key_article
--                                                  HAVING MIN(orders_per_week) > 0
--                                                                     ),
--
-- CREATE OR REPLACE TABLE temp.fpiet_ppltable (
--   id  NUMBER AUTOINCREMENT,
--   ppl VARCHAR(5)
-- );
-- INSERT INTO temp.fpiet_ppltable (ppl)
-- VALUES ('PPL1'),
--        ('PPL2'),
--        ('PPL3');
--
--
-- WITH
--   data_filters AS (
--   SELECT
--     '2021-10-01':: DATE AS begin_date_df,
--     '2022-10-01':: DATE AS end_date_df
--                   )

/*
MAAK EEN SELECTIE ARTICLE ID'S:
- MET SALES VOOR EN NA PERIODE
- IN CAT 1 = ''
- MISSCHIEN NOG MET REGIONAL PRICING (IS ER EEN BOUNDARY CASE MET EERST DEFAULT EN DAN PPL?)
*/
--
-- SELECT dma.key_article, dma.art_supply_chain_name, dma.art_p_cat_lev_1,
--   FROM dm_article AS DMA
--   INNER JOIN ft_orderline as FTO
--   ON dma.key_article = FTO.key_article
--   INNER JOIN data_filters
--   ON dma.
-- WHERE DMA.art_p_cat_lev_1 = 'Kaas'
-- AND fto.
-- ORDER BY FTO.key_order_date ASC
-- LIMIT 500;

-- SELECT
--   dma.key_article,
--   dmdt.key_date,
--   dmdt.date,
--   ppl.ppl,
--   ftp.price_in_cents,
--   dma.art_supply_chain_name   AS article_name,
--   dma.art_quality_tier,
--   dma.art_p_cat_lev_1_id,
--   dma.art_p_cat_lev_1,
--   dma.art_p_cat_lev_2,
--   dma.art_p_cat_lev_3,
--   dma.art_p_cat_lev_4,
--   dma.art_in_store_date_last_added,
--   dmdt.calendar_week::VARCHAR AS week,
--   dmaah.assortment_status
-- FROM dm_article AS dma
--   CROSS JOIN dm_date AS dmdt
--   INNER JOIN data_filters
--              ON dmdt.date BETWEEN data_filters.begin_date_df AND data_filters.end_date_df
--   CROSS JOIN temp.fpiet_ppltable AS ppl
--   INNER JOIN ft_price AS ftp
--              ON dma.key_article = ftp.key_article AND
--                 dmdt.date BETWEEN ftp.price_start_date_time::DATE AND ftp.price_end_date_time::DATE
--                AND ppl.ppl = ftp.price_line_type
--                AND ftp.price_type = 'PicNic'
--                AND ftp.price_line_type <> 'default'
--   INNER JOIN sandbox.dm_article_assortment_history AS dmaah
--              ON dma.key_article = dmaah.key_article AND dmdt.key_date = dmaah.key_date AND
--                 dmaah.assortment_status = 'In'
-- WHERE dma.art_p_cat_lev_1_id = '26714' AND -- Kaas --DIT WERKT NIET?
-- --       dma.ART_ASSORTMENT_STATUS = 'In' -- article is in at end of tracking period
-- -- dma.art_p_cat_lev_1_id = '21736' AND -- Drinken
-- -- dmdt.date BETWEEN data_filters.begin_date_df AND data_filters.end_date_df AND
--       dma.art_in_store_date_last_added::DATE < data_filters.begin_date_df
--    OR dma.art_in_store_date_last_added::DATE IS NULL
--    --   AND (
--    --       dma.art_in_store_date_last_removed::DATE > data_filters.end_date_df OR
--    --       dma.art_in_store_date_last_removed::DATE IS NULL)
-- ORDER BY dma.art_in_store_date_last_added DESC, key_article, key_date, ppl
-- LIMIT 500;
