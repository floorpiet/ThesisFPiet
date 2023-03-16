/*
  Building the base structure for AOR model
  - Skeleton includes CROSS JOIN of article x date x PPL (initializing index)
*/

SET start_date = '20201214'; -- lagged data included so start from week 51 2020
SET end_date = '20230101'; -- week 53 2022
SET start_date_2023set = '20221210'; --for different set
SET end_date_2023set='20230301'; --for different set
SET category = 'Kaas'--'Drinken'--,
SET start_date2 = '2021-01-01'; -- use assortment from within this period
SET end_date2 = '2023-01-01' ;

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
