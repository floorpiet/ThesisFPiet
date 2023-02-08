/*
 Get the demand for a given order-date, article and price line
 AOR calculated as nr articles sold in PPL on a date divided by total nr orders on that date in PPL.
 Only regular sales used (excl. promo sales)
 -> interpretation: probability that article is ordered (on a specified date and PPL)

PAY ATTENTION: 2 QUERIES TO RUN
 */
 */
 */


CREATE OR REPLACE TABLE temp.fpiet_aor_ppl AS (
                                              SELECT -- count orders per PPL (for divisor AOR)
                                                     -- ftol.key_article,
                                                     ftol.key_order_date,
                                                     do.order_price_line_type,
                                                     COUNT(DISTINCT ftol.key_order)   AS nr_orders_day_ppl,
                                                     SUM(ftol.orig_regular_sales_qty) AS sum_regular_sales,
                                                     SUM(ftol.orig_promo_qty)         AS sum_promo_sales
                                              FROM dim.ft_orderline AS ftol
                                                INNER JOIN dm_order AS do --NOTE: only valid until june 2022 (correct structure being worked out)
                                                           ON ftol.key_order = do.key_order
                                              WHERE ftol.key_order_date BETWEEN $start_date AND $end_date
                                              GROUP BY ftol.key_order_date, do.order_price_line_type
                                              ORDER BY ftol.key_order_date, do.order_price_line_type
                                              );

CREATE OR REPLACE TABLE temp.fpiet_aor AS (
                                          SELECT
                                            ftol.key_article,
                                            ftol.key_order_date,
                                            do.order_price_line_type                                  AS ppl,
                                            aorppl.nr_orders_day_ppl,
                                            IFF(SUM(ftol.orig_regular_sales_qty) > 0, SUM(ftol.orig_regular_sales_qty),
                                                0)                                                    AS sum_regular_art_sales_ppl,
                                            --SUM(ftol.orig_promo_qty)                              AS sum_promo_art_sales_ppl,
                                            DIV0(sum_regular_art_sales_ppl, aorppl.nr_orders_day_ppl) AS aor
                                          FROM ft_orderline AS ftol
                                            INNER JOIN dm_order AS do
                                                       ON ftol.key_order = do.key_order
                                            RIGHT OUTER JOIN temp.fpiet_aor_ppl AS aorppl
                                                             ON ftol.key_order_date = aorppl.key_order_date
                                                               AND
                                                                do.order_price_line_type = aorppl.order_price_line_type
                                          GROUP BY ftol.key_article,
                                                   ftol.key_order_date,
                                                   do.order_price_line_type,
                                                   aorppl.nr_orders_day_ppl
                                          ORDER BY key_order_date, key_article, ppl
                                          )
;


/*
DEPRECATED:
*/

/*
Skeleton + AOR combined:
*/

CREATE OR REPLACE TABLE temp.fpiet_ppltable (
  id  NUMBER AUTOINCREMENT,
  ppl VARCHAR(5)
);
INSERT INTO temp.fpiet_ppltable (ppl)
VALUES ('PPL1'),
       ('PPL2'),
       ('PPL3');

WITH
  data_filters AS (
  SELECT
    '2021-10-01':: DATE AS begin_date_df,
    '2022-10-01':: DATE AS end_date_df
                  ),

  skeleton AS (
  SELECT
    dma.key_article,
    dmdt.key_date,
    dmdt.date,
    ppl.ppl,
    ftp.price_in_cents,
    dma.art_supply_chain_name   AS article_name,
    dma.art_quality_tier,
    dma.art_p_cat_lev_2,
    dma.art_p_cat_lev_3,
    dma.art_p_cat_lev_4,
    dmdt.calendar_week::VARCHAR AS week
  FROM dm_article AS dma
    CROSS JOIN dm_date AS dmdt
    CROSS JOIN temp.fpiet_ppltable AS ppl
    INNER JOIN data_filters
               ON dmdt.date BETWEEN data_filters.begin_date_df AND data_filters.end_date_df
    INNER JOIN ft_price AS ftp
               ON dma.key_article = ftp.key_article AND
                  dmdt.date BETWEEN ftp.price_start_date_time::DATE AND ftp.price_end_date_time::DATE
                 AND ppl.ppl = ftp.price_line_type
                 AND ftp.price_type = 'PicNic'
                 AND ftp.price_line_type <> 'default'
  WHERE dma.art_p_cat_lev_1_id = '26714' --AND -- Kaas
        -- dma.art_p_cat_lev_1_id = '21736' AND -- Drinken
        -- dmdt.date BETWEEN data_filters.begin_date_df AND data_filters.end_date_df AND
  ORDER BY key_article, key_date, ppl
              )
  ,
  ft_art_order_week AS ( -- For each article (125 pcs in skeleton - 'Kaas' subgroup) count the nr orders for all weeks
  SELECT DISTINCT
    skeleton.key_article,
    dmd.key_week,
    COUNT(DISTINCT ftol.key_order) AS nr_orders_week
  FROM skeleton
    CROSS JOIN dm_date AS dmd
    LEFT OUTER JOIN ft_orderline AS ftol
                    ON skeleton.key_article = ftol.key_article
                      AND skeleton.key_date = ftol.key_order_date
    INNER JOIN dm_price_region AS dmpr
               ON ftol.key_price_region = dmpr.key_price_region AND dmpr.price_region_price_line_type <> 'default'
    INNER JOIN data_filters
               ON dmd.date BETWEEN (data_filters.begin_date_df - 14) AND (data_filters.end_date_df + 14)
  GROUP BY skeleton.key_article, dmd.key_week
  ORDER BY dmd.key_week
                       )
  ,
  select_articles
    AS ( --specify here what articles to use for AOR calculation (ones that have sales throughout whole period of analysis)
  SELECT DISTINCT
    ftaow.key_article
  FROM ft_art_order_week AS ftaow
  GROUP BY ftaow.key_article
  HAVING MIN(nr_orders_week) > 0
       ),
  ft_order_per_ppl_date AS ( -- count orders per PPL (for divisor AOR)
  SELECT
    ftol.key_order_date,
    dmpr.price_region_price_line_type,
    COUNT(DISTINCT ftol.key_order)   AS nr_orders_day_ppl,
    SUM(ftol.orig_regular_sales_qty) AS sum_regular_sales,
    SUM(ftol.orig_promo_qty)         AS sum_promo_sales
    --   ,DIV0(sum_regular_sales, nr_orders) AS aor_over_all_prods
  FROM ft_orderline AS ftol
    INNER JOIN dm_price_region AS dmpr
               ON ftol.key_price_region = dmpr.key_price_region
                 AND dmpr.price_region_price_line_type <> 'default'
    INNER JOIN dm_date AS dmd
               ON ftol.key_order_date = dmd.key_date
    INNER JOIN data_filters
               ON dmd.date BETWEEN data_filters.begin_date_df AND data_filters.end_date_df
  GROUP BY dmpr.price_region_price_line_type, ftol.key_order_date--, ft_art_order_week.nr_orders_week
  ORDER BY key_order_date
                           )
SELECT
  ftol.key_article,
  dma.article_id,
  ftol.key_order_date,
  ftoppd.price_region_price_line_type                       AS ppl,
  ftoppd.nr_orders_day_ppl,
  SUM(ftol.orig_regular_sales_qty)                          AS sum_regular_art_sales_ppl,
  --SUM(ftol.orig_promo_qty)                              AS sum_promo_art_sales_ppl,
  DIV0(sum_regular_art_sales_ppl, ftoppd.nr_orders_day_ppl) AS aor
FROM ft_orderline AS ftol
  INNER JOIN dm_article AS dma
             ON ftol.key_article = dma.key_article
  INNER JOIN ft_art_order_week
  INNER JOIN dm_price_region AS dmpr
             ON ftol.key_price_region = dmpr.key_price_region
  INNER JOIN ft_order_per_ppl_date AS ftoppd
             ON ftol.key_order_date = ftoppd.key_order_date
               AND dmpr.price_region_price_line_type = ftoppd.price_region_price_line_type
  INNER JOIN dm_date AS dmd
             ON ftol.key_order_date = dmd.key_date
  --   INNER JOIN data_filters
  --              ON dmd.date BETWEEN data_filters.begin_date_df AND data_filters.end_date_df
GROUP BY ftol.key_article,
         dma.article_id,
         ftol.key_order_date,
         ftoppd.price_region_price_line_type,
         ftoppd.nr_orders_day_ppl
ORDER BY key_order_date, key_article


