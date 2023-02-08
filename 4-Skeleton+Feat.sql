/*
  Create full set by combining skeleton with AOR and other features
 */


CREATE OR REPLACE TABLE temp.fpiet_basic_dataset AS (
                                                    WITH
                                                      promo AS (
                                                      SELECT
                                                        dma.key_article,
                                                        dmd.key_date,
                                                        CONCAT(dmp.promo_mechanism, '/', dmp.promo_name) AS promo_metadata
                                                      FROM dim.ft_promotion_article_history AS fpa
                                                        INNER JOIN dim.dm_promotion AS dmp
                                                                   ON dmp.key_promotion = fpa.key_promotion
                                                        INNER JOIN dim.dm_article AS dma
                                                                   ON dma.key_article = fpa.key_article
                                                                     AND dma.art_p_cat_lev_1 = $category
                                                        INNER JOIN dim.dm_date AS dmd
                                                                   ON TO_DATE(fpa.promo_art_active_start_date) <=
                                                                      dmd.date AND
                                                                      TO_DATE(fpa.promo_art_active_end_date) >= dmd.date
                                                                     AND dmd.key_date >= $start_date AND dmd.key_date <= $end_date
                                                      WHERE fpa.promo_art_is_deleted = 'no'
                                                      ORDER BY key_date ASC
                                                               ),
                                                      unavailability AS (
                                                      SELECT
                                                        dmd.key_date,
                                                        dma.key_article,
                                                        -- Note: Columns have to be summed up separately to avoid issues in computation when one of them is NULL
                                                        SUM(event_date_unavailable_deliveryline_count) /
                                                        (SUM(order_date_orderline_count) +
                                                         SUM(event_date_unavailable_deliveryline_count)) AS unavailability_perc
                                                      FROM dim.ft_warehouse_article_daily AS ftwad
                                                        INNER JOIN dim.dm_date AS dmd
                                                                   ON ftwad.key_date = dmd.key_date
                                                                     AND dmd.key_date >= $start_date AND dmd.key_date <= $end_date
                                                        INNER JOIN dm_article AS dma
                                                                   ON ftwad.key_article = dma.key_article
                                                                     AND dma.art_p_cat_lev_1 = $category
                                                      GROUP BY dmd.key_date, dma.key_article
                                                                        ),
                                                      weather AS (
                                                      SELECT
                                                        dmd.key_date,
                                                        AVG(dmwd.wthr_dly_temperature_high) AS avg_high_temp,
                                                        MODE(dmwd.wthr_dly_icon)            AS weather_type
                                                      FROM dm_weather_daily AS dmwd
                                                        INNER JOIN dim.dm_date AS dmd
                                                                   ON dmwd.wthr_dly_date = dmd.date
                                                                     AND dmd.key_date >= $start_date AND dmd.key_date <= $end_date
                                                      GROUP BY dmd.key_date
                                                      ORDER BY dmd.key_date
                                                                 )
                                                    SELECT
                                                      skeleton.key_article,
                                                      skeleton.key_date,
                                                      skeleton.date,
                                                      dma.art_supply_chain_name                                AS article_name,
                                                      skeleton.ppl,
                                                      aor.aor,
                                                      IFNULL(fp.price_in_cents_ppl,fdp.price_in_cents_default) AS price_in_cents, -- give default price when there are no ppl prices
--                                                       promo.promo_metadata,
                                                      IFF(promo.promo_metadata IS NULL, 0, 1)                  AS promo_dummy,
                                                      unav.unavailability_perc,
                                                      weather.avg_high_temp,
                                                      weather.weather_type,
                                                      dma.art_quality_tier                                     AS article_tier,
                                                      dma.art_p_cat_lev_2                                      AS article_cat_2,
                                                      dma.art_p_cat_lev_3                                      AS article_cat_3,
                                                      dma.art_p_cat_lev_4                                      AS article_cat_4,
                                                      aorppl.nr_orders_day_ppl,
                                                      aor.sum_regular_art_sales_ppl,
                                                      skeleton.key_week,
                                                      dma.article_id,
                                                      dma.art_p_cat_lev_1       AS article_cat_1
                                                    FROM temp.fpiet_skeleton AS skeleton
                                                      left OUTER JOIN dm_article AS dma
                                                                 ON skeleton.key_article = dma.key_article
                                                      LEFT OUTER JOIN temp.fpiet_aor AS aor
                                                                      ON skeleton.key_article = aor.key_article
                                                                        AND skeleton.key_date = aor.key_order_date
                                                                        AND skeleton.ppl = aor.ppl
                                                      LEFT OUTER JOIN temp.fpiet_prices AS fp -- table with zero or one price per day/ppl/article (ftp might have multiple)
                                                                      ON skeleton.key_article = fp.key_article AND
                                                                         skeleton.date = fp.date
                                                                        AND skeleton.ppl = fp.ppl
                                                      LEFT OUTER JOIN temp.fpiet_defprices AS fdp
                                                                      ON skeleton.key_article = fdp.key_article AND
                                                                         skeleton.date = fdp.date
                                                                        AND skeleton.ppl = fdp.ppl
                                                      LEFT OUTER JOIN promo
                                                                      ON skeleton.key_article = promo.key_article
                                                                        AND skeleton.key_date = promo.key_date
                                                      LEFT OUTER JOIN unavailability AS unav
                                                                      ON skeleton.key_article = unav.key_article
                                                                        AND skeleton.key_date = unav.key_date
                                                      LEFT OUTER JOIN weather
                                                                      ON skeleton.key_date = weather.key_date
                                                      left outer join temp.fpiet_AOR_ppl as aorppl
                                                                      on skeleton.key_date = aorppl.key_order_date
                                                                      and skeleton.ppl = aorppl.order_price_line_type
                                                    WHERE skeleton.ppl <> 'default' -- has no aor so not useful (did use default data in prices)
                                                    order by key_article, date,ppl
                                                    );

select * from temp.fpiet_basic_dataset
order by key_article,key_date ,ppl;


/*Depr:*/

CREATE OR REPLACE TABLE temp.fpiet_ppltable (
  id  NUMBER AUTOINCREMENT,
  ppl VARCHAR(5)
);
INSERT INTO temp.fpiet_ppltable (ppl)
VALUES ('PPL1'),
       ('PPL2'),
       ('PPL3');

WITH
  data_filters AS ( -- can be modified later on
  SELECT
    '2021-01-01':: DATE AS begin_date_df,
    '2023-01-01':: DATE AS end_date_df
                  ),

  skeleton AS ( -- copied from skeleton query
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
    CROSS JOIN data_filters
    INNER JOIN ft_price AS ftp
               ON dma.key_article = ftp.key_article AND
                  dmdt.date BETWEEN ftp.price_start_date_time::DATE AND ftp.price_end_date_time::DATE
                 AND ppl.ppl = ftp.price_line_type
                 AND ftp.price_type = 'PicNic'
                 AND ftp.price_line_type <> 'default'
  WHERE dma.art_p_cat_lev_1_id = '26714' AND -- Kaas
    -- dma.art_p_cat_lev_1_id = '21736' AND -- Drinken
    dmdt.date BETWEEN data_filters.begin_date_df AND data_filters.end_date_df
    AND dma.art_in_store_date_last_added::DATE < data_filters.begin_date_df AND (
        dma.art_in_store_date_last_removed::DATE > data_filters.end_date_df OR
        dma.art_in_store_date_last_removed::DATE IS NULL)
  ORDER BY key_article, key_date, ppl
              ),
  ft_order_per_ppl_date
    AS ( -- need to specify here what orderlines to use for AOR calculation (filter on date; need to filter on sales period article)
  SELECT
    ftol.key_order_date,
    dmpr.price_region_price_line_type,
    COUNT(DISTINCT ftol.key_order)   AS nr_orders_day_ppl,
    SUM(ftol.orig_regular_sales_qty) AS sum_regular_sales,
    SUM(ftol.orig_promo_qty)         AS sum_promo_sales
    --   DIV0(sum_regular_sales, nr_orders) AS aor_over_all_prods
  FROM ft_orderline AS ftol
    INNER JOIN dm_price_region AS dmpr
               ON ftol.key_price_region = dmpr.key_price_region
                 AND dmpr.price_region_price_line_type <> 'default'
    INNER JOIN dm_date AS dmd
               ON ftol.key_order_date = dmd.key_date
    INNER JOIN data_filters
               ON dmd.date BETWEEN data_filters.begin_date_df AND data_filters.end_date_df
  GROUP BY dmpr.price_region_price_line_type, ftol.key_order_date
  ORDER BY key_order_date
       ),
  ft_aor_table AS (
  SELECT
    ftol.key_article,
    dma.article_id,
    ftol.key_order_date,
    ftoppd.price_region_price_line_type                       AS ppl,
    ftoppd.nr_orders_day_ppl,
    SUM(ftol.orig_regular_sales_qty)                          AS sum_regular_art_sales_ppl,
    --SUM(ftol.orig_promo_qty)                              AS sum_promo_art_sales_ppl, -- evt: CASE(when promo sales>0 -> in_promo=1)
    DIV0(sum_regular_art_sales_ppl, ftoppd.nr_orders_day_ppl) AS aor
  FROM ft_orderline AS ftol
    INNER JOIN dm_article AS dma
               ON ftol.key_article = dma.key_article
    INNER JOIN dm_price_region AS dmpr
               ON ftol.key_price_region = dmpr.key_price_region
    INNER JOIN ft_order_per_ppl_date AS ftoppd
               ON ftol.key_order_date = ftoppd.key_order_date
                 AND dmpr.price_region_price_line_type = ftoppd.price_region_price_line_type
    --   INNER JOIN dm_date AS dmd
    --              ON ftol.key_order_date = dmd.key_date
    --   INNER JOIN data_filters
    --              ON dmd.date BETWEEN data_filters.begin_date_df AND data_filters.end_date_df
  GROUP BY ftol.key_article,
           dma.article_id,
           ftol.key_order_date,
           ftoppd.price_region_price_line_type,
           ftoppd.nr_orders_day_ppl
  ORDER BY key_order_date, key_article
                  )
  ,
  ft_unavailability AS (
  SELECT
    skeleton.key_date,
    skeleton.key_article,
    -- Note: Columns have to be summed up separately to avoid issues in computation when one of them is NULL
    SUM(event_date_unavailable_deliveryline_count) /
    (SUM(order_date_orderline_count) + SUM(event_date_unavailable_deliveryline_count)) AS unavailability_perc
  FROM skeleton
    INNER JOIN dim.ft_warehouse_article_daily AS ftwad
               ON skeleton.key_article = ftwad.key_article AND
                  skeleton.key_date = ftwad.key_date
  GROUP BY skeleton.key_date, skeleton.key_article
                       )
  ,
  ft_weather AS ( -- involves GROUPBY clause so don't include in final query directly
  SELECT
    skeleton.key_date,
    AVG(dmwd.wthr_dly_temperature_high) AS avg_high_temp
  FROM skeleton
    INNER JOIN dm_weather_daily AS dmwd
               ON skeleton.date = dmwd.wthr_dly_date
  GROUP BY key_date
                )
SELECT
  skeleton.*,
  ftaor.aor,
  --   ftaor.nr_orders_day_ppl,
  --   ftaor.sum_regular_art_sales_ppl,
  CONCAT(dmp.promo_mechanism, '/', dmp.promo_name) AS promo_metadata,
  ftu.unavailability_perc,
  ftw.avg_high_temp
FROM skeleton
  INNER JOIN ft_aor_table AS ftaor
             ON skeleton.key_article = ftaor.key_article
               AND skeleton.key_date = ftaor.key_order_date
               AND skeleton.ppl = ftaor.ppl
  LEFT OUTER JOIN dim.ft_promotion_article_history AS fpa -- LEFT OUTER JOIN: use all skeleton entries (also whenever there's no promo for that art+date)
                  ON skeleton.key_article = fpa.key_article AND
                     TO_DATE(fpa.promo_art_active_start_date) <= skeleton.date AND
                     TO_DATE(fpa.promo_art_active_end_date) >= skeleton.date AND
                     fpa.promo_art_is_deleted = 'no'
  LEFT OUTER JOIN dim.dm_promotion dmp -- or is INNER JOIN ok?
                  ON dmp.key_promotion = fpa.key_promotion
  INNER JOIN ft_unavailability AS ftu
             ON skeleton.key_article = ftu.key_article AND skeleton.key_date = ftu.key_date
  INNER JOIN ft_weather AS ftw
             ON skeleton.key_date = ftw.key_date
LIMIT 500;


