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
                                                                     AND dmd.key_date >= $start_date AND
                                                                      dmd.key_date <= $end_date
                                                      WHERE fpa.promo_art_is_deleted = 'no'
                                                      ORDER BY key_date ASC
                                                               ),
                                                      unavailability AS (
                                                      SELECT
                                                        dmd.key_date,
                                                        dma.key_article,
                                                        -- Note: Columns have to be summed up separately to avoid issues in computation when one of them is NULL (can not use a function)
                                                        SUM(event_date_unavailable_deliveryline_count) /
                                                        (SUM(order_date_orderline_count) +
                                                         SUM(event_date_unavailable_deliveryline_count)) AS unavailability_perc
                                                      FROM dim.ft_warehouse_article_daily AS ftwad
                                                        INNER JOIN dim.dm_date AS dmd
                                                                   ON ftwad.key_date = dmd.key_date
                                                                     AND dmd.key_date >= $start_date AND
                                                                      dmd.key_date <= $end_date
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
                                                                     AND dmd.key_date >= $start_date AND
                                                                      dmd.key_date <= $end_date
                                                      GROUP BY dmd.key_date
                                                      ORDER BY dmd.key_date
                                                                 )
                                                    SELECT
                                                      skeleton.key_article,
                                                      skeleton.key_date,
                                                      skeleton.date,
                                                      dma.art_supply_chain_name                                 AS article_name,
                                                      skeleton.ppl,
                                                      CASE
                                                        WHEN skeleton.key_date > '20210620' THEN 'post'
                                                          ELSE 'pre'
                                                      end as shift,
                                                      CASE WHEN shift = 'post' THEN CASE
                                                          WHEN skeleton.ppl = 'PPL1' then 'postPPL1'
                                                          WHEN skeleton.ppl = 'PPL2' then 'postPPL2'
                                                          WHEN skeleton.ppl = 'PPL3' then 'postPPL3'
                                                        END
                                                      ELSE skeleton.ppl
                                                      END AS ppln,
                                                      aor.aor,
                                                      IFNULL(fp.price_in_cents_ppl, fdp.price_in_cents_default) AS price_in_cents, -- give default price when there are no ppl prices
                                                      --                                                       promo.promo_metadata,
                                                      IFF(promo.promo_metadata IS NULL, 0, 1)                   AS promo_dummy,
                                                      unav.unavailability_perc,
                                                      weather.avg_high_temp,
                                                      weather.weather_type,
                                                      dma.art_quality_tier                                      AS article_tier,
                                                      dma.art_p_cat_lev_2                                       AS article_cat_2,
                                                      dma.art_p_cat_lev_3                                       AS article_cat_3,
                                                      dma.art_p_cat_lev_4                                       AS article_cat_4,
                                                      aorppl.nr_orders_day_ppl,
                                                      aor.sum_regular_art_sales_ppl,
                                                      skeleton.key_week,
                                                      dma.article_id,
                                                      dma.art_p_cat_lev_1                                       AS article_cat_1,
                                                      CASE
                                                        WHEN dma.art_content_volume_uom = 'ml'
                                                          THEN dma.art_content_volume
                                                        WHEN dma.art_content_volume_uom = 'liter'
                                                          THEN dma.art_content_volume * 1000
                                                      END                                                       AS art_content_volume, -- for drinks
--                                                       CASE
--                                                         WHEN dma.art_content_weight_uom = 'gram'
--                                                           THEN dma.art_content_weight
--                                                         WHEN dma.art_content_weight_uom = 'kilo'
--                                                           THEN dma.art_content_weight * 1000
--                                                       END                                                       AS art_content_weight, -- for kaas
                                                      CASE
                                                        WHEN dma.art_is_multipack = 'yes' THEN 1
                                                        WHEN dma.art_is_multipack = 'no'  THEN 0
                                                      END                                                       AS art_is_multipack -- for drinks
                                                    FROM temp.fpiet_skeleton AS skeleton
                                                      LEFT OUTER JOIN dm_article AS dma
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
                                                      LEFT OUTER JOIN temp.fpiet_aor_ppl AS aorppl
                                                                      ON skeleton.key_date = aorppl.key_order_date
                                                                        AND skeleton.ppl = aorppl.order_price_line_type
                                                    WHERE skeleton.ppl <> 'default' -- has no aor so not useful (did use default data in prices)
                                                    ORDER BY key_article, date, ppl
                                                    );


