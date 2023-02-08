-- week aggregation start

CREATE OR REPLACE TABLE temp.fpiet_dataset_drinks AS (
                                                     WITH
                                                       artsincat AS (
                                                       SELECT
                                                         COUNT(DISTINCT key_article) AS nr_articles_in_cat,
                                                         --key_date,
                                                         key_week
                                                       FROM dm_date dmd
                                                         LEFT OUTER JOIN dm_article dma
                                                                         ON dmd.date >= dma.art_in_store_date_last_added::DATE
                                                                           AND (dmd.date <=
                                                                                art_in_store_date_last_removed::DATE OR
                                                                                art_in_store_date_last_removed IS NULL)
                                                       WHERE key_date BETWEEN $start_date AND $end_date
                                                         AND dma.art_p_cat_lev_1 = $category
                                                       GROUP BY key_week
                                                                    ),
                                                       artsincat2 AS (
                                                         select COUNT(DISTINCT key_article) AS nr_articles_in_cat,
                                                         --key_date,
                                                         key_week,
                                                         dmd.week_start_date,
                                                         dma.art_p_cat_lev_2
                                                       FROM dm_date dmd
                                                         LEFT OUTER JOIN dm_article dma
                                                                         ON dmd.date >= dma.art_in_store_date_last_added::DATE
                                                                           AND (dmd.date <=
                                                                                art_in_store_date_last_removed::DATE OR
                                                                                art_in_store_date_last_removed IS NULL)
                                                       WHERE key_date BETWEEN $start_date AND $end_date
                                                         AND dma.art_p_cat_lev_1 = $category
                                                       GROUP BY dma.art_p_cat_lev_2, key_week,dmd.week_start_date
                                                       ORDER BY key_week
                                                                     )
                                                     SELECT
                                                       bset.key_week,
                                                       dmd.year_calendar_week,
                                                       dmd.week_start_date,
                                                       bset.key_article,
                                                       article_name,
                                                       ppl,
                                                       SUM(sum_regular_art_sales_ppl)      AS "product order amt",
                                                       AVG(bset.price_in_cents)            AS "Avg sell price",
                                                       AVG(ftp.price_in_cents)             AS "Avg purchase price",
                                                       MAX(promo_dummy)                    AS promo_dummy,
                                                       AVG(IFNULL(unavailability_perc, 0)) AS "AVG unavailability_perc",
                                                       AVG(avg_high_temp)                  AS avg_high_temp,
                                                       SUM(nr_orders_day_ppl)              AS "total order amt",
                                                       article_tier,
                                                       dma.art_brand_tier,
                                                       IFNULL(dma.art_packaging, 'Other')  AS "Packaging",
                                                       article_cat_2,
                                                       article_cat_3,
                                                       article_cat_4,
                                                       aic.nr_articles_in_cat,
                                                       aic2.NR_ARTICLES_IN_CAT as nr_articles_in_cat_2,
                                                       bset.article_id
                                                     FROM temp.fpiet_basic_dataset AS bset
                                                       LEFT OUTER JOIN dim.ft_price AS ftp
                                                                       ON bset.key_article = ftp.key_article
                                                                         AND
                                                                          bset.date BETWEEN ftp.price_start_date_time::DATE AND ftp.price_end_date_time::DATE
                                                                         AND ftp.price_type = 'Purchasing'
                                                                         AND ftp.price_line_type = 'default'
                                                       LEFT OUTER JOIN dm_article dma
                                                                       ON bset.key_article = dma.key_article
                                                       INNER JOIN artsincat aic
                                                                  ON bset.key_week = aic.key_week
                                                       INNER JOIN dm_date AS dmd
                                                                  ON bset.key_week = dmd.key_week
                                                      INNER JOIN ARTSINCAT2 aic2
                                                                  ON bset.key_week = aic2.key_week
                                                                  and bset.article_cat_2 = aic2.ART_P_CAT_LEV_2
                                                     GROUP BY bset.key_week,
                                                              dmd.year_calendar_week,
                                                              dmd.week_start_date,
                                                              bset.key_article,
                                                              article_name,
                                                              ppl,
                                                              article_tier,
                                                              dma.art_brand_tier,
                                                              dma.art_packaging,
                                                              article_cat_2,
                                                              article_cat_3,
                                                              article_cat_4,
                                                              aic.nr_articles_in_cat,
                                                              aic2.NR_ARTICLES_IN_CAT,
                                                              bset.article_id
                                                     ORDER BY key_article, key_week
                                                     );


SELECT *
FROM temp.fpiet_dataset_drinks
ORDER BY key_article, key_week, ppl

WITH
  artsincat AS (
  SELECT
    COUNT(DISTINCT key_article) AS nr_articles_in_cat,
    --key_date,
    key_week
  FROM dm_date dmd
    LEFT OUTER JOIN dm_article dma
                    ON dmd.date >= dma.art_in_store_date_last_added::DATE
                      AND (dmd.date <= art_in_store_date_last_removed::DATE OR art_in_store_date_last_removed IS NULL)
  WHERE key_date BETWEEN $start_date AND $end_date
    AND dma.art_p_cat_lev_1 = $category
  GROUP BY key_week
  ORDER BY key_week
               )
SELECT
  bset.key_week,
  dmd.year_calendar_week,
  dmd.week_start_date,
  bset.key_article,
  article_name,
  ppl,
  SUM(sum_regular_art_sales_ppl)      AS "product order amt",
  AVG(bset.price_in_cents)            AS "Avg sell price",
  AVG(ftp.price_in_cents)             AS "Avg purchase price",
  MAX(promo_dummy),
  AVG(IFNULL(unavailability_perc, 0)) AS "AVG unavailability_perc",
  AVG(avg_high_temp),
  SUM(nr_orders_day_ppl)              AS "total order amt",
  article_tier,
  dma.art_brand_tier,
  IFNULL(dma.art_packaging, 'Other')  AS "Packaging",
  article_cat_2,
  article_cat_3,
  article_cat_4,
  aic.nr_articles_in_cat,
  bset.article_id
FROM temp.fpiet_basic_dataset AS bset
  LEFT OUTER JOIN dim.ft_price AS ftp
                  ON bset.key_article = ftp.key_article
                    AND
                     bset.date BETWEEN ftp.price_start_date_time::DATE AND ftp.price_end_date_time::DATE
                    AND ftp.price_type = 'Purchasing'
                    AND ftp.price_line_type = 'default'
  LEFT OUTER JOIN dm_article dma
                  ON bset.key_article = dma.key_article
  INNER JOIN artsincat aic
             ON bset.key_week = aic.key_week
  INNER JOIN dm_date AS dmd
             ON bset.key_week = dmd.key_week
GROUP BY bset.key_week,
         dmd.year_calendar_week,
         dmd.week_start_date,
         bset.key_article,
         article_name,
         ppl,
         article_tier,
         dma.art_brand_tier,
         dma.art_packaging,
         article_cat_2,
         article_cat_3,
         article_cat_4,
         aic.nr_articles_in_cat,
         bset.article_id
ORDER BY key_article, key_week
;


SELECT
  COUNT(DISTINCT bset.key_article)
FROM temp.fpiet_basic_dataset AS bset



