/*
 Get the demand for a given order-date, article and price line
 AOR calculated as nr articles sold in PPL on a date divided by total nr orders on that date in PPL.
 Only regular sales used (excl. promo sales)
 -> interpretation: probability that article is ordered (on a specified date and PPL)
 */

CREATE OR REPLACE TABLE temp.fpiet_aor AS (
                                          WITH aorppl AS (--first table: for order info per PPL
                                            SELECT -- count orders per PPL (for divisor AOR)
                                                     -- ftol.key_article,
                                                     ftol.key_order_date,
                                                     do.order_price_line_type,
                                                     COUNT(DISTINCT ftol.key_order)   AS nr_orders_day_ppl,
                                                     SUM(ftol.orig_regular_sales_qty) AS sum_regular_sales,
                                                     SUM(ftol.orig_promo_qty)         AS sum_promo_sales
                                              FROM dim.ft_orderline AS ftol
                                                INNER JOIN dm_order AS do --NOTE: was only valid until june 2022 (PPL shift - now fixed)
                                                           ON ftol.key_order = do.key_order
                                              WHERE ftol.key_order_date BETWEEN $start_date AND $end_date
                                              GROUP BY ftol.key_order_date, do.order_price_line_type
                                              ORDER BY ftol.key_order_date, do.order_price_line_type
                                                         )
                                          SELECT
                                            ftol.key_article,
                                            ftol.key_order_date,
                                            do.order_price_line_type                                  AS ppl,
                                            aorppl.nr_orders_day_ppl,
                                            IFF(SUM(ftol.orig_regular_sales_qty) > 0, SUM(ftol.orig_regular_sales_qty),
                                                0)                                                    AS sum_regular_art_sales_ppl,
                                            --SUM(ftol.orig_promo_qty)                              AS sum_promo_art_sales_ppl, -- decided to disregard promo data
                                            DIV0(sum_regular_art_sales_ppl, aorppl.nr_orders_day_ppl) AS aor
                                          FROM ft_orderline AS ftol
                                            INNER JOIN dm_order AS do
                                                       ON ftol.key_order = do.key_order
                                            RIGHT OUTER JOIN aorppl --temp.fpiet_aor_ppl AS aorppl (earlier version used temp table instead of WITH AS () -statement
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




