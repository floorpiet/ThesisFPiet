/*
THERE CAN BE MANY PRICES FOR AN ARTICLE ON ONE DAY so therefore use RN
two tables: one for ppl price and one for default price (default price table needs different join w.r.t. skeleton)

PAY ATTENTION: 2 QUERIES TO RUN
*/

CREATE OR REPLACE TABLE temp.fpiet_prices AS (
                                             WITH
                                               all_prices AS
                                                 (
                                               SELECT
                                                 skeleton.*,
                                                 IFF(skeleton.ppl = 'default', NULL, ftp.price_in_cents)                                                                AS price_in_cents_ppl,
                                                 IFF(skeleton.ppl = 'default', ftp.price_in_cents, NULL)                                                                AS price_in_cents_default,
                                                 -- Need to use row number in order to use last changed price on date only
                                                 ROW_NUMBER() OVER (PARTITION BY skeleton.key_article,skeleton.key_date,skeleton.ppl ORDER BY price_end_date_time DESC) AS rn
                                               FROM temp.fpiet_skeleton AS skeleton
                                                 left outer JOIN dim.ft_price AS ftp
                                                            ON skeleton.key_article = ftp.key_article
                                                              AND
                                                               skeleton.date BETWEEN ftp.price_start_date_time::DATE AND ftp.price_end_date_time::DATE
                                                              AND ftp.price_type = 'PicNic'
                                                              AND skeleton.ppl = ftp.price_line_type
                                                 )
                                             SELECT *
                                             FROM all_prices

                                             WHERE rn = '1'
                                             ORDER BY key_article, key_date, ppl
                                             )

CREATE OR REPLACE TABLE temp.fpiet_defprices AS (
                                             WITH
                                               all_prices AS
                                                 (
                                               SELECT
                                                 skeleton.*,
                                                 ftp.price_in_cents AS price_in_cents_default,
                                                 -- Need to use row number in order to use last changed price on date only
                                                 ROW_NUMBER() OVER (PARTITION BY skeleton.key_article,skeleton.key_date,skeleton.ppl ORDER BY price_end_date_time DESC) AS rn
                                               FROM temp.fpiet_skeleton AS skeleton
                                                 left outer JOIN dim.ft_price AS ftp
                                                            ON skeleton.key_article = ftp.key_article
                                                              AND
                                                               skeleton.date BETWEEN ftp.price_start_date_time::DATE AND ftp.price_end_date_time::DATE
                                                              AND ftp.price_type = 'PicNic'
                                                              and ftp.price_line_type = 'default'
                                              order by key_article, key_date, ppl
                                                 )
                                             SELECT *
                                             FROM all_prices
                                             WHERE rn = '1'
                                             ORDER BY key_article, key_date, ppl
                                             )

select*from temp.fpiet_prices


