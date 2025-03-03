
WITH site_translation AS (
SELECT 'FL - Gainsville Porta Serve' AS site, 'FL - Gainsville Porta Serve' AS translated_site
UNION ALL
SELECT 'SC - Littlejohn', 'SC - Littlejohn'
UNION ALL
SELECT 'FL - Premier & Prestigious', 'FL - Southwest FL'
UNION ALL
SELECT 'TN - MC Septic', 'TN - MC Septic'
UNION ALL
SELECT 'TX - ACP', 'TX - ACP'
UNION ALL
SELECT 'NY - A - John', 'NY - A - John'
UNION ALL
SELECT 'Cards Trial Group', 'NY - A - John'
UNION ALL
SELECT 'TN - FusionSite (Clark)', 'TN - FusionSite (Nashville Combined)'
UNION ALL
SELECT 'GA -PSI Augusta', 'GA/SC - PSI'
UNION ALL
SELECT 'NY - A - 1 Portable Toilets', 'NY - A - John'
UNION ALL
SELECT 'VA - R&R', 'VA - R&R'
UNION ALL
SELECT 'PA - Port A Bowl', 'PA - Port A Bowl'
UNION ALL
SELECT 'NC -  ASC', 'NC -  ASC'
UNION ALL
SELECT 'TN - ETP ', 'TN - ETP'
UNION ALL
SELECT 'WI - Stranders', 'WI - Stranders'
UNION ALL
SELECT 'NC - Denver', 'NC -  ASC'
UNION ALL
SELECT 'TN - FusionSite (Woodycrest)', 'TN - FusionSite (Nashville Combined)'
UNION ALL
SELECT 'OH - Rent - A - John', 'OH - Rent - A - John'
UNION ALL
SELECT 'FL - Freedom', 'FL - Freedom'
UNION ALL
SELECT 'AR - Fay', 'AR - Fay'
UNION ALL
SELECT 'AR - Little Rock', 'AR - Little Rock'
UNION ALL
SELECT 'TN - Memphis - Safety Quip', 'TN - Memphis - Safety Quip'
UNION ALL
SELECT 'GA - GCI', 'GA - GCI'
UNION ALL
SELECT 'NC - Griffin Hook trucks', 'NC - Griffin'
UNION ALL
SELECT 'TN - FusionSite (Nashville)', 'TN - FusionSite (Nashville Combined)'
UNION ALL
SELECT 'MS - Gotta Go', 'MS - Gotta Go'
UNION ALL
SELECT 'OH - C&L and Safeway', 'OH - C&L and Safeway'
UNION ALL
SELECT 'NC - Griffin Waste Pump Trucks', 'NC - Griffin'
UNION ALL
SELECT 'KY - Bullitt Sep. Service', 'KY - Bullitt Sep. Service'
UNION ALL
SELECT 'KY - moon portables', 'KY - Moon'
UNION ALL
SELECT 'SC - PSI Columbia', 'GA/SC - PSI'
UNION ALL
SELECT 'KY - Lex', 'KY - Lex'
UNION ALL
SELECT 'NC - Greensboro', 'NC -  ASC'
UNION ALL
SELECT 'FL - JW Craft', 'FL - Southwest FL'
UNION ALL
SELECT 'KY - Moon leasing', 'KY - Moon'
UNION ALL
SELECT 'PA - Malvern', 'PA - Port A Bowl'
UNION ALL
SELECT 'KY - Moon Minis', 'KY - Moon'
UNION ALL
SELECT 'TN - Chattanooga -Bolles', 'TN - Chattanooga -Bolles'
UNION ALL
SELECT 'WI - Buckys', 'WI - Buckys'
UNION ALL
SELECT 'IA - Cedar Rapids', 'IA - Cedar Rapids'
UNION ALL
SELECT 'TX - Forza', 'TX - Forza'
UNION ALL
SELECT 'Lubbock HD ', 'TX - Forza'
UNION ALL
SELECT 'SAN ANGELO- TOPS SEPTIC', 'TX - Forza'
UNION ALL
SELECT 'KERMIT', 'TX - Forza'
UNION ALL
SELECT 'LUBBOCK', 'TX - Forza'
UNION ALL
SELECT 'SEMINOLE', 'TX - Forza'
UNION ALL
SELECT 'Goldthwaite', 'TX - Forza'
UNION ALL
SELECT 'MS - American Johnny', 'MS - American Johnny'
UNION ALL
SELECT 'TN - Maxwell Septic', 'TN - Maxwell Septic'
UNION ALL
SELECT 'TX - J Bar', 'TX - J Bar'
)

SELECT 
    site,
    translated_site
FROM site_translation