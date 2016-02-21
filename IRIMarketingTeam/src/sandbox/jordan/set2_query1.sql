(select distinct sp10.IRI_Key,sp10.OUTLET,sp10.WEEK_KEY,ds.Market_Name, 
ds.Date_Year,ds.OPEN_WEEK,ds.CLSD_WEEK,ds.Date_Year, 
sp10.CATEGORY,sp10.OUTLET,sp10.DOLLARS,sp10.UNITS,sp10.Count_Rows 
FROM data_delivery_stores_01_to_11 as ds, summary_product_2010 as sp10
where  (ds.Market_Name='LOS ANGELES' or ds.Market_Name='CHICAGO')
and sp10.Date_Year=ds.Date_Year
and ds.IRI_KEY = sp10.IRI_Key 
and (sp10.CATEGORY = 'BR' OR sp10.CATEGORY = 'SS')
LIMIT 200)
union
(select distinct sp11.IRI_Key,sp11.OUTLET,sp11.WEEK_KEY,ds2.Market_Name, 
ds2.Date_Year,ds2.OPEN_WEEK,ds2.CLSD_WEEK,ds2.Date_Year, 
sp11.CATEGORY,sp11.OUTLET,sp11.DOLLARS,sp11.UNITS,sp11.Count_Rows 
FROM data_delivery_stores_01_to_11 as ds2, summary_product_2011 as sp11 
where  (ds2.Market_Name='LOS ANGELES' or ds2.Market_Name='CHICAGO')
and (sp11.CATEGORY = 'BR' OR sp11.CATEGORY = 'SS')
and sp11.Date_Year=ds2.Date_Year
and ds2.IRI_KEY = sp11.IRI_Key 
Limit 200)
