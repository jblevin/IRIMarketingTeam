select distinct sp11.IRI_Key,sp11.OUTLET,sp11.WEEK_KEY,ds.Market_Name, 
ds.Date_Year,ds.OPEN_WEEK,ds.CLSD_WEEK,ds.Date_Year, 
sp11.CATEGORY,sp11.OUTLET,sp11.DOLLARS,sp11.UNITS,sp11.Count_Rows 
FROM data_delivery_stores_01_to_11 as ds, summary_product_2010 as sp11 
where  (ds.Market_Name='LOS ANGELES' or ds.Market_Name='CHICAGO')
and sp11.Date_Year=ds.Date_Year
and ds.IRI_KEY = sp11.IRI_Key 
and (OR sp11.CATEGORY= 'TB' OR sp11.CATEGORY = 'SO' OR sp11.CATEGORY='CF' )
LIMIT 1000