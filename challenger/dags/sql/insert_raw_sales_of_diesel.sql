INSERT INTO public.raw_sales_of_diesel
select 
        cast(ano || '-' || unnest(array['01' , '02' , '03' , '04' , '05' , '06' , '07' , '08' , '09' , '10' , '11' , '12']) || '-' || '01' as date) as year_month,
        estado as uf,
        combustivel as product,
        regiao as unit,
        unnest(array[jan , fev , mar , abr , mai, jun, jul, ago, set, out, nov, dez]) as volume,
        CURRENT_TIMESTAMP as created_at 
from public.sales_of_diesel;

TRUNCATE TABLE public.sales_of_diesel;
