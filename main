from .hist_functions import extract_ht, extract_pvd, delta_detection, update_chaneged_ht, insert_chaneged_pvd, löschroutine_ht
from .dm_functions import etl_dim_kunde, extract_dims, update_fact_ht, load_fact_ht, löschroutine_fact




def historisation():
    extract_ht()
    extract_pvd()
    delta_detection()
    update_chaneged_ht()
    insert_chaneged_pvd()
    löschroutine_ht()
    
    
    
    
def data_mart():
    extract_ht()
    etl_dim_kunde()
    extract_dims()
    update_fact_ht()
    load_fact_ht()
    löschroutine_fact()
    
