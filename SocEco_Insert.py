import numpy
import psycopg2  # Python SQL driver for PostgreSQL
import psycopg2.extras
import pandas as pd

# Try to connect to an existing database
print('Connexion a la base de donnees...')
USERNAME="marru"
PASSWORD="mdpbdd" # `a remplacer par le mot de passe d’acces aux bases
try:
    conn = psycopg2.connect(host='localhost', dbname=USERNAME,user=USERNAME,password=PASSWORD)
except Exception as e :
    exit("Connexion impossible a la base de donnees: " + str(e))
    
print('Connecte a la base de donnees')
#preparation de l’execution des requetes (a ne faire qu’une fois)
cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)


# ---- Import et parsage de DD-indic-reg-dep_2008_2019 ---- #
xls = pd.ExcelFile("DD-indic-reg-dep_2008_2019.xls")
df_soc_reg = pd.read_excel(xls,"Social", skiprows=5, skipfooter=115)
df_soc_dep = pd.read_excel(xls, "Social",skiprows=33,skipfooter=5)
df_eco_reg = pd.read_excel(xls,"Economie", skiprows=5,skipfooter=114)
df_eco_dep = pd.read_excel(xls, "Economie", skiprows=35, skipfooter=2)
libelle_list_Economie = [2,3,3,4,5,5,6,6,7,7,,8,8,9,10,10]
libelle_list_social = []

