import numpy
import psycopg2  # Python SQL driver for PostgreSQL
import psycopg2.extras
import pandas as pd
from RegDep_Insert import insert

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

# Import et parsage d'Evolution Population 2012-2020
xls = pd.ExcelFile("Evolution_population_2012-2020.xlsx")
df_pop_dep = pd.read_excel(xls,"DEP", skiprows=3, skipfooter=2)
df_pop_reg = pd.read_excel(xls, "REG",skiprows=3, skipfooter=2)

df_pop_dep.rename( columns = {'Unnamed: 0' : 'Numero', 'Unnamed: 1': 'NomDep'}, inplace = True)
df_pop_reg.rename( columns = {'Unnamed: 0' : 'Numero', 'Unnamed: 1':'NomReg'})
col_pop_dep = df_pop_dep.columns.values.tolist()


# Création de la table Indicateur libelle
cur.execute("""CREATE TABLE public.indicateur_libelle(
IdI int PRIMARY KEY NOT NULL,
libelle varchar(30) NOT NULL); """)
print("Table indicateur_libelle creee avec succes dans PostgreSQL")

# Insertion de la table Indicateur libelle
# Create the pandas DataFrame
data = []
libelle_list = ["Population","Taux d'activité (%)","Taux d'emploi (%)","Part des diplomés",
                "Part des jeunes diplomés de 18-25 ans (%)","Travail en voiture (%)",
                "Travail en transport en commun (%)", "Travail en autre moyen de transport",
                "Poids de l'économie sociale dans les emplois salariés du territoire (%)",
                "Effort de R&D (%)","Esperance de vie à la naissance",
                "Disparité de niveau de vie (Rapport interdécile", "Taux de pauvreté (%)",
                "Part des jeunes non insérés (%)",
                "Part de la population éloignée de plus de 7 minutes des services de santé de proximité (%)",
                "Part de la population estimée en zone inondable (%)"]

for i in range(len(libelle_list)):
  data.append([i+1,libelle_list[i]])
df_indicateur_libelle = pd.DataFrame(data, columns = ["IdI","libelle"])

# Insertion
columns_list = [0,1]
command = "INSERT INTO indicateur_libelle(IdI,libelle) VALUES (%s,%s)"
insert(df_indicateur_libelle, command, columns_list, "indicateur_libelle")



# Création de la table Indicateur Région
cur.execute("""CREATE TABLE public.indicateurR(
IdR int NOT NULL references region(IdR),
IdI int NOT NULL references indicateur_libelle(IdI),
annee int NOT NULL,
valeur float NOT NULL,
PRIMARY KEY (IdR, IdI, annee)); """)
print("Table indicateurR creee avec succes dans PostgreSQL")

# Création de la table Indicateur Département
cur.execute("""CREATE TABLE public.indicateurD(
IdD int NOT NULL references departement(IdD),
IdI int NOT NULL references indicateur_libelle(IdI),
annee int NOT NULL,
valeur float NOT NULL,
PRIMARY KEY (IdD, IdI, annee)); """)
print("Table indicateurD creee avec succes dans PostgreSQL")


