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

# Import et parsage d'Evolution Population 2012-2020
xls = pd.ExcelFile("Evolution_population_2012-2020.xlsx")
df_pop_dep = pd.read_excel(xls,"DEP", skiprows=3, skipfooter=2)
df_pop_reg = pd.read_excel(xls, "REG",skiprows=3, skipfooter=2)

df_pop_dep.rename( columns = {'Unnamed: 0' : 'Numero', 'Unnamed: 1': 'NomDep'}, inplace = True)
df_pop_reg.rename( columns = {'Unnamed: 0' : 'Numero', 'Unnamed: 1':'NomReg'})
col_pop_dep = df_pop_dep.columns.values.tolist()

# Fonction d'insertion dans les tables
def insert(dataframe, command, liste, nom=""):
    print("Execution sur la base de donnees de la commande d’insertion avec les valeurs")
    for i in range(dataframe.shape[0]):
        values = []
        for j in liste:
            if type(dataframe.iloc[i][j]) is numpy.int64:
                values.append(int(dataframe.iloc[i][j]))
            elif dataframe.iloc[i][j] == "2A":
                values.append(201)
            elif dataframe.iloc[i][j] == "2B":
                values.append(202)
            else:
                values.append(dataframe.iloc[i][j])
        try:
            # Lancement de la requête.
            cur.execute(command, values)
        except Exception as e:
            # en cas d’erreur, fermeture de la connexion
            cur.close()
            conn.close()
            exit("error when running: " + command + " : " + str(e))
    # Nombre de lignes inserees
    count = cur.rowcount
    print(count,f"enregistrement(s) insere(s) avec succes dans la table {nom}.")


# Création de la table Indicateur libelle
cur.execute("""CREATE TABLE public.idlibelle(
IdI int PRIMARY KEY NOT NULL,
libelle varchar(100) NOT NULL); """)
print("Table idlibelle creee avec succes dans PostgreSQL")

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
command = "INSERT INTO idlibelle(IdI,libelle) VALUES (%s,%s)"
insert(df_indicateur_libelle, command, columns_list, "idlibelle")


# Création de la table Indicateur Région
cur.execute("""CREATE TABLE public.indicateurR(
IdR int NOT NULL references region(IdR),
IdI int NOT NULL references idlibelle(IdI),
annee int NOT NULL,
valeur float NOT NULL,
PRIMARY KEY (IdR, IdI, annee)); """)
print("Table indicateurR creee avec succes dans PostgreSQL")

# Insertion dans la table Indicateur Région
columns_list = [0,2]
command = "INSERT INTO indicateurR(IdR,nom,libelle) VALUES (%s,%s,2012)"
insert(df_pop_reg, command, columns_list, "indicateurR")

# Création de la table Indicateur Département
cur.execute("""CREATE TABLE public.indicateurD(
IdD int NOT NULL references departement(IdD),
IdI int NOT NULL references idlibelle(IdI),
annee int NOT NULL,
valeur float NOT NULL,
PRIMARY KEY (IdD, IdI, annee)); """)
print("Table indicateurD creee avec succes dans PostgreSQL")

cur.close()
conn.commit()
conn.close()