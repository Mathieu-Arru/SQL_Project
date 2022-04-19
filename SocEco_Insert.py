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
df_eco_dep.columns = ["ID","Nom",2,3,3,4,5,5,6,6,7,7,8,8,9,10,10]
df_eco_reg.columns = ["ID","Nom",2,3,3,4,5,5,6,6,7,7,8,8,9,10,10]
df_soc_dep.columns = ["ID","Nom",11,11,11,11,11,11,12,13,13,14,14,14,15,16,16]
df_soc_reg.columns = ["ID","Nom",11,11,11,11,11,11,12,13,13,14,14,14,15,16,16]

# Fonction d'insertion dans les tables
def insert(dataframe, command, liste, nom=""):
    print("Execution sur la base de donnees de la commande d’insertion avec les valeurs")
    for i in range(1,dataframe.shape[0]):
        values = []
        
        if dataframe.iloc[i][0] == "M" or dataframe.iloc[i][0] == "F" or dataframe.iloc[i][0] == "P" or dataframe.iloc[i][0] == "FHM":
            continue
        for j in liste:
            if type(dataframe.iloc[i][j]) is numpy.int64:
                values.append(int(dataframe.iloc[i][j]))
            elif dataframe.iloc[i][j] == "2A":
                values.append(201)
            elif dataframe.iloc[i][j] == "2B":
                values.append(202)
            elif type(dataframe.iloc[i][j]) is not numpy.int64:
              continue
            else:
                values.append(dataframe.iloc[i][j])
        values.append(dataframe.iloc[0][liste[-1]])
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

# Insertion dans la table Indicateur Région
columns_list = [0,2]
command = "INSERT INTO indicateurR(IdR,valeur,annee,IdI,sexe) VALUES (%s,%s,%s,2,1)"
insert(df_soc_reg, command, columns_list, "indicateurR")
