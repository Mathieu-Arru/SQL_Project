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
df_soc_reg = pd.read_excel(xls,"Social", skiprows=5, skipfooter=116,header=None)
df_soc_dep = pd.read_excel(xls, "Social",skiprows=33,skipfooter=5, header=None)
df_eco_reg = pd.read_excel(xls,"Economie", skiprows=5,skipfooter=114,header=None)
df_eco_dep = pd.read_excel(xls, "Economie", skiprows=35, skipfooter=2,header=None)


# Fonction d'insertion dans les tables
def insert(dataframe, command, liste, indic, nom=""):
    print("Execution sur la base de donnees de la commande d’insertion avec les valeurs")
    for i in range(1,dataframe.shape[0]):
        values = []
        values.append(dataframe.iloc[0][liste[-1]])
        values.append(indic[liste[-1]])
        if dataframe.iloc[i][0] == "M" or dataframe.iloc[i][0] == "F" or dataframe.iloc[i][0] == "P" or dataframe.iloc[i][0] == "FHM":
            continue
        for j in liste:
            print(type(dataframe.iloc[i][j]), dataframe.iloc[i][j])
            if pd.isna(dataframe.iloc[i][j]) or dataframe.iloc[i][j] == 'nd' or dataframe.iloc[i][j] == 'nd ' :
                continue
            if dataframe.iloc[i][j] == "2A":
                values.append(201)
            elif dataframe.iloc[i][j] == "2B":
                values.append(202)
            else:
                values.append(dataframe.iloc[i][j])
        print(values)
        if len(values) == 4:
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
eco_indic = [0,1,2,3,3,4,5,5,6,6,7,7,8,8,9,10,10]
soc_indic = [0,1,11,11,11,11,11,11,12,13,13,14,14,14,15,16,16]

for i in range(2,16):
    columns_list = [0,i]
    if i in [2,3,4]:
        command = "INSERT INTO indicateurR(annee,IdI,IdR,valeur,sexe) VALUES (%s,%s,%s,%s,1)"
        insert(df_soc_reg, command, columns_list,soc_indic, "indicateurR")
        command = "INSERT INTO indicateurD(annee,IdI,IdD,valeur,sexe) VALUES (%s,%s,%s,%s,1)"
        insert(df_soc_dep, command, columns_list,soc_indic, "indicateurR")
    elif i in [5,6,7]:
        command = "INSERT INTO indicateurR(annee,IdI,IdR,valeur,sexe) VALUES (%s,%s,%s,%s,2)"
        insert(df_soc_reg, command, columns_list,soc_indic, "indicateurR")
        command = "INSERT INTO indicateurD(annee,IdI,IdD,valeur,sexe) VALUES (%s,%s,%s,%s,2)"
        insert(df_soc_dep, command, columns_list,soc_indic, "indicateurR")
    else:
        command = "INSERT INTO indicateurR(annee,IdI,IdR,valeur,sexe) VALUES (%s,%s,%s,%s,0)"
        insert(df_soc_reg, command, columns_list,soc_indic, "indicateurR")
        command = "INSERT INTO indicateurD(annee,IdI,IdD,valeur,sexe) VALUES (%s,%s,%s,%s,0)"
        insert(df_soc_dep, command, columns_list,soc_indic, "indicateurR")


cur.close()
conn.commit()
conn.close()