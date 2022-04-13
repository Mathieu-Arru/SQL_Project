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


# Création de la table Régions
df_regions = pd.read_csv("region2020.csv", sep=",")
cur.execute("""CREATE TABLE public.region(
IdR int PRIMARY KEY NOT NULL,
nom varchar(30) NOT NULL,
libelle varchar(30) NOT NULL);""")
print("Table region creee avec succes dans PostgreSQL")

# Création de la table Départements
df_departements = pd.read_csv("departement2020.csv", sep=",")
cur.execute("""CREATE TABLE public.departement(
IdD int PRIMARY KEY NOT NULL,
IdR int NOT NULL references region(IdR),
nom varchar(30) NOT NULL,
libelle varchar(30) NOT NULL); """)
print("Table departement creee avec succes dans PostgreSQL")


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

  
# Insertion de la table Régions
columns_list = [0,3,5]
command = "INSERT INTO region(IdR,nom,libelle) VALUES (%s,%s,%s)"
insert(df_regions, command, columns_list, "region")

# Insertion de la table Départements
columns_list = [0,1,4,6]
command = "INSERT INTO departement(IdD,IdR,nom,libelle) VALUES (%s,%s,%s,%s)"
insert(df_departements, command, columns_list, "departement")

cur.close()
conn.commit()
conn.close()