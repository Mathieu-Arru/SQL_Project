import numpy
import psycopg2 # Python SQL driver for PostgreSQL
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
# Preparation de l’execution des requetes (a ne faire qu’une fois)
cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

# Création de la table Régions
df_regions= pd.read_csv("region2020.csv",sep =",")
cur.execute("""CREATE TABLE public.REGIONS(
reg int PRIMARY KEY NOT NULL,
cheflieu char(5) NOT NULL,
tncc int NOT NULL,
ncc varchar(30) NOT NULL,
nccenr varchar(30) NOT NULL,
libelle varchar(30) NOT NULL; """)
print("Table REGIONS creee avec succes dans PostgreSQL")

# Création de la table Départements 
df_departements= pd.read_csv("departement2020.csv",sep =",")
cur.execute("""CREATE TABLE public.DEPARTEMENTS(
dep int PRIMARY KEY NOT NULL,
reg int NOT NULL references REGIONS(reg),
cheflieu char(5) NOT NULL,
tncc int NOT NULL,
ncc varchar(30) NOT NULL,
nccenr varchar(30) NOT NULL,
libelle varchar(30) NOT NULL; """)
print("Table DEPARTEMENTS creee avec succes dans PostgreSQL")





