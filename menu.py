import psycopg2  # Python SQL driver for PostgreSQL
import psycopg2.extras
import pandas as pd

# Try to connect to an existing database
#Connexion à la base de données
print('Connexion à la base de données...')
USERNAME="lomontagne"
PASSWORD="Awerti95£" 
try:
    conn = psycopg2.connect(host='localhost', dbname=USERNAME,user=USERNAME,password=PASSWORD)
except Exception as e :
    exit("Connexion impossible `a la base de donn ees: " + str(e))
print('Connecté à la base de données')
cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

#Fonction du menu principal
def menu_user():
    print("""Que voulez vous faire ?\n
    1-> Afficher la liste des régions\n
    2-> Afficher les départements d'un région de votre choix\n
    3-> Afficher les données thématiques d'un département\n
    4-> Afficher la population d'un département pour une année donnée\n""")
    choice = input()
    if choice <= 0 or choice > 4:
        print("Choix invalide")
        menu_user()
    
    if choice == 1:
        liste_regions()

    if choice == 2:
        print("De quelle region voulez-vous afficher les départements ?")
        region = input()
        dep_by_region(region)

    if choice == 3:
        print("Choisissez un département :")
        departement = input()
        print("""Quel thème :\n
        1-> Economique\n
        2-> Social""")
        theme = int(input())
        while theme != 1 and theme != 2:
            print("Choix invalide")
            theme = int(input())
        data_by_dep(departement, theme)
    
    if choice == 4:
        print("Choisissez un département :")
        departement = input()
        print("Choisissez une année :")
        annee = input()
        pop_by_year(departement, annee)

#Fonction pour afficher la liste des régions
def liste_regions():
    command = "SELECT libelle FROM region"
    print('Exécution sur la base de données de la requête: ',command)
    try:
    # Lancement de la requête.
        cur.execute(command)
    except Exception as e :
    #fermeture de la connexion
        cur.close()
        conn.close()
        exit("error when running: " + command + " : " + str(e))

    #Récupération de la requête
    regions = cur.fetchall()
    for region in regions:
        print(region)

#Fonction pour affihcer tous les départements d'une région
def dep_by_region(region):

    region = region.replace("é", "e")
    region = region.replace("ô", "o")
    region = region.replace("î", "i")
    region = region.replace("Î", "I")
    region = region.replace("-", " ")
    choice = [region.upper()]
    print(region)
    command = "SELECT D.libelle FROM departement D JOIN region R ON D.idr = R.idr WHERE R.nom LIKE %s;"
# Lancement de la requête.
    try:
        cur.execute(command,choice)
    except Exception as e :
    #fermeture de la connexion
        cur.close()
        conn.close()
        exit("error when running: " + command + " : " + str(e))
    
    deps = cur.fetchall()

    for dep in deps:
        print(dep)

#Fonction pour affihcer les données d'un département par theme (eco ou socio)
def data_by_dep(departement, theme):
    departement = departement.replace("é", "e")
    departement = departement.replace("è", "e")
    departement = departement.replace("ô", "o")
    departement = departement.replace("-", " ")
    departement = departement.replace("'", " ")
    choice = [departement.upper(),theme]

    command = """SELECT iD.annee,iD.valeur 
                FROM indicateurd iD
                INNER JOIN departement D ON iD.idd=D.idd 
                INNER JOIN idlibelle I ON iD.idi=I.idi
                WHERE D.nom LIKE %s
                AND I.theme = %s;"""

    try:
        cur.execute(command,choice)
    except Exception as e :
    #fermeture de la connexion
        cur.close()
        conn.close()
        exit("error when running: " + command + " : " + str(e))
    
    values = cur.fetchall()

    for value in values:
        print(value[0],"\t",value[1])

#Fonction pour afficher la population d'un département sur une année données
def pop_by_year(departement, annee):
    departement = departement.replace("é", "e")
    departement = departement.replace("è", "e")
    departement = departement.replace("ô", "o")
    departement = departement.replace("-", " ")
    departement = departement.replace("'", " ")
    choice = [departement.upper(),annee]

    command = """SELECT id.valeur FROM indicateurd id 
                JOIN departement D ON id.idd=D.idd
                WHERE D.nom like %s 
                AND id.idi=1 
                AND id.annee= %s"""
    
    try:
        cur.execute(command,choice)
    except Exception as e :
    #fermeture de la connexion
        cur.close()
        conn.close()
        exit("error when running: " + command + " : " + str(e))
    
    values = cur.fetchall()
    if(not values):
        print("Données non disponibles")
    for value in values:
        print("En", annee,"la population était de",value)