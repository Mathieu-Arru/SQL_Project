from click import command
import psycopg2  # Python SQL driver for PostgreSQL
import psycopg2.extras
import pandas as pd

# Try to connect to an existing database
#Connexion à la base de données
print('Connexion à la base de données...')
USERNAME="marru"
PASSWORD="mdpbdd"
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
    choice = int(input())
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


#Fonction pour afficher tous les départements d'une région
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


#Fonction pour afficher les données d'un département par theme (eco ou socio)
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


#Fonction pour afficher la population d'un département sur une année donnée
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

# afficher la liste des départements où le taux de pauvreté en 2018 était compris entre 15% et 20 %,
#  classés du plus fort taux au plus faible.
def question1():
    command = """ SELECT D.libelle FROM departement D
    JOIN indicateurd ID on D.idd = ID.idd
    WHERE ID.idi = 13 
    AND ID.annee = 2018
    AND ID.valeur BETWEEN 15 AND 20
    ORDER BY ID.valeur DESC;
    """
    
    try:
        cur.execute(command)
    except Exception as e :
    #fermeture de la connexion
        cur.close()
        conn.close()
        exit("error when running: " + command + " : " + str(e))
        
    values = cur.fetchall()
    
    print("la liste des départements où le taux de pauvreté en 2018 :")
    for value in values:
        print(value)

#Quels sont les départements dont la région avait un effort de recherche 
# et développement strictement supérieur à 2 % en 2014 ? Afficher aussi le taux d’activité en 2017 pour ces départements.
def question2():
    command = """ SELECT D.libelle, ID.valeur FROM departement D
    INNER JOIN indicateurd ID on D.idd = ID.idd
    INNER JOIN indicateurr IR on IR.idr = D.idr
    WHERE IR.idi = 10
    AND IR.annee = 2014
    AND IR.valeur > 2
    INTERSECT
    SELECT D.libelle, ID.valeur FROM departement D
    JOIN indicateurd ID on D.idd = ID.idd
    WHERE idi = 2
    AND annee = 2017
    """

    try:
        cur.execute(command)
    except Exception as e :
    #fermeture de la connexion
        cur.close()
        conn.close()
        exit("error when running: " + command + " : " + str(e))
        
    values = cur.fetchall()
    
    print("la liste des départements dont la région avait un effort de recherche ")
    for value in values:
        print(value)


# Afficher la différence entre l’espérance de vie des hommes et des femmes en 2019 pour 
# tous les départements de la région ayant le plus grand taux de pauvreté en 2018.
def question3():
    command = """
    """


# Quelle est la population totale en 2017 de tous les départements
#  où la part des jeunes non insérés (en 2017) est supérieure à 25% ?
def question4():
    command = """
    SELECT SUM(ID.valeur) FROM indicateurd ID
    WHERE ID.annee = 2017
    AND ID.idi::boolean = 1
    IN (SELECT valeur FROM indicateurd ID
    WHERE ID.annee = 2017
    AND ID.idi = 14
    AND ID.valeur > 25)
    """

    try:
        cur.execute(command)
    except Exception as e :
    #fermeture de la connexion
        cur.close()
        conn.close()
        exit("error when running: " + command + " : " + str(e))
        
    values = cur.fetchall()
    
    print("la population totale en 2017 de tous les départements")
    for value in values:
        print(value)

# En 2014, quelle était l’espérance de vie des femmes et des hommes dans les régions 
# dont le taux d’emplois était supérieur à 63% et dont la part d’utilisation de la voiture 
# pour se rendre au travail était moins de 75% ?
def question5():
    command = """ SELECT IR.valeur FROM  indicateurr IR
    JOIN region R on R.idr = IR.idr
    WHERE IR.idi = 11
    AND IR.annee = 2014
    INTERSECT
    SELECT IR.valeur FROM  indicateurr IR
    WHERE IR.idi = 
    """

question4()