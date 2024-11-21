import os

def create_data_lake_structure(base_path, directories):

    print("Création de la structure du Data Lake...\n")

    for dir_path in directories:
        full_path = os.path.join(base_path, dir_path)
        try:
            os.makedirs(full_path, exist_ok=True)
            print(f"Dossier créé (ou déjà existant) : {full_path}")
        except Exception as e:
            print(f"Erreur lors de la création du dossier : {full_path}")
            print(f"Détails de l'erreur : {e}")

    print("\nStructure du Data Lake créée !")

base_path = "./"

directories = [
    "raw_data/transactions/",         # Données brutes des transactions
    "raw_data/web_logs/",             # Logs bruts des serveurs web
    "raw_data/social_media/",         # Données brutes des réseaux sociaux
    "raw_data/real_time_streams/",    # Données en temps réel (streams)
    "processed_data/transactions/",  # Données transformées des transactions
    "processed_data/web_logs/",      # Logs transformés
    "processed_data/social_media/",  # Données transformées des réseaux sociaux
    "processed_data/real_time/",     # Streams transformés
    "analytics/",                    # Données prêtes pour l'analyse
]

create_data_lake_structure(base_path, directories)
