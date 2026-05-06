from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text
import os
import traceback

# Chemins des fichiers
EXCEL_PATH = "/mnt/c/Users/hp/Desktop/airflow/data/fatf_data.xlsx"
CONTINENT_PATH = "/mnt/c/Users/hp/Desktop/airflow/data/pays_continent.xlsx"

TMP_TRANSFORM = "/tmp/fatf_transform.csv"
TMP_FINAL = "/tmp/fatf_final.csv"

# Connexion PostgreSQL
DB_URI = "postgresql+psycopg2://hp:hp@localhost:5432/FATF"

# ======================
# Tâche 1: Chargement des données
# ======================
def load_data():
    try:
        print("📂 Chargement du fichier Excel...")
        df = pd.read_excel(EXCEL_PATH, engine="openpyxl", skiprows=4)

        # Nettoyage des colonnes
        df.columns = df.columns.str.strip().str.lower().str.replace("\n", "", regex=True)
        df = df.dropna(how="all").reset_index(drop=True)

        # Renommage de la première colonne
        df.rename(columns={df.columns[0]: "country"}, inplace=True)
        df["country"] = df["country"].astype(str).str.strip().str.lower()

        # Sauvegarde
        df.to_csv(TMP_TRANSFORM, index=False)
        print(f"✅ load_data OK - {len(df)} lignes chargées")
        
    except Exception as e:
        print(f"❌ Erreur dans load_data: {str(e)}")
        raise

# ======================
# Tâche 2: Transformation des données
# ======================
def transform_data():
    try:
        if not os.path.exists(TMP_TRANSFORM):
            raise Exception(f"❌ Fichier {TMP_TRANSFORM} non trouvé")

        print("🔄 Transformation des données...")
        df = pd.read_csv(TMP_TRANSFORM)

        # Mapping des valeurs
        fatf_mapping = {"C": 4, "LC": 3, "PC": 2, "NC": 1}
        io_mapping = {"HE": 4, "SE": 3, "ME": 2, "LE": 1}

        # Colonnes à transformer
        r_cols = [c for c in df.columns if c.startswith("r.")]
        io_cols = [c for c in df.columns if c.startswith("io")]

        # Application des mappings
        for col in r_cols:
            df[col] = df[col].astype(str).str.upper().str.strip().map(fatf_mapping)

        for col in io_cols:
            df[col] = df[col].astype(str).str.upper().str.strip().map(io_mapping)

        # Remplacement des NaN par 0
        df[r_cols] = df[r_cols].fillna(0)
        df[io_cols] = df[io_cols].fillna(0)

        # Calcul des moyennes
        df["r_avg"] = df[r_cols].replace(0, pd.NA).mean(axis=1)
        df["io_avg"] = df[io_cols].replace(0, pd.NA).mean(axis=1)

        # Score AML (70% Recommendations, 30% IO)
        df["aml_score"] = df["r_avg"] * 0.7 + df["io_avg"] * 0.3

        # Niveau de risque
        df["risk_level"] = pd.cut(
            df["aml_score"],
            bins=[0, 1.5, 2.5, 3.5, 4.1],
            labels=["HIGH", "MEDIUM", "LOW", "COMPLIANT"],
            right=False
        )

        # Sauvegarde
        df.to_csv(TMP_FINAL, index=False)
        print(f"✅ transform_data OK - {len(df)} lignes transformées")
        
    except Exception as e:
        print(f"❌ Erreur dans transform_data: {str(e)}")
        raise

# ======================
# Tâche 3: Ajout du continent
# ======================
def add_continent():
    try:
        if not os.path.exists(TMP_FINAL):
            raise Exception(f"❌ Fichier {TMP_FINAL} non trouvé")

        print("🌍 Ajout des continents...")
        df = pd.read_csv(TMP_FINAL)
        df_continent = pd.read_excel(CONTINENT_PATH)

        # Nettoyage du fichier continent
        df_continent.columns = df_continent.columns.str.strip().str.lower()
        if 'pays' in df_continent.columns:
            df_continent.rename(columns={"pays": "country"}, inplace=True)
        
        # Standardisation des noms de pays
        df["country"] = df["country"].str.strip().str.lower()
        df_continent["country"] = df_continent["country"].str.strip().str.lower()

        # Fusion
        df = df.merge(df_continent, on="country", how="left")
        
        # Vérification des pays sans continent
        missing_continent = df[df['continent'].isna()]['country'].unique()
        if len(missing_continent) > 0:
            print(f"⚠️ Attention: {len(missing_continent)} pays sans continent: {missing_continent[:5]}...")

        # Sauvegarde
        df.to_csv(TMP_FINAL, index=False)
        print(f"✅ add_continent OK - {len(df)} lignes avec continent")
        
    except Exception as e:
        print(f"❌ Erreur dans add_continent: {str(e)}")
        raise

# ======================
# Tâche 4: Chargement dans PostgreSQL (SOLUTION FINALE - utilise psycopg2 directement)
# ======================
def load_postgres():
    import psycopg2
    from psycopg2.extras import execute_values
    import io
    
    try:
        print("🚀 Démarrage du chargement PostgreSQL...")

        # Vérification du fichier
        if not os.path.exists(TMP_FINAL):
            raise Exception(f"❌ Fichier {TMP_FINAL} non trouvé")

        # Lecture des données
        df = pd.read_csv(TMP_FINAL)
        print(f"📊 {len(df)} lignes à charger")
        print(f"📋 Colonnes: {df.columns.tolist()}")

        # Nettoyage des données
        df = df.fillna(0)
        
        # Conversion des colonnes numériques
        numeric_cols = ["r_avg", "io_avg", "aml_score"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
                print(f"📈 {col}: moyenne={df[col].mean():.2f}, min={df[col].min():.2f}, max={df[col].max():.2f}")

        # Connexion directe avec psycopg2
        print(f"🔌 Connexion à PostgreSQL avec psycopg2...")
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="FATF",
            user="hp",
            password="hp"
        )
        
        # Test de connexion
        cur = conn.cursor()
        cur.execute("SELECT version()")
        version = cur.fetchone()
        print(f"✅ Connecté à PostgreSQL: {version[0].split(',')[0]}")
        
        # Suppression de la table si elle existe
        print("🗑️ Suppression de l'ancienne table si elle existe...")
        cur.execute("DROP TABLE IF EXISTS aml_cft_ratings CASCADE")
        conn.commit()
        
        # Création de la table
        print("📝 Création de la table...")
        
        # Générer la structure de la table à partir du DataFrame
        columns = df.columns.tolist()
        column_defs = []
        for col in columns:
            if col in numeric_cols:
                column_defs.append(f'"{col}" FLOAT')
            elif col == 'risk_level':
                column_defs.append(f'"{col}" VARCHAR(20)')
            elif col == 'continent':
                column_defs.append(f'"{col}" VARCHAR(50)')
            else:
                column_defs.append(f'"{col}" TEXT')
        
        create_table_sql = f"""
        CREATE TABLE aml_cft_ratings (
            {', '.join(column_defs)}
        )
        """
        cur.execute(create_table_sql)
        conn.commit()
        print("   - Table créée")
        
        # Insertion des données avec execute_values (plus rapide)
        print("💾 Insertion des données...")
        
        # Convertir le DataFrame en liste de tuples
        data_tuples = [tuple(x) for x in df.to_numpy()]
        
        # Préparer la requête d'insertion
        columns_str = ', '.join([f'"{col}"' for col in columns])
        placeholders = '%s' * len(columns)
        placeholders = ', '.join(['%s'] * len(columns))
        
        insert_sql = f'INSERT INTO aml_cft_ratings ({columns_str}) VALUES %s'
        
        # Insertion par lots de 500 lignes
        batch_size = 500
        for i in range(0, len(data_tuples), batch_size):
            batch = data_tuples[i:i+batch_size]
            execute_values(cur, insert_sql, batch)
            conn.commit()
            print(f"   - {min(i+batch_size, len(data_tuples))}/{len(data_tuples)} lignes insérées")
        
        cur.close()
        conn.close()
        
        print(f"✅ {len(df)} lignes insérées dans la table 'aml_cft_ratings'")
        
        # Vérification avec SQLAlchemy pour la lecture (optionnel)
        from sqlalchemy import create_engine
        engine = create_engine(DB_URI)
        
        with engine.connect() as check_conn:
            count = check_conn.execute(text("SELECT COUNT(*) FROM aml_cft_ratings")).fetchone()[0]
            print(f"✅ Vérification: {count} lignes dans la table")
            
            # Afficher un aperçu
            sample = check_conn.execute(text("SELECT country, aml_score, risk_level FROM aml_cft_ratings LIMIT 5")).fetchall()
            print("\n📊 Aperçu des données:")
            for row in sample:
                print(f"   - {row[0]}: score={row[1]:.2f}, risque={row[2]}")
            
            # Statistiques par continent
            stats = check_conn.execute(text("""
                SELECT continent, COUNT(*) as nb, AVG(aml_score) as avg_score
                FROM aml_cft_ratings 
                WHERE continent IS NOT NULL AND continent != '0'
                GROUP BY continent
                ORDER BY avg_score DESC
            """)).fetchall()
            
            if stats:
                print("\n📊 Statistiques par continent:")
                for stat in stats:
                    print(f"   - {stat[0]}: {stat[1]} pays, score moyen={stat[2]:.2f}")

        print("✅ SUCCÈS - Chargement PostgreSQL terminé")
        
    except Exception as e:
        print(f"❌ ERREUR dans load_postgres: {str(e)}")
        print(traceback.format_exc())
        raise

# ======================
# Tâche 5: Vérification PostgreSQL
# ======================
def verify_postgres():
    """Vérifie que les données ont bien été chargées"""
    try:
        print("🔍 Vérification des données...")
        engine = create_engine(DB_URI)
        
        with engine.connect() as conn:
            # Vérifier si la table existe
            table_exists = conn.execute(text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'aml_cft_ratings'
                )
            """)).fetchone()[0]
            
            if not table_exists:
                print("❌ La table 'aml_cft_ratings' n'existe pas!")
                return
            
            # Statistiques générales
            stats = conn.execute(text("""
                SELECT 
                    COUNT(*) as total_pays,
                    COUNT(DISTINCT continent) as nb_continents,
                    AVG(aml_score) as score_moyen,
                    MIN(aml_score) as score_min,
                    MAX(aml_score) as score_max
                FROM aml_cft_ratings
            """)).fetchone()
            
            print(f"\n📊 Statistiques générales:")
            print(f"   - Total pays: {stats[0]}")
            print(f"   - Nombre de continents: {stats[1]}")
            print(f"   - Score AML moyen: {stats[2]:.2f}")
            print(f"   - Score min: {stats[3]:.2f}")
            print(f"   - Score max: {stats[4]:.2f}")
            
            # Distribution des risques
            risks = conn.execute(text("""
                SELECT risk_level, COUNT(*) as nb
                FROM aml_cft_ratings
                WHERE risk_level IS NOT NULL
                GROUP BY risk_level
                ORDER BY 
                    CASE risk_level
                        WHEN 'HIGH' THEN 1
                        WHEN 'MEDIUM' THEN 2
                        WHEN 'LOW' THEN 3
                        WHEN 'COMPLIANT' THEN 4
                        ELSE 5
                    END
            """)).fetchall()
            
            print(f"\n⚠️ Distribution des niveaux de risque:")
            for risk in risks:
                print(f"   - {risk[0]}: {risk[1]} pays ({risk[1]/stats[0]*100:.1f}%)")
                
        print("✅ Vérification terminée")
        
    except Exception as e:
        print(f"❌ Erreur vérification: {e}")
        raise

# ======================
# Définition du DAG
# ======================
with DAG(
    dag_id="fatf_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["fatf", "etl", "aml"],
    description="Pipeline ETL pour les données FATF"
) as dag:

    t1 = PythonOperator(
        task_id="load_data", 
        python_callable=load_data
    )
    
    t2 = PythonOperator(
        task_id="transform_data", 
        python_callable=transform_data
    )
    
    t3 = PythonOperator(
        task_id="add_continent", 
        python_callable=add_continent
    )
    
    t4 = PythonOperator(
        task_id="load_postgres", 
        python_callable=load_postgres
    )
    
    t5 = PythonOperator(
        task_id="verify_postgres",
        python_callable=verify_postgres
    )

    # Ordre d'exécution
    t1 >> t2 >> t3 >> t4 >> t5