
# Projet Lakehouse – Suivi de la Qualité de l'Eau (LTM)

## Objectif

Concevoir une architecture **Lakehouse** pour intégrer, transformer, modéliser et analyser des données issues du programme **Long Term Monitoring (LTM)** portant sur la qualité de l’eau dans plusieurs états américains.

---

## Données Sources

Trois fichiers CSV fournis par un organisme environnemental :

- `Site_Information_2022_8_1.csv` : Coordonnées et informations sur les sites de prélèvement.
- `Methods_2022_8_1.csv` : Méthodologies d’analyse des paramètres.
- `LTM_Data_2022_8_1.csv` : Mesures sur le terrain.

---

## Architecture du Projet

Le projet repose sur une architecture **Lakehouse** à trois couches :

### Bronze (Staging)
- Ingestion brute des fichiers CSV.
- Détection du schéma et encodage.

### Silver (Curated Layer)
- Nettoyage des données (valeurs aberrantes, types, formats).
- Transformation en format long.
- Gestion des Slowly Changing Dimensions (SCD) :
  - **Type 1** : pour les dimensions stables (`site`).
  - **Type 2** : pour les dimensions avec historique (`method`).

### Gold (Analytique)
- Table de faits finale prête pour l’analyse.
- Jointures enrichies avec les dimensions.
- Format long optimisé pour Power BI.

---

## ⚙Traitements PySpark

### 1. Nettoyage des Données

```python
valeurs_aberrantes = ["", "NA", "null", "-1"]
for col in df.columns:
    df = df.withColumn(col, F.when(F.col(col).isin(valeurs_aberrantes), None).otherwise(F.col(col)))
```

- Conversion des types (`float`, `int`, `timestamp`)
- Uniformisation des noms via `regexp_replace`

---

### 2. Transformation en Format Long

```python
param_cols = [...]  # liste des colonnes métriques
df_long = df.selectExpr("SITE_ID", "PROGRAM_ID", ..., f"stack({len(param_cols)}, ...)")
```

---

### 3. Gestion des SCD

```python
window = Window.partitionBy("PROGRAM_ID", "PARAMETER").orderBy(F.desc("END_YEAR"))
df_method_scd1 = df_method.withColumn("row_num", F.row_number().over(window)).filter("row_num = 1")
```

---

### 4. Jointures Dimensionnelles

```python
df_joint = df_fait_long.join(df_method, on=["PROGRAM_ID", "PARAMETER"], how="left")
df_joint = df_joint.join(df_site.drop("PROGRAM_ID"), on="SITE_ID", how="left")
```

---

### 5. Enregistrement de la Table Finale

```python
df_gold.write.format("delta").mode("overwrite").saveAsTable("gold_water_quality")
```

---

## Tables & Types de SCD

| Table                     | Type        | Description                                           |
|--------------------------|-------------|-------------------------------------------------------|
| `silver_dim_site`        | Dimension 1 | Informations sur les sites                            |
| `silver_dim_method_scd1` | Dimension 1 | Dernière méthode connue par paramètre                 |
| `silver_dim_method_scd2` | Dimension 2 | Historique des méthodes                               |
| `silver_ltm_data_scd1`   | Faits SCD1  | Dernières mesures                                     |
| `silver_ltm_data_scd2`   | Faits SCD2  | Historique complet                                    |
| `gold_water_quality`     | Faits       | Table de faits enrichie pour analyse finale           |

---

##  Visualisations Power BI

1. **Carte des Mesures par Site**
   - Données : `LATDD`, `LONDD`, `COUNT(VALUE)`
   - Objectif : Visualiser la répartition géographique.

2. **Histogramme - Nombre de Mesures par Paramètre**
   - Données : `PARAMETER`, `COUNT(VALUE)`
   - Objectif : Identifier les paramètres les plus surveillés.

3. **Évolution Annuelle des Mesures**
   - Données : `YEAR`, `COUNT(VALUE)`
   - Objectif : Suivre les tendances annuelles de surveillance.


---

## Rendu Final

- Rapport PDF
- Table Delta finale : `gold_water_quality`
  Rapport Power BI
- Repository Git contenant tous les scripts PySpark

---

## Conclusion

Ce projet inclut toutes les étapes nécessaires à la construction d’un pipeline analytique moderne :

- Intégration multi-source
- Architecture Lakehouse robuste
- Modèle en étoile avec gestion des dimensions
- 🕰Implémentation des SCD Types 1 et 2
- Visualisations orientées décision environnementale
