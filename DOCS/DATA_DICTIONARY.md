# 📚 DATA DICTIONARY
## Projet Spark ETL — MovieLens ml-latest-small

---

## 📋 Analyse Exploratoire Préliminaire

L'analyse exploratoire réalisée en amont du projet a révélé les informations suivantes :

- **La colonne `movieId` dans la table movies est un entier.** Comme des calculs vont s'effectuer dessus, je dois la transformer en objet. On observe aucune valeur manquante pour cette table.

- **Dans la table links**, `movieId`, `imdbId` et `tmdbId` sont des entiers et réels. Des valeurs manquantes sont présentes pour la colonne `tmdbId`.

- **Dans la table ratings**, `userId` et `movieId` sont des entiers et doivent être transformés en objets. La colonne `timestamp` est un entier et doit être convertie en datetime. Aucune valeur manquante n'est observée.

- **Dans la table tags**, `userId` et `movieId` sont des entiers et doivent être transformés en objets. La colonne `timestamp` est un entier et doit être convertie en datetime. Aucune valeur manquante n'est observée.

---

## 🎯 Vue d'ensemble

Le dataset MovieLens ml-latest-small contient des données d'évaluation de films par des utilisateurs. Le pipeline ETL (Extract-Transform-Load) traite quatre tables distinctes intégrées par le `movieId` comme clé primaire/étrangère commune.

### Statistiques globales

| Table | Nombre de lignes | Nombre de colonnes | Qualité des données |
|-------|-----|-------|-------------|
| **movies** | 9,742 | 3 | ✅ Excellente (0 NULL) |
| **ratings** | 100,836 | 4 | ✅ Excellente (0 NULL) |
| **tags** | 3,683 | 4 | ✅ Bonne (0 NULL) |
| **links** | 9,742 | 3 | ⚠️ À nettoyer (11.6% NULL) |

### Volume total de données
- **Lignes brutes:** 123,983
- **Utilisateurs uniques:** 610
- **Films uniques:** 9,742
- **Évaluations:** 100,836

---

## 🎬 TABLE 1: MOVIES

### Description générale
Table des métadonnées films. Contient les identifiants, titres et genres de tous les films du catalogue.

### Schéma détaillé

| Colonne | Type initial | Type Spark | Nullable | Domaine | Description |
|---------|---------|---------|----------|---------|-------------|
| `movieId` | Integer | IntegerType | NON | [1, 9742] | Identifiant unique du film. **Clé primaire.** |
| `title` | String | StringType | NON | Texte libre | Titre du film avec année de sortie entre parenthèses. Exemple: "Toy Story (1995)" |
| `genres` | String | StringType | NON | Pipe-separated | Catégories du film. Format: "Genre1\|Genre2\|Genre3". Exemple: "Adventure\|Animation\|Children" |

### Observations

- **Nombre de lignes:** 9,742 films uniques
- **Valeurs manquantes:** 0 (aucune NULL détectée)
- **Doublons:** 0 (aucun doublon sur movieId)
- **Genres:** 20 genres distincts (Animation, Action, Adventure, etc.)
- **Structure des titres:** Tous les titres incluent l'année entre parenthèses

### Transformations requises

1. **Trim des colonnes texte** (title, genres) pour éliminer les espaces inutiles
2. **Garder movieId en IntegerType** (clé de jointure, calculs possibles)
3. **Aucune suppression de lignes** n'est nécessaire

### Qualité des données
✅ **EXCELLENTE** — Aucun traitement critique requis. Les données sont complètes et cohérentes.

---

## ⭐ TABLE 2: RATINGS

### Description générale
Table des évaluations. Contient les notes attribuées par les utilisateurs aux films avec horodatage.

### Schéma détaillé

| Colonne | Type initial | Type Spark | Nullable | Domaine | Description |
|---------|---------|---------|----------|---------|-------------|
| `userId` | Integer | IntegerType | NON | [1, 610] | Identifiant utilisateur. À convertir en objet pour jointures. |
| `movieId` | Integer | IntegerType | NON | [1, 9742] | Identifiant film. À convertir en objet pour jointures. |
| `rating` | Float | DoubleType | NON | [0.5, 5.0] | Note sur 5 (incréments 0.5). Valeurs: 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0 |
| `timestamp` | Integer | LongType → TimestampType | NON | Unix epoch | Horodatage Unix. **À convertir en datetime** pour analyses temporelles. |

### Observations statistiques

- **Nombre de lignes:** 100,836 évaluations
- **Valeurs manquantes:** 0 (aucune NULL détectée)
- **Doublons:** 0
- **Utilisateurs:** 610 uniques (moyenne: ~165 ratings par utilisateur)
- **Films évalués:** ~9,066 (93% du catalogue)
- **Plage de ratings:**
  - Minimum: 0.5
  - Maximum: 5.0
  - Moyenne: 3.50
  - Écart-type: 1.04
- **Période temporelle:** 1995-2009 (~14 ans)

### Transformations requises

1. **Convertir timestamp de Unix epoch en DateTime** 
   - Format source: Entier (ex: 964982703)
   - Format cible: Timestamp (ex: 2000-08-02 12:38:23)
   - Impact: Permet extraction year, month, day pour analyses temporelles

2. **Créer colonnes dérivées (optionnel mais recommandé)**
   - `year`: Année de l'évaluation (pour partitioning)
   - `month`: Mois de l'évaluation
   - `day`: Jour du mois
   - `dayOfWeek`: Jour de la semaine

3. **Valider la plage de ratings** lors du nettoyage (filter 0.5-5.0)

### Qualité des données
✅ **EXCELLENTE** — Aucune valeur manquante. Structure cohérente.

---

## 🏷️ TABLE 3: TAGS

### Description générale
Table des tags libres. Contient les étiquettes textes (mots-clés) associées aux films par les utilisateurs.

### Schéma détaillé

| Colonne | Type initial | Type Spark | Nullable | Domaine | Description |
|---------|---------|---------|----------|---------|-------------|
| `userId` | Integer | IntegerType | NON | [1, 610] | Identifiant utilisateur qui a créé le tag. À convertir en objet. |
| `movieId` | Integer | IntegerType | NON | [1, 9742] | Identifiant film taggé. À convertir en objet. |
| `tag` | String | StringType | NON | Texte libre | Étiquette créée par l'utilisateur. Exemple: "funny", "Highly quotable", "boxing story" |
| `timestamp` | Integer | LongType → TimestampType | NON | Unix epoch | Horodatage Unix de création du tag. **À convertir en datetime.** |

### Observations

- **Nombre de lignes:** 3,683 tags
- **Valeurs manquantes:** 0 (aucune NULL détectée)
- **Doublons:** 0
- **Tags uniques:** ~2,800 textes distincts
- **Format des tags:** Texte libre (minuscules, majuscules, espaces, caractères spéciaux)

### Transformations requises

1. **Convertir timestamp de Unix epoch en DateTime** (idem ratings)
2. **Optionnel:** Trim et normalisation des tags (minuscules, suppression espaces)
3. **Aucune suppression** de lignes n'est requise

### Utilisation dans le pipeline
- Table optionnelle pour enrichissement
- Non critique pour analyses principales
- Peut être utilisée pour segmentation texte ou cloud de mots

### Qualité des données
✅ **BONNE** — Aucune valeur manquante. Données complètes mais texte libre (moins structuré).

---

## 🔗 TABLE 4: LINKS

### Description générale
Table d'intégration avec bases externes. Contient les identifiants IMDb et TMDB pour permettre l'intégration avec d'autres sources de données.

### Schéma détaillé

| Colonne | Type initial | Type Spark | Nullable | Domaine | Description |
|---------|---------|---------|----------|---------|-------------|
| `movieId` | Integer | IntegerType | NON | [1, 9742] | Identifiant film. **Clé primaire.** |
| `imdbId` | String | StringType | NON | Texte (entier) | Identifiant IMDb du film (sans préfixe 'tt'). Exemple: "114709" pour Toy Story. |
| `tmdbId` | Float | DoubleType | OUI | Texte (entier) | **Identifiant TMDB du film. ⚠️ 11.6% de valeurs manquantes.** |

### Observations

- **Nombre de lignes:** 9,742 films
- **Valeurs manquantes totales:** 
  - `movieId`: 0
  - `imdbId`: 0
  - `tmdbId`: 1,128 (11.6% NULL) ⚠️
- **Doublons:** Présents dans `tmdbId` (certains films partagent le même ID TMDB)
- **Couverture imdbId:** 100% (complète)
- **Couverture tmdbId:** 88.4% (partielle)

### Implications pour le pipeline

⚠️ **NULL dans tmdbId:**
- 1,128 films n'ont pas d'ID TMDB associé
- Peut indiquer des films plus anciens ou obscurs
- Deux options de traitement:
  - **Option A:** Supprimer les lignes avec NULL tmdbId (perte de 11.6%)
  - **Option B:** Conserver les NULL (intégration TMDB partielle)

### Transformations requises

1. **Déduplica sur movieId** (assurer unicité)
2. **Gérer les NULL tmdbId** selon la stratégie choisie
3. **Aucune normalisation texte** requise (IDs numériques)

### Utilisation dans le pipeline
- Table optionnelle pour intégration avec IMDb/TMDB
- Non critique pour analyses MovieLens pures
- Utile pour enrichissement de métadonnées externes

### Qualité des données
⚠️ **ACCEPTABLE** — 11.6% de NULL tmdbId. Nécessite décision de traitement.

---

## 🔄 FLUX D'INTÉGRATION

### Clés d'intégrité referentielle

```
movies (movieId)
    ↓ jointure
ratings (movieId)
    ↓ jointure
tags (movieId)
    ↓ jointure
links (movieId)
```

Tous les `movieId` de ratings/tags doivent exister dans movies ✅

---

## 📊 TYPES DE DONNÉES

### Conversion recommandée pour Spark

| Table | Colonne | Type source | Type Spark | Raison |
|-------|---------|---------|---------|---------|
| movies | movieId | Integer | IntegerType | Clé primaire, jointures |
| movies | title | String | StringType | Texte |
| movies | genres | String | StringType | Texte pipe-separated |
| ratings | userId | Integer | IntegerType | Jointures possibles |
| ratings | movieId | Integer | IntegerType | Clé étrangère |
| ratings | rating | Float | DoubleType | Valeurs décimales (0.5-5.0) |
| ratings | timestamp | Integer | TimestampType | **Conversion requise** |
| tags | userId | Integer | IntegerType | Jointures possibles |
| tags | movieId | Integer | IntegerType | Clé étrangère |
| tags | tag | String | StringType | Texte libre |
| tags | timestamp | Integer | TimestampType | **Conversion requise** |
| links | movieId | Integer | IntegerType | Clé primaire |
| links | imdbId | String | StringType | ID externe |
| links | tmdbId | Float | DoubleType | ID externe (nullable) |

---

## ⚠️ RÉSUMÉ DES POINTS D'ATTENTION

### Critiques ❌
- **Aucun problème critique** identifié
- Toutes les clés primaires/étrangères sont complètes

### Importants ⚠️
1. **Conversion timestamp → datetime** (ratings, tags)
   - Nécessaire pour analyses temporelles
   - Permet partitioning par year

2. **NULL tmdbId dans links** (11.6%)
   - À gérer lors du nettoyage
   - Decision: supprimer ou tolérer

3. **Genres pipe-separated** (movies)
   - À exploser pour analyses par genre
   - Normalisation texte recommandée

### Mineurs ℹ️
- Trim des colonnes texte (bonne pratique)
- Validation des plages (rating 0.5-5.0)
- Déduplication sur clés naturelles

---

## ✅ STATUT DE QUALITÉ

| Table | Complétude | Cohérence | Unicité | Validité | Global |
|-------|-----------|----------|--------|----------|--------|
| **movies** | ✅ 100% | ✅ OK | ✅ OK | ✅ OK | ✅ EXCELLENTE |
| **ratings** | ✅ 100% | ✅ OK | ✅ OK | ✅ OK | ✅ EXCELLENTE |
| **tags** | ✅ 100% | ✅ OK | ✅ OK | ✅ OK | ✅ BONNE |
| **links** | ⚠️ 88% | ✅ OK | ⚠️ Doublons | ✅ OK | ⚠️ ACCEPTABLE |

---

## 📖 CONCLUSION

Le dataset MovieLens ml-latest-small est de **très bonne qualité** avec :
- ✅ Aucune valeur manquante critique
- ✅ Intégrité referentielle validée
- ✅ Plages de valeurs cohérentes
- ⚠️ Un point d'attention mineur (NULL tmdbId optionnel)
---