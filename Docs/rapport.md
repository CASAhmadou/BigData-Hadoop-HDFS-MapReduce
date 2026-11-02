# RAPPORT DE TRAVAUX PRATIQUES
## TP1 – Big Data : le traitement batch avec Hadoop HDFS et MapReduce

**Thème :** Hadoop, HDFS et MapReduce : WordCount, puis calcul du total des ventes par magasin
**Support du TP :** [TP1 – Big Data, INSAT](https://insatunisia.github.io/TP-BigData/tp1/)
**Environnement :** Windows 11, WSL2 Ubuntu 22.04 et Docker Desktop
**Langage de programmation :** Java 8
**Outil de construction :** Maven
**Framework Big Data :** Apache Hadoop 3.3.6

---

## Sommaire

1. Introduction
2. Objectifs du TP
3. Environnement technique
4. Mise en place du cluster Hadoop
5. Préparation des données dans HDFS
6. Création du projet Maven
7. Développement de l'application WordCount
8. Exécution locale de WordCount dans Visual Studio Code
9. Compilation et création du fichier JAR
10. Déploiement et exécution de WordCount sur le cluster
11. Interfaces Web de supervision Hadoop
12. Application : total des ventes par magasin
13. Difficultés rencontrées et solutions apportées
14. Bilan des travaux réalisés
15. Conclusion
16. Annexe : résultats complets du total des ventes par magasin

---

# 1. Introduction

Dans le cadre de ce TP, nous avons découvert et mis en pratique les principales fonctionnalités de l'écosystème Hadoop, notamment le système de fichiers distribué HDFS et le modèle de programmation MapReduce.

L'objectif principal était de comprendre comment Hadoop permet de traiter des données à grande échelle en répartissant les opérations sur plusieurs machines ou conteneurs.

Le TP s'est déroulé en deux temps :

1. Réalisation d'une première application MapReduce, **WordCount**, qui compte le nombre d'occurrences de chaque mot dans un fichier texte. Elle a été exécutée en local puis sur un cluster Hadoop distribué déployé avec Docker.
2. Réalisation de l'**application demandée** : un job MapReduce qui calcule, à partir du fichier `purchases.txt`, le **total des ventes par magasin**.

# 2. Objectifs du TP

Les objectifs de ce travail pratique étaient les suivants :

- Installer et configurer l'environnement de développement Java.
- Découvrir et utiliser Maven pour gérer un projet Java.
- Comprendre la structure d'un projet Hadoop MapReduce.
- Développer une application WordCount avec Java.
- Exécuter le programme localement depuis Visual Studio Code.
- Créer un fichier JAR contenant les dépendances nécessaires.
- Découvrir le fonctionnement de HDFS.
- Déployer et exécuter une application MapReduce sur un cluster Hadoop.
- Vérifier les résultats du traitement dans HDFS.
- Concevoir un job MapReduce personnalisé : le total des ventes par magasin.

# 3. Environnement technique

| Outil | Version / Description |
|---|---|
| Système principal | Windows 11 |
| Environnement Linux | WSL2 – Ubuntu 22.04.5 LTS |
| Java | OpenJDK 8 |
| Maven | Apache Maven 3.6.3 |
| Hadoop | Hadoop 3.3.6 (image `liliasfaxi/hadoop-cluster`) |
| Docker | Docker 29.9.2 |
| Docker Compose | 5.6.1 |
| Éditeur de code | Visual Studio Code |

## 3.1. Configuration de Java

Le TP impose Java 8, la version avec laquelle Hadoop est compilé. Nous avons donc installé et sélectionné OpenJDK 8 :

```bash
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export PATH="$JAVA_HOME/bin:$PATH"
java -version
```

Résultat obtenu :

```text
openjdk version "1.8.0_504"
OpenJDK Runtime Environment
OpenJDK 64-Bit Server VM
```

La commande `mvn -version` a confirmé l'utilisation de Maven 3.6.3 avec Java 8.

## 3.2. Configuration de Docker

Docker Desktop a été installé et configuré avec l'intégration WSL2. Nous avons vérifié son fonctionnement avec les commandes suivantes :

```bash
docker --version
docker compose version
docker run --rm hello-world
```

Le test `hello-world` a confirmé que Docker fonctionnait correctement.

# 4. Mise en place du cluster Hadoop

## 4.1. Récupération de l'image Hadoop

```bash
docker pull liliasfaxi/hadoop-cluster:latest
```

## 4.2. Création du réseau Docker

Le réseau `hadoop` permet aux conteneurs de communiquer entre eux :

```bash
docker network create --driver=bridge hadoop
```

## 4.3. Démarrage des conteneurs

Le cluster est constitué de trois conteneurs :

- `hadoop-master` : nœud principal (NameNode, ResourceManager).
- `hadoop-worker1` : premier nœud de travail (DataNode, NodeManager).
- `hadoop-worker2` : deuxième nœud de travail (DataNode, NodeManager).

```bash
docker run -itd --net=hadoop -p 9870:9870 -p 8088:8088 -p 7077:7077 -p 16010:16010 \
  --name hadoop-master --hostname hadoop-master liliasfaxi/hadoop-cluster:latest

docker run -itd -p 8040:8042 --net=hadoop \
  --name hadoop-worker1 --hostname hadoop-worker1 liliasfaxi/hadoop-cluster:latest

docker run -itd -p 8041:8042 --net=hadoop \
  --name hadoop-worker2 --hostname hadoop-worker2 liliasfaxi/hadoop-cluster:latest
```

La commande `docker ps` a affiché les trois conteneurs avec le statut `Up`.

## 4.4. Démarrage des services Hadoop

```bash
docker exec -it hadoop-master bash
./start-hadoop.sh
```

Cette commande démarre les services suivants : NameNode, DataNodes, Secondary NameNode, ResourceManager et NodeManagers. Le cluster Hadoop était alors opérationnel.

# 5. Préparation des données dans HDFS

HDFS (Hadoop Distributed File System) est le système de fichiers distribué de Hadoop. Il découpe les fichiers en blocs et les répartit, avec réplication, sur les DataNodes du cluster.

Dans le conteneur `hadoop-master` :

```bash
hdfs dfs -mkdir -p input
hdfs dfs -put purchases.txt input
hdfs dfs -ls input
hdfs dfs -tail input/purchases.txt
```

Le fichier `input/purchases.txt` (environ 211 Mo) était bien présent dans HDFS. Chaque ligne décrit un achat, avec six champs séparés par des **tabulations** :

```text
date    heure    magasin    produit    coût    paiement
```

Exemple de ligne :

```text
2012-01-01	09:00	San Jose	Men's Clothing	214.05	Amex
```

Principales commandes HDFS utilisées :

| Commande | Rôle |
|---|---|
| `hdfs dfs -ls` | Lister le contenu d'un répertoire |
| `hdfs dfs -mkdir` | Créer un répertoire |
| `hdfs dfs -put` | Copier un fichier local vers HDFS |
| `hdfs dfs -get` | Copier un fichier HDFS vers le disque local |
| `hdfs dfs -cat` / `-tail` | Afficher tout le contenu / la fin d'un fichier |
| `hdfs dfs -rm -r` | Supprimer un fichier ou un répertoire |

# 6. Création du projet Maven

## 6.1. Structure du projet

Le projet `wordcount` regroupe les deux applications du TP :

```text
wordcount/
├── pom.xml
├── .vscode/
│   └── launch.json
└── src/
    └── main/
        ├── java/
        │   └── hadoop/mapreduce/tp1/
        │       ├── TokenizerMapper.java     (WordCount)
        │       ├── IntSumReducer.java       (WordCount)
        │       ├── WordCount.java           (WordCount)
        │       ├── SalesMapper.java         (Ventes par magasin)
        │       ├── SalesCombiner.java       (Ventes par magasin)
        │       ├── SalesReducer.java        (Ventes par magasin)
        │       └── SalesByStore.java        (Ventes par magasin)
        └── resources/
            ├── input/
            │   ├── file.txt
            │   └── purchases_sample.txt
            ├── output/
            └── output_sales/
```

## 6.2. Configuration du fichier pom.xml

Le fichier `pom.xml` déclare les dépendances Hadoop 3.3.6 :

- `hadoop-common`
- `hadoop-mapreduce-client-core`
- `hadoop-hdfs`
- `hadoop-mapreduce-client-common`
- `hadoop-mapreduce-client-jobclient`

Le compilateur est configuré en Java 8 (`maven.compiler.source` et `maven.compiler.target` à 8).

Le plugin **maven-assembly-plugin** produit un JAR « fat » (`jar-with-dependencies`) qui contient le code et toutes ses dépendances. La classe principale est déclarée dans le manifeste :

```xml
<plugin>
  <groupId>org.apache.maven.plugins</groupId>
  <artifactId>maven-assembly-plugin</artifactId>
  <version>3.6.0</version>
  <configuration>
    <archive>
      <manifest>
        <mainClass>hadoop.mapreduce.tp1.WordCount</mainClass>
      </manifest>
    </archive>
    <descriptorRefs>
      <descriptorRef>jar-with-dependencies</descriptorRef>
    </descriptorRefs>
  </configuration>
  <executions>
    <execution>
      <id>make-assembly</id>
      <phase>package</phase>
      <goals><goal>single</goal></goals>
    </execution>
  </executions>
</plugin>
```

Pour la seconde application, `mainClass` a été remplacé par `hadoop.mapreduce.tp1.SalesByStore` (voir section 12.5).

# 7. Développement de l'application WordCount

Un job MapReduce se compose de deux types de programmes :

- **Mapper** : extrait les données utiles sous forme de paires clé/valeur.
- **Reducer** : reçoit, pour chaque clé, la liste de ses valeurs (triées et regroupées par Hadoop lors de la phase *shuffle & sort*) et applique le traitement voulu : somme, moyenne, maximum, etc.

## 7.1. Le Mapper : TokenizerMapper.java

Le Mapper lit chaque ligne et la découpe en mots avec `StringTokenizer`. Pour chaque mot, il émet la paire `(mot, 1)`.

Pour la ligne `Hello Hadoop!`, il produit `(Hello, 1)` et `(Hadoop!, 1)`.

## 7.2. Le Reducer : IntSumReducer.java

Le Reducer additionne les valeurs associées à chaque mot. Par exemple, `Hello : [1, 1]` devient `Hello : 2`.

Ce même Reducer est aussi utilisé comme **Combiner**. Comme l'addition est associative et commutative, on peut pré-agréger côté Mapper sans fausser le résultat.

## 7.3. La classe principale : WordCount.java

Elle charge la configuration Hadoop, crée le `Job`, déclare le Mapper, le Combiner et le Reducer, les types de sortie (`Text`, `IntWritable`) ainsi que les chemins d'entrée et de sortie (`args[0]`, `args[1]`), puis lance le traitement avec `job.waitForCompletion(true)`.

# 8. Exécution locale de WordCount dans Visual Studio Code

## 8.1. Préparation du fichier d'entrée

Fichier `src/main/resources/input/file.txt` :

```text
Hello Wordcount!
Hello Hadoop!
```

## 8.2. Configuration du lancement

Dans `.vscode/launch.json`, nous avons créé une configuration `WordCount` (classe principale `hadoop.mapreduce.tp1.WordCount`) avec les arguments suivants :

```json
"args": [
    "src/main/resources/input/file.txt",
    "src/main/resources/output"
]
```

## 8.3. Résultat obtenu

Après un lancement avec F5, le fichier `src/main/resources/output/part-r-00000` contenait :

```text
Hadoop!       1
Hello         2
Wordcount!    1
```

Le programme compte donc correctement les occurrences de chaque mot.

# 9. Compilation et création du fichier JAR

Dans le terminal Ubuntu (WSL) :

```bash
cd ~/tp-bigdata/wordcount
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export PATH="$JAVA_HOME/bin:$PATH"
mvn clean package
```

Résultat : `BUILD SUCCESS`. Le JAR avec dépendances a été généré :

```text
target/wordcount-1.0-SNAPSHOT-jar-with-dependencies.jar
```

# 10. Déploiement et exécution de WordCount sur le cluster

```bash
# Depuis WSL : copie du JAR dans le conteneur maître
docker cp target/wordcount-1.0-SNAPSHOT-jar-with-dependencies.jar hadoop-master:/root/wordcount.jar

# Dans le conteneur hadoop-master
docker exec -it hadoop-master bash
hdfs dfs -ls input
hadoop jar wordcount.jar input output
hdfs dfs -ls output
hdfs dfs -tail output/part-r-00000
```

Hadoop a pris en charge la lecture des données depuis HDFS, leur découpage en *splits*, leur traitement par les Mappers, le tri et le regroupement par clé, puis l'agrégation par les Reducers. Le résultat a été écrit dans HDFS, dans le répertoire `output`.

# 11. Interfaces Web de supervision Hadoop

| Interface | URL | Rôle |
|---|---|---|
| NameNode | http://localhost:9870 | État de HDFS, fichiers stockés, DataNodes |
| ResourceManager (YARN) | http://localhost:8088 | Suivi des applications et des jobs MapReduce |
| NodeManager worker1 / worker2 | http://localhost:8040 / http://localhost:8041 | Activité des nœuds de travail |

Les jobs `word count` et `sales by store` apparaissent dans la liste des applications du ResourceManager.

# 12. Application : total des ventes par magasin

## 12.1. Énoncé

> Écrire un job MapReduce permettant, à partir du fichier *purchases* initial, de déterminer le total des ventes par magasin.

## 12.2. Analyse du problème

Il s'agit d'une agrégation de type `GROUP BY magasin, SUM(coût)`, que l'on décompose en MapReduce de la façon suivante :

| Étape | Entrée | Sortie |
|---|---|---|
| Map | une ligne de `purchases.txt` | `(magasin, coût)` |
| Combine | `(magasin, [coûts partiels])` | `(magasin, somme partielle)` |
| Shuffle & Sort | — | regroupement par magasin |
| Reduce | `(magasin, [sommes partielles])` | `(magasin, total)` |

Trois différences importantes par rapport à WordCount :

1. **Découpage sur la tabulation**, et non sur les espaces. Plusieurs noms de magasins contiennent des espaces (« San Jose », « New York », « Baton Rouge »…). Un découpage par `StringTokenizer` les aurait coupés et aurait décalé les colonnes.
2. **Valeurs décimales** : le coût est un réel, on utilise donc `DoubleWritable` au lieu d'`IntWritable`.
3. **Types de sortie différents entre Map et Reduce** : le Reducer écrit le total formaté en `Text` (2 décimales), alors que le Mapper émet des `DoubleWritable`. Il faut donc déclarer explicitement les types intermédiaires, et utiliser un Combiner distinct du Reducer.

## 12.3. Le Mapper : SalesMapper.java

```java
package hadoop.mapreduce.tp1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SalesMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private final Text store = new Text();
    private final DoubleWritable cost = new DoubleWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // Découpage sur la tabulation : les noms de magasins contiennent des espaces
        String[] fields = value.toString().split("\t");

        // On ignore les lignes mal formées
        if (fields.length != 6) {
            return;
        }

        try {
            store.set(fields[2].trim());
            cost.set(Double.parseDouble(fields[4].trim()));
            context.write(store, cost);
        } catch (NumberFormatException e) {
            // Montant illisible : ligne ignorée
        }
    }
}
```

Le Mapper est **robuste aux données sales** : une ligne qui n'a pas exactement six champs, ou dont le montant n'est pas numérique, est ignorée au lieu de faire échouer toute la tâche.

## 12.4. Le Combiner et le Reducer

**SalesCombiner.java** : somme partielle côté Mapper. Il réduit fortement le volume de données transférées pendant le *shuffle*, puisqu'on passe d'environ 4 millions de paires à une paire par magasin et par split.

```java
public class SalesCombiner extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    private final DoubleWritable result = new DoubleWritable();

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
        double sum = 0;
        for (DoubleWritable val : values) {
            sum += val.get();
        }
        result.set(sum);
        context.write(key, result);
    }
}
```

**SalesReducer.java** : somme finale, écrite avec deux décimales. `Locale.US` garantit le point comme séparateur décimal et évite la notation scientifique (`1.0E7`) que produirait `DoubleWritable.toString()` pour des montants de l'ordre de 10 millions.

```java
public class SalesReducer extends Reducer<Text, DoubleWritable, Text, Text> {

    private final Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
        double sum = 0;
        for (DoubleWritable val : values) {
            sum += val.get();
        }
        result.set(String.format(Locale.US, "%.2f", sum));
        context.write(key, result);
    }
}
```

## 12.5. La classe principale : SalesByStore.java

```java
public class SalesByStore {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: SalesByStore <input> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "sales by store");
        job.setJarByClass(SalesByStore.class);

        job.setMapperClass(SalesMapper.class);
        job.setCombinerClass(SalesCombiner.class);
        job.setReducerClass(SalesReducer.class);

        // Types intermédiaires (sortie du Mapper)
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        // Types de sortie finale (Reducer)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
```

Dans `pom.xml`, la classe principale du plugin assembly a été modifiée pour pointer sur ce job :

```xml
<mainClass>hadoop.mapreduce.tp1.SalesByStore</mainClass>
```

## 12.6. Test en local

Conformément à la consigne (« toujours tester en local avant de lancer un job sur le cluster »), un échantillon de 8 lignes a été placé dans `src/main/resources/input/purchases_sample.txt` :

```text
2012-01-01	09:00	San Jose	Men's Clothing	214.05	Amex
2012-01-01	09:00	Fort Worth	Women's Clothing	153.57	Visa
2012-01-01	09:00	San Diego	Music	66.08	Cash
2012-01-01	09:00	Pittsburgh	Pet Supplies	493.51	Discover
2012-01-01	09:00	Omaha	Children's Clothing	235.63	MasterCard
2012-01-01	09:00	San Jose	Cameras	396.84	Visa
2012-01-01	09:01	Fort Worth	Toys	32.21	Cash
2012-01-01	09:01	San Diego	Books	107.50	Amex
```

Configuration de lancement ajoutée dans `.vscode/launch.json` :

```json
{
  "type": "java",
  "name": "SalesByStore",
  "request": "launch",
  "mainClass": "hadoop.mapreduce.tp1.SalesByStore",
  "args": ["src/main/resources/input/purchases_sample.txt",
           "src/main/resources/output_sales"]
}
```

Résultat attendu (calculé à la main) dans `output_sales/part-r-00000` :

```text
Fort Worth	185.78
Omaha	235.63
Pittsburgh	493.51
San Diego	173.58
San Jose	610.89
```

Par exemple, San Jose = 214.05 + 396.84 = 610.89.

## 12.7. Exécution sur le cluster

```bash
# WSL : compilation et copie du JAR
mvn clean package
docker cp target/wordcount-1.0-SNAPSHOT-jar-with-dependencies.jar hadoop-master:/root/sales.jar

# Conteneur hadoop-master : exécution et lecture du résultat
docker exec -it hadoop-master bash
hadoop jar sales.jar input output_sales
hdfs dfs -cat output_sales/part-r-00000
```

Le job s'est exécuté avec succès sur l'intégralité du fichier `purchases.txt` stocké dans HDFS.

## 12.8. Résultats et analyse

Le fichier de sortie contient **103 magasins**, triés par ordre alphabétique : c'est l'effet de la phase *shuffle & sort*, qui trie les clés avant de les transmettre au Reducer.

**Statistiques globales :**

| Indicateur | Valeur |
|---|---|
| Nombre de magasins | 103 |
| Total des ventes (tous magasins) | 1 034 457 953,26 |
| Moyenne par magasin | 10 043 281,10 |
| Médiane | 10 047 052,76 (Hialeah) |
| Écart-type | 56 692,14 |
| Maximum | 10 190 080,26 (Philadelphia) |
| Minimum | 9 919 559,86 (Chandler) |

**Top 5 des magasins :**

| Rang | Magasin | Total des ventes |
|---|---|---|
| 1 | Philadelphia | 10 190 080,26 |
| 2 | Durham | 10 153 890,21 |
| 3 | Laredo | 10 144 604,98 |
| 4 | Newark | 10 144 052,80 |
| 5 | Cincinnati | 10 139 505,74 |

**Les 5 magasins les plus faibles :**

| Magasin | Total des ventes |
|---|---|
| San Jose | 9 936 721,41 |
| Seattle | 9 936 267,37 |
| Anchorage | 9 933 500,40 |
| Jersey City | 9 920 141,87 |
| Chandler | 9 919 559,86 |

**Interprétation :** les totaux sont remarquablement homogènes. Tous les magasins réalisent environ 10 millions de ventes, et l'écart entre le premier et le dernier n'est que d'environ 2,7 %. L'écart-type (≈ 56 700) représente à peine 0,6 % de la moyenne. Cette uniformité suggère que le jeu de données `purchases.txt` a été **généré de façon synthétique**, avec des magasins et des montants tirés aléatoirement de manière équiprobable : il est destiné à l'apprentissage et ne reflète pas un vrai réseau de magasins.

La liste complète des résultats figure en annexe (section 16).

# 13. Difficultés rencontrées et solutions apportées

## 13.1. Erreur de lecture du fichier pom.xml

Maven a signalé :

```text
Non-parseable POM
processing instruction can not have PITarget with reserved xml name
```

**Cause :** des caractères ou des espaces étaient présents avant la déclaration `<?xml ... ?>`.
**Solution :** la déclaration XML a été placée tout au début du fichier. La compilation a ensuite réussi.

## 13.2. Chemin du fichier d'entrée introuvable

Le programme cherchait `tp-bigdata/wordcount/wordcount/src/main/resources/input/file.txt` : le dossier `wordcount` était répété dans le chemin.

**Solution :** les chemins de `launch.json` ont été adaptés au répertoire de travail (`src/main/resources/...`).

## 13.3. Avertissements SLF4J et Log4j

Plusieurs bindings SLF4J ont été détectés et aucun appender Log4j n'était configuré. Ces avertissements sont sans conséquence sur l'exécution, qui a abouti normalement.

## 13.4. Commande `hdfs` introuvable

```text
Command 'hdfs' not found
```

**Cause :** la commande a été tapée dans le terminal Ubuntu (WSL), alors que Hadoop n'est installé que dans les conteneurs Docker.
**Solution :** entrer d'abord dans le conteneur maître avec `docker exec -it hadoop-master bash`. Nous avons retenu la répartition suivante :

| Terminal | Commandes |
|---|---|
| WSL (Ubuntu) | `mvn`, `rm`, `docker cp`, `docker exec` |
| Conteneur `hadoop-master` | `hdfs dfs ...`, `hadoop jar ...` |

## 13.5. Répertoire de sortie déjà existant

```text
org.apache.hadoop.mapred.FileAlreadyExistsException:
Output directory file:/home/.../src/main/resources/output already exists
```

**Cause :** Hadoop refuse d'écraser un répertoire de sortie existant. C'est une sécurité qui évite de perdre les résultats d'un job précédent.
**Solution :** supprimer le répertoire avant chaque relance, ou utiliser un nouveau nom de sortie :

```bash
rm -rf src/main/resources/output        # en local
hdfs dfs -rm -r output_sales            # dans HDFS
```

## 13.6. Choix de la classe exécutée par `hadoop jar`

**Problème :** le manifeste du JAR désignait `WordCount` comme classe principale. Or, quand le manifeste contient une `Main-Class`, `hadoop jar` l'exécute systématiquement et traite tout nom de classe ajouté dans la commande comme un simple argument.
**Solution :** modifier `<mainClass>` dans `pom.xml` pour pointer sur `hadoop.mapreduce.tp1.SalesByStore`, puis recompiler avec `mvn clean package`.

# 14. Bilan des travaux réalisés

| Opération | Résultat |
|---|---|
| Installation et configuration de Java 8 | Réussie |
| Installation et vérification de Maven | Réussie |
| Installation et vérification de Docker | Réussie |
| Mise en place du cluster Hadoop (1 master + 2 workers) | Réussie |
| Démarrage des services Hadoop | Réussi |
| Chargement de `purchases.txt` dans HDFS | Réussi |
| Création du projet Maven | Réussie |
| Développement et test local de WordCount | Réussis |
| Compilation Maven et création du JAR avec dépendances | Réussies |
| Exécution de WordCount sur le cluster | Réussie |
| Développement du job « total des ventes par magasin » | Réussi |
| Exécution du job sur le cluster (`purchases.txt` complet) | Réussie |
| Vérification des résultats dans HDFS (103 magasins) | Réussie |

# 15. Conclusion

Ce TP nous a permis de maîtriser le cycle complet d'une application MapReduce : conception, développement en Java, test en local, packaging avec Maven, déploiement et exécution sur un cluster Hadoop conteneurisé avec Docker, puis lecture des résultats dans HDFS.

WordCount nous a fait découvrir le rôle du Mapper, qui transforme les données, et celui du Reducer, qui les agrège. L'application « total des ventes par magasin » nous a ensuite amenés à adapter ce schéma à un cas réel. Nous avons dû découper des données tabulées, manipuler des valeurs décimales, déclarer des types intermédiaires distincts et utiliser un Combiner pour limiter les échanges réseau pendant le *shuffle*. Le job a traité les quelque 211 Mo du fichier `purchases.txt` et produit le chiffre d'affaires de chacun des 103 magasins, pour un total d'environ 1,03 milliard.

Les difficultés rencontrées (configuration du POM, chemins, distinction entre l'hôte et le conteneur, répertoire de sortie existant, classe principale du JAR) nous ont aussi permis de mieux comprendre le fonctionnement concret de Hadoop.

La prochaine étape est le **projet** du cours : choisir une source de données (par exemple un dataset Kaggle ou une source de streaming) et concevoir une architecture Big Data qui combine traitement batch, traitement en streaming et visualisation temps réel.

# 16. Annexe : résultats complets du total des ventes par magasin

Contenu de `output_sales/part-r-00000` (HDFS), obtenu avec `hdfs dfs -cat output_sales/part-r-00000` :

| # | Magasin | Total des ventes |
|---|---|---|
| 1 | Albuquerque | 10 052 311.42 |
| 2 | Anaheim | 10 076 416.36 |
| 3 | Anchorage | 9 933 500.40 |
| 4 | Arlington | 10 072 207.97 |
| 5 | Atlanta | 9 997 146.70 |
| 6 | Aurora | 9 992 970.92 |
| 7 | Austin | 10 057 158.90 |
| 8 | Bakersfield | 10 031 208.92 |
| 9 | Baltimore | 10 096 521.45 |
| 10 | Baton Rouge | 10 131 273.23 |
| 11 | Birmingham | 10 076 606.52 |
| 12 | Boise | 10 039 166.74 |
| 13 | Boston | 10 039 473.28 |
| 14 | Buffalo | 10 001 941.19 |
| 15 | Chandler | 9 919 559.86 |
| 16 | Charlotte | 10 112 531.34 |
| 17 | Chesapeake | 10 038 504.92 |
| 18 | Chicago | 10 062 522.07 |
| 19 | Chula Vista | 9 974 951.34 |
| 20 | Cincinnati | 10 139 505.74 |
| 21 | Cleveland | 10 067 835.84 |
| 22 | Colorado Springs | 10 061 105.87 |
| 23 | Columbus | 10 035 241.03 |
| 24 | Corpus Christi | 9 976 522.77 |
| 25 | Dallas | 10 066 548.45 |
| 26 | Denver | 10 031 534.87 |
| 27 | Detroit | 9 979 260.76 |
| 28 | Durham | 10 153 890.21 |
| 29 | El Paso | 10 016 409.97 |
| 30 | Fort Wayne | 10 132 594.02 |
| 31 | Fort Worth | 10 120 830.65 |
| 32 | Fremont | 10 053 242.36 |
| 33 | Fresno | 9 976 260.26 |
| 34 | Garland | 10 071 043.92 |
| 35 | Gilbert | 10 062 115.19 |
| 36 | Glendale | 10 044 493.97 |
| 37 | Greensboro | 10 033 781.39 |
| 38 | Henderson | 10 053 416.05 |
| 39 | Hialeah | 10 047 052.76 |
| 40 | Honolulu | 10 006 273.49 |
| 41 | Houston | 10 042 106.27 |
| 42 | Indianapolis | 10 090 272.77 |
| 43 | Irvine | 10 084 867.45 |
| 44 | Irving | 10 133 944.08 |
| 45 | Jacksonville | 10 072 003.33 |
| 46 | Jersey City | 9 920 141.87 |
| 47 | Kansas City | 9 968 118.73 |
| 48 | Laredo | 10 144 604.98 |
| 49 | Las Vegas | 10 054 257.98 |
| 50 | Lexington | 10 084 510.95 |
| 51 | Lincoln | 10 069 485.40 |
| 52 | Long Beach | 10 006 380.25 |
| 53 | Los Angeles | 10 084 576.80 |
| 54 | Louisville | 10 008 566.47 |
| 55 | Lubbock | 9 958 119.15 |
| 56 | Madison | 10 032 035.54 |
| 57 | Memphis | 10 038 565.32 |
| 58 | Mesa | 10 053 642.60 |
| 59 | Miami | 9 947 316.07 |
| 60 | Milwaukee | 10 064 482.65 |
| 61 | Minneapolis | 10 011 757.78 |
| 62 | Nashville | 9 961 450.51 |
| 63 | New Orleans | 9 949 257.75 |
| 64 | New York | 10 085 293.55 |
| 65 | Newark | 10 144 052.80 |
| 66 | Norfolk | 10 088 563.17 |
| 67 | North Las Vegas | 10 029 652.51 |
| 68 | Oakland | 9 947 292.52 |
| 69 | Oklahoma City | 10 118 986.25 |
| 70 | Omaha | 10 026 642.34 |
| 71 | Orlando | 10 074 922.52 |
| 72 | Philadelphia | 10 190 080.26 |
| 73 | Phoenix | 10 079 076.70 |
| 74 | Pittsburgh | 10 090 124.82 |
| 75 | Plano | 10 046 103.61 |
| 76 | Portland | 10 007 635.77 |
| 77 | Raleigh | 10 061 442.54 |
| 78 | Reno | 10 079 955.16 |
| 79 | Richmond | 9 992 941.59 |
| 80 | Riverside | 10 006 695.42 |
| 81 | Rochester | 10 067 606.92 |
| 82 | Sacramento | 10 123 468.18 |
| 83 | Saint Paul | 10 057 233.57 |
| 84 | San Antonio | 10 014 441.70 |
| 85 | San Bernardino | 9 965 152.04 |
| 86 | San Diego | 9 966 038.39 |
| 87 | San Francisco | 9 995 570.54 |
| 88 | San Jose | 9 936 721.41 |
| 89 | Santa Ana | 10 050 309.93 |
| 90 | Scottsdale | 10 037 929.85 |
| 91 | Seattle | 9 936 267.37 |
| 92 | Spokane | 10 083 362.98 |
| 93 | St. Louis | 10 002 105.14 |
| 94 | St. Petersburg | 9 986 495.54 |
| 95 | Stockton | 10 006 412.64 |
| 96 | Tampa | 10 106 428.55 |
| 97 | Toledo | 10 020 768.88 |
| 98 | Tucson | 9 998 252.47 |
| 99 | Tulsa | 10 064 955.90 |
| 100 | Virginia Beach | 10 086 553.50 |
| 101 | Washington | 10 139 363.39 |
| 102 | Wichita | 10 083 643.21 |
| 103 | Winston–Salem | 10 044 011.83 |