##### Оценка популярности языков программирования в полнотекстовых статьях.
###### В этом задании используются полнотекстовые данные из Википедии, чтобы создать метрику того, насколько популярен язык программирования.
###### Для показа производительности реализовал вычесления разными методами:
1. occurrencesOfLang, rankLangs
2. makeIndex, rankLangsUsingIndex
3. rankLangsReduceByKey

   [wikipedia.dat (133 МБ)]:  <https://moocs.scala-lang.org/~dockermoocs/bigdata/wikipedia.dat>
[About cloning a repository]: <https://docs.github.com/en/repositories/creating-and-managing-repositories/cloning-a-repository>
__________________________________________________________
1. Clone this project
2. Importing an sbt project into IntelliJ
    + File menu -> New -> Project from Existing Sources...
    + Select a file that contains your sbt project description build.sbt. Click OK.

3. Download data - [wikipedia.dat (133 МБ)] and place it in the folder:
   -`src/main/resources/wikipedia` in your project directory
4. **_Run_** `src/main/scala/wikipedia/WikipediaRanking.scala`

### Out
__________________________________________________________
```
root
 |-- language: string (nullable = false)
 |-- count: integer (nullable = false)

+-----------+-----+
|   language|count|
+-----------+-----+
| JavaScript| 4293|
|       Java| 2547|
|        CSS| 1890|
|         C#| 1650|
|        C++| 1249|
|        PHP|  804|
|     Python|  740|
|     MATLAB|  600|
|       Perl|  360|
|Objective-C|  344|
|       Ruby|  322|
|    Haskell|  140|
|      Scala|   80|
|     Groovy|   32|
|    Clojure|   30|
+-----------+-----+

Processing Part 1: naive ranking took {your_time} ms.
Processing Part 2: ranking using inverted index took {your_time} ms.
Processing Part 3: ranking using reduceByKey took {your_time} ms.
```
