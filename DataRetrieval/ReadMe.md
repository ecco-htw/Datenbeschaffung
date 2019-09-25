# Daten Management im Rahmen des HTW ECCO Projektstudiums

Scala Spark Programm zur Datenbeschaffung und Datenspeicherung

## Vorraussetzung
### IDE
* Für Eclipse `sbt eclipse`, Projekt import und die Properties wie im Bild setzen:
  ![Scala compiler settings](eclipse_properties.png)
* Für IntelliJ import als sbt Projekt

### Umgebungsvariablen
Damit das Programm laufen kann sind Authentifizierungsdaten notwendig, diese werden
über Umgebungsvariablen in das Programm gelesen. Die benötigten Umgebungsvariablen
sind die Folgenden:

* MONGO_HOST
* MONGO_PORT
* MONGO_USER
* MONGO_PASSWORD
* MONGO_DB
* MONGO_COLLECTION

Aus Sicherheitsgründen veröffentlichen wir die zugehörigen Werte nicht.

**Wichtiger Hinweis:** Die Umgebungsvariablen werden auf dem Server über `/home/local/.profile` gesetzt. Wird sie angepasst, sollte der Server neu gestartet werden.

## Programm starten
Vor dem Starten: Bitte siehe Kapitel *Vorraussetzung*.

* Im Terminal: `sbt run`

## Dokumentation
Der Quelltext wurde nach den [Scala Doc Konventionen](https://docs.scala-lang.org/style/scaladoc.html) geschrieben.
Die Kommentare können entweder in den jeweiligen Scala Dateien selbst gelesen werden,
oder es kann mit dem Befehl `sbt doc` die Dokumentation im HTML-Format exportiert werden
und im Browser durch das Öffnen der Datei *target/scala-2.11/api/index.html* angezeigt werden.

## Architektur
Das Programm ist in fünf 'Packages' gegliedert mit jeweils einer Scala-Datei. Zu jeder Scala-Datei wird im Folgenden die zugehörige Funktion innerhalb des Programms beschrieben um einen Überblick
über die Komponenten zu erhalten.

### Skizze
Die Zeichnung ist mit draw.io erstellt worden und visualisiert die Abläufe des Programms.
Die Zeichnung kann durch importieren der zur Verfügung gestellten ECCO-Datenbeschaffung-Skizze.xml
Datei in draw.io überarbeitet werden.
![ECCO-Datenbeschaffung-Skizze](ECCO-Datenbeschaffung-Skizze.png)

### eccoutil/ArgoFloatException.scala
Die Datei 'ArgoFloatException' enthält wie der Name bereits andeutet eine eigene Exception um
dem Nutzer des Programms ggf. eine sinnvolle Fehlermeldung zurückzugeben.

### main/RunProcedure.scala
In 'RunProcedure' befindet sich das Object mit der *main*-Methode um das Programm zu starten.
Wird das Programm gestartet, würd zunnächst ein "Global Update" durchgeführt. Es wird ein Objekt der Klasse "Global Updater" verwendet, um den Stand der Datenbank mit dem des FTP-Servers abzugleichen und alle in der DB fehlenden Einträge in dieser zu speichern (*globalUpdater.update()*).

Nachdem das "Global Update" erfolgreich ausgeführt wurde, geht das Programm unter Verwendung des Ftp-Observers in einen Status über, in dem es in regelmäßigen Abständen nach einer neuen "Weekly List" vom FTP-Server fragt. 

Existiert diese wird sie automatisch durch die Callback-Methode des Observers (*doWeeklyUpdate*) mithilfe eines Objekts der Klasse *WeeklyUpdater* heruntergeladen und ihre Inhalte in der DB gespeichert. 

### netcdfhandling/BuoyData.scala
///Nicht mehr vorhanden <br/>
Die Klasse *BuoyData* wird mit einer URL zu einer NetCDF Datei initialisiert, die
auf einem FTP Server liegt. Diese NetCDF-Datei kann dann in den Arbeitsspeicher in
ein [Java NetCDF Objekt](https://www.unidata.ucar.edu/software/thredds/current/netcdf-java/documentation.htm) gelesen werden. Das Java NetCDF Objekt kann dann über die BuoyData Klasse in
verschiedene Formate umgewandelt werden. Die Formate sind:
* **Scala immutable Map**, wobei die Schlüssel die Variablennamen sind und die Werte die jeweiligen Daten
* **Spark RDD**, dabei enthält jede Spark Row die Daten zu einem Messzyklus einer Boje
* **Spark DataFrame**, das Schema für das DataFrame wird automatisch aus den NetCDF Dateien
 gefolgert
 //TODO:
### preprocessing/IndexFile.scala
Dieser Klasse repräsentiert eine Index Datei (GlobalList/WeeklyList) des FTP-Servers. Ihre Hauptaufgabe ist es die Einträge der Index Datei auf Objekte der Case-Klasse *IndexFileEntry* abzubilden. Die für uns relevanten und letzendlich in der Case-Class gespeicherten Felder eines Eintrages sind "dateUpdate"(*date*) und der Subpfad der dazugehörigen NetCDF-Datei(*path*). <br/>
Andere Module können über das Feld *data*(RDD) auf die IndexFileEntry-Objekte zugreifen.
### preprocessing/GlobalUpdater.scala
### preprocessing/WeeklyUpdater.scala
### EccoSpark.scala
### netcdfhandling/NetCDFConverter.scala
### netcdfhandling/Package.scala

### observer/FtpObserver.scala
//Stimmt noch <br/>
Die FtpObserver Klasse ist als Akka actor implementiert. Damit entspricht jede
Instanz der Klasse einem Worker Thread. Wir benötigen für unseren Anwendungsfall
nur einen Worker Thread.

Die Aufgabe dieser Klasse ist es den FTP Server periodisch (bisher ein mal täglich) nach Veränderungen der Index Datei abzufragen. Wenn eine Änderung erkannt wurde, dann wird eine
Callback-Methode aufgerufen die beim erstellen einer Instanz der Klasse übergeben werden kann.

Akka actors kommunizieren über Nachrichten, dafür müssen die Nachrichten die empfangen werden können zuvor definiert werden. Der FtpObserver definiert drei Nachrichten:
* **Start:** Dies startet den Initialisierungsprozess und setzt einen Akka timer mit dem der actor sich selbst die Polling Nachricht in einem bestimmten Zeitintervall (einen Tag) übermittelt
* **Polling:** Startet eine Anfrage an den FTP Server ob der Index sich verändert hat
* **Stop:** Dies beendet den Actor (Wird momentan nicht genutzt, da der Downloader langfristig laufen soll)

### preprocessing/ThisWeekList.scala
//nicht mehr vorhanden <br/>
Die Klasse ThisWeekList lädt den Index zu den neu hinzugekommenen Daten auf dem FTP-Server
in ein Spark DataFrame. Dabei enthält jede Spark Row des DataFrames die Daten zu einer
der neu hinzugekommenen Dateien. Von den Daten werden bisher nur die relativen Pfade
der Dateien genutzt.

## Hilfreiche Links
* [Argo DOI](http://www.argodatamgt.org/Access-to-data/Argo-DOI-Digital-Object-Identifier)
* Argo Data FTP: ftp://ftp.ifremer.fr/ifremer/argo
* [SciSpark-API](https://scispark.jpl.nasa.gov/api/)
* [Java-NetCDF-API](https://www.unidata.ucar.edu/software/thredds/v4.3/netcdf-java/v4.3/javadoc/index.html)
* [Apache Zeppelin](https://zeppelin.apache.org/)

## Am Anfang empfohlen
Mittels notebooks von Apache Zeppelin Daten explorieren, weil geringer Konfigurationsaufwand.
Importiere die Notebooks vom Ordner Zeppelin-Notebooks um loszulegen.

## Troubleshooting
* Fragen an [Slack-Gruppe](https://htw-ai-wise-2016.slack.com)
* Bei gelösten Problemen bitte *ReadMe.md* erweitern
