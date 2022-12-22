package db2views;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;

/**
* <p>
* Klasse fuer das Bereitstellen von Daten fuer den MDM-HUB 
* Die Basis stellen die in der Klasse {@link ViewParser} berechnete HashMap dar
* </p>
* @version 1.0
* @author integration-factory
*/

public class HubApi {
	/** Variable fuer Instanz der Klasse Logging */
	private Logging logging;
	/** Variable fuer Instanz der Klasse Init */
	private Init init;
	/** Variable fuer Instanz der Klasse ViewParser */
	private ViewParser viewParser;

	
/**
* Klassenkonstruktor - bei der Instanziierung werden die Klasseninstanzen fuer {@link Init}, {@link ViewParser} und {@link Logging} uebergeben
*@param init Instanz der Klasse Init
*@param viewParser Instanz der Klasse ViewParser
*@param logging Instanz der Logging  
*/		
public HubApi(Init init,ViewParser viewParser, Logging logging) throws IOException{
	this.init = init;    
	this.viewParser = viewParser;
	this.logging = logging;
	  
}


/**
* Laden von Metadaten in die HUB-Datei db2view_lineage.csv auf Basis der entsprechender HashMap 
*/
public void generateHubFiles () throws IOException
{
	this.generateLineageCsv();
	
}



/**
* Laden von Metadaten in die HUB-Datei db2view_lineage.csv auf Basis der HashMap lineageHashMap
* 
*/
private void generateLineageCsv()  throws IOException
{
	String targetDirectory = this.init.getParametervalue("zielverzeichnis");
		
		
	File lineageCsv = new File(targetDirectory+"db2views_lineage.csv");
	FileOutputStream lineageCsvFos = new FileOutputStream(lineageCsv,true);
		 
	BufferedWriter lineageCsvBw = new BufferedWriter(new OutputStreamWriter(lineageCsvFos));
		
	
	
	Map<Integer, Map<String, String>> lineageHashMap = new HashMap<>();
	
	lineageHashMap = this.viewParser.getLineageHashMap();
	
	
	int lineageHashMapSize = lineageHashMap.size();
	
  	String vonDatenobjektkennung;
  	String zuDatenobjektkennung;
  	String fileRow;
	
	for (int currPos = 0; currPos < lineageHashMapSize; currPos ++)
	 {
	
		vonDatenobjektkennung = lineageHashMap.get(currPos).get("vonDatenobjektkennung").trim();
		zuDatenobjektkennung = lineageHashMap.get(currPos).get("zuDatenobjektkennung").trim();
	     
		fileRow = vonDatenobjektkennung+","+zuDatenobjektkennung;
		
		lineageCsvBw.write(fileRow);
		lineageCsvBw.newLine();
			 
					 
	 }
	
		
	lineageCsvBw.close();


}




}




