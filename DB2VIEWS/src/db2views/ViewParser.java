package db2views;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;




/**
* <p>
* Klasse fuer das Parsen der DB2-Views und dem Ableiten des Lineage
* Erzeugt Lineage-Informationen auf Objekt- und Attributebene
* </p>
* @version 1.0
* @author integration-factory
*/
public class ViewParser {

/** Variable fuer Instanz der Klasse Logging */
private Logging logging;
/** Variable fuer Instanz der Klasse DB2Client */
private DB2Client db2client;
/** HashMap fuer Lineage-Informationen */
private Map<Integer, Map<String, String>> lineageHashMap = new HashMap<>();






/**
* Klassenkonstruktor - bei der Instanziierung werden die Klasseninstanzen fuer {@link DB2Client} und {@link Logging} uebergeben  
*@param db2client Instanz der Klasse DB2CLient
*@param logging Instanz der Logging  
* @throws IOException 
* @throws SQLException
*/	
public ViewParser(DB2Client db2client, Logging logging) throws IOException, SQLException{
	this.db2client = db2client; 
	this.logging = logging;
	
	this.createLineageHashMap();
	 
	 
}


/**
* Erzeugung der Lineage-Informationen und Speicherung in einer Lineage-HashMap  
* @throws IOException 
* @throws SQLException
*/	
private void createLineageHashMap() throws IOException, SQLException{
	/** HashMap fuer View-Dependencies */

	Map<Integer, Map<String, String>> viewDepHashMap = new HashMap<>();
	
	viewDepHashMap = this.db2client.getViewDependencies();
	
	
	int viewDepHashMapSize = viewDepHashMap.size();
	
  	Map<String, String> hashMapInsert = new HashMap<>();
	
  	String vonDatenobjektkennung;
  	String zuDatenobjektkennung;
  	
  	
  	
	 for (int currPos = 0; currPos < viewDepHashMapSize; currPos ++)
	 {
		 vonDatenobjektkennung= "<DB2 "+viewDepHashMap.get(currPos).get("view_schema")+">"
				 				+"/"+viewDepHashMap.get(currPos).get("view_name");
		 
		 zuDatenobjektkennung= "<DB2 "+viewDepHashMap.get(currPos).get("reference_schema")+">"
	 				+"/"+viewDepHashMap.get(currPos).get("reference_object_name");
		 
		 hashMapInsert.put("vonDatenobjektkennung", vonDatenobjektkennung);
		 this.lineageHashMap.put(currPos, hashMapInsert);
		 
		 hashMapInsert.put("zuDatenobjektkennung", zuDatenobjektkennung);
		 this.lineageHashMap.put(currPos, hashMapInsert);
		
		
		 hashMapInsert = new HashMap<>();

			 
	 }	

}

/**
* Get-Methode fuer die Lineage-HashMap  
* @throws IOException 
* @return HashMap
*/	
public Map<Integer, Map<String, String>> getLineageHashMap() throws IOException{
	//this.printHashMap(this.lineageHashMap);
	return this.lineageHashMap;
}


/**
* Parst View-Scripts und erzeugt das Lineage auf Attribut-Ebene  
* @throws IOException 
*/
public void parseViews() throws IOException{
	//String convViewStatement = viewStatement.replaceAll("\\s+", " ").trim().toLowerCase();
	//System.out.println("String vorher:"+viewStatement);
	//System.out.println("String vorher:"+convViewStatement);
	
	//this.createDb2ObjectsHashMap();
	//this.printHashMap(this.db2ObjectsHashMap);
	
	//String[] viewParts = convViewStatement.split(" ");
	
	//System.out.println("View Parts:"+ Arrays.toString(viewParts));
	
	
	 
}


/**
* Methode fuer das Print-Out eines HashMap-Inhalts auf die Console 
* @param hashMap die ausgegeben werden soll
*/
public  void printHashMap( Map<Integer, Map<String, String>>  hashMap)
	 {	  
		 
		 int hashMapSize = hashMap.size();
		 
			 
		 for (int curr_pos = 0; curr_pos < hashMapSize; curr_pos ++)
		 {
			 hashMap.get(curr_pos).forEach((k,v) -> System.out.println("Key "+k +" ##### Value "+v)); 
			 
		 }
				
		 
	}

}