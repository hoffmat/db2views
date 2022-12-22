package db2views;

import java.io.IOException;
import java.sql.SQLException;



/**
* <p>
* Hauptklasse der DB2View-Lineage Applikation
* </p> 
* <p>
* Steuert alle Schritte fuer die Erzeugung des DB2View-Lineage
 *</p>
 *<p>
 *Instanziiert die Klassen {@link Init}, {@link Logging}, {@link DB2Client} , {@link ViewParser}und {@link HubApi} 
**</p>
* @version 1.0
* @author integration-factory
*/
public class Main {
	
	
	/**
	*Instanziiert die Klassen {@link Init}, {@link Logging}, {@link DB2Client} , {@link ViewParser}und {@link HubApi} 
	*@param args String-Array fuer Applikationsparameter
	*/
	public static void main(String[] args) throws IOException, SQLException {
		/** Intiliasierung der Applikation */	
		Init init = new Init();	
		Logging logging = new Logging(init);
		
		DB2Client db2Client = new DB2Client();
		
		ViewParser viewParser = new ViewParser(db2Client, logging);	
		HubApi hubApi = new HubApi(init, viewParser, logging);	
		hubApi.generateHubFiles();
		
		//db2ViewParser.getLineageHashMap();
		
		logging.closeLogFile();
		System.out.println("Ende der Verarbeitung");	
}


}
