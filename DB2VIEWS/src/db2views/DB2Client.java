package db2views;

import java.io.IOException;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;

                                                           

/**
* <p>
* Klasse fuer den Aufbau einer Datenbankverbindung zu DB2 und Abfrage von DB2-Dictionary Tabellen 
* Der Zugriff erfolgt über JDBC
* </p>
* @version 1.0
* @author integration-factory
*/
public class DB2Client 
{
	/** JDBC-Url Prefix */
	private String urlPrefix = "jdbc:db2:";
	/** JDBC-Url*/
	private String url;
	/** DB2 User */
	private String user;
	/** DB2 Passwort */
	private String password;
	/** DB2 Connection */
	private Connection con;
	/** SQL Statement */
	private Statement stmt;
	/** Resultset View-Dependencies */
	private ResultSet qViewDepRs;
	/** Resultset View-Scripts */
	private ResultSet qViewScriptsRs;
	/** Resultset Objekt-Columns */
	private ResultSet qObjectColumnsRs;
	/** HashMap View-Dependencies (temporaer genutzt)*/
	private Map<Integer, Map<String, String>> viewDepHashMap = new HashMap<>();

	/**
	* Klassenkonstruktor 
	 * @throws SQLException 
	 * @throws IOException
	*/		
	public DB2Client() throws IOException, SQLException{
		this.initDriver();
		this.queryViewDependencies();
		   
	}	

	/**
	* Initialisierung des JDBC-Drivers 
	 * @throws IOException 
	*/		
	private void initDriver() throws IOException{
		 try 
		    {                                                                        
		      
		      Class.forName("com.ibm.db2.jcc.DB2Driver");    
		      System.out.println("JDBC Treiber geladen");
		    
		    }
		 catch (ClassNotFoundException e)
		    {
		      System.err.println("Konnte JDBC driver nicht laden");
		      System.out.println("Exception: " + e);
		      e.printStackTrace();
		    }

		   
	}	
	
	/**
	* Aufbau der Datenbankverbindung zu DB2 
	 * @throws IOException 
	 * @throws SQLException 
	*/
	private void createConnection() throws IOException, SQLException{
		this.con = DriverManager.getConnection (this.url, this.user, this.password);                 
	    
	    this.con.setAutoCommit(false);
	    //System.out.println("**** Created a JDBC connection to the data source");
		   
	}	
	
	
	/**
	* Abfrage der View/Objekt-Dependencies und Speicherung in einem ResultSet (Ergebnis wird aktuell durch HashMap simuliert)
	 * @throws IOException 
	 * @throws SQLException 
	*/
	private void queryViewDependencies() throws IOException, SQLException{
	    /*
		String sqlStatement = "select tabschema as schema_name,\r\n"
				+ " tabname as view_name,\r\n"
				+ " bschema as referenced_schema_name,\r\n"
				+ " bname as referenced_object_name,\r\n"
				+ " btype as object_type\r\n"
				+ "	from syscat.tabdep\r\n"
				+ "	where dtype = 'V' \r\n"
				+ " and tabschema not like 'SYS%'";
		
		this.stmt = this.con.createStatement();                                            
	    
	    this.qViewDepRs = this.stmt.executeQuery(sqlStatement);                   
	    this.stmt.close();
	    */
		
		
	    Map<String, String> hashMapInsert = new HashMap<>();
		
		/** Daten simulieren bis DB-Connection funktioniert */
		int i = 0;
		
		hashMapInsert.put("view_schema", "#QxDS000");
		this.viewDepHashMap.put(i, hashMapInsert);
		
		hashMapInsert.put("view_name", "QV17T200");
		this.viewDepHashMap.put(i, hashMapInsert);

		hashMapInsert.put("reference_schema", "#QxDBA00");
		this.viewDepHashMap.put(i, hashMapInsert);

		hashMapInsert.put("reference_object_name", "Q17TA20");
		this.viewDepHashMap.put(i, hashMapInsert);

		hashMapInsert.put("reference_object_type", "T");
		this.viewDepHashMap.put(i, hashMapInsert);

		hashMapInsert = new HashMap<>();
		
		i++;

		hashMapInsert.put("view_schema", "#QxDS000");
		this.viewDepHashMap.put(i, hashMapInsert);
		
		hashMapInsert.put("view_name", "QV17T200");
		this.viewDepHashMap.put(i, hashMapInsert);

		hashMapInsert.put("reference_schema", "#QxDBA00");
		this.viewDepHashMap.put(i, hashMapInsert);

		hashMapInsert.put("reference_object_name", "QV17TB33");
		this.viewDepHashMap.put(i, hashMapInsert);

		hashMapInsert.put("reference_object_type", "V");
		this.viewDepHashMap.put(i, hashMapInsert);

		hashMapInsert = new HashMap<>();
		
		i++;

		hashMapInsert.put("view_schema", "#QxAX111");
		this.viewDepHashMap.put(i, hashMapInsert);
		
		hashMapInsert.put("view_name", "QV99999");
		this.viewDepHashMap.put(i, hashMapInsert);

		hashMapInsert.put("reference_schema", "#QYTTTT");
		this.viewDepHashMap.put(i, hashMapInsert);

		hashMapInsert.put("reference_object_name", "Q187T9");
		this.viewDepHashMap.put(i, hashMapInsert);

		hashMapInsert.put("reference_object_type", "T");
		this.viewDepHashMap.put(i, hashMapInsert);

		hashMapInsert = new HashMap<>();
		
		i++;

		hashMapInsert.put("view_schema", "#QxAX111");
		this.viewDepHashMap.put(i, hashMapInsert);
		
		hashMapInsert.put("view_name", "QV99999");
		this.viewDepHashMap.put(i, hashMapInsert);

		hashMapInsert.put("reference_schema", "#QYTTTT");
		this.viewDepHashMap.put(i, hashMapInsert);

		hashMapInsert.put("reference_object_name", "QYYYYY");
		this.viewDepHashMap.put(i, hashMapInsert);

		hashMapInsert.put("reference_object_type", "T");
		this.viewDepHashMap.put(i, hashMapInsert);

		hashMapInsert = new HashMap<>();
	    
	}
	
	

	/**
	* Abfrage der View-Scripts und Speicherung in einem ResultSet 
	 * @throws IOException 
	 * @throws SQLException 
	*/
	private void queryViewScripts() throws IOException, SQLException{
		
		String sqlStatement = "select  viewschema as schema_name,\r\n" 
		        + " viewname as view_name,\r\n"
		        + " text as  view_script \r\n"
		        + "from syscat.views \r\n"
		        + " where viewschema not like 'SYS%' \r\n"
		        + " order by schema_name, view_name";
		
		this.stmt = this.con.createStatement();                                            
	    
	    this.qViewScriptsRs = this.stmt.executeQuery(sqlStatement);                   
	    this.stmt.close();
	    
	}
	
	/**
	* Abfrage aller Columns von allen Views und Tabellen und Speicherung in einem ResultSet 
	 * @throws IOException 
	 * @throws SQLException 
	*/
	private void queryObjectColumns() throws IOException, SQLException{

		
		String sqlStatement = "select tabschema as object_schema_name,\r\n" 
		        + " tabname as object_name,\r\n"
		        + " colname as  column_name \r\n"
		        + " from syscat.columns \r\n"
		        + " where tabschema not like 'SYS%' \r\n"
		        + " order by 1, 2";
		
		this.stmt = this.con.createStatement();                                            
	    
	    this.qObjectColumnsRs = this.stmt.executeQuery(sqlStatement);                   
	    this.stmt.close();
	    
	}

	/**
	* Schliessen aller ResultSets 
	 * @throws IOException 
	 * @throws SQLException 
	*/
	private void closeResultsets() throws IOException, SQLException{
		
		this.qViewDepRs.close();
		this.qViewScriptsRs.close();
		this.qObjectColumnsRs.close();
		   
	}	
	
		
	
	/**
	* Schliessen der Datenbankverbindung 
	 * @throws IOException 
	 * @throws SQLException 
	*/
	public void closeConnection() throws IOException, SQLException{
		 this.closeResultsets();
		 this.con.close();
		   
	}
	
	/* Umstellen, wenn DB2-Connect funktioniert 
	public ResultSet getViewDependencies() throws IOException, SQLException{
		 
		return qViewDepRs;
		   
	}	
	*/
	
	/**
	* Get-Methode fuer die View/Objekt-Dependencies 
	 * @throws IOException 
	 * @throws SQLException 
	 * @return ResultSet
	*/
	public Map<Integer, Map<String, String>> getViewDependencies() throws IOException, SQLException{
		 
		return viewDepHashMap;
		   
	}
	
	/**
	* Get-Methode fuer die View-Scripts 
	 * @throws IOException 
	 * @throws SQLException 
	 * @return ResultSet 
	*/
	public ResultSet getViewScripts() throws IOException, SQLException{
		 
		return qViewScriptsRs;
		   
	}
	
	/**
	* Get-Methode fuer die Columns von Objekten
	 * @throws IOException 
	 * @throws SQLException
	 * @return ResultSet 
	*/
	public ResultSet getObjectColumns() throws IOException, SQLException{
		 
		return qObjectColumnsRs;
		   
	}
	
	
}    