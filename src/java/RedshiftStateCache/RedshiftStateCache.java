
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.regex.*;
import java.sql.*;
import java.lang.ref.WeakReference;
import java.text.*;
import com.amazon.redshift.jdbc42.*;
import com.unboundid.util.args.StringArgument;

public class RedshiftStateCache
{
    // Map from schema -> table -> last updated
    private AtomicReference<Map<String, ? extends Map<String, java.util.Date>>> tableLastUpdatedRef;
    private ScheduledFuture<?> handle;
    String nativeQueryRegexPattern = "from\\s*(?<tablename>[^\\s]*)(\\s|$)";
    private ISetTableLastUpdated rtq;
    DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

    public RedshiftStateCache(int tableRefreshSeconds, String dbHost, String dbUser, String dbPass, String dbName, int dbPort)
    {
        init(tableRefreshSeconds, new RunnableTableQuerier(dbHost, dbUser, dbPass, dbName, dbPort));
    }

    public RedshiftStateCache(int tableRefreshSeconds, ISetTableLastUpdated tableLastUpdatedSetter)
    {
        init(tableRefreshSeconds, tableLastUpdatedSetter);
    }

    private void init(int tableRefreshSeconds, ISetTableLastUpdated tableLastUpdatedSetter)
    {
        System.out.println("CREATING REDSHIFT STATE CACHE WITH " + tableLastUpdatedSetter.getClass().getName());
        tableLastUpdatedRef = new AtomicReference<Map<String, ? extends Map<String, java.util.Date>>>();
        tableLastUpdatedRef.set(new HashMap<String, HashMap<String, java.util.Date>>());
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        rtq = tableLastUpdatedSetter;
        rtq.setTableLastUpdatedRef(tableLastUpdatedRef);
        handle = scheduler.scheduleAtFixedRate(rtq, 0, tableRefreshSeconds, TimeUnit.SECONDS);
    }

    public boolean cancel()
    {
        return handle.cancel(true);
    }

    public boolean shouldReturnCachedResult(String qualifiedTableName, long lastUpdated)
    {
        System.out.println("INPUT TABLE: " + qualifiedTableName);
        ArrayList<String> a = new ArrayList<String>(Arrays.asList(qualifiedTableName));
        return shouldReturnCachedResult(a, lastUpdated);
    }

    public boolean shouldReturnCachedResult(Iterable<String> qualifiedTableNames, long lastUpdatedTime)
    {
        try
        {
            java.util.Date lastUpdated = new java.util.Date(lastUpdatedTime);
            System.out.println("LAST UPDATED: " + lastUpdated.toString());
            for(String table : qualifiedTableNames)
            {
                table = table.toUpperCase();
                System.out.println("**TABLE: " + table);
                // If the table is in the form schema.table, we can lookup the exact table
                if(table.contains("."))
                {
                    // If it's an unrecognized schema or table, we can't say whether or not to use the cached value so just return false
                    String[] split = table.split("\\.");
                    if(!tableLastUpdatedRef.get().containsKey(split[0]) || !tableLastUpdatedRef.get().get(split[0]).containsKey(split[1]))
                    {
                        return false;
                    }
                    // Otherwise check the last time the table was updated
                    System.out.println("LAST UPDATED: " + lastUpdatedTime + "TABLE LAST UPDATED: " + tableLastUpdatedRef.get().get(split[0]).get(split[1]).getTime());
                    if(lastUpdated.getTime() < tableLastUpdatedRef.get().get(split[0]).get(split[1]).getTime())
                    {
                        return false;
                    }
                }
                // The schema wasn't specified, so to be safe check all schemas for that table name
                else
                {
                    boolean foundAtLeastOne = false;
                    for ( String key : tableLastUpdatedRef.get().keySet() ) {
                        foundAtLeastOne |= tableLastUpdatedRef.get().get(key).containsKey(table);
                        if(tableLastUpdatedRef.get().get(key).containsKey(table) && lastUpdated.getTime() < tableLastUpdatedRef.get().get(key).get(table).getTime())
                        {
                            return false;
                        }
                    }
                    if(!foundAtLeastOne)
                    {
                        //Unrecognized table
                        return false;
                    }
                }
            }
            // The cached object is more recent than the last updated time of any tables in the query, so
            // return the cached result
            return true;
        }
        catch(Exception e)
        {
            System.out.println("ERRRROORRRR: " + e.toString());
            return false;
        }
        finally{
            System.out.println("FINALLY EXITING!");
        }
    }

    public boolean shouldReturnCachedResult(clojure.lang.PersistentArrayMap query, long lastUpdated)
    {
        System.out.println("QUERY: " + query);
        
        System.out.println("********* NATIVE QUERY **********");
        // Pull all table names from the query
        String queryText = (String)(((clojure.lang.PersistentArrayMap)(query.get(clojure.lang.Keyword.intern("native")))).get(clojure.lang.Keyword.intern("query")));
        Pattern pattern = Pattern.compile(nativeQueryRegexPattern, java.util.regex.Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(queryText);
        System.out.println("QUERY TEXT: " + queryText);
        ArrayList<String> tables = new ArrayList<String>();
        while (matcher.find()) {
            tables.add(matcher.group(1).toUpperCase());
        }
        return shouldReturnCachedResult(tables, lastUpdated);
    }

    public boolean isValid()
    {
        return rtq.isValid();
    }

    public int getMaxTtlSeconds()
    {
        return 3600;
    }

    private class RunnableTableQuerier implements ISetTableLastUpdated 
    {
        private static final String query = "WITH    lastupdate AS ( SELECT tbl, MAX(endtime) AS last_updated FROM stl_insert GROUP BY tbl), " +
                                                "table_names AS (SELECT relname::char(100) AS table_name, n.nspname AS schema, pg_class.oid FROM pg_class " + 
                                                                "JOIN pg_catalog.pg_namespace n ON n.oid = pg_class.relnamespace) " +
                                        "SELECT lastupdate.tbl, table_names.schema, table_names.table_name, lastupdate.last_updated "  +
                                        "FROM lastupdate " +
                                        "JOIN table_names ON lastupdate.tbl = table_names.oid ";
        private int successiveFailureCount = 0;
        private AtomicReference<Map<String, ? extends Map<String, java.util.Date>>> tableLastUpdatedRef;
        private boolean isValid;
        private Properties connectionProps = new Properties();
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String dbPass;
        String dbConnectionString;
        public RunnableTableQuerier(String dbHost, String dbUser, String dbPass, String dbName, int dbPort)
        {
            isValid = false;
            connectionProps.setProperty("ssl", "true");  
            connectionProps.setProperty("sslmode", "verify-full");
            connectionProps.setProperty("user", dbUser);
            connectionProps.setProperty("password", dbPass);
            dbConnectionString = String.format("jdbc:redshift://%s:%d/%s", dbHost, dbPort, dbName); 
            df.setTimeZone(TimeZone.getTimeZone("GMT"));
           // connectionProps.setProperty("socketTimeout", "5");
            //connectionProps.setProperty("loginTimeout", "5");
        }
        
        @Override
        public void setTableLastUpdatedRef(AtomicReference<Map<String, ? extends Map<String, java.util.Date>>> tableLastUpdatedRef)
        {
            this.tableLastUpdatedRef = tableLastUpdatedRef;
        }

        @Override
        public boolean isValid()
        {
            return isValid;
        }

        @Override
        public void run()
        {
            HashMap<String, HashMap<String, java.util.Date>> next = new HashMap<String, HashMap<String, java.util.Date>>();
            Connection conn = null;
            Statement stmt = null;
            try
            {
                System.out.println("Querying last update time of tables...");
                conn = DriverManager.getConnection(dbConnectionString, connectionProps);
                stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(query);
                ResultSetMetaData rsmd = rs.getMetaData();
                
                while (rs.next()) 
                {
                    String schema = rs.getString("schema").trim().toUpperCase();
                    if(!next.containsKey(schema))
                    {
                        next.put(schema, new HashMap<String, java.util.Date>());
                    }
                    java.util.Date d = df.parse(rs.getString("last_updated").split("\\.")[0]);

                    //System.out.println(schema + "." + rs.getString("table_name").trim().toUpperCase() + ":\t" + df.format(d) + " " + d.getTime());
                    
                    next.get(schema).put(rs.getString("table_name").trim().toUpperCase(), d);
                }
                successiveFailureCount = 0;
                tableLastUpdatedRef.set(next);
                isValid = true;
                System.out.println("***** CACHE UPDATED *****" + next.size());
            }
            catch(Exception e)
            {
                System.out.println("Error getting last update time of tables: " + e.toString());
                // If we've failed 3 times in a row, something's wrong so disable the cache until it recovers
                if(++successiveFailureCount > 3)
                {
                    System.out.println("***** REDSHIFT STATE CACHE OFFLINE *****");
                    isValid = false;
                }
            }
        }
    }
}