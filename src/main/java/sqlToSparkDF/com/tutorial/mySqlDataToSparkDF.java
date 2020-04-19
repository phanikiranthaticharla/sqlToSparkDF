package sqlToSparkDF.com.tutorial;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * CREDITS: crunchify.com, code to connect to mysql is taken from https://crunchify.com/java-mysql-jdbc-hello-world-tutorial-create-connection-insert-data-and-retrieve-data-from-mysql/
 *
 * Write data to MyQL
 * Read the data from MySQL
 * Create a Spark Data Frame
 * Write the data as parquet
 *  @author Phani Kiran Thaticharla
 */

public class mySqlDataToSparkDF {

    static Connection sqlConn = null;
    static PreparedStatement sqlStatement = null;
    static String jdbcUrl = "jdbc:mysql://localhost:3306/phaniDB";
    static String dbUserName = "root";
    static String dbPassword = "password";
    static SparkSession spark = SparkSession.builder().appName("documentation").master("local").getOrCreate();

    public static void main(String[] argv) {
        mySqlDataToSparkDF obj = new mySqlDataToSparkDF();
        mySQLConnection();
        obj.createParquetUsingSparkDFReader();
    }

    private void createParquetUsingSparkDFReader() {
        /*public Dataset<Row> jdbc(String url,
                         String table,
                         java.util.Properties properties)

         */
        String table = "(Select companyName from Employee) Emp";
        Properties properties = new Properties();
        properties.put("user", dbUserName);
        properties.put("password", dbPassword);
        //SparkSession spark = SparkSession.builder().appName("documentation").master("local").getOrCreate();
        DataFrameReader dfr = new DataFrameReader(spark);
        Dataset<Row> df = dfr.jdbc(jdbcUrl, table, properties);
        df.show();
        df.write().parquet("/home/phani/parquet_files/dataframereader_to_parquet");

    }

    private static void mySQLConnection() {
        try {
            log("--------Make JDBC Connection------------");
            makeJDBCConnection();

            log("\n---------- Adding companies to DB ----------");
            addDataToDB("Intel", "SC, CA, US", 100000, "https://intel.com");
            addDataToDB("Google", "Mountain View, CA, US", 50000, "https://google.com");
            addDataToDB("Apple", "Cupertino, CA, US", 30000, "http://apple.com");

            log("\n---------- Let's get Data from DB ----------");
            getDataFromDB();

            sqlStatement.close();
            sqlConn.close(); // connection close

        } catch (SQLException e) {

            e.printStackTrace();
        }
    }

    private static void makeJDBCConnection() {

        try {
            Class.forName("com.mysql.jdbc.Driver");
            log("JDBC Driver is Registered!");
        } catch (ClassNotFoundException e) {
            log("JDBC driver not found, add correct maven dependency");
            e.printStackTrace();
            return;
        }

        try {
            // DriverManager: The basic service for managing a set of JDBC drivers.
            sqlConn = DriverManager.getConnection(jdbcUrl, dbUserName, dbPassword);
            if (sqlConn != null) {
                log("Connection Successful! Enjoy. Now it's time to push data");
            } else {
                log("Failed to make connection!");
            }
        } catch (SQLException e) {
            log("MySQL Connection Failed!");
            e.printStackTrace();
            return;
        }

    }

    private static void addDataToDB(String companyName, String address, int totalEmployee, String webSite) {

        try {
            String insertQueryStatement = "INSERT  INTO  Employee  VALUES  (?,?,?,?)";

            sqlStatement = sqlConn.prepareStatement(insertQueryStatement);
            sqlStatement.setString(1, companyName);
            sqlStatement.setString(2, address);
            sqlStatement.setInt(3, totalEmployee);
            sqlStatement.setString(4, webSite);

            // execute insert SQL statement
            sqlStatement.executeUpdate();
            log(companyName + " added successfully");
        } catch (

                SQLException e) {
            e.printStackTrace();
        }
    }

    private static void getDataFromDB() {

        try {
            // MySQL Select Query Tutorial
            String getQueryStatement = "SELECT * FROM Employee";

            sqlStatement = sqlConn.prepareStatement(getQueryStatement);

            // Execute the Query, and get a java ResultSet
            ResultSet rs = sqlStatement.executeQuery();

            // df.printSchema();
            // Let's iterate through the java ResultSet
            List<Employee> list = new ArrayList<Employee>();
            while (rs.next()) {
                String name = rs.getString("companyName");
                String address = rs.getString("address");
                int employeeCount = rs.getInt("totalEmployee");
                String website = rs.getString("website");
                Employee e = new Employee(name, address, employeeCount, website);
                list.add(e);
                // Simply Print the results
                System.out.format("%s, %s, %s, %s\n", name, address, employeeCount, website);
            }

            Dataset<Row> sqlDF = spark.createDataFrame(list, Employee.class);
            sqlDF.printSchema();
            sqlDF.write().parquet("/home/phani/parquet_files/rs_to_parquet");

        } catch (

                SQLException e) {
            e.printStackTrace();
        }

    }

    // Simple log utility
    private static void log(String string) {
        System.out.println(string);

    }
}