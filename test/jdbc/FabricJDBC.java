import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

public class FabricJDBC {
    public static void main(String[] args) {
        String url = "jdbc:fabric://localhost:5124/x12gen?user=admin&password=admin";
        try {
            // Register the driver (optional in newer Java versions)
            Class.forName("com.k2view.fabric.jdbcex.k2driver.FabricDriver");

            // Establish connection
            Connection conn = DriverManager.getConnection(url);
            System.out.println("Connected to Fabric!");

            // Query the LU
            Statement stmt = conn.createStatement();
            stmt.executeQuery("GET X12Map_02.'TXN-1001'");
            //stmt.executeQuery("GET X12Map_02.'TXN-19568B2C91C7'");
            //ResultSet rs = stmt.executeQuery("GET X12Map_02.'TXN-1001'; SELECT * FROM biz_person_name");
            //ResultSet rs = stmt.executeQuery("SELECT * FROM biz_person_name");
            ResultSet rs = stmt.executeQuery("SELECT * FROM biz_person_entity WHERE transaction_id = 'TXN-1001' ");

            // Get metadata to retrieve column headers
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnCount = rsmd.getColumnCount();

            // Print column headers
            System.out.println("Column Headers:");
            for (int i = 1; i <= columnCount; i++) {
                System.out.print(rsmd.getColumnName(i));
                if (i < columnCount) System.out.print(", ");
            }
            System.out.println("\n" + "-".repeat(50)); // Separator line

            // Print all columns for each row
            while (rs.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    System.out.print(rs.getString(i)); // Fetch as string for simplicity
                    if (i < columnCount) System.out.print(", ");
                }
                System.out.println(); // New line after each row
            }

            // Clean up
            rs.close();
            stmt.close();
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
