import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Small self-contained JDBC test:
 * 1) Set LU context via GET X12Map_02.'<transaction_id>'
 * 2) Insert one row into biz_person_name (with minimal parent rows if missing)
 * 3) Read corresponding rows from biz_person_name and print them
 *
 * Usage:
 *   java -cp .:fabric-jdbc-8.1.7_22.jar BizPersonNameRoundTrip \
 *     "jdbc:fabric://localhost:5124/x12gen?user=admin&password=admin" TXN-1001
 */
public class BizPersonNameRoundTrip {
    public static void main(String[] args) {
        String jdbcUrl = args.length > 0
                ? args[0]
                : "jdbc:fabric://localhost:5124/masking?user=admin&password=admin";
        String transactionId = args.length > 1 ? args[1] : "TXN-1001";

        String personId = "P-DEMO-1";
        String personNameId = "PN-DEMO-1";

        try {
            Class.forName("com.k2view.fabric.jdbcex.k2driver.FabricDriver");

            try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
                conn.setAutoCommit(false);
                System.out.println("Connected to Fabric");

                // Set LU transaction context for all subsequent SQL operations.
                try (Statement st = conn.createStatement()) {
                    String getCmd = String.format("GET X12Map_02.'%s'", transactionId);
                    st.execute("set SYNC=FORCE;");
                    st.execute(getCmd);

                    System.out.println("Context set: " + getCmd);
                }

                ensureTransactionEntity(conn, transactionId);
                ensurePersonEntity(conn, transactionId, personId);

                // Keep sample id deterministic: remove prior demo row then insert fresh row.
                try (PreparedStatement del = conn.prepareStatement(
                        "DELETE FROM biz_person_name WHERE person_name_id = ?")) {
                    del.setString(1, personNameId);
                    int deleted = del.executeUpdate();
                    System.out.println("Deleted existing demo rows from biz_person_name: " + deleted);
                }

                try (PreparedStatement ins = conn.prepareStatement(
                        "INSERT INTO biz_person_name (person_name_id, transaction_id, person_id, first_name, last_name, middle_name) " +
                        "VALUES (?, ?, ?, ?, ?, ?)")) {
                    ins.setString(1, personNameId);
                    ins.setString(2, transactionId);
                    ins.setString(3, personId);
                    ins.setString(4, "ELENA");
                    ins.setString(5, "CARTER");
                    ins.setString(6, "M");
                    int inserted = ins.executeUpdate();
                    System.out.println("Inserted into biz_person_name rows: " + inserted);
                }

                conn.commit();
                System.out.println("Insert transaction committed");

                // Read from masked table.
                try (PreparedStatement sel = conn.prepareStatement(
                        "SELECT * FROM X12Map_02.biz_person_name;")) {
                    //sel.setString(1, personNameId);
                    try (ResultSet rs = sel.executeQuery()) {
                        printResultSet("biz_person_name", rs);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void ensureTransactionEntity(Connection conn, String transactionId) throws SQLException {
        if (exists(conn, "SELECT 1 FROM biz_transaction_entity WHERE transaction_id = ?", transactionId)) {
            return;
        }

        try (PreparedStatement ins = conn.prepareStatement(
                "INSERT INTO biz_transaction_entity (transaction_id, interchange_control_no, functional_group_control_no, " +
                "transaction_set_control_no, message_type, version, source_format, created_date, created_time) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
            ins.setString(1, transactionId);
            ins.setString(2, "000000901");
            ins.setString(3, "901");
            ins.setString(4, "0001");
            ins.setString(5, "837P");
            ins.setString(6, "005010X222A1");
            ins.setString(7, "X12");
            ins.setString(8, "20260412");
            ins.setString(9, "1200");
            ins.executeUpdate();
            System.out.println("Inserted parent row into biz_transaction_entity");
        }
    }

    private static void ensurePersonEntity(Connection conn, String transactionId, String personId) throws SQLException {
        if (exists(conn, "SELECT 1 FROM biz_person_entity WHERE person_id = ?", personId)) {
            return;
        }

        try (PreparedStatement ins = conn.prepareStatement(
                "INSERT INTO biz_person_entity (person_id, transaction_id, person_type, mastering_note) VALUES (?, ?, ?, ?)")) {
            ins.setString(1, personId);
            ins.setString(2, transactionId);
            ins.setString(3, "subscriber_patient");
            ins.setString(4, "demo row for biz_person_name insert test");
            ins.executeUpdate();
            System.out.println("Inserted parent row into biz_person_entity");
        }
    }

    private static boolean exists(Connection conn, String sql, String param) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, param);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    private static void printResultSet(String tableName, ResultSet rs) throws SQLException {
        ResultSetMetaData md = rs.getMetaData();
        int cols = md.getColumnCount();

        System.out.println("Read from " + tableName + ":");
        for (int i = 1; i <= cols; i++) {
            System.out.print(md.getColumnName(i));
            if (i < cols) System.out.print(", ");
        }
        System.out.println();

        int rowCount = 0;
        while (rs.next()) {
            rowCount++;
            for (int i = 1; i <= cols; i++) {
                System.out.print(rs.getString(i));
                if (i < cols) System.out.print(", ");
            }
            System.out.println();
        }

        if (rowCount == 0) {
            System.out.println("(no rows returned)");
        }
    }
}
