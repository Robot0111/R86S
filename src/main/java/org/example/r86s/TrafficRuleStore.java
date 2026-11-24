package org.example.r86s;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

final class TrafficRuleStore {
    private final AppConfig.Database database;
    private final String host;

    TrafficRuleStore(AppConfig.Database database, String host) {
        this.database = database;
        this.host = host;
    }

    List<TrafficRule> list() throws SQLException {
        var rows = new ArrayList<TrafficRule>();
        try (var connection = database.connect(); var select = connection.prepareStatement("""
                SELECT rule_id, rule_kind, rule_target, rule_label
                FROM r86s_traffic_rule WHERE host_id = ? ORDER BY rule_kind, rule_target
                """)) {
            select.setString(1, host);
            try (var result = select.executeQuery()) {
                while (result.next()) {
                    rows.add(new TrafficRule(result.getString(1), result.getString(2), result.getString(3), result.getString(4)));
                }
            }
        }
        return rows;
    }

    List<TrafficRule> forTraffic() throws SQLException {
        try {
            return list();
        } catch (SQLException error) {
            // The current traffic page can still use the built-in rules before the migration is applied.
            if (missingTable(error)) return List.of();
            throw error;
        }
    }

    void insert(TrafficRule rule) throws SQLException {
        try (var connection = database.connect(); var insert = connection.prepareStatement("""
                INSERT INTO r86s_traffic_rule (rule_kind, rule_target, rule_label, host_id, rule_id)
                VALUES (?, ?, ?, ?, ?)
                """)) {
            bind(insert, rule);
            insert.executeUpdate();
        }
    }

    boolean update(TrafficRule rule) throws SQLException {
        try (var connection = database.connect(); var update = connection.prepareStatement("""
                UPDATE r86s_traffic_rule SET rule_kind = ?, rule_target = ?, rule_label = ?
                WHERE host_id = ? AND rule_id = ?
                """)) {
            bind(update, rule);
            return update.executeUpdate() == 1;
        }
    }

    boolean delete(String id) throws SQLException {
        try (var connection = database.connect(); var delete = connection.prepareStatement("""
                DELETE FROM r86s_traffic_rule WHERE host_id = ? AND rule_id = ?
                """)) {
            delete.setString(1, host);
            delete.setString(2, id);
            return delete.executeUpdate() == 1;
        }
    }

    private void bind(PreparedStatement statement, TrafficRule rule) throws SQLException {
        statement.setString(1, rule.kind());
        statement.setString(2, rule.target());
        statement.setString(3, rule.label());
        statement.setString(4, host);
        statement.setString(5, rule.id());
    }

    static boolean missingTable(SQLException error) {
        return switch (error.getErrorCode()) {
            case 942, 42102, 42104 -> true; // Oracle ORA-00942 and H2 table-not-found variants.
            default -> false;
        };
    }

    static boolean duplicate(SQLException error) {
        return error.getErrorCode() == 1 || "23505".equals(error.getSQLState());
    }

    static boolean legacyKindConstraint(SQLException error) {
        String message = error.getMessage();
        if (message == null) return false;
        return switch (error.getErrorCode()) {
            case 12899, 22001 -> message.contains("RULE_KIND");
            case 2290, 23513 -> message.contains("R86S_TRAFFIC_RULE_KIND_CK");
            default -> false;
        };
    }
}
