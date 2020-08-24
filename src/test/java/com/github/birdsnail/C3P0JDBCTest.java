package com.github.birdsnail;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;

import static com.github.birdsnail.DataFlyWithFlink.INSERT_OR_UPDATE;

/**
 * @author BirdSnail
 * @date 2020/8/7
 */
public class C3P0JDBCTest {

	public static final String ORACLE_DRIVER = "oracle.jdbc.driver.OracleDriver";
	public static final String ORACLE_URL = "jdbc:oracle:thin:@192.168.152.11:1521:orcl";

	public static void main(String[] args) throws PropertyVetoException, SQLException {
		ComboPooledDataSource ds = new ComboPooledDataSource();
//		ds.setDriverClass("com.mysql.cj.jdbc.Driver");
		ds.setDriverClass(ORACLE_DRIVER);
//		ds.setJdbcUrl("jdbc:mysql://192.168.152.58:3306/flink_kafka");
		ds.setJdbcUrl(ORACLE_URL);
		ds.setUser("sbb");
		ds.setPassword("sbb2018");
		ds.setMaxPoolSize(5);
		ds.setMinPoolSize(1);
		ds.setInitialPoolSize(1);
		ds.setMaxStatements(180);


		Connection connection = ds.getConnection();
		PreparedStatement ps = connection.prepareStatement(INSERT_OR_UPDATE);
		ps.setInt(1, 2);
		ps.setString(2, "lisi");
		ps.setString(3, "shanghai");
		ps.setObject(4, LocalDate.now());
		ps.setObject(5, LocalDateTime.now());
		ps.setInt(6, 300);

		int row = ps.executeUpdate();
		System.out.println("修改行数：" + row);
	}

}
