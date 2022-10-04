package org.example;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;

public class App {
    public static void main( String[] args ) {

        dbconnection();
        peopleData();
    }

    public static Connection dbconnection() {

        Connection connection = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/people_data?", "root", "israel4God");
            System.out.println("Connection successful");
            connection.setAutoCommit(false);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }

    public static void peopleData() {

        String csvFilePath = "C:\\Users\\Depittaz\\Documents\\People-datas.csv";
        try {
            BufferedReader br = new BufferedReader(new FileReader(csvFilePath));
            CSVParser peoplesData = CSVParser.parse(br, CSVFormat.EXCEL.withFirstRecordAsHeader().withIgnoreHeaderCase().withTrim());

            ArrayList<Datas> datas = new ArrayList<Datas>();
            for (CSVRecord pd : peoplesData) {
                Datas data1 = new Datas();
                data1.setFirstName(pd.get(0));
                data1.setLastName(pd.get(1));
                data1.setCompanyName(pd.get(2));
                data1.setAddress(pd.get(3));
                data1.setCity(pd.get(4));
                data1.setPhoneNo1(pd.get(5));
                data1.setPhoneNo2(pd.get(6));
                data1.setEmail(pd.get(7));
                datas.add(data1);
            }

            PreparedStatement statement = null;
            Connection con = dbconnection();
            String sql = "INSERT INTO data_value(firstName, lastName, companyName, address, city, phoneNo1, phoneNo2, email) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
            statement = con.prepareStatement(sql);

            String sql1 = "SELECT email FROM data_value";
            Statement statement1 = con.createStatement();
            ResultSet rs = statement1.executeQuery(sql1);
            ArrayList<String> email = new ArrayList<>();
            while (rs.next()){
                email.add(rs.getString("email"));
            }
            for (Datas data : datas) {
                if (!email.contains(data.getEmail())) {
                    statement.setString(1, data.getFirstName());
                    statement.setString(2, data.getLastName());
                    statement.setString(3, data.getCompanyName());
                    statement.setString(4, data.getAddress());
                    statement.setString(5, data.getCity());
                    statement.setString(6, data.getPhoneNo1());
                    statement.setString(7, data.getPhoneNo2());
                    statement.setString(8, data.getEmail());

                    statement.addBatch();
                    System.out.println("Data Upload successful");
                }else {
                    System.out.println("Record already exist!");
                }
            }
            statement.executeBatch();
            con.commit();
            con.close();

        } catch (SQLException ex) {
            ex.printStackTrace();
        } catch (FileNotFoundException ex) {
            ex.printStackTrace();
        } catch (IOException ex) {
            ex.printStackTrace();
        }

    }
}
