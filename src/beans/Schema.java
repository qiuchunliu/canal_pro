package beans;

import java.util.ArrayList;

public class Schema {

    public String from_database;
    public ArrayList<SingleTable> singleTables;


    public String getFrom_database() {
        return from_database;
    }

    public ArrayList<SingleTable> getSingleTables() {
        return singleTables;
    }
}
